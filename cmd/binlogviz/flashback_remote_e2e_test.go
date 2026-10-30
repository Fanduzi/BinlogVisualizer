// Package binlogviz runs flashback against a live MySQL server through the
// replication input, gated on BINLOGVIZ_FLASHBACK_E2E like the other tests.
// input: a local MySQL 8.0 with FULL row image and FULL row metadata.
// output: emitted undo SQL applied through mysql restores the baseline rows — the same contract a file input produces (ADR 0023).
// pos: live-server e2e for flashback.go's remote branch; unit tests own the rest.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

// TestFlashbackRemoteMySQL80 generates undo SQL from a replication dump and
// applies it: the applied rows must be the pre-incident baseline — the undo
// contract on the remote seam (ADR 0023).
func TestFlashbackRemoteMySQL80(t *testing.T) {
	if os.Getenv("BINLOGVIZ_FLASHBACK_E2E") == "" {
		t.Skip("set BINLOGVIZ_FLASHBACK_E2E=1 to run against a local MySQL server")
	}
	forceEnglishRuntimeOutput(t)
	if _, err := exec.LookPath("sudo"); err != nil && os.Getenv("BINLOGVIZ_MYSQL") == "" {
		t.Fatal("BINLOGVIZ_FLASHBACK_E2E requires sudo mysql or BINLOGVIZ_MYSQL")
	}
	vars := e2eMySQL(t, "SELECT @@log_bin, @@binlog_format, @@binlog_row_metadata, @@version")
	fields := strings.Fields(vars)
	if len(fields) < 4 || fields[0] != "1" || fields[1] != "ROW" || fields[2] != "FULL" || !strings.HasPrefix(fields[3], "8.0") {
		t.Fatalf("server is not MySQL 8.0 with FULL metadata ROW binlogging: %s", vars)
	}
	setupRedoTable(t)

	// The replication account is created before the dumped window: account
	// DDL inside the selected range would refuse the undo (ADR 0015).
	e2eMySQL(t, `
DROP USER IF EXISTS 'binlogviz_remote'@'127.0.0.1';
CREATE USER 'binlogviz_remote'@'127.0.0.1' IDENTIFIED WITH mysql_native_password BY 'fbrem-pw-123';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'binlogviz_remote'@'127.0.0.1';
FLUSH PRIVILEGES;`)
	t.Cleanup(func() {
		_, _ = e2eMySQLResult(t, "DROP USER IF EXISTS 'binlogviz_remote'@'127.0.0.1'")
	})
	t.Setenv("BINLOGVIZ_MYSQL_PASSWORD", "fbrem-pw-123")

	// Seal the account DDL away, then the incident is the only workload in
	// the newest binlog — the whole undo scope.
	e2eMySQL(t, "FLUSH LOGS")
	want := mysqlBatchValue(e2eMySQL(t, redoSelect()))
	e2eMySQL(t, `
UPDATE redot.t SET note = 'a2' WHERE id = 1;
INSERT INTO redot.t VALUES (3, 'c', X'CC', NULL);
DELETE FROM redot.t WHERE id = 2;`)

	logs := e2eMySQL(t, "SHOW BINARY LOGS")
	last := ""
	for _, line := range strings.Split(logs, "\n") {
		if f := strings.Fields(line); len(f) > 0 {
			last = f[0]
		}
	}
	if last == "" {
		t.Fatalf("SHOW BINARY LOGS returned no rows:\n%s", logs)
	}

	stdout, stderr, err := executeFlashbackLikeMain(t,
		"--host", "127.0.0.1", "--user", "binlogviz_remote",
		"--start-file", last, "--include-table", "redot.t")
	if err != nil {
		t.Fatalf("remote flashback: %v\n%s", err, stderr)
	}
	if !strings.Contains(stderr, "remote://127.0.0.1:3306") {
		t.Fatalf("resolved paths line missing the remote origin:\n%s", stderr)
	}
	for _, want := range []string{
		"XA START",
		"-- binlogviz-trailer",
	} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("remote undo script missing %q:\n%s", want, stdout)
		}
	}
	undo := strings.Join(undoStatements(stdout), "\n")
	for _, want := range []string{
		"INSERT INTO `redot`.`t` (`id`, `note`, `b`, `doc`) VALUES (2, 'b', X'BB'",
		"DELETE FROM `redot`.`t` WHERE `id` <=> 3",
		"UPDATE `redot`.`t` SET `id` = 1",
	} {
		if !strings.Contains(undo, want) {
			t.Fatalf("remote undo script missing %q:\n%s", want, stdout)
		}
	}

	out, err := e2eMySQLResult(t, committingFlashback(t, stdout))
	if err != nil {
		t.Fatalf("apply: %v\n%s\nscript:\n%s", err, out, stdout)
	}
	if got := mysqlBatchValue(e2eMySQL(t, redoSelect())); got != want {
		t.Fatalf("restored rows\n%s\nwant\n%s", got, want)
	}
}
