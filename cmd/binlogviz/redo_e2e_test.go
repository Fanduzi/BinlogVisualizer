// Package binlogviz runs the redo golden path against a live MySQL server,
// gated on BINLOGVIZ_FLASHBACK_E2E like the other server tests.
// input: a local MySQL 8.0 with FULL row image and FULL row metadata.
// output: emitted redo SQL replayed through mysql lands the logged rows; the remote --host variant emits the same script from a replication connection.
// pos: live-server e2e for redo.go; unit tests own the rest.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

// TestRedoReplayMySQL80 emits forward SQL for one incident window and applies
// it through mysql: the replayed rows must be the logged after images — the
// 2sql contract (ADR 0022).
func TestRedoReplayMySQL80(t *testing.T) {
	if os.Getenv("BINLOGVIZ_FLASHBACK_E2E") == "" {
		t.Skip("set BINLOGVIZ_FLASHBACK_E2E=1 to apply on a local MySQL server")
	}
	forceEnglishRuntimeOutput(t)
	if _, err := exec.LookPath("sudo"); err != nil && os.Getenv("BINLOGVIZ_MYSQL") == "" {
		t.Fatal("BINLOGVIZ_FLASHBACK_E2E requires sudo mysql or BINLOGVIZ_MYSQL")
	}
	ver := mysqlVersion(e2eMySQL(t, "SELECT @@version"))
	if !strings.HasPrefix(ver, "8.0") {
		t.Fatalf("MySQL 8.0 required for binlog_row_metadata=FULL, got %s", ver)
	}
	e2eMySQL(t, `
SET GLOBAL binlog_format = 'ROW';
SET GLOBAL binlog_row_image = 'FULL';
SET GLOBAL binlog_row_metadata = 'FULL';`)
	setupRedoTable(t)

	// The incident window: one UPDATE, one INSERT, one DELETE — every
	// forward shape redo emits.
	e2eMySQL(t, `
UPDATE redot.t SET note = 'a2', doc = JSON_OBJECT('n', 2) WHERE id = 1;
INSERT INTO redot.t VALUES (3, 'c', X'CC', JSON_OBJECT('n', 3));
DELETE FROM redot.t WHERE id = 2;`)
	want := mysqlBatchValue(e2eMySQL(t, redoSelect()))
	path := e2eIncidentBinlog(t)

	stdout, stderr, err := executeRedoLikeMain(t, path, "--include-schema", "redot")
	if err != nil {
		t.Fatalf("redo: %v\n%s", err, stderr)
	}
	assertRedoScript(t, stdout)

	// Roll the table back to the baseline, replay the script, and the rows
	// must equal the logged end state.
	e2eMySQL(t, `
DELETE FROM redot.t;
INSERT INTO redot.t VALUES (1, 'a', X'AA', JSON_OBJECT('n', 1)), (2, 'b', X'BB', NULL);`)
	out, err := e2eMySQLResult(t, stdout)
	if err != nil {
		t.Fatalf("replay: %v\n%s\nscript:\n%s", err, out, stdout)
	}
	if got := mysqlBatchValue(e2eMySQL(t, redoSelect())); got != want {
		t.Fatalf("replayed rows\n%s\nwant\n%s", got, want)
	}
}

// TestRedoRemoteMySQL80 emits the same script through the replication input:
// the remote seam must feed the identical planner path (ADR 0022).
func TestRedoRemoteMySQL80(t *testing.T) {
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
	e2eMySQL(t, `
UPDATE redot.t SET note = 'a2' WHERE id = 1;
INSERT INTO redot.t VALUES (3, 'c', X'CC', NULL);
DELETE FROM redot.t WHERE id = 2;`)
	want := mysqlBatchValue(e2eMySQL(t, redoSelect()))

	// Bound the dump at the newest manifest file so only the incident
	// window replays.
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
	e2eMySQL(t, `
DROP USER IF EXISTS 'binlogviz_remote'@'127.0.0.1';
CREATE USER 'binlogviz_remote'@'127.0.0.1' IDENTIFIED WITH mysql_native_password BY 'redorem-pw-123';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'binlogviz_remote'@'127.0.0.1';
FLUSH PRIVILEGES;`)
	t.Cleanup(func() {
		_, _ = e2eMySQLResult(t, "DROP USER IF EXISTS 'binlogviz_remote'@'127.0.0.1'")
	})
	t.Setenv("BINLOGVIZ_MYSQL_PASSWORD", "redorem-pw-123")

	stdout, stderr, err := executeRedoLikeMain(t,
		"--host", "127.0.0.1", "--user", "binlogviz_remote",
		"--start-file", last, "--include-table", "redot.t")
	if err != nil {
		t.Fatalf("remote redo: %v\n%s", err, stderr)
	}
	assertRedoScript(t, stdout)

	e2eMySQL(t, `
DELETE FROM redot.t;
INSERT INTO redot.t VALUES (1, 'a', X'AA', JSON_OBJECT('n', 1)), (2, 'b', X'BB', NULL);`)
	out, err := e2eMySQLResult(t, stdout)
	if err != nil {
		t.Fatalf("replay: %v\n%s\nscript:\n%s", err, out, stdout)
	}
	if got := mysqlBatchValue(e2eMySQL(t, redoSelect())); got != want {
		t.Fatalf("replayed rows\n%s\nwant\n%s", got, want)
	}
}

func setupRedoTable(t *testing.T) {
	t.Helper()
	e2eMySQL(t, `
DROP DATABASE IF EXISTS redot;
CREATE DATABASE redot;
CREATE TABLE redot.t (
  id INT NOT NULL PRIMARY KEY,
  note VARCHAR(32) NULL,
  b VARBINARY(8) NULL,
  doc JSON NULL
);
INSERT INTO redot.t VALUES (1, 'a', X'AA', JSON_OBJECT('n', 1)), (2, 'b', X'BB', NULL);`)
	e2eMySQL(t, "FLUSH LOGS")
	t.Cleanup(func() {
		_, _ = e2eMySQLResult(t, "DROP DATABASE IF EXISTS redot;")
	})
}

func redoSelect() string {
	return "SELECT id, note, HEX(b), doc FROM redot.t ORDER BY id"
}

func assertRedoScript(t *testing.T, sql string) {
	t.Helper()
	for _, want := range []string{
		"UPDATE `redot`.`t` SET `id` = 1",
		"INSERT INTO `redot`.`t` (`id`, `note`, `b`, `doc`) VALUES (3, 'c', X'CC'",
		"DELETE FROM `redot`.`t` WHERE `id` <=> 2;",
		"BEGIN;",
		"COMMIT;",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("script missing %q:\n%s", want, sql)
		}
	}
}

func executeRedoLikeMain(t *testing.T, args ...string) (string, string, error) {
	t.Helper()
	cmd := NewRootCommand()
	cmd.SetArgs(append([]string{"redo"}, args...))
	return captureStdoutStderrRun(t, func() error {
		err := cmd.Execute()
		if err != nil {
			PrintCommandError(os.Stderr, err)
		}
		return err
	})
}
