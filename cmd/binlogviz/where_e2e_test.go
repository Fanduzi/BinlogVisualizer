// Package binlogviz runs --where against a live MySQL server, gated on
// BINLOGVIZ_FLASHBACK_E2E like the other server tests (ADR 0024).
// input: a local MySQL 8.0 with FULL row image and FULL row metadata.
// output: redo --where replays only the matching rows through mysql, and
// flashback --where's undo statements cover the same matching set.
// pos: live-server e2e for the --where flag; unit and fixture tests own the
// rest.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

// TestRedoWhereReplayMySQL80 emits forward SQL for one incident window under
// --where and applies it through mysql: only rows the predicate proves land
// — the surgical-recovery contract (ADR 0024).
func TestRedoWhereReplayMySQL80(t *testing.T) {
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
	e2eMySQL(t, `
DROP DATABASE IF EXISTS wheret;
CREATE DATABASE wheret;
CREATE TABLE wheret.t (id INT NOT NULL PRIMARY KEY, note VARCHAR(32) NULL);
INSERT INTO wheret.t VALUES (1, 'a'), (2, 'b'), (500, 'x'), (501, 'y');
FLUSH LOGS`)
	t.Cleanup(func() {
		_, _ = e2eMySQLResult(t, "DROP DATABASE IF EXISTS wheret;")
	})

	// The incident window: small ids change, large ids change the same way.
	e2eMySQL(t, `
UPDATE wheret.t SET note = 'a2' WHERE id = 1;
UPDATE wheret.t SET note = 'x2' WHERE id = 500;
INSERT INTO wheret.t VALUES (4, 'd');
INSERT INTO wheret.t VALUES (502, 'z');
DELETE FROM wheret.t WHERE id = 2;
DELETE FROM wheret.t WHERE id = 501;`)
	path := e2eIncidentBinlog(t)

	stdout, stderr, err := executeRedoLikeMain(t, path,
		"--include-schema", "wheret", "--where", "id < 100")
	if err != nil {
		t.Fatalf("redo --where: %v\n%s", err, stderr)
	}
	dml := strings.Join(redoDML(stdout), "\n")
	for _, want := range []string{
		"`note` = 'a2'",
		"VALUES (4, 'd')",
		"WHERE `id` <=> 2",
	} {
		if !strings.Contains(dml, want) {
			t.Fatalf("script missing %q:\n%s", want, stdout)
		}
	}
	for _, drop := range []string{"x2", "'z'", "500", "501"} {
		if strings.Contains(dml, drop) {
			t.Fatalf("filtered rows emitted %q:\n%s", drop, stdout)
		}
	}

	// Roll back to the baseline, replay the script, and only the matching
	// rows changed: the predicate is a selection, not a rewrite (ADR 0024).
	e2eMySQL(t, `
DELETE FROM wheret.t;
INSERT INTO wheret.t VALUES (1, 'a'), (2, 'b'), (500, 'x'), (501, 'y');`)
	out, err := e2eMySQLResult(t, stdout)
	if err != nil {
		t.Fatalf("replay: %v\n%s\nscript:\n%s", err, out, stdout)
	}
	got := mysqlBatchValue(e2eMySQL(t, "SELECT id, note FROM wheret.t ORDER BY id"))
	want := "1\ta2\n4\td\n500\tx\n501\ty"
	if got != want {
		t.Fatalf("replayed rows\n%s\nwant\n%s", got, want)
	}
}

// TestFlashbackWhereMySQL80 emits the undo under --where from a live FULL
// binlog and asserts the unwrapped undo statements cover only the matching
// rows (ADR 0024).
func TestFlashbackWhereMySQL80(t *testing.T) {
	if os.Getenv("BINLOGVIZ_FLASHBACK_E2E") == "" {
		t.Skip("set BINLOGVIZ_FLASHBACK_E2E=1 to run against a local MySQL server")
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
	e2eMySQL(t, `
DROP DATABASE IF EXISTS wheret;
CREATE DATABASE wheret;
CREATE TABLE wheret.t (id INT NOT NULL PRIMARY KEY, note VARCHAR(32) NULL);
INSERT INTO wheret.t VALUES (1, 'a'), (2, 'b'), (500, 'x');
FLUSH LOGS`)
	t.Cleanup(func() {
		_, _ = e2eMySQLResult(t, "DROP DATABASE IF EXISTS wheret;")
	})

	e2eMySQL(t, `
DELETE FROM wheret.t WHERE id = 1;
DELETE FROM wheret.t WHERE id = 500;
INSERT INTO wheret.t VALUES (4, 'd');`)
	path := e2eIncidentBinlog(t)

	stdout, stderr, err := executeFlashbackLikeMain(t, path,
		"--include-schema", "wheret", "--where", "id < 100")
	if err != nil {
		t.Fatalf("flashback --where: %v\n%s", err, stderr)
	}
	var dml []string
	for _, stmt := range undoStatements(stdout) {
		if strings.HasPrefix(stmt, "INSERT") || strings.HasPrefix(stmt, "UPDATE") || strings.HasPrefix(stmt, "DELETE") {
			dml = append(dml, stmt)
		}
	}
	if len(dml) != 2 {
		t.Fatalf("filtered undo emitted %d DML:\n%s\nscript:\n%s", len(dml), strings.Join(dml, "\n"), stdout)
	}
	joined := strings.Join(dml, "\n")
	if !strings.Contains(joined, "INSERT INTO `wheret`.`t` (`id`, `note`) VALUES (1, 'a')") {
		t.Fatalf("missing delete undo:\n%s", joined)
	}
	if !strings.Contains(joined, "DELETE FROM `wheret`.`t` WHERE `id` <=> 4") {
		t.Fatalf("missing insert undo:\n%s", joined)
	}
}
