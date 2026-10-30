// Package binlogviz runs the split-output path against a live MySQL server,
// gated on BINLOGVIZ_FLASHBACK_E2E like the other server tests.
// input: a local MySQL 8.0 with FULL row image and FULL row metadata.
// output: flashback --file-per-table files each verify+apply alone and restore
// both tables; redo --max-statements-per-file parts replay in MANIFEST order.
// pos: live-server e2e for split output; unit tests own the rest.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"binlogviz/internal/splitout"
)

// manifestFiles parses a MANIFEST: `#` lines are comments, the rest are file
// names in apply order (ADR 0025).
func manifestFiles(t *testing.T, dir string) []string {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(dir, splitout.ManifestName))
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, line := range strings.Split(string(body), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		names = append(names, line)
	}
	return names
}

// TestSplitFlashbackPerTableRoundTrip splits one two-table incident into
// per-table scripts. Each file is verified with guard.ApplySQL and applied
// on its own; in MANIFEST order they must restore the pre-incident tables —
// the per-file atomicity contract (ADR 0025).
func TestSplitFlashbackPerTableRoundTrip(t *testing.T) {
	if os.Getenv("BINLOGVIZ_FLASHBACK_E2E") == "" {
		t.Skip("set BINLOGVIZ_FLASHBACK_E2E=1 to round-trip on a local MySQL server")
	}
	forceEnglishRuntimeOutput(t)
	if _, err := exec.LookPath("sudo"); err != nil && os.Getenv("BINLOGVIZ_MYSQL") == "" {
		t.Fatal("BINLOGVIZ_FLASHBACK_E2E requires sudo mysql or BINLOGVIZ_MYSQL")
	}
	e2eMySQL(t, `
SET GLOBAL binlog_format = 'ROW';
SET GLOBAL binlog_row_image = 'FULL';
SET GLOBAL binlog_row_metadata = 'FULL';`)
	vars := e2eMySQL(t, "SELECT @@log_bin, @@binlog_format, @@binlog_row_image, @@binlog_row_metadata, @@gtid_mode, @@version")
	fields := strings.Fields(vars)
	if len(fields) < 6 || fields[0] != "1" || fields[1] != "ROW" || fields[2] != "FULL" || fields[3] != "FULL" || fields[4] != "ON" || !strings.HasPrefix(fields[5], "8.0") {
		t.Fatalf("server is not MySQL 8.0 ROW/GTID/FULL: %s", vars)
	}

	e2eMySQL(t, `
DROP DATABASE IF EXISTS splitfb;
CREATE DATABASE splitfb;
CREATE TABLE splitfb.a (id INT NOT NULL PRIMARY KEY, v INT NOT NULL);
CREATE TABLE splitfb.b (id INT NOT NULL PRIMARY KEY, v INT NOT NULL);
INSERT INTO splitfb.a VALUES (1, 10), (2, 20);
INSERT INTO splitfb.b VALUES (1, 100), (2, 200);`)
	t.Cleanup(func() {
		_, _ = e2eMySQLResult(t, "DROP DATABASE IF EXISTS splitfb;")
	})
	const checksumSQL = "CHECKSUM TABLE splitfb.a, splitfb.b"
	beforeSum := e2eMySQL(t, checksumSQL)
	e2eMySQL(t, "FLUSH LOGS")

	// One incident touching both tables in a single transaction, so the
	// per-table files genuinely split a transaction's undo work.
	e2eMySQL(t, `
START TRANSACTION;
UPDATE splitfb.a SET v = 11 WHERE id = 1;
DELETE FROM splitfb.a WHERE id = 2;
INSERT INTO splitfb.b VALUES (3, 300);
UPDATE splitfb.b SET v = 201 WHERE id = 2;
COMMIT;`)
	if after := e2eMySQL(t, checksumSQL); after == beforeSum {
		t.Fatalf("incident did not change checksums:\n%s", after)
	}
	path := e2eIncidentBinlog(t)

	dir := filepath.Join(t.TempDir(), "out")
	stdout, stderr, err := executeFlashbackLikeMain(t, path, "--output-dir", dir, "--file-per-table")
	if err != nil {
		t.Fatalf("split flashback: %v\n%s", err, stderr)
	}
	if stdout != "" {
		t.Fatalf("split output went to stdout:\n%s", stdout)
	}
	names := manifestFiles(t, dir)
	if len(names) != 2 {
		t.Fatalf("manifest = %v, want the two table files", names)
	}
	for _, name := range names {
		if name != "splitfb.a.sql" && name != "splitfb.b.sql" {
			t.Fatalf("unexpected file name %q in %v", name, names)
		}
	}
	// Verify each file independently, then apply them in MANIFEST order.
	for _, name := range names {
		body, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		e2eMySQL(t, committingFlashback(t, string(body)))
	}
	if got := e2eMySQL(t, checksumSQL); got != beforeSum {
		t.Fatalf("checksum\nbefore:\n%s\nrestored:\n%s", beforeSum, got)
	}
}

// TestSplitRedoPartsRoundTrip forces --max-statements-per-file 1 so
// transactions span part files; replaying the parts in MANIFEST order must
// land the logged end state (ADR 0025).
func TestSplitRedoPartsRoundTrip(t *testing.T) {
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

	e2eMySQL(t, `
UPDATE redot.t SET note = 'a2', doc = JSON_OBJECT('n', 2) WHERE id = 1;
INSERT INTO redot.t VALUES (3, 'c', X'CC', JSON_OBJECT('n', 3));
DELETE FROM redot.t WHERE id = 2;`)
	want := mysqlBatchValue(e2eMySQL(t, redoSelect()))
	path := e2eIncidentBinlog(t)

	dir := filepath.Join(t.TempDir(), "out")
	stdout, stderr, err := executeRedoLikeMain(t, path, "--include-schema", "redot", "--output-dir", dir, "--max-statements-per-file", "1")
	if err != nil {
		t.Fatalf("split redo: %v\n%s", err, stderr)
	}
	if stdout != "" {
		t.Fatalf("split output went to stdout:\n%s", stdout)
	}
	names := manifestFiles(t, dir)
	if len(names) < 3 {
		t.Fatalf("manifest = %v, want several one-statement parts", names)
	}

	e2eMySQL(t, `
DELETE FROM redot.t;
INSERT INTO redot.t VALUES (1, 'a', X'AA', JSON_OBJECT('n', 1)), (2, 'b', X'BB', NULL);`)
	for _, name := range names {
		body, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		if out, err := e2eMySQLResult(t, string(body)); err != nil {
			t.Fatalf("apply %s: %v\n%s\npart:\n%s", name, err, out, body)
		}
	}
	if got := mysqlBatchValue(e2eMySQL(t, redoSelect())); got != want {
		t.Fatalf("replayed rows\n%s\nwant\n%s", got, want)
	}
}
