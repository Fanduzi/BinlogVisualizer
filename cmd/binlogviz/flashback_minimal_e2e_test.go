package binlogviz

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// A MINIMAL-metadata server is what MySQL 8.0 ships with, and its binlog says
// nothing about column names or the key. The definition supplied to
// --schema-file must make the undo usable there: named columns, the keyed
// WHERE, and an apply that restores the rows (ADR 0030).
func TestFlashbackMinimalMetadataMySQL80(t *testing.T) {
	e2eMinimalMetadataUndo(t, "8.0", "8.4", "9.")
}

// MySQL 5.7 has no row metadata at all, which is the same gap in a different
// shape: the definition is the only source of names and the key.
func TestFlashbackMinimalMetadataMySQL57(t *testing.T) {
	e2eMinimalMetadataUndo(t, "5.7")
}

// e2eBinlogPosition returns the position after the statements written so far,
// so a later run can select what follows without selecting the DDL.
func e2eBinlogPosition(t *testing.T) string {
	t.Helper()
	out := e2eMySQL(t, "SHOW MASTER STATUS")
	fields := strings.Fields(out)
	for i, f := range fields {
		if f == "mysql-bin." || strings.HasPrefix(f, "mysql-bin.") {
			if i+1 < len(fields) {
				return fields[i+1]
			}
		}
	}
	t.Fatalf("no position in SHOW MASTER STATUS:\n%s", out)
	return ""
}

func e2eMinimalMetadataUndo(t *testing.T, prefixes ...string) {
	t.Helper()
	if os.Getenv("BINLOGVIZ_FLASHBACK_E2E") == "" {
		t.Skip("set BINLOGVIZ_FLASHBACK_E2E=1 to round-trip on a local MySQL server")
	}
	forceEnglishRuntimeOutput(t)
	ver := mysqlVersion(e2eMySQL(t, "SELECT @@version"))
	wanted := false
	for _, prefix := range prefixes {
		if strings.HasPrefix(ver, prefix) {
			wanted = true
		}
	}
	if !wanted {
		t.Fatalf("server %s is not one of %v", ver, prefixes)
	}
	hasMetadata := !strings.HasPrefix(ver, "5.7")

	const ddl = "CREATE TABLE minmeta.t (id INT NOT NULL, status VARCHAR(16), amount DECIMAL(10,2), PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4"

	e2eMySQL(t, `
SET GLOBAL binlog_format = 'ROW';
SET GLOBAL binlog_row_image = 'FULL';`)
	if hasMetadata {
		e2eMySQL(t, "SET GLOBAL binlog_row_metadata = 'MINIMAL'")
		t.Cleanup(func() { e2eMySQL(t, "SET GLOBAL binlog_row_metadata = 'FULL'") })
	}
	e2eMySQL(t, "DROP DATABASE IF EXISTS minmeta")
	e2eMySQL(t, "CREATE DATABASE minmeta")
	e2eMySQL(t, ddl)
	// One row the incident never touches, so the restored state is not simply
	// "empty table"; it goes in before the binlog the undo reads.
	e2eMySQL(t, "INSERT INTO minmeta.t VALUES (10,'seed',1.00)")
	e2eMySQL(t, "FLUSH LOGS")
	e2eMySQL(t, `
INSERT INTO minmeta.t VALUES (1,'paid',9.99),(2,'pending',5.00),(3,'paid',7.25);
UPDATE minmeta.t SET status='shipped' WHERE id=2;
DELETE FROM minmeta.t WHERE id=3;`)

	path := e2eIncidentBinlog(t)

	// The definition as it was at incident time: the dump convention, not a
	// description of the table today.
	schemaPath := filepath.Join(t.TempDir(), "schema.sql")
	if err := os.WriteFile(schemaPath, []byte(ddl+";\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	sql, stderr, err := executeFlashbackLikeMain(t, path, "--include-schema", "minmeta", "--schema-file", schemaPath)
	if err != nil {
		t.Fatalf("flashback: %v\n%s", err, stderr)
	}
	if !strings.Contains(stderr, "--schema-file") {
		t.Fatalf("stderr must name where the key came from:\n%s", stderr)
	}
	if !flashbackScriptHas(sql, "`status` = 'pending'") {
		t.Fatalf("the undo must name the definition's columns:\n%s", sql)
	}
	if !flashbackScriptHas(sql, "WHERE `id` <=> 2") {
		t.Fatalf("the undo must address the row by the definition's key:\n%s", sql)
	}
	if flashbackScriptHas(sql, "@1=") || flashbackScriptHas(sql, "@1 ") {
		t.Fatalf("the undo must not fall back to positional columns:\n%s", sql)
	}

	e2eMySQL(t, committingFlashback(t, sql))
	after := e2eMySQL(t, "SELECT id,status,amount FROM minmeta.t ORDER BY id")
	// The incident inserted 1, 2, and 3, updated 2, and deleted 3, so the undo
	// leaves only the row it never saw.
	if want := e2eMySQL(t, "SELECT 10,'seed',1.00"); after != want {
		t.Fatalf("the undo must restore the pre-incident rows:\nwant=%s\ngot=%s", want, after)
	}

	// --show-rows prints the same names, and says where they came from.
	out, stderr, err := executeAnalyzeLikeMain(t, path, "--show-rows", "--include-schema", "minmeta", "--schema-file", schemaPath)
	if err != nil {
		t.Fatalf("analyze: %v\n%s", err, stderr)
	}
	if !strings.Contains(out, "column names come from --schema-file") {
		t.Fatalf("the summary must name the source of the labels:\n%s", out)
	}
	if strings.Contains(out, "@1=") {
		t.Fatalf("--show-rows must print named columns:\n%s", out)
	}
}

// The dump describes the incident-time key; a later ALTER changed it. The
// planner observes that ALTER (it is in the parsed file, outside the selected
// range), so the undo must stop addressing the row by the stale key and match
// every column instead — and say so (ADR 0030).
func TestFlashbackStaleDefinitionKeyMySQL80(t *testing.T) {
	if os.Getenv("BINLOGVIZ_FLASHBACK_E2E") == "" {
		t.Skip("set BINLOGVIZ_FLASHBACK_E2E=1 to round-trip on a local MySQL server")
	}
	forceEnglishRuntimeOutput(t)
	if ver := mysqlVersion(e2eMySQL(t, "SELECT @@version")); strings.HasPrefix(ver, "5.7") {
		t.Skip("the ALTER-and-restart path is exercised on 8.0")
	}
	const ddl = "CREATE TABLE stale.k (id INT NOT NULL, note VARCHAR(8) NOT NULL, PRIMARY KEY (note)) DEFAULT CHARSET=utf8mb4"

	e2eMySQL(t, `
SET GLOBAL binlog_format = 'ROW';
SET GLOBAL binlog_row_image = 'FULL';
SET GLOBAL binlog_row_metadata = 'MINIMAL';`)
	t.Cleanup(func() { e2eMySQL(t, "SET GLOBAL binlog_row_metadata = 'FULL'") })
	e2eMySQL(t, "DROP DATABASE IF EXISTS stale")
	e2eMySQL(t, "CREATE DATABASE stale")
	e2eMySQL(t, ddl)
	e2eMySQL(t, "ALTER TABLE stale.k DROP PRIMARY KEY, ADD PRIMARY KEY (id)")
	pos := e2eBinlogPosition(t)
	e2eMySQL(t, `
INSERT INTO stale.k VALUES (1,'a'),(2,'b'),(3,'c');
UPDATE stale.k SET note='bb' WHERE id=2;
DELETE FROM stale.k WHERE id=3;`)

	path := e2eIncidentBinlog(t)
	schemaPath := filepath.Join(t.TempDir(), "schema.sql")
	if err := os.WriteFile(schemaPath, []byte(ddl+";\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	sql, stderr, err := executeFlashbackLikeMain(t, path, "--include-schema", "stale", "--schema-file", schemaPath, "--start-position", pos)
	if err != nil {
		t.Fatalf("flashback: %v\n%s", err, stderr)
	}
	if !strings.Contains(stderr, "changed by a later ALTER") {
		t.Fatalf("the refusal to trust the stale key must be announced:\n%s", stderr)
	}
	if strings.Contains(stderr, "addresses the row by the PRIMARY KEY declared in --schema-file") {
		t.Fatalf("the run claims to use a key it dropped:\n%s", stderr)
	}
	if !flashbackScriptHas(sql, "`id` <=>") {
		t.Fatalf("a dropped key must match every column, not the stale key:\n%s", sql)
	}
}
