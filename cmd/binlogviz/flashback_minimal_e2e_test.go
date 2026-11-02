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
