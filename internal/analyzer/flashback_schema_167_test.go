package analyzer

import (
	"strings"
	"testing"

	"binlogviz/internal/model"
)

func TestSchemaFileConcatenatedDumpsBindEachHeader(t *testing.T) {
	both := "-- Host: localhost    Database: p160\n" +
		"CREATE TABLE `g` (\n  `id` int NOT NULL,\n  `b` int,\n  `c` int\n);\n" +
		"-- Host: localhost    Database: p160b\n" +
		"CREATE TABLE `g` (\n  `id` int NOT NULL,\n  `a` int,\n  `z` int\n);\n"
	g := generatedTables{}
	g.noteScript(both)
	for _, tc := range []struct{ db, want string }{{"p160", "id, b, c"}, {"p160b", "id, a, z"}} {
		cols, fromFile, _, ok := g.definition(tc.db, "g")
		if !ok || !fromFile {
			t.Fatalf("%s.g: no file definition", tc.db)
		}
		if got := schemaColNames(cols); got != tc.want {
			t.Fatalf("%s.g columns = %s, want %s", tc.db, got, tc.want)
		}
	}
	if _, _, _, ok := g.definition("", "g"); ok {
		t.Fatal("unqualified g must not be defined")
	}
}

func TestSchemaFileConcatenatedDumpConflictingFlagStaysAmbiguous(t *testing.T) {
	both := "-- Host: localhost    Database: p160\nCREATE TABLE `g` (`id` int);\n" +
		"-- Host: localhost    Database: p160b\nCREATE TABLE `h` (`id` int);\n"
	g := generatedTables{flagDB: "p160"}
	g.noteScript(both)
	if _, _, _, ok := g.definition("p160", "g"); !ok {
		t.Fatal("p160.g should bind: header matches --schema-file-db")
	}
	if _, _, _, ok := g.definition("p160b", "h"); ok {
		t.Fatal("p160b.h must not bind when its header conflicts with --schema-file-db")
	}
	if !g.ambiguousDB {
		t.Fatal("conflicting header must be reported")
	}
}

func TestSchemaFileAlreadyHasBinlogAddColumn(t *testing.T) {
	dump := "-- Host: localhost    Database: p164\n" +
		"CREATE TABLE `al2` (\n  `id` int NOT NULL,\n  `a` int DEFAULT NULL,\n  `b` int DEFAULT NULL,\n" +
		"  `c` int GENERATED ALWAYS AS ((`a` + 1)) STORED,\n  PRIMARY KEY (`id`)\n);\n"
	g := generatedTables{}
	g.noteScript(dump)
	g.note("p164", "ALTER TABLE p164.al2 ADD COLUMN c INT AS (a + 1) STORED")
	cols, _, _, ok := g.definition("p164", "al2")
	if !ok {
		t.Fatal("al2 lost its definition")
	}
	if got := schemaColNames(cols); got != "id, a, b, c" {
		t.Fatalf("columns = %s, want id, a, b, c", got)
	}
	if _, gen := generatedNameSet(cols)["c"]; !gen {
		t.Fatal("c must stay generated")
	}

	// A different column under the same name is not this ALTER; keep the
	// old behavior so the row check refuses instead of guessing.
	g2 := generatedTables{}
	g2.noteScript(dump)
	g2.note("p164", "ALTER TABLE p164.al2 ADD COLUMN c INT AS (a + 2) STORED")
	cols, _, _, _ = g2.definition("p164", "al2")
	if got := schemaColNames(cols); got == "id, a, b, c" {
		t.Fatalf("a different expression must not be treated as already applied: %s", got)
	}
}

func TestSchemaFileAmbiguousNameWarnsForEveryTable(t *testing.T) {
	const create = "CREATE TABLE `x1` (\n  `id` int NOT NULL,\n  `g` int GENERATED ALWAYS AS ((`id` + 1)) VIRTUAL\n);\n"
	mk := func(schema string) model.FlashRow {
		return model.FlashRow{Schema: schema, Table: "x1", Op: "DELETE", Columns: []string{"id", "g"}, Cols: intCols(2), After: []string{"1", "2"}}
	}
	_, warns, err := flashSchemaResult(t, create, "", mk("p164"), mk("p164b"))
	if err != nil {
		t.Fatal(err)
	}
	all := strings.Join(warns, "\n")
	for _, table := range []string{"p164.x1", "p164b.x1"} {
		if !strings.Contains(all, table+": no table definition") {
			t.Fatalf("missing per-table warning for %s:\n%s", table, all)
		}
	}
}
