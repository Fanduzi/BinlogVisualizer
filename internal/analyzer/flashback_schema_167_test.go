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
	g := newSchemaCatalog("")
	g.loadFile(both)
	for _, tc := range []struct{ db, want string }{{"p160", "id, b, c"}, {"p160b", "id, a, z"}} {
		res := g.resolve(tc.db, "g")
		cols := res.cols
		if res.kind != resolveExact || !res.checkRows {
			t.Fatalf("%s.g: no file definition", tc.db)
		}
		if got := schemaColNames(cols); got != tc.want {
			t.Fatalf("%s.g columns = %s, want %s", tc.db, got, tc.want)
		}
	}
	if g.peek("", "g").kind != resolveMissing {
		t.Fatal("unqualified g must not be defined")
	}
}

func TestSchemaFileConcatenatedDumpConflictingFlagStaysAmbiguous(t *testing.T) {
	both := "-- Host: localhost    Database: p160\nCREATE TABLE `g` (`id` int);\n" +
		"-- Host: localhost    Database: p160b\nCREATE TABLE `h` (`id` int);\n"
	g := newSchemaCatalog("p160")
	g.loadFile(both)
	if g.resolve("p160", "g").kind != resolveExact {
		t.Fatal("p160.g should bind: header matches --schema-file-db")
	}
	if g.resolve("p160b", "h").kind != resolveMissing {
		t.Fatal("p160b.h must not bind when its header conflicts with --schema-file-db")
	}
	if len(g.warnings()) != 1 {
		t.Fatalf("conflicting header must be reported: %q", g.warnings())
	}
}

func TestSchemaFileAlreadyHasBinlogAddColumn(t *testing.T) {
	dump := "-- Host: localhost    Database: p164\n" +
		"CREATE TABLE `al2` (\n  `id` int NOT NULL,\n  `a` int DEFAULT NULL,\n  `b` int DEFAULT NULL,\n" +
		"  `c` int GENERATED ALWAYS AS ((`a` + 1)) STORED,\n  PRIMARY KEY (`id`)\n);\n"
	g := newSchemaCatalog("")
	g.loadFile(dump)
	g.observeDDL("p164", "ALTER TABLE p164.al2 ADD COLUMN c INT AS (a + 1) STORED")
	res := g.resolve("p164", "al2")
	cols := res.cols
	if res.kind != resolveExact {
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
	g2 := newSchemaCatalog("")
	g2.loadFile(dump)
	g2.observeDDL("p164", "ALTER TABLE p164.al2 ADD COLUMN c INT AS (a + 2) STORED")
	cols = g2.resolve("p164", "al2").cols
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
