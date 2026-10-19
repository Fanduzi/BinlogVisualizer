package schemacatalog

import (
	"testing"

	"binlogviz/internal/mysqlexpr"
)

func TestSchemaFileConcatenatedDumpsBindEachHeader(t *testing.T) {
	both := "-- Host: localhost    Database: p160\n" +
		"CREATE TABLE `g` (\n  `id` int NOT NULL,\n  `b` int,\n  `c` int\n);\n" +
		"-- Host: localhost    Database: p160b\n" +
		"CREATE TABLE `g` (\n  `id` int NOT NULL,\n  `a` int,\n  `z` int\n);\n"
	g := New("")
	g.LoadFile(both)
	for _, tc := range []struct{ db, want string }{{"p160", "id, b, c"}, {"p160b", "id, a, z"}} {
		res := g.Resolve(tc.db, "g")
		cols := res.Cols
		if res.Kind != Exact || !res.CheckRows {
			t.Fatalf("%s.g: no file definition", tc.db)
		}
		if got := mysqlexpr.SchemaColNames(cols); got != tc.want {
			t.Fatalf("%s.g columns = %s, want %s", tc.db, got, tc.want)
		}
	}
	if g.Peek("", "g").Kind != Missing {
		t.Fatal("unqualified g must not be defined")
	}
}

func TestSchemaFileConcatenatedDumpConflictingFlagStaysAmbiguous(t *testing.T) {
	both := "-- Host: localhost    Database: p160\nCREATE TABLE `g` (`id` int);\n" +
		"-- Host: localhost    Database: p160b\nCREATE TABLE `h` (`id` int);\n"
	g := New("p160")
	g.LoadFile(both)
	if g.Resolve("p160", "g").Kind != Exact {
		t.Fatal("p160.g should bind: header matches --schema-file-db")
	}
	if g.Resolve("p160b", "h").Kind != Missing {
		t.Fatal("p160b.h must not bind when its header conflicts with --schema-file-db")
	}
	if len(g.Warnings()) != 1 {
		t.Fatalf("conflicting header must be reported: %q", g.Warnings())
	}
}

func TestSchemaFileAlreadyHasBinlogAddColumn(t *testing.T) {
	dump := "-- Host: localhost    Database: p164\n" +
		"CREATE TABLE `al2` (\n  `id` int NOT NULL,\n  `a` int DEFAULT NULL,\n  `b` int DEFAULT NULL,\n" +
		"  `c` int GENERATED ALWAYS AS ((`a` + 1)) STORED,\n  PRIMARY KEY (`id`)\n);\n"
	g := New("")
	g.LoadFile(dump)
	g.ObserveDDL("p164", "ALTER TABLE p164.al2 ADD COLUMN c INT AS (a + 1) STORED")
	res := g.Resolve("p164", "al2")
	cols := res.Cols
	if res.Kind != Exact {
		t.Fatal("al2 lost its definition")
	}
	if got := mysqlexpr.SchemaColNames(cols); got != "id, a, b, c" {
		t.Fatalf("columns = %s, want id, a, b, c", got)
	}
	if _, gen := mysqlexpr.GeneratedNameSet(cols)["c"]; !gen {
		t.Fatal("c must stay generated")
	}

	// A different column under the same name is not this ALTER; keep the
	// old behavior so the row check refuses instead of guessing.
	g2 := New("")
	g2.LoadFile(dump)
	g2.ObserveDDL("p164", "ALTER TABLE p164.al2 ADD COLUMN c INT AS (a + 2) STORED")
	cols = g2.Resolve("p164", "al2").Cols
	if got := mysqlexpr.SchemaColNames(cols); got == "id, a, b, c" {
		t.Fatalf("a different expression must not be treated as already applied: %s", got)
	}
}
