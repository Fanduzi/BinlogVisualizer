package schemacatalog

import (
	"reflect"
	"strings"
	"testing"

	"binlogviz/internal/model"
	"binlogviz/internal/mysqlexpr"
)

func warningText(ws []model.FlashbackWarning) string {
	lines := make([]string, len(ws))
	for i, w := range ws {
		lines[i] = w.Line()
	}
	return strings.Join(lines, "\n")
}

// Tests through the schema catalog interface: loadFile, observeTable,
// observeDDL, resolve/peek, warnings.

const catalogMixT = "USE `P185L`;\nCREATE TABLE `MixT` (\n" +
	"  `id` int NOT NULL,\n" +
	"  `Gc` int GENERATED ALWAYS AS ((`id` + 1)) VIRTUAL,\n" +
	"  PRIMARY KEY (`id`)\n);\n"

func TestSchemaCatalogExact(t *testing.T) {
	c := New("")
	c.LoadFile("-- Host: localhost    Database: shop\nCREATE TABLE `t` (`id` int, `g` int AS (`id` + 1) VIRTUAL);\n")
	res := c.Resolve("shop", "t")
	if res.Kind != Exact || !res.CheckRows || res.Deferred || res.FileTable != "" {
		t.Fatalf("file table: %+v", res)
	}
	if got := mysqlexpr.SchemaColNames(res.Cols); got != "id, g" {
		t.Fatalf("cols = %s", got)
	}

	c.ObserveDDL("shop", "CREATE TABLE shop.b (id INT PRIMARY KEY, v INT AS (id * 2) STORED)")
	res = c.Resolve("shop", "b")
	if res.Kind != Exact || res.CheckRows {
		t.Fatalf("binlog DDL table must not need a row check: %+v", res)
	}
	if _, ok := mysqlexpr.GeneratedNameSet(res.Cols)["v"]; !ok {
		t.Fatalf("generated v lost: %+v", res.Cols)
	}
}

func TestSchemaCatalogUnqualifiedBindsToOnlySchema(t *testing.T) {
	c := New("")
	c.LoadFile("CREATE TABLE `x1` (`id` int NOT NULL);\n")
	if dropped := c.ObserveTable("p1", "x1"); len(dropped) != 0 {
		t.Fatalf("first schema dropped %v", dropped)
	}
	res := c.Resolve("p1", "x1")
	if res.Kind != Exact || !res.Deferred || !res.CheckRows {
		t.Fatalf("unqualified bind: %+v", res)
	}
	if dropped := c.ObserveTable("p2", "x1"); !reflect.DeepEqual(dropped, []string{"x1"}) {
		t.Fatalf("second schema must drop the binding, got %v", dropped)
	}
	for _, schema := range []string{"p1", "p2"} {
		res = c.Resolve(schema, "x1")
		if res.Kind != Ambiguous || !reflect.DeepEqual(res.Candidates, []string{"p1.x1", "p2.x1"}) {
			t.Fatalf("%s.x1: %+v", schema, res)
		}
	}
	if w := warningText(c.Warnings()); !strings.Contains(w, "more than one schema (p1, p2)") {
		t.Fatalf("warnings: %s", w)
	}
}

func TestSchemaCatalogCaseFold(t *testing.T) {
	c := New("")
	c.LoadFile(catalogMixT)
	if res := c.Peek("p185l", "mixt"); res.Kind != Missing {
		t.Fatalf("peek must not bind: %+v", res)
	}
	res := c.Resolve("p185l", "mixt")
	if res.Kind != Fold || res.FileTable != "P185L.MixT" || !res.CheckRows {
		t.Fatalf("fold: %+v", res)
	}
	if again := c.Peek("p185l", "mixt"); again.Kind != Fold || again.FileTable != "P185L.MixT" {
		t.Fatalf("peek after fold: %+v", again)
	}

	// A mixed-case binlog name may be another table on a
	// lower_case_table_names=0 server: named, not used.
	mixed := New("")
	mixed.LoadFile(catalogMixT)
	res = mixed.Resolve("P185L", "mixt")
	if res.Kind != Ambiguous || !reflect.DeepEqual(res.Candidates, []string{"P185L.MixT"}) {
		t.Fatalf("mixed-case: %+v", res)
	}

	// Two file tables that fold to one name are not guessed between.
	two := New("")
	two.LoadFile(catalogMixT + "CREATE TABLE `mixT` (`id` int NOT NULL);\n")
	res = two.Resolve("p185l", "mixt")
	if res.Kind != Ambiguous || len(res.Candidates) != 2 {
		t.Fatalf("two candidates: %+v", res)
	}
}

func TestSchemaCatalogMissing(t *testing.T) {
	c := New("")
	if res := c.Resolve("shop", "none"); res.Kind != Missing {
		t.Fatalf("empty catalog: %+v", res)
	}
	c.LoadFile("-- Host: localhost    Database: shop\nCREATE TABLE `t` (`id` int);\n")
	if res := c.Resolve("shop", "none"); res.Kind != Missing {
		t.Fatalf("other table: %+v", res)
	}
	c.ObserveDDL("shop", "DROP TABLE shop.t")
	if res := c.Resolve("shop", "t"); res.Kind != Missing {
		t.Fatalf("dropped table: %+v", res)
	}
}

func TestSchemaCatalogRefusedReasons(t *testing.T) {
	c := New("")
	c.ObserveDDL("shop", "CREATE TABLE shop.gen (id INT PRIMARY KEY, g INT AS (id + 1) VIRTUAL)")
	for _, stmt := range []string{
		"CREATE TABLE shop.sel AS SELECT 1",
		"CREATE TABLE shop.like1 LIKE shop.missing",
		"ALTER TABLE shop.fresh ADD COLUMN g INT AS (id + 1) VIRTUAL",
		"CREATE TABLE shop.broken",
	} {
		c.ObserveDDL("shop", stmt)
	}
	for table, want := range map[string]RefuseReason{
		"sel":    CreateSelect,
		"like1":  LikeSource,
		"fresh":  AlterUnknown,
		"broken": UnreadableCreate,
	} {
		res := c.Resolve("shop", table)
		if res.Kind != Refused || res.Reason != want {
			t.Fatalf("%s: %+v, want reason %d", table, res, want)
		}
	}
	// RENAME carries the refusal and its reason.
	c.ObserveDDL("shop", "RENAME TABLE shop.sel TO shop.sel2")
	if res := c.Resolve("shop", "sel2"); res.Kind != Refused || res.Reason != CreateSelect {
		t.Fatalf("renamed: %+v", res)
	}
	// LIKE a refused table is refused too.
	c.ObserveDDL("shop", "CREATE TABLE shop.like2 LIKE shop.sel2")
	if res := c.Resolve("shop", "like2"); res.Kind != Refused || res.Reason != LikeSource {
		t.Fatalf("like refused: %+v", res)
	}
	if res := c.Resolve("shop", "gen"); res.Kind != Exact {
		t.Fatalf("readable table: %+v", res)
	}
}
