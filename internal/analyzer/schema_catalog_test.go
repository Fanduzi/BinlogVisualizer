package analyzer

import (
	"reflect"
	"strings"
	"testing"

	"binlogviz/internal/i18n"
	"binlogviz/internal/model"
)

// Tests through the schema catalog interface: loadFile, observeTable,
// observeDDL, resolve/peek, warnings.

const catalogMixT = "USE `P185L`;\nCREATE TABLE `MixT` (\n" +
	"  `id` int NOT NULL,\n" +
	"  `Gc` int GENERATED ALWAYS AS ((`id` + 1)) VIRTUAL,\n" +
	"  PRIMARY KEY (`id`)\n);\n"

func TestSchemaCatalogExact(t *testing.T) {
	c := newSchemaCatalog("")
	c.loadFile("-- Host: localhost    Database: shop\nCREATE TABLE `t` (`id` int, `g` int AS (`id` + 1) VIRTUAL);\n")
	res := c.resolve("shop", "t")
	if res.kind != resolveExact || !res.checkRows || res.deferred || res.fileTable != "" {
		t.Fatalf("file table: %+v", res)
	}
	if got := schemaColNames(res.cols); got != "id, g" {
		t.Fatalf("cols = %s", got)
	}

	c.observeDDL("shop", "CREATE TABLE shop.b (id INT PRIMARY KEY, v INT AS (id * 2) STORED)")
	res = c.resolve("shop", "b")
	if res.kind != resolveExact || res.checkRows {
		t.Fatalf("binlog DDL table must not need a row check: %+v", res)
	}
	if _, ok := generatedNameSet(res.cols)["v"]; !ok {
		t.Fatalf("generated v lost: %+v", res.cols)
	}
}

func TestSchemaCatalogUnqualifiedBindsToOnlySchema(t *testing.T) {
	c := newSchemaCatalog("")
	c.loadFile("CREATE TABLE `x1` (`id` int NOT NULL);\n")
	if dropped := c.observeTable("p1", "x1"); len(dropped) != 0 {
		t.Fatalf("first schema dropped %v", dropped)
	}
	res := c.resolve("p1", "x1")
	if res.kind != resolveExact || !res.deferred || !res.checkRows {
		t.Fatalf("unqualified bind: %+v", res)
	}
	if dropped := c.observeTable("p2", "x1"); !reflect.DeepEqual(dropped, []string{"x1"}) {
		t.Fatalf("second schema must drop the binding, got %v", dropped)
	}
	for _, schema := range []string{"p1", "p2"} {
		res = c.resolve(schema, "x1")
		if res.kind != resolveAmbiguous || !reflect.DeepEqual(res.candidates, []string{"p1.x1", "p2.x1"}) {
			t.Fatalf("%s.x1: %+v", schema, res)
		}
	}
	if w := warningText(c.warnings()); !strings.Contains(w, "more than one schema (p1, p2)") {
		t.Fatalf("warnings: %s", w)
	}
}

func TestSchemaCatalogCaseFold(t *testing.T) {
	c := newSchemaCatalog("")
	c.loadFile(catalogMixT)
	if res := c.peek("p185l", "mixt"); res.kind != resolveMissing {
		t.Fatalf("peek must not bind: %+v", res)
	}
	res := c.resolve("p185l", "mixt")
	if res.kind != resolveFold || res.fileTable != "P185L.MixT" || !res.checkRows {
		t.Fatalf("fold: %+v", res)
	}
	if again := c.peek("p185l", "mixt"); again.kind != resolveFold || again.fileTable != "P185L.MixT" {
		t.Fatalf("peek after fold: %+v", again)
	}
	if got := refusalLabel(res, "p185l", "mixt"); got != "p185l.mixt (schema file table P185L.MixT, letter case differs)" {
		t.Fatalf("label = %q", got)
	}
	if got := refusalLabel(c.peek("p185l", "other"), "p185l", "other"); got != "p185l.other" {
		t.Fatalf("plain label = %q", got)
	}

	// A mixed-case binlog name may be another table on a
	// lower_case_table_names=0 server: named, not used.
	mixed := newSchemaCatalog("")
	mixed.loadFile(catalogMixT)
	res = mixed.resolve("P185L", "mixt")
	if res.kind != resolveAmbiguous || !reflect.DeepEqual(res.candidates, []string{"P185L.MixT"}) {
		t.Fatalf("mixed-case: %+v", res)
	}

	// Two file tables that fold to one name are not guessed between.
	two := newSchemaCatalog("")
	two.loadFile(catalogMixT + "CREATE TABLE `mixT` (`id` int NOT NULL);\n")
	res = two.resolve("p185l", "mixt")
	if res.kind != resolveAmbiguous || len(res.candidates) != 2 {
		t.Fatalf("two candidates: %+v", res)
	}
}

func TestSchemaCatalogMissing(t *testing.T) {
	c := newSchemaCatalog("")
	if res := c.resolve("shop", "none"); res.kind != resolveMissing {
		t.Fatalf("empty catalog: %+v", res)
	}
	c.loadFile("-- Host: localhost    Database: shop\nCREATE TABLE `t` (`id` int);\n")
	if res := c.resolve("shop", "none"); res.kind != resolveMissing {
		t.Fatalf("other table: %+v", res)
	}
	c.observeDDL("shop", "DROP TABLE shop.t")
	if res := c.resolve("shop", "t"); res.kind != resolveMissing {
		t.Fatalf("dropped table: %+v", res)
	}
}

func TestSchemaCatalogRefusedReasons(t *testing.T) {
	c := newSchemaCatalog("")
	c.observeDDL("shop", "CREATE TABLE shop.gen (id INT PRIMARY KEY, g INT AS (id + 1) VIRTUAL)")
	for _, stmt := range []string{
		"CREATE TABLE shop.sel AS SELECT 1",
		"CREATE TABLE shop.like1 LIKE shop.missing",
		"ALTER TABLE shop.fresh ADD COLUMN g INT AS (id + 1) VIRTUAL",
		"CREATE TABLE shop.broken",
	} {
		c.observeDDL("shop", stmt)
	}
	for table, want := range map[string]refuseReason{
		"sel":    refuseCreateSelect,
		"like1":  refuseLikeSource,
		"fresh":  refuseAlterUnknown,
		"broken": refuseUnreadableCreate,
	} {
		res := c.resolve("shop", table)
		if res.kind != resolveRefused || res.reason != want {
			t.Fatalf("%s: %+v, want reason %d", table, res, want)
		}
	}
	// RENAME carries the refusal and its reason.
	c.observeDDL("shop", "RENAME TABLE shop.sel TO shop.sel2")
	if res := c.resolve("shop", "sel2"); res.kind != resolveRefused || res.reason != refuseCreateSelect {
		t.Fatalf("renamed: %+v", res)
	}
	// LIKE a refused table is refused too.
	c.observeDDL("shop", "CREATE TABLE shop.like2 LIKE shop.sel2")
	if res := c.resolve("shop", "like2"); res.kind != resolveRefused || res.reason != refuseLikeSource {
		t.Fatalf("like refused: %+v", res)
	}
	if res := c.resolve("shop", "gen"); res.kind != resolveExact {
		t.Fatalf("readable table: %+v", res)
	}
}

// #225: every refusal of a folded table carries the label, in every language
// and on every refusal line, not only the first English one.
func TestSchemaCatalogFoldLabelOnEveryRefusal(t *testing.T) {
	const file = "USE `P225`;\n" +
		"CREATE TABLE `MixA` (`id` int NOT NULL, `Ga` int GENERATED ALWAYS AS ((`id` + 1)) VIRTUAL, PRIMARY KEY (`id`));\n" +
		"CREATE TABLE `MixB` (`id` int NOT NULL, `Gb` int GENERATED ALWAYS AS ((`id` + 2)) VIRTUAL, PRIMARY KEY (`id`));\n"
	mk := func(table, gen string) model.FlashRow {
		return model.FlashRow{Schema: "p225", Table: table, Op: "DELETE", Columns: []string{"id", gen}, Cols: intCols(2), Before: []string{"1", "9"}, PK: []int{0}}
	}
	if err := i18n.Init("en"); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ lang, a, b string }{
		{"en", "p225.mixa (schema file table P225.MixA, letter case differs): ", "p225.mixb (schema file table P225.MixB, letter case differs): "},
		{"zh-CN", "p225.mixa（schema file 中的表为 P225.MixA，仅大小写不同）：", "p225.mixb（schema file 中的表为 P225.MixB，仅大小写不同）："},
	} {
		i18n.SetLanguage(tc.lang)
		a := New(Options{Flashback: true, SchemaSQL: file})
		consumeFlash(t, a, flashEvent(mk("mixa", "Ga")), flashEvent(mk("mixb", "Gb")))
		_, err := a.FlashbackSQL()
		i18n.SetLanguage("en")
		if err == nil {
			t.Fatalf("%s: wrong generated values were accepted", tc.lang)
		}
		lines := strings.Split(err.Error(), "\n")
		if len(lines) != 2 || !strings.HasPrefix(lines[0], tc.a) || !strings.HasPrefix(lines[1], tc.b) {
			t.Fatalf("%s: refusal lines:\n%s", tc.lang, err)
		}
	}
}
