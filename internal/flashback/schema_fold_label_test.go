package flashback_test

import (
	"binlogviz/internal/analyzer"
	"strings"
	"testing"

	"binlogviz/internal/i18n"
	"binlogviz/internal/model"
)

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
		a := analyzer.New(analyzer.Options{Flashback: true, SchemaSQL: file})
		consumeFlash(t, a, flashEvent(mk("mixa", "Ga")), flashEvent(mk("mixb", "Gb")))
		_, err := a.Planner().SQL()
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
