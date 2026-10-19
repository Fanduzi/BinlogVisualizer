package flashback

import (
	"strings"
	"testing"

	"binlogviz/internal/model"
)

func TestRenderUndoOmitsGeneratedColumns(t *testing.T) {
	cols := map[string]struct{}{"virt": {}, "stor": {}}
	deleted := model.FlashRow{
		Schema: "shop", Table: "gen", Op: "DELETE",
		Columns: []string{"id", "base", "virt", "stor"},
		Before:  []string{"2", "20", "21", "40"},
		PK:      []int{0},
	}
	sql, err := renderUndoStatement(deleted, cols, "")
	if err != nil {
		t.Fatal(err)
	}
	if sql != "T\nINSERT INTO `shop`.`gen` (`id`, `base`) VALUES (2, 20);" {
		t.Fatalf("insert: %s", sql)
	}

	updated := model.FlashRow{
		Schema: "shop", Table: "gen", Op: "UPDATE",
		Columns: []string{"id", "base", "Virt", "stor"},
		Before:  []string{"1", "10", "11", "20"},
		After:   []string{"1", "11", "12", "22"},
		PK:      []int{0},
	}
	sql, err = renderUndoStatement(updated, cols, "")
	if err != nil {
		t.Fatal(err)
	}
	if sql != "E\nUPDATE `shop`.`gen` SET `id` = 1, `base` = 10 WHERE `id` <=> 1;" {
		t.Fatalf("update: %s", sql)
	}

	heap := model.FlashRow{
		Schema: "shop", Table: "heap", Op: "UPDATE", NoPK: true,
		Columns: []string{"id", "virt", "note"},
		Before:  []string{"1", "2", "'a'"},
		After:   []string{"1", "3", "'b'"},
	}
	sql, err = renderUndoStatement(heap, map[string]struct{}{"virt": {}}, "")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sql, "matches every column") || !strings.Contains(sql, "`virt` <=> 3") {
		t.Fatalf("no-pk where: %s", sql)
	}
	if strings.Contains(sql, "SET `virt`") || strings.Contains(sql, ", `virt`") {
		t.Fatalf("no-pk assigned generated column: %s", sql)
	}
	if !strings.Contains(sql, "UPDATE `shop`.`heap` SET `id` = 1, `note` = 'a' WHERE `id` <=> 1 AND `virt` <=> 3 AND `note` <=> 'b' LIMIT 1;") {
		t.Fatalf("no-pk sql: %s", sql)
	}

	keyed := model.FlashRow{
		Schema: "shop", Table: "g", Op: "INSERT",
		Columns: []string{"g", "base"},
		After:   []string{"11", "10"},
		PK:      []int{0},
	}
	sql, err = renderUndoStatement(keyed, map[string]struct{}{"g": {}}, "")
	if err != nil {
		t.Fatal(err)
	}
	if sql != "E\nDELETE FROM `shop`.`g` WHERE `g` <=> 11;" {
		t.Fatalf("generated pk: %s", sql)
	}

	onlyGen := model.FlashRow{
		Schema: "shop", Table: "gen", Op: "DELETE",
		Columns: []string{"virt"}, Before: []string{"1"}, PK: []int{0},
	}
	if _, err = renderUndoStatement(onlyGen, cols, ""); err == nil || !strings.Contains(err.Error(), "shop.gen: cannot undo a row whose columns are all generated") {
		t.Fatalf("all generated: %v", err)
	}
}

func TestRelaxedModesDropOnlyWhatTheRowNeeds(t *testing.T) {
	gen := map[string]struct{}{"g": {}}
	cases := []struct {
		name string
		row  model.FlashRow
		gen  map[string]struct{}
		want []string
	}{
		{"plain", model.FlashRow{Op: "DELETE", Columns: []string{"id"}, Before: []string{"1"}}, nil, nil},
		{"enum0", model.FlashRow{Op: "DELETE", NonStrict: true, Columns: []string{"id"}, Before: []string{"1"}}, nil, []string{"STRICT_ALL_TABLES", "STRICT_TRANS_TABLES", "TRADITIONAL"}},
		{"zerodate", model.FlashRow{Op: "UPDATE", ZeroDate: true, Columns: []string{"id"}, Before: []string{"1"}}, nil, []string{"NO_ZERO_DATE", "NO_ZERO_IN_DATE", "TRADITIONAL"}},
		{"gen null", model.FlashRow{Op: "DELETE", Columns: []string{"id", "g"}, Before: []string{"1", "NULL"}}, gen, []string{"ERROR_FOR_DIVISION_BY_ZERO", "TRADITIONAL"}},
		{"gen value", model.FlashRow{Op: "DELETE", Columns: []string{"id", "g"}, Before: []string{"1", "5"}}, gen, nil},
		{"gen null kept", model.FlashRow{Op: "DELETE", Columns: []string{"id", "g"}, Before: []string{"1", "NULL"}}, nil, nil},
		{"insert undo", model.FlashRow{Op: "INSERT", Columns: []string{"id", "g"}, After: []string{"1", "NULL"}}, gen, nil},
	}
	for _, c := range cases {
		got := relaxedModes(c.row, c.gen)
		if strings.Join(got, ",") != strings.Join(c.want, ",") {
			t.Fatalf("%s: got %v want %v", c.name, got, c.want)
		}
	}
	wrapped := sqlModeWrap("DELETE FROM t;", []string{"NO_ZERO_DATE", "TRADITIONAL"})
	if strings.Contains(wrapped, "STRICT_TRANS_TABLES") || strings.Contains(wrapped, "SET SESSION sql_mode") || !strings.Contains(wrapped, "',NO_ZERO_DATE,'") || !strings.HasPrefix(wrapped, "SET @binlogviz_next_mode =") || !strings.HasSuffix(wrapped, "\nDELETE FROM t;") {
		t.Fatalf("wrap:\n%s", wrapped)
	}
}
