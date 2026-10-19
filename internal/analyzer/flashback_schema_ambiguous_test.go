package analyzer

import (
	"strings"
	"testing"

	"binlogviz/internal/model"
)

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
