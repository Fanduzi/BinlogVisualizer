package redo

import (
	"strings"
	"testing"

	"binlogviz/internal/model"
	"binlogviz/internal/mysqlexpr"
)

// whereOption compiles the --where predicate the CLI would hand the planner.
func whereOption(t *testing.T, expr string) Options {
	t.Helper()
	w, err := mysqlexpr.CompileWhere(expr)
	if err != nil {
		t.Fatalf("CompileWhere(%q): %v", expr, err)
	}
	return Options{Where: w}
}

func TestWhereInsertUsesAfterImage(t *testing.T) {
	// The predicate reads the image the row's operation matches on: INSERT's
	// after image (ADR 0024).
	keep := insertRow()
	keep.After = []any{int64(399), "a"}
	drop := insertRow()
	drop.After = []any{int64(401), "b"}
	_, sql, err := plan(t, whereOption(t, "id < 400"),
		ev("GTID", "k1", "uuid:1"),
		ev("BEGIN", "k1", "uuid:1"),
		ev("ROWS", "k1", "uuid:1", keep, drop),
		ev("XID", "k1", "uuid:1"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sql, "VALUES (399, 'a')") || strings.Contains(sql, "401") {
		t.Fatalf("insert filter:\n%s", sql)
	}
}

func TestWhereUpdateAndDeleteUseBeforeImage(t *testing.T) {
	// The before image is the row the predicate means: an UPDATE whose
	// before id is 399 is kept even when the after id is 900 (ADR 0024).
	upd := model.RedoRow{
		Schema: "shop", Table: "orders", Op: "UPDATE",
		Columns: []string{"id", "note"}, Names: model.RowNamesFull,
		Cols:   []model.FlashCol{intCol(), varcharCol()},
		Before: []any{int64(399), "a"}, After: []any{int64(900), "b"},
		PK: []int{0},
	}
	del := model.RedoRow{
		Schema: "shop", Table: "orders", Op: "DELETE",
		Columns: []string{"id", "note"}, Names: model.RowNamesFull,
		Cols:   []model.FlashCol{intCol(), varcharCol()},
		Before: []any{int64(401), "a"},
		PK:     []int{0},
	}
	_, sql, err := plan(t, whereOption(t, "id < 400"),
		ev("ROWS", "k1", "", upd),
		ev("ROWS", "k2", "", del))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sql, "SET `id` = 900") {
		t.Fatalf("update kept by before image:\n%s", sql)
	}
	if strings.Contains(sql, "DELETE FROM") {
		t.Fatalf("delete 401 should be filtered:\n%s", sql)
	}
}

func TestWhereMissingColumnRefuses(t *testing.T) {
	// A predicate naming a column the row does not have is ERROR 1054, not a
	// silent no-match (ADR 0024).
	_, _, err := plan(t, whereOption(t, "nosuch < 400"), ev("ROWS", "k1", "", insertRow()))
	if err == nil {
		t.Fatal("unknown column filtered anyway")
	}
	ref, ok := err.(*Refusal)
	if !ok || ref.Code != "where_column" {
		t.Fatalf("err = %#v", err)
	}
}

func TestWhereMinimalNamesFromSchemaFile(t *testing.T) {
	// MINIMAL metadata resolves the predicate's names through --schema-file;
	// without it the existing names refusal stands (ADR 0024).
	row := insertRow()
	row.Names = model.RowNamesPositional
	row.Columns = []string{"@1", "@2"}
	row.After = []any{int64(399), "a"}
	schema := Options{SchemaFileDB: "shop", SchemaSQL: "CREATE TABLE `orders` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `note` varchar(64) CHARACTER SET utf8mb4 DEFAULT NULL,\n" +
		"  PRIMARY KEY (`id`)\n)"}
	schema.Where = whereOption(t, "id < 400").Where
	_, sql, err := plan(t, schema, ev("ROWS", "k1", "", row))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sql, "VALUES (399, 'a')") {
		t.Fatalf("MINIMAL + schema-file filter:\n%s", sql)
	}

	noSchema := whereOption(t, "id < 400")
	_, _, err = plan(t, noSchema, ev("ROWS", "k1", "", row))
	ref, ok := err.(*Refusal)
	if !ok || ref.Code != "names" {
		t.Fatalf("MINIMAL without schema-file: err = %#v", err)
	}
}

func TestWherePartialTransactionWarns(t *testing.T) {
	// A transaction the predicate replays in part warns like a table or
	// --dml selection does: dropping rows silently would be worse (ADR 0024).
	keep := insertRow()
	keep.After = []any{int64(399), "a"}
	drop := insertRow()
	drop.After = []any{int64(401), "b"}
	p, sql, err := plan(t, whereOption(t, "id < 400"),
		ev("GTID", "k1", "uuid:1"),
		ev("BEGIN", "k1", "uuid:1"),
		ev("ROWS", "k1", "uuid:1", keep),
		ev("ROWS", "k1", "uuid:1", drop),
		ev("XID", "k1", "uuid:1"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sql, "399") || strings.Contains(sql, "401") {
		t.Fatalf("partial filter:\n%s", sql)
	}
	found := false
	for _, w := range p.Warnings() {
		if strings.Contains(w, "partly replayed") {
			found = true
		}
	}
	if !found {
		t.Fatalf("no partial-transaction warning: %#v", p.Warnings())
	}
}

func TestWhereAllRowsFilteredEmitsNothing(t *testing.T) {
	drop := insertRow()
	drop.After = []any{int64(401), "b"}
	p, _, err := plan(t, whereOption(t, "id < 400"), ev("ROWS", "k1", "", drop))
	if err != nil {
		t.Fatal(err)
	}
	if p.Statements() != 0 {
		t.Fatalf("filtered statements = %d", p.Statements())
	}
}

func TestWhereComposesWithSelection(t *testing.T) {
	// --where ANDs with the other selectors: this row matches the predicate
	// but the row event already carries ProblemKind, so the checked-image
	// refusal fires before the filter runs (ADR 0024).
	row := insertRow()
	row.ProblemKind = model.FlashProblemImage
	_, _, err := plan(t, whereOption(t, "id < 400"), ev("ROWS", "k1", "", row))
	ref, ok := err.(*Refusal)
	if !ok || ref.Code != "row_image" {
		t.Fatalf("checked refusal should win: %#v", err)
	}
}
