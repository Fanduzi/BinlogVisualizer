package flashback_test

import (
	"strings"
	"testing"
	"time"

	"binlogviz/internal/analyzer"
	"binlogviz/internal/flashback"
	"binlogviz/internal/model"
	"binlogviz/internal/mysqlexpr"
)

// whereFlashAnalyzer runs flashback with the compiled --where predicate, the
// same Options the CLI fills (ADR 0024).
func whereFlashAnalyzer(t *testing.T, expr string) *analyzer.Analyzer {
	t.Helper()
	w, err := mysqlexpr.CompileWhere(expr)
	if err != nil {
		t.Fatalf("CompileWhere(%q): %v", expr, err)
	}
	return analyzer.New(analyzer.Options{Flashback: true, RowWhere: w})
}

func whereRows(t *testing.T, a *analyzer.Analyzer, events ...model.NormalizedEvent) string {
	t.Helper()
	base := time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC)
	for i := range events {
		events[i].Timestamp = base
		if events[i].BinlogPath == "" {
			events[i].BinlogPath = "mysql-bin.000001"
		}
		if err := a.Consume(events[i]); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := a.Finalize(); err != nil {
		t.Fatal(err)
	}
	sql, err := a.Planner().SQL()
	if err != nil {
		t.Fatal(err)
	}
	return sql
}

func TestWhereUndoReadsTheMatchedImage(t *testing.T) {
	// The predicate reads the image the operation matches on: the before
	// image for DELETE/UPDATE, the after image for INSERT (ADR 0024).
	a := whereFlashAnalyzer(t, "id < 400")
	sql := whereRows(t, a,
		model.NormalizedEvent{EventType: "ROWS", Operation: "DELETE", Schema: "shop", Table: "users",
			FlashRows: []model.FlashRow{{
				Schema: "shop", Table: "users", Op: "DELETE", Columns: []string{"id"}, Before: []string{"399"}, PK: []int{0},
			}}},
		model.NormalizedEvent{EventType: "ROWS", Operation: "DELETE", Schema: "shop", Table: "users",
			FlashRows: []model.FlashRow{{
				Schema: "shop", Table: "users", Op: "DELETE", Columns: []string{"id"}, Before: []string{"401"}, PK: []int{0},
			}}},
		model.NormalizedEvent{EventType: "ROWS", Operation: "INSERT", Schema: "shop", Table: "users",
			FlashRows: []model.FlashRow{{
				Schema: "shop", Table: "users", Op: "INSERT", Columns: []string{"id"}, After: []string{"399"}, PK: []int{0},
			}}},
		model.NormalizedEvent{EventType: "ROWS", Operation: "INSERT", Schema: "shop", Table: "users",
			FlashRows: []model.FlashRow{{
				Schema: "shop", Table: "users", Op: "INSERT", Columns: []string{"id"}, After: []string{"401"}, PK: []int{0},
			}}},
		model.NormalizedEvent{EventType: "ROWS", Operation: "UPDATE", Schema: "shop", Table: "users",
			FlashRows: []model.FlashRow{{
				Schema: "shop", Table: "users", Op: "UPDATE", Columns: []string{"id"}, Before: []string{"399"}, After: []string{"900"}, PK: []int{0},
			}}},
	)
	// Undo reverses the stream: the UPDATE undo restores id=399, the INSERT
	// undo deletes id=399, the DELETE undo re-inserts id=399.
	if !strings.Contains(sql, "INSERT INTO `shop`.`users` (`id`) VALUES (399)") {
		t.Fatalf("delete undo missing:\n%s", sql)
	}
	if strings.Contains(sql, "401") {
		t.Fatalf("filtered rows emitted:\n%s", sql)
	}
	if !strings.Contains(sql, "DELETE FROM `shop`.`users` WHERE `id` <=> 399") {
		t.Fatalf("insert undo missing:\n%s", sql)
	}
	// The UPDATE matched on its before image (399), not its after (900).
	if !strings.Contains(sql, "SET `id` = 399") || !strings.Contains(sql, "WHERE `id` <=> 900") {
		t.Fatalf("update undo missing:\n%s", sql)
	}
}

func TestWhereUndoRefusesUnknownColumn(t *testing.T) {
	// A predicate naming a column the row does not have refuses
	// where_column — the MySQL ERROR 1054 shape (ADR 0024).
	a := whereFlashAnalyzer(t, "nosuch < 400")
	if err := a.Consume(model.NormalizedEvent{EventType: "ROWS", Operation: "DELETE", Schema: "shop", Table: "users",
		FlashRows: []model.FlashRow{{
			Schema: "shop", Table: "users", Op: "DELETE", Columns: []string{"id"}, Before: []string{"399"}, PK: []int{0},
		}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Finalize(); err != nil {
		t.Fatal(err)
	}
	sql, err := a.Planner().SQL()
	if err == nil || sql != "" {
		t.Fatalf("sql %q err %v", sql, err)
	}
	reason, table, ok := flashback.RefusalDetails(err)
	if !ok || reason != "where_column" || !strings.Contains(table, "users") {
		t.Fatalf("refusal: %q %q %v", reason, table, ok)
	}
}

func TestWhereUndoPartialTransactionWarns(t *testing.T) {
	// A transaction the predicate undoes only in part warns, exactly like a
	// table or --dml selection does (ADR 0024).
	const gtid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	a := whereFlashAnalyzer(t, "id < 400")
	base := time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC)
	events := []model.NormalizedEvent{
		{Timestamp: base, EventType: "GTID", GTID: gtid + ":1", BinlogPath: "mysql-bin.000001", PositionStart: 100, PositionEnd: 140},
		{Timestamp: base, EventType: "BEGIN", BinlogPath: "mysql-bin.000001", PositionStart: 140, PositionEnd: 160},
		{Timestamp: base, EventType: "ROWS", Operation: "DELETE", Schema: "shop", Table: "users", BinlogPath: "mysql-bin.000001", PositionStart: 160, PositionEnd: 200,
			FlashRows: []model.FlashRow{{
				Schema: "shop", Table: "users", Op: "DELETE", Columns: []string{"id"}, Before: []string{"399"}, PK: []int{0},
			}}},
		{Timestamp: base, EventType: "ROWS", Operation: "DELETE", Schema: "shop", Table: "users", BinlogPath: "mysql-bin.000001", PositionStart: 200, PositionEnd: 240,
			FlashRows: []model.FlashRow{{
				Schema: "shop", Table: "users", Op: "DELETE", Columns: []string{"id"}, Before: []string{"401"}, PK: []int{0},
			}}},
		{Timestamp: base, EventType: "XID", BinlogPath: "mysql-bin.000001", PositionStart: 240, PositionEnd: 260},
	}
	for _, ev := range events {
		if err := a.Consume(ev); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := a.Finalize(); err != nil {
		t.Fatal(err)
	}
	sql, err := a.Planner().SQL()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sql, "VALUES (399)") || strings.Contains(sql, "401") {
		t.Fatalf("partial filter:\n%s", sql)
	}
	found := false
	for _, warning := range a.Planner().Warnings() {
		if strings.Contains(warning, "only partly undone") && strings.Contains(warning, gtid+":1") {
			found = true
		}
	}
	if !found {
		t.Fatalf("no partial-transaction warning: %#v", a.Planner().Warnings())
	}
}

func TestWhereUndoCheckedRefusalWins(t *testing.T) {
	// Checked-image refusals still fire before the predicate: a row the
	// binlog could not capture is never filtered into compliance (ADR 0024).
	a := whereFlashAnalyzer(t, "id < 400")
	if err := a.Consume(model.NormalizedEvent{EventType: "ROWS", Operation: "DELETE", Schema: "shop", Table: "users",
		FlashRows: []model.FlashRow{{
			Schema: "shop", Table: "users", Op: "DELETE", Columns: []string{"id"}, Before: []string{"399"},
			ProblemKind: model.FlashProblemImage, PK: []int{0},
		}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Finalize(); err != nil {
		t.Fatal(err)
	}
	sql, err := a.Planner().SQL()
	if err == nil || sql != "" {
		t.Fatalf("sql %q err %v", sql, err)
	}
	if reason, _, ok := flashback.RefusalDetails(err); !ok || reason != "row_image" {
		t.Fatalf("checked refusal should win: %v", err)
	}
}
