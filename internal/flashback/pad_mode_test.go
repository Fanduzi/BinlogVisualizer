package flashback_test

import (
	"strings"
	"testing"
	"time"

	"binlogviz/internal/analyzer"
	"binlogviz/internal/model"
)

// The header must strip PAD_CHAR_TO_FULL_LENGTH before CREATE PROCEDURE, and
// a keyed UPDATE or DELETE undo must be marked so a 0-row match cannot commit.
func TestFlashbackStripsPadAndMarksKeyedUndos(t *testing.T) {
	sql := flashSQL(t, analyzer.Options{},
		flashRow("UPDATE", false, []int{0}),
		flashRow("DELETE", false, []int{0}),
		flashRow("INSERT", false, []int{0}),
	)
	const strip = "SET @binlogviz_saved_mode = @@SESSION.sql_mode, SESSION sql_mode = REPLACE(REPLACE(@@SESSION.sql_mode, 'PAD_CHAR_TO_FULL_LENGTH', ''), 'NO_BACKSLASH_ESCAPES', '')"
	if !strings.Contains(sql, strip) {
		t.Fatalf("header does not strip PAD_CHAR_TO_FULL_LENGTH:\n%s", sql)
	}
	if !strings.Contains(sql, "binlogviz: undo committed") {
		t.Fatal("script has no commit marker")
	}
	if !strings.Contains(sql, `E\nUPDATE`) || !strings.Contains(sql, `E\nDELETE`) {
		t.Fatalf("keyed UPDATE/DELETE undo is not required to affect one row:\n%s", sql)
	}
	if !strings.Contains(sql, `T\nINSERT`) {
		t.Fatalf("INSERT undo should be counted and not required to match one row:\n%s", sql)
	}
	if strings.Contains(sql, `E\nINSERT`) {
		t.Fatal("INSERT undo must not require one row")
	}

	guarded := flashSQL(t, analyzer.Options{FlashbackConflict: "guard"}, flashRow("UPDATE", false, []int{0}))
	if strings.Contains(guarded, `E\nUPDATE`) || !strings.Contains(guarded, `T\nUPDATE`) {
		t.Fatal("--conflict guard may affect 0 rows")
	}
	noPK := flashSQL(t, analyzer.Options{}, flashRow("UPDATE", true, nil))
	if strings.Contains(noPK, `E\nUPDATE`) || !strings.Contains(noPK, `T\nUPDATE`) {
		t.Fatal("a no-PK undo may affect 0 rows")
	}
}

func flashRow(op string, noPK bool, pk []int) model.FlashRow {
	return model.FlashRow{
		Schema: "shop", Table: "c", Op: op, NoPK: noPK, PK: pk,
		Columns: []string{"k", "v"},
		Before:  []string{"'k4'", "4"},
		After:   []string{"'k4'", "104"},
	}
}

func flashSQL(t *testing.T, opts analyzer.Options, rows ...model.FlashRow) string {
	t.Helper()
	opts.Flashback = true
	a := analyzer.New(opts)
	base := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	ev := model.NormalizedEvent{
		Timestamp: base, EventType: "ROWS", Operation: "UPDATE", Schema: "shop", Table: "c",
		BinlogPath: "mysql-bin.000001", PositionStart: 10, PositionEnd: 40,
		FlashRows: rows,
	}
	if err := a.Consume(ev); err != nil {
		t.Fatal(err)
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
