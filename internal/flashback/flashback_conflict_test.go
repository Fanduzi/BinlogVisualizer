package flashback_test

import (
	"binlogviz/internal/analyzer"
	"binlogviz/internal/flashback"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"binlogviz/internal/model"
)

const conflictUUID = "3e11fa47-71ca-11e1-9e33-c80aa9429562"

type conflictTxn struct {
	gno  string
	rows []model.FlashRow
}

func orderUpdate(id, before, after string) model.FlashRow {
	return model.FlashRow{Schema: "shop", Table: "orders", Op: "UPDATE", Columns: []string{"id", "amount"},
		Before: []string{id, before}, After: []string{id, after}, PK: []int{0}}
}

// conflictRun selects GTID :5 and feeds every transaction in order.
func conflictRun(t *testing.T, policy string, txns ...conflictTxn) *analyzer.Analyzer {
	t.Helper()
	return conflictRunOpts(t, analyzer.Options{FlashbackConflict: policy}, txns...)
}

func conflictRunOpts(t *testing.T, opts analyzer.Options, txns ...conflictTxn) *analyzer.Analyzer {
	t.Helper()
	selector, err := analyzer.ParseGTIDSelector([]string{conflictUUID + ":5"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	opts.Flashback, opts.GTIDSelector = true, selector
	a := analyzer.New(opts)
	base := time.Date(2026, 10, 14, 6, 5, 6, 0, time.UTC)
	pos := int64(4)
	ev := func(e model.NormalizedEvent) {
		e.Timestamp, e.BinlogPath, e.PositionStart = base, "mysql-bin.000001", pos
		pos += 40
		e.PositionEnd = pos
		if err := a.Consume(e); err != nil {
			t.Fatal(err)
		}
	}
	for _, txn := range txns {
		ev(model.NormalizedEvent{EventType: "GTID", GTID: conflictUUID + ":" + txn.gno})
		ev(model.NormalizedEvent{EventType: "BEGIN"})
		for _, row := range txn.rows {
			ev(model.NormalizedEvent{EventType: "ROWS", Operation: row.Op, Schema: row.Schema, Table: row.Table, RowCount: 1,
				FlashRows: []model.FlashRow{row}})
		}
		ev(model.NormalizedEvent{EventType: "XID"})
	}
	if _, err := a.Finalize(); err != nil {
		t.Fatal(err)
	}
	return a
}

// #259: an undo must not silently overwrite a later change to the same row.
func TestFlashbackRefusesLaterChangeToSameRow(t *testing.T) {
	a := conflictRun(t, "",
		conflictTxn{"5", []model.FlashRow{orderUpdate("1014", "18.50", "17.50"), orderUpdate("1015", "1.00", "2.00")}},
		conflictTxn{"6", []model.FlashRow{orderUpdate("1014", "17.50", "18.50")}},
	)
	var b strings.Builder
	_, err := a.Planner().WriteFlashback(&b)
	var conflict *flashback.FlashbackConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("want a conflict refusal, got %v", err)
	}
	if b.Len() != 0 {
		t.Fatal("a refused script must print nothing")
	}
	if conflict.Count != 1 || len(conflict.Listed) != 1 || conflict.Listed[0].Table != "shop.orders" ||
		conflict.Listed[0].Key != "id=1014" || conflict.Listed[0].LaterGTID != conflictUUID+":6" {
		t.Fatalf("conflict: %+v", conflict)
	}
	if !strings.Contains(err.Error(), "1014") || !strings.Contains(err.Error(), "--conflict guard") {
		t.Fatalf("message: %v", err)
	}
}

func TestFlashbackNoConflictForOtherRowsOrSameTransaction(t *testing.T) {
	a := conflictRun(t, "",
		conflictTxn{"5", []model.FlashRow{orderUpdate("1", "1", "2"), orderUpdate("1", "2", "3")}},
		conflictTxn{"6", []model.FlashRow{orderUpdate("2", "1", "2")}},
	)
	if _, err := a.Planner().WriteFlashback(&countWriter{}); err != nil {
		t.Fatal(err)
	}
}

// An earlier change to the row is not a conflict: the undo restores its after image.
func TestFlashbackEarlierChangeIsNotConflict(t *testing.T) {
	a := conflictRun(t, "",
		conflictTxn{"4", []model.FlashRow{orderUpdate("1", "0", "1")}},
		conflictTxn{"5", []model.FlashRow{orderUpdate("1", "1", "2")}},
	)
	if _, err := a.Planner().WriteFlashback(&countWriter{}); err != nil {
		t.Fatal(err)
	}
}

// --conflict guard writes the script, and each undo matches the whole
// after image, so a row changed later is left alone.
func TestFlashbackConflictGuardMatchesPostImage(t *testing.T) {
	del := model.FlashRow{Schema: "shop", Table: "orders", Op: "DELETE", Columns: []string{"id", "amount"}, Before: []string{"7", "1.00"}, PK: []int{0}}
	ins := model.FlashRow{Schema: "shop", Table: "orders", Op: "INSERT", Columns: []string{"id", "amount"}, After: []string{"8", "2.00"}, PK: []int{0}}
	a := conflictRun(t, "guard",
		conflictTxn{"5", []model.FlashRow{orderUpdate("1014", "18.50", "17.50"), del, ins}},
		conflictTxn{"6", []model.FlashRow{orderUpdate("1014", "17.50", "18.50")}},
	)
	sql, err := a.Planner().SQL()
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"UPDATE `shop`.`orders` SET `id` = 1014, `amount` = 18.50 WHERE `id` <=> 1014 AND `amount` <=> 17.50",
		"DELETE FROM `shop`.`orders` WHERE `id` <=> 8 AND `amount` <=> 2.00",
		"INSERT INTO `shop`.`orders` (`id`, `amount`) SELECT 7, 1.00 FROM DUAL WHERE NOT EXISTS (SELECT 1 FROM `shop`.`orders` WHERE `id` <=> 7)",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("missing %s in:\n%s", want, sql)
		}
	}
}

// A key-moving UPDATE undo under --conflict guard skips the move-back when a
// later row reuses the old key, instead of failing with ERROR 1062 (#270).
// The default policy still refuses; an unmoved key gets no extra condition.
func TestFlashbackConflictGuardKeyMoveSkipsReusedKey(t *testing.T) {
	move := func(id, to string) model.FlashRow {
		return model.FlashRow{Schema: "e", Table: "pk", Op: "UPDATE", Columns: []string{"id", "v"}, Before: []string{id, "0"}, After: []string{to, "0"}, PK: []int{0}}
	}
	same := model.FlashRow{Schema: "e", Table: "pk", Op: "UPDATE", Columns: []string{"id", "v"}, Before: []string{"9", "0"}, After: []string{"9", "1"}, PK: []int{0}}
	reuse := model.FlashRow{Schema: "e", Table: "pk", Op: "INSERT", Columns: []string{"id", "v"}, After: []string{"2", "7"}, PK: []int{0}}
	txns := []conflictTxn{
		{"5", []model.FlashRow{move("1", "101"), move("2", "102"), same}},
		{"6", []model.FlashRow{reuse}},
	}
	refuse := conflictRun(t, "", txns...)
	if _, err := refuse.Planner().SQL(); err == nil {
		t.Fatal("default policy must refuse the reused key")
	}
	sql, err := conflictRun(t, "guard", txns...).Planner().SQL()
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"UPDATE `e`.`pk` SET `id` = 2, `v` = 0 WHERE `id` <=> 102 AND `v` <=> 0 AND NOT EXISTS (SELECT 1 FROM (SELECT 1 FROM `e`.`pk` WHERE `id` <=> 2 AND NOT (`id` <=> 102) LIMIT 1) AS binlogviz_key)",
		"UPDATE `e`.`pk` SET `id` = 1, `v` = 0 WHERE `id` <=> 101 AND `v` <=> 0 AND NOT EXISTS (SELECT 1 FROM (SELECT 1 FROM `e`.`pk` WHERE `id` <=> 1 AND NOT (`id` <=> 101) LIMIT 1) AS binlogviz_key)",
		"UPDATE `e`.`pk` SET `id` = 9, `v` = 0 WHERE `id` <=> 9 AND `v` <=> 1",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("missing %s in:\n%s", want, sql)
		}
	}
	plain, err := conflictRun(t, "", txns[0]).Planner().SQL()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(plain, "binlogviz_key") {
		t.Fatalf("default policy must not add the key check:\n%s", plain)
	}
}

func TestFlashbackPlanReportsRowsAndConflicts(t *testing.T) {
	a := conflictRun(t, "",
		conflictTxn{"5", []model.FlashRow{orderUpdate("1014", "18.50", "17.50"), orderUpdate("1015", "1", "2")}},
		conflictTxn{"6", []model.FlashRow{orderUpdate("1014", "17.50", "18.50")}},
	)
	var b strings.Builder
	err := a.Planner().WritePlan(&b)
	var conflict *flashback.FlashbackConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("the plan keeps the refusal exit: %v", err)
	}
	var plan flashback.FlashbackPlan
	if jerr := json.Unmarshal([]byte(b.String()), &plan); jerr != nil {
		t.Fatalf("%v: %s", jerr, b.String())
	}
	if plan.PlanVersion != 2 || plan.Transactions != 1 || plan.Rows != 2 || plan.Conflicts.Count != 1 ||
		plan.ConflictPolicy != "refuse" || !plan.WouldRefuse || len(plan.Tables) != 1 || plan.Tables[0].Update != 2 {
		t.Fatalf("plan: %s", b.String())
	}
	if plan.Refusal == nil || plan.Refusal.Kind != "conflict" || !strings.Contains(plan.Refusal.Message, "1014") {
		t.Fatalf("refusal: %s", b.String())
	}
}

// #270 item 2: a refusal other than a conflict (exit 6) still prints the
// plan, so a script gets JSON for every refusal.
func TestFlashbackPlanOnRefusalNamesReason(t *testing.T) {
	selector, err := analyzer.ParseGTIDSelector([]string{conflictUUID + ":5"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	a := analyzer.New(analyzer.Options{Flashback: true, FlashbackPlan: true, GTIDSelector: selector})
	base := time.Date(2026, 10, 14, 6, 5, 6, 0, time.UTC)
	pos := int64(4)
	for _, e := range []model.NormalizedEvent{
		{EventType: "GTID", GTID: conflictUUID + ":5"},
		{EventType: "BEGIN"},
		// No row images: a non-FULL binlog_row_image, refused.
		{EventType: "ROWS", Operation: "UPDATE", Schema: "shop", Table: "orders", RowCount: 1},
		{EventType: "XID"},
	} {
		e.Timestamp, e.BinlogPath, e.PositionStart = base, "mysql-bin.000001", pos
		pos += 40
		e.PositionEnd = pos
		if err := a.Consume(e); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := a.Finalize(); err != nil {
		t.Fatal(err)
	}
	var b strings.Builder
	perr := a.Planner().WritePlan(&b)
	if perr == nil {
		t.Fatal("the plan keeps the refusal error")
	}
	var plan flashback.FlashbackPlan
	if jerr := json.Unmarshal([]byte(b.String()), &plan); jerr != nil {
		t.Fatalf("a refusal must still print the plan: %v: %q", jerr, b.String())
	}
	if !plan.WouldRefuse || plan.Refusal == nil || plan.Refusal.Kind != "refused" || plan.Refusal.Message != perr.Error() ||
		plan.Rows != 0 || plan.Tables == nil || plan.Warnings == nil || plan.Conflicts.Listed == nil {
		t.Fatalf("plan: %s", b.String())
	}
	var raw map[string]any
	if err := json.Unmarshal([]byte(b.String()), &raw); err != nil {
		t.Fatal(err)
	}
	refusal, _ := raw["refusal"].(map[string]any)
	if refusal["reason"] != "row_capture" || refusal["table"] != "shop.orders" {
		t.Fatalf("plan refusal reason: %s", b.String())
	}
}

// A plan with nothing refused has refusal null and warnings as objects.
func TestFlashbackPlanCleanRunHasNullRefusal(t *testing.T) {
	a := conflictRunOpts(t, analyzer.Options{FlashbackPlan: true},
		conflictTxn{"5", []model.FlashRow{orderUpdate("1", "1", "2")}},
	)
	var b strings.Builder
	if err := a.Planner().WritePlan(&b); err != nil {
		t.Fatal(err)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal([]byte(b.String()), &raw); err != nil {
		t.Fatal(err)
	}
	var warnings []model.FlashbackWarning
	if err := json.Unmarshal(raw["warnings"], &warnings); err != nil {
		t.Fatal(err)
	}
	// No CREATE TABLE was logged: one coded warning, no "warning:" prefix.
	if string(raw["refusal"]) != "null" || string(raw["would_refuse"]) != "false" || len(warnings) != 1 ||
		warnings[0].Code != model.WarnTableDefinitionMissing || !strings.HasPrefix(warnings[0].Message, "shop.orders: ") {
		t.Fatalf("plan: %s", b.String())
	}
	// Stderr keeps the v0.25.0 text.
	if lines := a.Planner().Warnings(); len(lines) != 1 || lines[0] != "warning: "+warnings[0].Message {
		t.Fatalf("stderr lines: %q", lines)
	}
}

// #270 item 3: --conflict-limit lists every conflict an operator must review.
func TestFlashbackConflictLimit(t *testing.T) {
	var sel, later []model.FlashRow
	for i := 0; i < 25; i++ {
		id := strconv.Itoa(1000 + i)
		sel = append(sel, orderUpdate(id, "1", "2"))
		later = append(later, orderUpdate(id, "2", "3"))
	}
	for _, tc := range []struct{ limit, listed int }{{0, 20}, {3, 3}, {-1, 25}} {
		a := conflictRunOpts(t, analyzer.Options{FlashbackConflictLimit: tc.limit}, conflictTxn{"5", sel}, conflictTxn{"6", later})
		var b strings.Builder
		_ = a.Planner().WritePlan(&b)
		var plan flashback.FlashbackPlan
		if err := json.Unmarshal([]byte(b.String()), &plan); err != nil {
			t.Fatal(err)
		}
		if plan.Conflicts.Count != 25 || len(plan.Conflicts.Listed) != tc.listed || plan.Conflicts.Omitted != 25-tc.listed {
			t.Fatalf("limit %d: count %d listed %d omitted %d", tc.limit, plan.Conflicts.Count, len(plan.Conflicts.Listed), plan.Conflicts.Omitted)
		}
		var conflict *flashback.FlashbackConflictError
		if _, err := a.Planner().WriteFlashback(&countWriter{}); !errors.As(err, &conflict) || len(conflict.Listed) != tc.listed {
			t.Fatalf("limit %d: exit 7 details list %v", tc.limit, err)
		}
	}
}

// #267: a later change that is itself selected is undone by the same
// script, so it is not a conflict. Only unselected later changes count.
func TestFlashbackSelectedLaterChangeIsNotConflict(t *testing.T) {
	selector, err := analyzer.ParseGTIDSelector([]string{conflictUUID + ":5-6"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	a := analyzer.New(analyzer.Options{Flashback: true, GTIDSelector: selector})
	base := time.Date(2026, 10, 14, 6, 5, 6, 0, time.UTC)
	pos := int64(4)
	ev := func(e model.NormalizedEvent) {
		e.Timestamp, e.BinlogPath, e.PositionStart = base, "mysql-bin.000001", pos
		pos += 40
		e.PositionEnd = pos
		if err := a.Consume(e); err != nil {
			t.Fatal(err)
		}
	}
	for _, txn := range []conflictTxn{
		{"5", []model.FlashRow{orderUpdate("1", "1", "2"), orderUpdate("2", "1", "2")}},
		{"6", []model.FlashRow{orderUpdate("1", "2", "3")}}, // selected: undone too
		{"7", []model.FlashRow{orderUpdate("2", "2", "3")}}, // not selected: conflict
	} {
		ev(model.NormalizedEvent{EventType: "GTID", GTID: conflictUUID + ":" + txn.gno})
		ev(model.NormalizedEvent{EventType: "BEGIN"})
		for _, row := range txn.rows {
			ev(model.NormalizedEvent{EventType: "ROWS", Operation: row.Op, Schema: row.Schema, Table: row.Table, RowCount: 1, FlashRows: []model.FlashRow{row}})
		}
		ev(model.NormalizedEvent{EventType: "XID"})
	}
	if _, err := a.Finalize(); err != nil {
		t.Fatal(err)
	}
	_, err = a.Planner().WriteFlashback(&countWriter{})
	var conflict *flashback.FlashbackConflictError
	if !errors.As(err, &conflict) || conflict.Count != 1 || conflict.Listed[0].Key != "id=2" || conflict.Listed[0].LaterGTID != conflictUUID+":7" {
		t.Fatalf("want only the unselected :7 change, got %v %+v", err, conflict)
	}
}

// A whole-file undo (no selectors) has no conflicts: every later change is selected.
func TestFlashbackWholeRangeHasNoConflict(t *testing.T) {
	a := analyzer.New(analyzer.Options{Flashback: true})
	base := time.Date(2026, 10, 14, 6, 5, 6, 0, time.UTC)
	pos := int64(4)
	for i, gno := range []string{"1", "2", "3"} {
		for _, e := range []model.NormalizedEvent{
			{EventType: "GTID", GTID: conflictUUID + ":" + gno}, {EventType: "BEGIN"},
			{EventType: "ROWS", Operation: "UPDATE", Schema: "shop", Table: "orders", RowCount: 1,
				FlashRows: []model.FlashRow{orderUpdate("1", string(rune('0'+i)), string(rune('1'+i)))}},
			{EventType: "XID"},
		} {
			e.Timestamp, e.BinlogPath, e.PositionStart = base, "mysql-bin.000001", pos
			pos += 40
			e.PositionEnd = pos
			if err := a.Consume(e); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := a.Finalize(); err != nil {
		t.Fatal(err)
	}
	if rows, err := a.Planner().WriteFlashback(&countWriter{}); err != nil || rows != 3 {
		t.Fatalf("rows %d err %v", rows, err)
	}
}
