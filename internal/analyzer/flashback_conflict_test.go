package analyzer

import (
	"encoding/json"
	"errors"
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
func conflictRun(t *testing.T, policy string, txns ...conflictTxn) *Analyzer {
	t.Helper()
	selector, err := ParseGTIDSelector([]string{conflictUUID + ":5"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	a := New(Options{Flashback: true, GTIDSelector: selector, FlashbackConflict: policy})
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
	_, err := a.WriteFlashback(&b)
	var conflict *FlashbackConflictError
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
	if _, err := a.WriteFlashback(&countWriter{}); err != nil {
		t.Fatal(err)
	}
}

// An earlier change to the row is not a conflict: the undo restores its after image.
func TestFlashbackEarlierChangeIsNotConflict(t *testing.T) {
	a := conflictRun(t, "",
		conflictTxn{"4", []model.FlashRow{orderUpdate("1", "0", "1")}},
		conflictTxn{"5", []model.FlashRow{orderUpdate("1", "1", "2")}},
	)
	if _, err := a.WriteFlashback(&countWriter{}); err != nil {
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
	sql, err := a.FlashbackSQL()
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"UPDATE `shop`.`orders` SET `id` = 1014, `amount` = 18.50 WHERE `id` <=> 1014 AND `amount` <=> 17.50;",
		"DELETE FROM `shop`.`orders` WHERE `id` <=> 8 AND `amount` <=> 2.00;",
		"INSERT INTO `shop`.`orders` (`id`, `amount`) SELECT 7, 1.00 FROM DUAL WHERE NOT EXISTS (SELECT 1 FROM `shop`.`orders` WHERE `id` <=> 7);",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("missing %s in:\n%s", want, sql)
		}
	}
}

func TestFlashbackPlanReportsRowsAndConflicts(t *testing.T) {
	a := conflictRun(t, "",
		conflictTxn{"5", []model.FlashRow{orderUpdate("1014", "18.50", "17.50"), orderUpdate("1015", "1", "2")}},
		conflictTxn{"6", []model.FlashRow{orderUpdate("1014", "17.50", "18.50")}},
	)
	var b strings.Builder
	err := a.WriteFlashbackPlan(&b)
	var conflict *FlashbackConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("the plan keeps the refusal exit: %v", err)
	}
	var plan FlashbackPlan
	if jerr := json.Unmarshal([]byte(b.String()), &plan); jerr != nil {
		t.Fatalf("%v: %s", jerr, b.String())
	}
	if plan.PlanVersion != 1 || plan.Transactions != 1 || plan.Rows != 2 || plan.Conflicts.Count != 1 ||
		plan.ConflictPolicy != "refuse" || !plan.WouldRefuse || len(plan.Tables) != 1 || plan.Tables[0].Update != 2 {
		t.Fatalf("plan: %s", b.String())
	}
}
