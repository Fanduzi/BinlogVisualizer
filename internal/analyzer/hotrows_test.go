package analyzer

import (
	"fmt"
	"testing"
	"time"

	"binlogviz/internal/model"
)

func TestHotRowCountsDistinctTransactionsAndSkipsUnknownKeys(t *testing.T) {
	agg := newHotRowAggregator(8)
	base := time.Date(2026, 10, 6, 14, 0, 1, 0, time.UTC)
	touch := func(op, key, txn string, at time.Time, gtid string, pos int64) {
		agg.Consume(model.NormalizedEvent{
			Timestamp:    at,
			Schema:       "shop",
			Table:        "counters",
			Operation:    op,
			KeyStatus:    model.KeyStatusHasPK,
			RowKeys:      []string{key},
			TxnKey:       txn,
			TxnGTID:      gtid,
			TxnStartPath: "mysql-bin.000001",
			TxnStartPos:  pos,
		})
	}
	touch("UPDATE", "id=7", "t1", base, "sid:1", 100)
	touch("UPDATE", "id=7", "t1", base, "sid:1", 100)
	touch("UPDATE", "id=7", "t2", base.Add(time.Second), "sid:2", 200)
	touch("INSERT", "id=4", "t3", base.Add(2*time.Second), "sid:3", 300)
	touch("DELETE", "id=9", "t4", base.Add(3*time.Second), "sid:4", 400)
	agg.Consume(model.NormalizedEvent{
		Schema: "shop", Table: "heap", Operation: "UPDATE",
		KeyStatus: model.KeyStatusNoPK, RowKeys: []string{"id=1"}, RowCount: 12,
	})
	agg.Consume(model.NormalizedEvent{
		Schema: "shop", Table: "orders", Operation: "UPDATE",
		KeyStatus: model.KeyStatusUnknown, RowKeys: []string{"@1=1"},
	})

	report := agg.Snapshot()
	if len(report.Rows) != 2 || report.Rows[0].PrimaryKey != "id=7" || report.Rows[0].Touches != 3 || report.Rows[0].Transactions != 2 {
		t.Fatalf("ranking=%+v", report.Rows)
	}
	if report.Rows[0].FirstGTID != "sid:1" || report.Rows[0].FirstPos != 100 || report.Rows[0].LastGTID != "sid:2" || report.Rows[0].LastPos != 200 {
		t.Fatalf("span=%+v", report.Rows[0])
	}
	if report.Rows[0].FirstTime != base || report.Rows[0].LastTime != base.Add(time.Second) {
		t.Fatalf("times=%s %s", report.Rows[0].FirstTime, report.Rows[0].LastTime)
	}
	if report.Rows[1].PrimaryKey != "id=9" || report.Rows[1].Touches != 1 {
		t.Fatalf("delete=%+v", report.Rows[1])
	}
	if len(report.Unavailable) != 1 || report.Unavailable[0].Table != "orders" || report.Unavailable[0].Reason != model.HotRowReasonMetadata {
		t.Fatalf("gaps=%+v", report.Unavailable)
	}
	for _, row := range report.Rows {
		if row.Table == "heap" || row.PrimaryKey == "@1=1" || row.PrimaryKey == "id=4" {
			t.Fatalf("ranked a non-key or insert: %+v", row)
		}
	}
}

func TestHotRowTrackLimitStaysBounded(t *testing.T) {
	const limit = 2
	agg := newHotRowAggregator(limit)
	base := time.Date(2026, 10, 6, 14, 0, 0, 0, time.UTC)
	for i := 0; i < 10; i++ {
		agg.Consume(hotEvent("id=7", fmt.Sprintf("hot-%d", i), base.Add(time.Duration(i)*time.Second), int64(1000+i)))
	}
	for i := 0; i < 3; i++ {
		agg.Consume(hotEvent(fmt.Sprintf("id=%d", 100+i), fmt.Sprintf("cold-%d", i), base, int64(2000+i)))
	}
	report := agg.Snapshot()
	if !report.Overflow || report.TrackLimit != limit || len(report.Rows) != limit {
		t.Fatalf("bound=%+v rows=%d", report.Overflow, len(report.Rows))
	}
	if report.Rows[0].PrimaryKey != "id=7" || report.Rows[0].Touches != 10 || report.Rows[0].Approximate || report.Rows[0].Transactions != 10 {
		t.Fatalf("exact hot row=%+v", report.Rows[0])
	}
	foundApprox := false
	for _, row := range report.Rows {
		if row.Approximate {
			foundApprox = true
		}
	}
	if !foundApprox {
		t.Fatal("expected the replaced key to be marked approximate")
	}
}

func hotEvent(pk, txn string, at time.Time, pos int64) model.NormalizedEvent {
	return model.NormalizedEvent{
		Timestamp:    at,
		Schema:       "shop",
		Table:        "counters",
		Operation:    "UPDATE",
		KeyStatus:    model.KeyStatusHasPK,
		RowKeys:      []string{pk},
		TxnKey:       txn,
		TxnGTID:      txn,
		TxnStartPath: "mysql-bin.000001",
		TxnStartPos:  pos,
	}
}
