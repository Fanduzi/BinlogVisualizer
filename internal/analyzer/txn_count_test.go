package analyzer

import (
	"testing"
	"time"

	"binlogviz/internal/model"
)

// TestTxnCountsWithoutKeySets pins the per-table and per-minute transaction
// counts after #364 replaced their key sets with a count and the last key.
// The analyzer hands a transaction's row events over contiguously, so
// several events of one transaction count once and each new key counts again.
func TestTxnCountsWithoutKeySets(t *testing.T) {
	ts := time.Date(2026, 10, 20, 10, 0, 0, 0, time.UTC)
	row := func(txn, table string, at time.Time) model.NormalizedEvent {
		return model.NormalizedEvent{Timestamp: at, EventType: "ROWS", Operation: "UPDATE", Schema: "shop", Table: table, RowCount: 1, TxnKey: txn}
	}
	events := []model.NormalizedEvent{
		row("txn-1", "a", ts), row("txn-1", "a", ts), row("txn-1", "b", ts),
		row("txn-2", "a", ts.Add(time.Second)),
		row("", "a", ts.Add(2*time.Second)),
		row("txn-3", "b", ts.Add(time.Minute)), row("txn-3", "a", ts.Add(time.Minute)), row("txn-3", "b", ts.Add(time.Minute)),
	}
	tables := NewTableAggregator()
	minutes := NewMinuteAggregator()
	for _, ev := range events {
		tables.Consume(ev)
		minutes.Consume(ev)
	}
	got := map[string]int{}
	for _, s := range tables.Snapshot() {
		got[s.Table] = s.TxnCount
	}
	if got["a"] != 3 || got["b"] != 2 {
		t.Fatalf("table txn counts %v, want a=3 b=2", got)
	}
	buckets := minutes.Snapshot()
	if len(buckets) != 2 || buckets[0].TxnCount != 2 || buckets[1].TxnCount != 1 {
		t.Fatalf("minute txn counts %+v, want 2 then 1", buckets)
	}
}
