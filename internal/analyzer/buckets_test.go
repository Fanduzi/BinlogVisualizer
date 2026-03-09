package analyzer

import (
	"testing"
	"time"

	"binlogviz/internal/model"
)

func TestMinuteAggregatorBucketsRowsByMinute(t *testing.T) {
	agg := NewMinuteAggregator()
	base := time.Date(2026, 3, 9, 10, 2, 10, 0, time.UTC)

	// Two events in the same minute (10:02)
	agg.Consume(model.NormalizedEvent{Timestamp: base, Schema: "shop", Table: "orders", RowCount: 2, TxnKey: "t1"})
	agg.Consume(model.NormalizedEvent{Timestamp: base.Add(30 * time.Second), Schema: "shop", Table: "orders", RowCount: 3, TxnKey: "t2"})

	buckets := agg.Snapshot()
	if len(buckets) != 1 {
		t.Fatalf("expected 1 bucket, got %d", len(buckets))
	}

	// Both events should be in the same minute bucket
	expectedMinute := time.Date(2026, 3, 9, 10, 2, 0, 0, time.UTC)
	if buckets[0].Minute != expectedMinute {
		t.Fatalf("expected minute %v, got %v", expectedMinute, buckets[0].Minute)
	}
	if buckets[0].TotalRows != 5 {
		t.Fatalf("expected 5 total rows, got %d", buckets[0].TotalRows)
	}
	if buckets[0].TxnCount != 2 {
		t.Fatalf("expected 2 transactions, got %d", buckets[0].TxnCount)
	}
}

func TestMinuteAggregatorSeparatesDifferentMinutes(t *testing.T) {
	agg := NewMinuteAggregator()
	base := time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC)

	// Event at 10:00
	agg.Consume(model.NormalizedEvent{Timestamp: base, Schema: "shop", Table: "orders", RowCount: 5, TxnKey: "t1"})

	// Event at 10:01
	agg.Consume(model.NormalizedEvent{Timestamp: base.Add(time.Minute), Schema: "shop", Table: "orders", RowCount: 3, TxnKey: "t2"})

	buckets := agg.Snapshot()
	if len(buckets) != 2 {
		t.Fatalf("expected 2 buckets, got %d", len(buckets))
	}

	// First bucket: 10:00 with 5 rows
	if buckets[0].TotalRows != 5 {
		t.Fatalf("expected first bucket 5 rows, got %d", buckets[0].TotalRows)
	}

	// Second bucket: 10:01 with 3 rows
	if buckets[1].TotalRows != 3 {
		t.Fatalf("expected second bucket 3 rows, got %d", buckets[1].TotalRows)
	}
}

func TestMinuteAggregatorCountsDistinctTransactions(t *testing.T) {
	agg := NewMinuteAggregator()
	base := time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC)

	// Multiple events from same transaction
	agg.Consume(model.NormalizedEvent{Timestamp: base, Schema: "shop", Table: "orders", RowCount: 2, TxnKey: "t1"})
	agg.Consume(model.NormalizedEvent{Timestamp: base.Add(10 * time.Second), Schema: "shop", Table: "users", RowCount: 1, TxnKey: "t1"})

	// Event from different transaction
	agg.Consume(model.NormalizedEvent{Timestamp: base.Add(20 * time.Second), Schema: "shop", Table: "orders", RowCount: 3, TxnKey: "t2"})

	buckets := agg.Snapshot()
	if len(buckets) != 1 {
		t.Fatalf("expected 1 bucket, got %d", len(buckets))
	}

	// Two distinct transactions in this minute
	if buckets[0].TxnCount != 2 {
		t.Fatalf("expected 2 distinct transactions, got %d", buckets[0].TxnCount)
	}
}

func TestMinuteAggregatorTracksPerTableRows(t *testing.T) {
	agg := NewMinuteAggregator()
	base := time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC)

	agg.Consume(model.NormalizedEvent{Timestamp: base, Schema: "shop", Table: "orders", RowCount: 5, TxnKey: "t1"})
	agg.Consume(model.NormalizedEvent{Timestamp: base.Add(10 * time.Second), Schema: "shop", Table: "users", RowCount: 3, TxnKey: "t1"})
	agg.Consume(model.NormalizedEvent{Timestamp: base.Add(20 * time.Second), Schema: "shop", Table: "orders", RowCount: 2, TxnKey: "t2"})

	buckets := agg.Snapshot()
	if len(buckets) != 1 {
		t.Fatalf("expected 1 bucket, got %d", len(buckets))
	}

	// orders: 5 + 2 = 7 rows
	if buckets[0].TableRows["shop.orders"] != 7 {
		t.Fatalf("expected shop.orders 7 rows, got %d", buckets[0].TableRows["shop.orders"])
	}

	// users: 3 rows
	if buckets[0].TableRows["shop.users"] != 3 {
		t.Fatalf("expected shop.users 3 rows, got %d", buckets[0].TableRows["shop.users"])
	}
}

func TestMinuteAggregatorSortsByTimeAscending(t *testing.T) {
	agg := NewMinuteAggregator()
	base := time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC)

	// Insert in non-chronological order
	agg.Consume(model.NormalizedEvent{Timestamp: base.Add(2 * time.Minute), Schema: "shop", Table: "orders", RowCount: 1, TxnKey: "t3"})
	agg.Consume(model.NormalizedEvent{Timestamp: base, Schema: "shop", Table: "orders", RowCount: 1, TxnKey: "t1"})
	agg.Consume(model.NormalizedEvent{Timestamp: base.Add(time.Minute), Schema: "shop", Table: "orders", RowCount: 1, TxnKey: "t2"})

	buckets := agg.Snapshot()
	if len(buckets) != 3 {
		t.Fatalf("expected 3 buckets, got %d", len(buckets))
	}

	// Should be sorted by time ascending
	expectedMinutes := []time.Time{
		base,
		base.Add(time.Minute),
		base.Add(2 * time.Minute),
	}

	for i, expected := range expectedMinutes {
		if buckets[i].Minute != expected {
			t.Fatalf("bucket %d: expected minute %v, got %v", i, expected, buckets[i].Minute)
		}
	}
}

func TestMinuteAggregatorIgnoresNonRowEvents(t *testing.T) {
	agg := NewMinuteAggregator()
	base := time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC)

	// Non-row events should be ignored
	agg.Consume(model.NormalizedEvent{Timestamp: base, EventType: "BEGIN", TxnKey: "t1"})
	agg.Consume(model.NormalizedEvent{Timestamp: base, EventType: "TABLE_MAP", Schema: "shop", Table: "orders", TxnKey: "t1"})
	agg.Consume(model.NormalizedEvent{Timestamp: base, EventType: "COMMIT", TxnKey: "t1"})

	buckets := agg.Snapshot()
	if len(buckets) != 0 {
		t.Fatalf("expected 0 buckets (non-row events ignored), got %d", len(buckets))
	}
}

func TestMinuteAggregatorIgnoresEventsWithoutSchemaOrTable(t *testing.T) {
	agg := NewMinuteAggregator()
	base := time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC)

	// Row events without schema/table should be ignored
	agg.Consume(model.NormalizedEvent{Timestamp: base, Operation: "INSERT", RowCount: 5, TxnKey: "t1"})
	agg.Consume(model.NormalizedEvent{Timestamp: base, Schema: "shop", Operation: "INSERT", RowCount: 3, TxnKey: "t2"})

	buckets := agg.Snapshot()
	if len(buckets) != 0 {
		t.Fatalf("expected 0 buckets (missing schema/table), got %d", len(buckets))
	}
}

func TestMinuteAggregatorReturnsDefensiveCopyOfTableRows(t *testing.T) {
	agg := NewMinuteAggregator()
	base := time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC)

	// Consume some events
	agg.Consume(model.NormalizedEvent{Timestamp: base, Schema: "shop", Table: "orders", RowCount: 5, TxnKey: "t1"})
	agg.Consume(model.NormalizedEvent{Timestamp: base, Schema: "shop", Table: "users", RowCount: 3, TxnKey: "t1"})

	// Get snapshot
	snapshot1 := agg.Snapshot()
	if len(snapshot1) != 1 {
		t.Fatalf("expected 1 bucket, got %d", len(snapshot1))
	}

	// Verify original values
	if snapshot1[0].TableRows["shop.orders"] != 5 {
		t.Fatalf("expected shop.orders 5 rows, got %d", snapshot1[0].TableRows["shop.orders"])
	}

	// MUTATE the returned snapshot's TableRows
	snapshot1[0].TableRows["shop.orders"] = 999
	snapshot1[0].TableRows["shop.malicious"] = 100

	// Get a fresh snapshot
	snapshot2 := agg.Snapshot()
	if len(snapshot2) != 1 {
		t.Fatalf("expected 1 bucket, got %d", len(snapshot2))
	}

	// Internal state should NOT be polluted by the mutation
	if snapshot2[0].TableRows["shop.orders"] != 5 {
		t.Fatalf("internal state polluted: expected shop.orders 5 rows, got %d", snapshot2[0].TableRows["shop.orders"])
	}

	// Malicious entry should not exist
	if _, exists := snapshot2[0].TableRows["shop.malicious"]; exists {
		t.Fatal("internal state polluted: shop.malicious should not exist")
	}

	// Consume more events after external mutation
	agg.Consume(model.NormalizedEvent{Timestamp: base, Schema: "shop", Table: "orders", RowCount: 2, TxnKey: "t2"})

	snapshot3 := agg.Snapshot()
	// Should be 5 + 2 = 7, not affected by the 999 we set earlier
	if snapshot3[0].TableRows["shop.orders"] != 7 {
		t.Fatalf("expected shop.orders 7 rows after additional event, got %d", snapshot3[0].TableRows["shop.orders"])
	}
}
