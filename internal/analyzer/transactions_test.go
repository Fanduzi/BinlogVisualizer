// Package analyzer verifies transaction reconstruction behavior from normalized events.
// input: synthetic normalized events including begin, rows, and commit boundaries.
// output: assertions for completed transaction metadata, row totals, and table maps.
// pos: focused regression coverage for analyzer transaction assembly helpers.
// note: if this file changes, keep internal/analyzer/README.md synchronized.
package analyzer

import (
	"testing"
	"time"

	"binlogviz/internal/model"
)

func TestTransactionBuilderAggregatesRowsAndTables(t *testing.T) {
	builder := NewTransactionBuilder()
	ts := time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC)

	events := []model.NormalizedEvent{
		{Timestamp: ts, EventType: "BEGIN"},
		{Timestamp: ts.Add(time.Second), EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 2},
		{Timestamp: ts.Add(2 * time.Second), EventType: "XID"},
	}

	for _, ev := range events {
		_ = builder.Consume(ev)
	}

	result := builder.Completed()
	if len(result) != 1 || result[0].TotalRows != 2 {
		t.Fatalf("unexpected transactions: %+v", result)
	}
}

func TestTransactionBuilderHandlesImplicitTransactions(t *testing.T) {
	builder := NewTransactionBuilder()
	ts := time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC)

	// Row event without explicit BEGIN - should create implicit transaction
	events := []model.NormalizedEvent{
		{Timestamp: ts, EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 3},
	}

	for _, ev := range events {
		_ = builder.Consume(ev)
	}

	// Implicit transaction should be completed on next event or flush
	builder.Flush()
	result := builder.Completed()
	if len(result) != 1 || result[0].TotalRows != 3 {
		t.Fatalf("unexpected transactions: %+v", result)
	}
}

func TestTransactionBuilderTracksTablesAndOperations(t *testing.T) {
	builder := NewTransactionBuilder()
	ts := time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC)

	events := []model.NormalizedEvent{
		{Timestamp: ts, EventType: "BEGIN"},
		{Timestamp: ts.Add(time.Second), EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 2},
		{Timestamp: ts.Add(2 * time.Second), EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "UPDATE", RowCount: 1},
		{Timestamp: ts.Add(3 * time.Second), EventType: "ROWS", Schema: "shop", Table: "users", Operation: "INSERT", RowCount: 5},
		{Timestamp: ts.Add(4 * time.Second), EventType: "XID"},
	}

	for _, ev := range events {
		_ = builder.Consume(ev)
	}

	result := builder.Completed()
	if len(result) != 1 {
		t.Fatalf("expected 1 transaction, got %d", len(result))
	}

	trx := result[0]
	if trx.TotalRows != 8 {
		t.Fatalf("expected 8 total rows, got %d", trx.TotalRows)
	}
	if trx.EventCount != 3 {
		t.Fatalf("expected 3 events, got %d", trx.EventCount)
	}
	if len(trx.Tables) != 2 {
		t.Fatalf("expected 2 tables, got %d", len(trx.Tables))
	}
	if trx.Tables["shop.orders"] != 3 {
		t.Fatalf("expected 3 rows for shop.orders, got %d", trx.Tables["shop.orders"])
	}
	if trx.Operations["INSERT"] != 7 {
		t.Fatalf("expected 7 INSERT rows, got %d", trx.Operations["INSERT"])
	}
}

func TestTransactionBuilderCalculatesDuration(t *testing.T) {
	builder := NewTransactionBuilder()
	ts := time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC)

	events := []model.NormalizedEvent{
		{Timestamp: ts, EventType: "BEGIN"},
		{Timestamp: ts.Add(5 * time.Second), EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 1},
		{Timestamp: ts.Add(10 * time.Second), EventType: "XID"},
	}

	for _, ev := range events {
		_ = builder.Consume(ev)
	}

	result := builder.Completed()
	if len(result) != 1 {
		t.Fatalf("expected 1 transaction, got %d", len(result))
	}

	trx := result[0]
	expectedDuration := 10 * time.Second
	if trx.Duration != expectedDuration {
		t.Fatalf("expected duration %v, got %v", expectedDuration, trx.Duration)
	}
	if trx.StartTime != ts {
		t.Fatalf("expected start time %v, got %v", ts, trx.StartTime)
	}
	if trx.EndTime != ts.Add(10*time.Second) {
		t.Fatalf("expected end time %v, got %v", ts.Add(10*time.Second), trx.EndTime)
	}
}

func TestTransactionBuilderHandlesMultipleTransactions(t *testing.T) {
	builder := NewTransactionBuilder()
	ts := time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC)

	events := []model.NormalizedEvent{
		{Timestamp: ts, EventType: "BEGIN"},
		{Timestamp: ts.Add(time.Second), EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 2},
		{Timestamp: ts.Add(2 * time.Second), EventType: "XID"},
		{Timestamp: ts.Add(3 * time.Second), EventType: "BEGIN"},
		{Timestamp: ts.Add(4 * time.Second), EventType: "ROWS", Schema: "shop", Table: "users", Operation: "INSERT", RowCount: 3},
		{Timestamp: ts.Add(5 * time.Second), EventType: "COMMIT"},
	}

	for _, ev := range events {
		_ = builder.Consume(ev)
	}

	result := builder.Completed()
	if len(result) != 2 {
		t.Fatalf("expected 2 transactions, got %d", len(result))
	}
	if result[0].TotalRows != 2 {
		t.Fatalf("expected first transaction with 2 rows, got %d", result[0].TotalRows)
	}
	if result[1].TotalRows != 3 {
		t.Fatalf("expected second transaction with 3 rows, got %d", result[1].TotalRows)
	}
}

func TestTransactionBuilderHandlesCOMMIT(t *testing.T) {
	builder := NewTransactionBuilder()
	ts := time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC)

	events := []model.NormalizedEvent{
		{Timestamp: ts, EventType: "BEGIN"},
		{Timestamp: ts.Add(time.Second), EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 4},
		{Timestamp: ts.Add(2 * time.Second), EventType: "COMMIT"},
	}

	for _, ev := range events {
		_ = builder.Consume(ev)
	}

	result := builder.Completed()
	if len(result) != 1 || result[0].TotalRows != 4 {
		t.Fatalf("unexpected transactions: %+v", result)
	}
}

func TestImplicitTransactionEndTimePreservedOnBegin(t *testing.T) {
	builder := NewTransactionBuilder()
	ts := time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC)

	// Implicit transaction: row event at t+5s
	// Then explicit BEGIN at t+10s should NOT overwrite implicit transaction's end time
	events := []model.NormalizedEvent{
		{Timestamp: ts.Add(5 * time.Second), EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 2},
		{Timestamp: ts.Add(10 * time.Second), EventType: "BEGIN"},
		{Timestamp: ts.Add(11 * time.Second), EventType: "ROWS", Schema: "shop", Table: "users", Operation: "INSERT", RowCount: 3},
		{Timestamp: ts.Add(12 * time.Second), EventType: "XID"},
	}

	for _, ev := range events {
		_ = builder.Consume(ev)
	}

	result := builder.Completed()
	if len(result) != 2 {
		t.Fatalf("expected 2 transactions, got %d", len(result))
	}

	// First transaction (implicit) should end at t+5s, not t+10s
	implicit := result[0]
	expectedEnd := ts.Add(5 * time.Second)
	if implicit.EndTime != expectedEnd {
		t.Fatalf("expected implicit transaction end time %v, got %v", expectedEnd, implicit.EndTime)
	}
	if implicit.Duration != 0 {
		t.Fatalf("expected implicit transaction duration 0 (single event), got %v", implicit.Duration)
	}

	// Second transaction (explicit) should end at t+12s
	explicit := result[1]
	expectedEndExplicit := ts.Add(12 * time.Second)
	if explicit.EndTime != expectedEndExplicit {
		t.Fatalf("expected explicit transaction end time %v, got %v", expectedEndExplicit, explicit.EndTime)
	}
}

func TestExplicitBeginWhileExplicitTransactionInFlightReturnsError(t *testing.T) {
	builder := NewTransactionBuilder()
	ts := time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC)

	// First explicit transaction
	_ = builder.Consume(model.NormalizedEvent{Timestamp: ts, EventType: "BEGIN"})
	_ = builder.Consume(model.NormalizedEvent{Timestamp: ts.Add(time.Second), EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 1})

	// Second BEGIN without COMMIT - should return error
	err := builder.Consume(model.NormalizedEvent{Timestamp: ts.Add(2 * time.Second), EventType: "BEGIN"})
	if err == nil {
		t.Fatal("expected error for BEGIN while explicit transaction in-flight")
	}

	// The in-flight transaction should NOT be silently completed
	result := builder.Completed()
	if len(result) != 0 {
		t.Fatalf("expected 0 completed transactions (state unchanged after error), got %d", len(result))
	}
}

func TestExplicitBeginErrorDoesNotMutateState(t *testing.T) {
	builder := NewTransactionBuilder()
	ts := time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC)

	// First explicit transaction with rows
	_ = builder.Consume(model.NormalizedEvent{Timestamp: ts, EventType: "BEGIN"})
	_ = builder.Consume(model.NormalizedEvent{Timestamp: ts.Add(time.Second), EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 5})

	// Second BEGIN - should return error
	err := builder.Consume(model.NormalizedEvent{Timestamp: ts.Add(2 * time.Second), EventType: "BEGIN"})
	if err == nil {
		t.Fatal("expected error for BEGIN while explicit transaction in-flight")
	}

	// After error, caller can choose to Flush the in-flight transaction
	builder.Flush()

	// Now the transaction should be completed
	result := builder.Completed()
	if len(result) != 1 {
		t.Fatalf("expected 1 completed transaction after Flush, got %d", len(result))
	}

	// The transaction should have the correct row count
	if result[0].TotalRows != 5 {
		t.Fatalf("expected 5 total rows, got %d", result[0].TotalRows)
	}
}

func TestExplicitBeginAfterImplicitTransactionIsOk(t *testing.T) {
	builder := NewTransactionBuilder()
	ts := time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC)

	// Implicit transaction
	_ = builder.Consume(model.NormalizedEvent{Timestamp: ts, EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 2})

	// BEGIN after implicit transaction should NOT return error
	err := builder.Consume(model.NormalizedEvent{Timestamp: ts.Add(5 * time.Second), EventType: "BEGIN"})
	if err != nil {
		t.Fatalf("unexpected error for BEGIN after implicit transaction: %v", err)
	}

	// Both transactions should be tracked
	_ = builder.Consume(model.NormalizedEvent{Timestamp: ts.Add(6 * time.Second), EventType: "ROWS", Schema: "shop", Table: "users", Operation: "INSERT", RowCount: 1})
	_ = builder.Consume(model.NormalizedEvent{Timestamp: ts.Add(7 * time.Second), EventType: "XID"})

	result := builder.Completed()
	if len(result) != 2 {
		t.Fatalf("expected 2 transactions, got %d", len(result))
	}
}

func TestTransactionBuilderTracksBinlogCoverageAcrossTransaction(t *testing.T) {
	builder := NewTransactionBuilder()
	ts := time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC)

	events := []model.NormalizedEvent{
		{
			Timestamp:     ts,
			EventType:     "BEGIN",
			BinlogPath:    "mysql-bin.000001",
			PositionStart: 120,
			PositionEnd:   140,
			BinlogBytes:   20,
		},
		{
			Timestamp:          ts.Add(time.Second),
			EventType:          "ROWS_QUERY",
			BinlogPath:         "mysql-bin.000001",
			PositionStart:      140,
			PositionEnd:        200,
			BinlogBytes:        60,
			QuerySQL:           "UPDATE orders SET status='done' WHERE id IN (1,2,3)",
			QueryOriginalBytes: 48,
		},
		{
			Timestamp:     ts.Add(2 * time.Second),
			EventType:     "ROWS",
			Schema:        "shop",
			Table:         "orders",
			Operation:     "UPDATE",
			RowCount:      3,
			BinlogPath:    "mysql-bin.000001",
			PositionStart: 200,
			PositionEnd:   260,
			BinlogBytes:   60,
		},
		{
			Timestamp:     ts.Add(3 * time.Second),
			EventType:     "XID",
			BinlogPath:    "mysql-bin.000002",
			PositionStart: 4,
			PositionEnd:   16,
			BinlogBytes:   12,
		},
	}

	for _, ev := range events {
		if err := builder.Consume(ev); err != nil {
			t.Fatalf("consume failed: %v", err)
		}
	}

	result := builder.Completed()
	if len(result) != 1 {
		t.Fatalf("expected 1 transaction, got %d", len(result))
	}

	txn := result[0]
	if txn.BinlogBytes != 152 {
		t.Fatalf("expected 152 binlog bytes, got %d", txn.BinlogBytes)
	}
	if txn.BinlogPathStart != "mysql-bin.000001" {
		t.Fatalf("expected start binlog mysql-bin.000001, got %q", txn.BinlogPathStart)
	}
	if txn.BinlogPathEnd != "mysql-bin.000002" {
		t.Fatalf("expected end binlog mysql-bin.000002, got %q", txn.BinlogPathEnd)
	}
	if txn.PositionStart != 120 {
		t.Fatalf("expected start position 120, got %d", txn.PositionStart)
	}
	if txn.PositionEnd != 16 {
		t.Fatalf("expected end position 16, got %d", txn.PositionEnd)
	}
}

func TestTransactionBuilderUsesFirstAndLastAvailableBinlogMetadata(t *testing.T) {
	builder := NewTransactionBuilder()
	ts := time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC)

	events := []model.NormalizedEvent{
		{Timestamp: ts, EventType: "BEGIN"},
		{
			Timestamp:     ts.Add(time.Second),
			EventType:     "ROWS",
			Schema:        "shop",
			Table:         "orders",
			Operation:     "INSERT",
			RowCount:      2,
			BinlogPath:    "mysql-bin.000010",
			PositionStart: 500,
			PositionEnd:   580,
			BinlogBytes:   80,
		},
		{Timestamp: ts.Add(2 * time.Second), EventType: "COMMIT"},
	}

	for _, ev := range events {
		if err := builder.Consume(ev); err != nil {
			t.Fatalf("consume failed: %v", err)
		}
	}

	result := builder.Completed()
	if len(result) != 1 {
		t.Fatalf("expected 1 transaction, got %d", len(result))
	}

	txn := result[0]
	if txn.BinlogPathStart != "mysql-bin.000010" {
		t.Fatalf("expected start binlog mysql-bin.000010, got %q", txn.BinlogPathStart)
	}
	if txn.BinlogPathEnd != "mysql-bin.000010" {
		t.Fatalf("expected end binlog mysql-bin.000010, got %q", txn.BinlogPathEnd)
	}
	if txn.PositionStart != 500 {
		t.Fatalf("expected start position 500, got %d", txn.PositionStart)
	}
	if txn.PositionEnd != 580 {
		t.Fatalf("expected end position 580, got %d", txn.PositionEnd)
	}
	if txn.BinlogBytes != 80 {
		t.Fatalf("expected 80 binlog bytes, got %d", txn.BinlogBytes)
	}
}

func TestTransactionBuilderUsesTxnSpanForSameFileBinlogBytes(t *testing.T) {
	builder := NewTransactionBuilder()
	ts := time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC)

	events := []model.NormalizedEvent{
		{Timestamp: ts, EventType: "BEGIN", BinlogPath: "mysql-bin.000008", PositionStart: 385, PositionEnd: 500, BinlogBytes: 115},
		{Timestamp: ts, EventType: "TABLE_MAP", BinlogPath: "mysql-bin.000008", PositionStart: 500, PositionEnd: 625, BinlogBytes: 125},
		{
			Timestamp:     ts.Add(time.Second),
			EventType:     "ROWS",
			Schema:        "dogfood_big",
			Table:         "t",
			Operation:     "INSERT",
			RowCount:      400000,
			BinlogPath:    "mysql-bin.000008",
			PositionStart: 625,
			PositionEnd:   77914917,
			BinlogBytes:   77914292,
		},
		{Timestamp: ts.Add(2 * time.Second), EventType: "XID", BinlogPath: "mysql-bin.000008", PositionStart: 77914917, PositionEnd: 77914948, BinlogBytes: 31},
	}
	for _, ev := range events {
		if err := builder.Consume(ev); err != nil {
			t.Fatalf("consume: %v", err)
		}
	}

	txns := builder.Completed()
	if len(txns) != 1 {
		t.Fatalf("expected 1 txn, got %d", len(txns))
	}
	txn := txns[0]
	if txn.PositionStart != 385 {
		t.Fatalf("pos_start=%d, want first event 385", txn.PositionStart)
	}
	if txn.PositionEnd != 77914948 {
		t.Fatalf("pos_end=%d, want XID end 77914948", txn.PositionEnd)
	}
	wantBytes := int64(77914948 - 385)
	if txn.BinlogBytes != wantBytes {
		t.Fatalf("binlog_bytes=%d, want pos_end-pos_start %d", txn.BinlogBytes, wantBytes)
	}
	if txn.TotalRows != 400000 {
		t.Fatalf("rows=%d", txn.TotalRows)
	}
}

func TestTransactionBuilderDoesNotInventStartWhenOnlyXIDIsKnown(t *testing.T) {
	builder := NewTransactionBuilder()
	ts := time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC)

	events := []model.NormalizedEvent{
		{Timestamp: ts, EventType: "BEGIN"},
		{Timestamp: ts, EventType: "ROWS", Schema: "dogfood_big", Table: "t", Operation: "INSERT", RowCount: 400000},
		{Timestamp: ts, EventType: "XID", BinlogPath: "mysql-bin.000008", PositionStart: 77914917, PositionEnd: 77914948, BinlogBytes: 31},
	}
	for _, ev := range events {
		if err := builder.Consume(ev); err != nil {
			t.Fatalf("consume: %v", err)
		}
	}

	txn := builder.Completed()[0]
	if txn.PositionStart != 77914917 || txn.PositionEnd != 77914948 {
		t.Fatalf("XID-only span should stay honest, got %d-%d", txn.PositionStart, txn.PositionEnd)
	}
	if txn.PositionStart == 385 {
		t.Fatal("must not invent start position 385")
	}
	if txn.BinlogBytes != 31 {
		t.Fatalf("XID-only binlog_bytes=%d, want 31", txn.BinlogBytes)
	}
}

func BenchmarkTransactionBuilderConsumeRows(b *testing.B) {
	ts := time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC)
	events := []model.NormalizedEvent{
		{Timestamp: ts, EventType: "BEGIN"},
		{Timestamp: ts.Add(time.Second), EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 2},
		{Timestamp: ts.Add(2 * time.Second), EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "UPDATE", RowCount: 1},
		{Timestamp: ts.Add(3 * time.Second), EventType: "ROWS", Schema: "shop", Table: "users", Operation: "DELETE", RowCount: 5},
		{Timestamp: ts.Add(4 * time.Second), EventType: "XID"},
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		builder := NewTransactionBuilder()
		for _, ev := range events {
			if err := builder.Consume(ev); err != nil {
				b.Fatalf("consume failed: %v", err)
			}
		}
		if len(builder.Completed()) != 1 {
			b.Fatalf("expected completed transaction")
		}
	}
}
