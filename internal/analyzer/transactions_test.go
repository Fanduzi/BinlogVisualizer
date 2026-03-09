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
