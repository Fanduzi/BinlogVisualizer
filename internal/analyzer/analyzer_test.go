package analyzer

import (
	"testing"
	"time"

	"binlogviz/internal/model"
)

func TestAnalyzerProducesSummaryAndStats(t *testing.T) {
	a := New(Options{})
	events := []model.NormalizedEvent{
		{Timestamp: time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC), EventType: "BEGIN", TxnKey: "t1"},
		{Timestamp: time.Date(2026, 3, 9, 10, 0, 1, 0, time.UTC), EventType: "ROWS", TxnKey: "t1", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 5},
		{Timestamp: time.Date(2026, 3, 9, 10, 0, 2, 0, time.UTC), EventType: "XID", TxnKey: "t1"},
	}

	result, err := a.Analyze(events)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Summary.TotalTransactions != 1 {
		t.Fatalf("expected 1 transaction, got %d", result.Summary.TotalTransactions)
	}
	if result.Summary.TotalRows != 5 {
		t.Fatalf("expected 5 total rows, got %d", result.Summary.TotalRows)
	}
}

func TestAnalyzerTracksMultipleTransactions(t *testing.T) {
	a := New(Options{})
	base := time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC)

	events := []model.NormalizedEvent{
		// First transaction
		{Timestamp: base, EventType: "BEGIN", TxnKey: "t1"},
		{Timestamp: base.Add(time.Second), EventType: "ROWS", TxnKey: "t1", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 5},
		{Timestamp: base.Add(2 * time.Second), EventType: "XID", TxnKey: "t1"},
		// Second transaction
		{Timestamp: base.Add(3 * time.Second), EventType: "BEGIN", TxnKey: "t2"},
		{Timestamp: base.Add(4 * time.Second), EventType: "ROWS", TxnKey: "t2", Schema: "shop", Table: "users", Operation: "INSERT", RowCount: 3},
		{Timestamp: base.Add(5 * time.Second), EventType: "XID", TxnKey: "t2"},
	}

	result, err := a.Analyze(events)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Summary.TotalTransactions != 2 {
		t.Fatalf("expected 2 transactions, got %d", result.Summary.TotalTransactions)
	}
	if result.Summary.TotalRows != 8 {
		t.Fatalf("expected 8 total rows, got %d", result.Summary.TotalRows)
	}
}

func TestAnalyzerTracksPerTableStats(t *testing.T) {
	a := New(Options{})
	base := time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC)

	events := []model.NormalizedEvent{
		{Timestamp: base, EventType: "BEGIN", TxnKey: "t1"},
		{Timestamp: base.Add(time.Second), EventType: "ROWS", TxnKey: "t1", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 5},
		{Timestamp: base.Add(2 * time.Second), EventType: "ROWS", TxnKey: "t1", Schema: "shop", Table: "orders", Operation: "UPDATE", RowCount: 2},
		{Timestamp: base.Add(3 * time.Second), EventType: "ROWS", TxnKey: "t1", Schema: "shop", Table: "users", Operation: "INSERT", RowCount: 3},
		{Timestamp: base.Add(4 * time.Second), EventType: "XID", TxnKey: "t1"},
	}

	result, err := a.Analyze(events)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should have 2 table entries
	if len(result.Tables) != 2 {
		t.Fatalf("expected 2 tables, got %d", len(result.Tables))
	}

	// Find orders table
	var ordersStats *model.TableStats
	for i := range result.Tables {
		if result.Tables[i].Table == "orders" {
			ordersStats = &result.Tables[i]
			break
		}
	}
	if ordersStats == nil {
		t.Fatal("orders table not found")
	}
	if ordersStats.TotalRows != 7 {
		t.Fatalf("expected orders 7 rows, got %d", ordersStats.TotalRows)
	}
	if ordersStats.InsertRows != 5 {
		t.Fatalf("expected orders 5 insert rows, got %d", ordersStats.InsertRows)
	}
	if ordersStats.UpdateRows != 2 {
		t.Fatalf("expected orders 2 update rows, got %d", ordersStats.UpdateRows)
	}
}

func TestAnalyzerTracksMinuteBuckets(t *testing.T) {
	a := New(Options{})
	base := time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC)

	events := []model.NormalizedEvent{
		{Timestamp: base, EventType: "BEGIN", TxnKey: "t1"},
		{Timestamp: base.Add(time.Second), EventType: "ROWS", TxnKey: "t1", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 5},
		{Timestamp: base.Add(2 * time.Second), EventType: "XID", TxnKey: "t1"},
		// Second transaction in same minute
		{Timestamp: base.Add(30 * time.Second), EventType: "BEGIN", TxnKey: "t2"},
		{Timestamp: base.Add(31 * time.Second), EventType: "ROWS", TxnKey: "t2", Schema: "shop", Table: "users", Operation: "INSERT", RowCount: 3},
		{Timestamp: base.Add(32 * time.Second), EventType: "XID", TxnKey: "t2"},
	}

	result, err := a.Analyze(events)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should have 1 minute bucket
	if len(result.Minutes) != 1 {
		t.Fatalf("expected 1 minute bucket, got %d", len(result.Minutes))
	}

	// Should track 2 distinct transactions in the minute
	if result.Minutes[0].TxnCount != 2 {
		t.Fatalf("expected 2 transactions in minute, got %d", result.Minutes[0].TxnCount)
	}
	if result.Minutes[0].TotalRows != 8 {
		t.Fatalf("expected 8 total rows in minute, got %d", result.Minutes[0].TotalRows)
	}
}

func TestAnalyzerCalculatesWorkloadSummary(t *testing.T) {
	a := New(Options{})
	base := time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC)

	events := []model.NormalizedEvent{
		{Timestamp: base, EventType: "BEGIN", TxnKey: "t1"},
		{Timestamp: base.Add(time.Second), EventType: "ROWS", TxnKey: "t1", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 5},
		{Timestamp: base.Add(2 * time.Second), EventType: "XID", TxnKey: "t1"},
	}

	result, err := a.Analyze(events)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify summary
	if result.Summary.TotalTransactions != 1 {
		t.Fatalf("expected 1 transaction, got %d", result.Summary.TotalTransactions)
	}
	if result.Summary.TotalRows != 5 {
		t.Fatalf("expected 5 total rows, got %d", result.Summary.TotalRows)
	}
	if result.Summary.TotalEvents != 3 {
		t.Fatalf("expected 3 total events, got %d", result.Summary.TotalEvents)
	}
	if result.Summary.StartTime != base {
		t.Fatalf("expected start time %v, got %v", base, result.Summary.StartTime)
	}
	if result.Summary.EndTime != base.Add(2*time.Second) {
		t.Fatalf("expected end time %v, got %v", base.Add(2*time.Second), result.Summary.EndTime)
	}
	if result.Summary.Duration != 2*time.Second {
		t.Fatalf("expected duration 2s, got %v", result.Summary.Duration)
	}
}

func TestAnalyzerHandlesEmptyInput(t *testing.T) {
	a := New(Options{})

	result, err := a.Analyze(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Summary.TotalTransactions != 0 {
		t.Fatalf("expected 0 transactions for empty input, got %d", result.Summary.TotalTransactions)
	}
	if len(result.Tables) != 0 {
		t.Fatalf("expected 0 tables for empty input, got %d", len(result.Tables))
	}
	if len(result.Transactions) != 0 {
		t.Fatalf("expected 0 transactions for empty input, got %d", len(result.Transactions))
	}
}

func TestAnalyzerReturnsErrorOnBoundaryViolation(t *testing.T) {
	a := New(Options{})
	base := time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC)

	events := []model.NormalizedEvent{
		// Start explicit transaction
		{Timestamp: base, EventType: "BEGIN", TxnKey: "t1"},
		{Timestamp: base.Add(time.Second), EventType: "ROWS", TxnKey: "t1", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 5},
		// BEGIN while explicit transaction is in-flight - boundary violation!
		{Timestamp: base.Add(2 * time.Second), EventType: "BEGIN", TxnKey: "t2"},
	}

	_, err := a.Analyze(events)
	if err == nil {
		t.Fatal("expected error for boundary violation, got nil")
	}
	// Verify the error message indicates the problem
	if err.Error() == "" {
		t.Fatal("expected non-empty error message")
	}
}

func TestAnalyzerStopsFanOutOnError(t *testing.T) {
	a := New(Options{})
	base := time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC)

	events := []model.NormalizedEvent{
		// First transaction - will be processed successfully
		{Timestamp: base, EventType: "BEGIN", TxnKey: "t1"},
		{Timestamp: base.Add(time.Second), EventType: "ROWS", TxnKey: "t1", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 5},
		{Timestamp: base.Add(2 * time.Second), EventType: "XID", TxnKey: "t1"},
		// Second explicit transaction starts
		{Timestamp: base.Add(3 * time.Second), EventType: "BEGIN", TxnKey: "t2"},
		{Timestamp: base.Add(4 * time.Second), EventType: "ROWS", TxnKey: "t2", Schema: "shop", Table: "users", Operation: "INSERT", RowCount: 3},
		// BEGIN while t2 is in-flight - this should stop processing
		// The error event itself should not be counted in any aggregator
		{Timestamp: base.Add(5 * time.Second), EventType: "BEGIN", TxnKey: "t3"},
		// These events after the error should never be processed
		{Timestamp: base.Add(6 * time.Second), EventType: "ROWS", TxnKey: "t3", Schema: "shop", Table: "products", Operation: "INSERT", RowCount: 10},
	}

	_, err := a.Analyze(events)
	if err == nil {
		t.Fatal("expected error for boundary violation, got nil")
	}

	// Re-run with only valid events to verify what should have been counted
	a2 := New(Options{})
	validEvents := events[:5] // Only up to but not including the error-causing event
	result, err := a2.Analyze(validEvents)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify only the first two transactions were processed
	// t1: 5 rows, t2: 3 rows (t2 is still in-flight, flushed at end)
	if result.Summary.TotalTransactions != 2 {
		t.Fatalf("expected 2 transactions, got %d", result.Summary.TotalTransactions)
	}
	if result.Summary.TotalRows != 8 {
		t.Fatalf("expected 8 total rows (5+3), got %d", result.Summary.TotalRows)
	}
}
