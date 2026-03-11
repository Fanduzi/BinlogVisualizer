package analyzer

import (
	"testing"
	"time"

	"binlogviz/internal/model"
)

func TestAnalyzerFiltersEventsOutsideWindow(t *testing.T) {
	start := time.Date(2026, 3, 9, 10, 0, 30, 0, time.UTC)
	end := time.Date(2026, 3, 9, 10, 1, 30, 0, time.UTC)

	a := New(Options{Start: &start, End: &end})
	events := []model.NormalizedEvent{
		// Before window - should be excluded
		{Timestamp: time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC), EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 5},
		// Inside window - should be included
		{Timestamp: time.Date(2026, 3, 9, 10, 1, 0, 0, time.UTC), EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 7},
		// After window - should be excluded
		{Timestamp: time.Date(2026, 3, 9, 10, 2, 0, 0, time.UTC), EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 3},
	}

	result, err := a.Analyze(events)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Summary.TotalRows != 7 {
		t.Fatalf("expected 7 total rows (only window events), got %d", result.Summary.TotalRows)
	}
	if result.Summary.TotalEvents != 1 {
		t.Fatalf("expected 1 total event (only window events), got %d", result.Summary.TotalEvents)
	}
}

func TestAnalyzerWindowStartOnly(t *testing.T) {
	start := time.Date(2026, 3, 9, 10, 1, 0, 0, time.UTC)

	a := New(Options{Start: &start})
	events := []model.NormalizedEvent{
		// Before start - should be excluded
		{Timestamp: time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC), EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 5},
		// At start (inclusive) - should be included
		{Timestamp: start, EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 7},
		// After start - should be included
		{Timestamp: time.Date(2026, 3, 9, 10, 2, 0, 0, time.UTC), EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 3},
	}

	result, err := a.Analyze(events)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Summary.TotalRows != 10 {
		t.Fatalf("expected 10 total rows (start and after), got %d", result.Summary.TotalRows)
	}
}

func TestAnalyzerWindowEndOnly(t *testing.T) {
	end := time.Date(2026, 3, 9, 10, 1, 0, 0, time.UTC)

	a := New(Options{End: &end})
	events := []model.NormalizedEvent{
		// Before end - should be included
		{Timestamp: time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC), EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 5},
		// At end (inclusive) - should be included
		{Timestamp: end, EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 7},
		// After end - should be excluded
		{Timestamp: time.Date(2026, 3, 9, 10, 2, 0, 0, time.UTC), EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 3},
	}

	result, err := a.Analyze(events)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Summary.TotalRows != 12 {
		t.Fatalf("expected 12 total rows (before and at end), got %d", result.Summary.TotalRows)
	}
}

func TestAnalyzerWindowInclusiveBoundaries(t *testing.T) {
	start := time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC)
	end := time.Date(2026, 3, 9, 10, 1, 0, 0, time.UTC)

	a := New(Options{Start: &start, End: &end})
	events := []model.NormalizedEvent{
		// Before start - excluded
		{Timestamp: start.Add(-1 * time.Second), EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 1},
		// Exactly at start - included (inclusive)
		{Timestamp: start, EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 2},
		// Exactly at end - included (inclusive)
		{Timestamp: end, EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 3},
		// After end - excluded
		{Timestamp: end.Add(1 * time.Second), EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 4},
	}

	result, err := a.Analyze(events)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Summary.TotalRows != 5 {
		t.Fatalf("expected 5 total rows (start + end inclusive), got %d", result.Summary.TotalRows)
	}
}

func TestAnalyzerWindowFiltersAllEvents(t *testing.T) {
	start := time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC)
	end := time.Date(2026, 3, 9, 13, 0, 0, 0, time.UTC)

	a := New(Options{Start: &start, End: &end})
	events := []model.NormalizedEvent{
		// All events are outside the window
		{Timestamp: time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC), EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 5},
		{Timestamp: time.Date(2026, 3, 9, 11, 0, 0, 0, time.UTC), EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 3},
	}

	result, err := a.Analyze(events)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Summary.TotalRows != 0 {
		t.Fatalf("expected 0 total rows (all filtered), got %d", result.Summary.TotalRows)
	}
	if result.Summary.TotalEvents != 0 {
		t.Fatalf("expected 0 total events (all filtered), got %d", result.Summary.TotalEvents)
	}
}

func TestAnalyzerWindowFiltersTransactionEvents(t *testing.T) {
	start := time.Date(2026, 3, 9, 10, 0, 30, 0, time.UTC)
	end := time.Date(2026, 3, 9, 10, 1, 30, 0, time.UTC)

	a := New(Options{Start: &start, End: &end})
	base := time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC)

	events := []model.NormalizedEvent{
		// Transaction 1: starts before window, has events inside window
		{Timestamp: base, EventType: "BEGIN", TxnKey: "t1"},
		{Timestamp: base.Add(15 * time.Second), EventType: "ROWS", TxnKey: "t1", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 5}, // before window
		{Timestamp: base.Add(45 * time.Second), EventType: "ROWS", TxnKey: "t1", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 3},  // inside window
		{Timestamp: base.Add(90 * time.Second), EventType: "XID", TxnKey: "t1"},                                                                   // inside window
	}

	result, err := a.Analyze(events)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Only events inside the window should be counted
	// BEGIN at 10:00:00 is before window - excluded
	// ROWS at 10:00:15 is before window (start is 10:00:30) - excluded
	// ROWS at 10:00:45 is inside window - included
	// XID at 10:01:30 is inside window (end is 10:01:30) - included
	if result.Summary.TotalRows != 3 {
		t.Fatalf("expected 3 total rows (only inside window), got %d", result.Summary.TotalRows)
	}
	if result.Summary.TotalEvents != 2 {
		t.Fatalf("expected 2 total events (inside window), got %d", result.Summary.TotalEvents)
	}

	// Transaction should only reflect the filtered events
	if len(result.Transactions) != 1 {
		t.Fatalf("expected 1 transaction, got %d", len(result.Transactions))
	}
	if result.Transactions[0].TotalRows != 3 {
		t.Fatalf("expected transaction with 3 rows (only window events), got %d", result.Transactions[0].TotalRows)
	}
}

func TestAnalyzerWindowNoFilteringWhenNil(t *testing.T) {
	a := New(Options{}) // No Start/End
	events := []model.NormalizedEvent{
		{Timestamp: time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC), EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 5},
		{Timestamp: time.Date(2026, 3, 9, 10, 1, 0, 0, time.UTC), EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 7},
		{Timestamp: time.Date(2026, 3, 9, 10, 2, 0, 0, time.UTC), EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 3},
	}

	result, err := a.Analyze(events)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Summary.TotalRows != 15 {
		t.Fatalf("expected 15 total rows (no filtering), got %d", result.Summary.TotalRows)
	}
}
