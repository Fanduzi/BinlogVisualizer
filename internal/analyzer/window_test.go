// Package analyzer verifies inclusive event-window and adjacent-boundary behavior.
// input: synthetic normalized events spanning window bounds, detail stores, filters, XA, and LOAD_DATA.
// output: assertions for event-scoped totals, explicit completeness, safe replay spans, parity, and no-window compatibility.
// pos: focused regression coverage for analyzer time-window reconstruction and integration contracts.
// note: if this file changes, keep internal/analyzer/README.md synchronized.
package analyzer

import (
	"errors"
	"path/filepath"
	"reflect"
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
		{Timestamp: base.Add(45 * time.Second), EventType: "ROWS", TxnKey: "t1", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 3}, // inside window
		{Timestamp: base.Add(90 * time.Second), EventType: "XID", TxnKey: "t1"},                                                                     // inside window
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

func TestAnalyzerWindowMarksTransactionClippedAtEnd(t *testing.T) {
	base := time.Date(2026, 8, 29, 11, 59, 13, 0, time.UTC)
	end := base.Add(2 * time.Second)
	events := []model.NormalizedEvent{
		{Timestamp: base, EventType: "BEGIN", BinlogPath: "mysql-bin.000001", PositionStart: 3183, PositionEnd: 3200},
		{Timestamp: base.Add(time.Second), EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 1, BinlogPath: "mysql-bin.000001", PositionStart: 3200, PositionEnd: 3234},
		{Timestamp: base.Add(3 * time.Second), EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 1, BinlogPath: "mysql-bin.000001", PositionStart: 3234, PositionEnd: 3400},
		{Timestamp: base.Add(4 * time.Second), EventType: "XID", BinlogPath: "mysql-bin.000001", PositionStart: 3400, PositionEnd: 3449},
	}

	result, err := New(Options{End: &end, LargeTxnRows: 0}).Analyze(events)
	if err != nil {
		t.Fatalf("Analyze returned error: %v", err)
	}
	if result.Summary.TotalRows != 1 || result.Summary.TotalEvents != 2 {
		t.Fatalf("inclusive event totals changed: %+v", result.Summary)
	}
	if result.Summary.PartialTransactions != 1 {
		t.Fatalf("partial_transactions=%d, want 1", result.Summary.PartialTransactions)
	}
	if len(result.Transactions) != 1 {
		t.Fatalf("transactions=%d, want retained partial evidence", len(result.Transactions))
	}
	txn := result.Transactions[0]
	if txn.Completeness != model.TransactionPartialEnd {
		t.Fatalf("completeness=%q, want %q", txn.Completeness, model.TransactionPartialEnd)
	}
	if txn.TotalRows != 1 || txn.PositionStart != 3183 || txn.PositionEnd != 3234 {
		t.Fatalf("retained transaction evidence changed: %+v", txn)
	}
	if txn.FullReplaySpan == nil || txn.FullReplaySpan.PositionStart != 3183 || txn.FullReplaySpan.PositionEnd != 3449 {
		t.Fatalf("full replay span=%+v, want 3183-3449", txn.FullReplaySpan)
	}
	if len(result.Diagnostics.LargestTransactions) != 0 || len(result.Patterns) != 0 || len(result.Timeseries.TxnSizeSeriesSummary.Buckets) != 0 {
		t.Fatalf("partial transaction leaked into whole-transaction conclusions: %+v", result.Diagnostics)
	}
	if len(result.Alerts) != 1 || result.Alerts[0].Type != "partial_transaction" {
		t.Fatalf("alerts=%+v, want structured partial_transaction evidence", result.Alerts)
	}
}

func TestAnalyzerWindowCompletenessDirectionsAndInclusiveBoundaries(t *testing.T) {
	base := time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)
	start := base.Add(time.Second)
	end := base.Add(3 * time.Second)
	for _, tt := range []struct {
		name   string
		events []model.NormalizedEvent
		want   model.TransactionCompleteness
		rows   int
	}{
		{
			name: "partial start",
			events: []model.NormalizedEvent{
				{Timestamp: base, EventType: "BEGIN"},
				{Timestamp: start, EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 1},
				{Timestamp: end, EventType: "XID"},
			},
			want: model.TransactionPartialStart,
			rows: 1,
		},
		{
			name: "partial both",
			events: []model.NormalizedEvent{
				{Timestamp: base, EventType: "BEGIN"},
				{Timestamp: start, EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 1},
				{Timestamp: end, EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 2},
				{Timestamp: end.Add(time.Second), EventType: "XID"},
			},
			want: model.TransactionPartialBoth,
			rows: 3,
		},
		{
			name: "exact inclusive boundaries",
			events: []model.NormalizedEvent{
				{Timestamp: start, EventType: "BEGIN"},
				{Timestamp: base.Add(2 * time.Second), EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 4},
				{Timestamp: end, EventType: "XID"},
			},
			want: model.TransactionComplete,
			rows: 4,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			result, err := New(Options{Start: &start, End: &end}).Analyze(tt.events)
			if err != nil {
				t.Fatalf("Analyze returned error: %v", err)
			}
			if len(result.Transactions) != 1 || result.Transactions[0].Completeness != tt.want || result.Summary.TotalRows != tt.rows {
				t.Fatalf("got summary=%+v transactions=%+v, want rows=%d completeness=%s", result.Summary, result.Transactions, tt.rows, tt.want)
			}
		})
	}
}

func TestAnalyzerWindowUnavailableContextIsUnknown(t *testing.T) {
	start := time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)
	result, err := New(Options{Start: &start}).Analyze([]model.NormalizedEvent{{
		Timestamp: start, EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 2,
	}})
	if err != nil {
		t.Fatalf("Analyze returned error: %v", err)
	}
	if len(result.Transactions) != 1 || result.Transactions[0].Completeness != model.TransactionUnknown || result.Transactions[0].FullReplaySpan != nil {
		t.Fatalf("unavailable context was guessed complete: %+v", result.Transactions)
	}
	if result.Summary.UnknownTransactions != 1 || len(result.Alerts) != 1 || result.Alerts[0].Type != "unknown_transaction" {
		t.Fatalf("unknown transaction summary/evidence missing: summary=%+v alerts=%+v", result.Summary, result.Alerts)
	}
}

func TestAnalyzerWindowControlOnlyFragmentDoesNotCreateTransaction(t *testing.T) {
	start := time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)
	end := start.Add(time.Second)
	result, err := New(Options{Start: &start, End: &end}).Analyze([]model.NormalizedEvent{
		{Timestamp: start, EventType: "BEGIN"},
		{Timestamp: end.Add(time.Second), EventType: "XID"},
	})
	if err != nil {
		t.Fatalf("Analyze returned error: %v", err)
	}
	if result.Summary.TotalTransactions != 0 || len(result.Transactions) != 0 || len(result.Alerts) != 0 {
		t.Fatalf("control-only fragment fabricated a transaction: %+v", result)
	}
}

func TestAnalyzerWindowCompletenessMatchesDuckDBDetailStore(t *testing.T) {
	base := time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)
	start := base.Add(time.Second)
	end := base.Add(2 * time.Second)
	events := []model.NormalizedEvent{
		{Timestamp: base, EventType: "BEGIN", BinlogPath: "mysql-bin.000044", PositionStart: 100, PositionEnd: 120},
		{Timestamp: start, EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 1, BinlogPath: "mysql-bin.000044", PositionStart: 120, PositionEnd: 180},
		{Timestamp: end, EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 2, BinlogPath: "mysql-bin.000045", PositionStart: 4, PositionEnd: 80},
		{Timestamp: end.Add(time.Second), EventType: "XID", BinlogPath: "mysql-bin.000045", PositionStart: 80, PositionEnd: 110},
	}
	opts := Options{Start: &start, End: &end, TopTransactions: 10}
	want, err := New(opts).Analyze(events)
	if err != nil {
		t.Fatalf("in-memory Analyze returned error: %v", err)
	}

	store, err := NewDuckDBStore(filepath.Join(t.TempDir(), "analysis.duckdb"), 10)
	if err != nil {
		if errors.Is(err, ErrDuckDBRequiresCGO) {
			t.Skip(err.Error())
		}
		t.Fatalf("NewDuckDBStore returned error: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	got, err := NewWithStore(opts, store).Analyze(events)
	if err != nil {
		t.Fatalf("DuckDB Analyze returned error: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("detail-store modes differ\nnone: %#v\nduckdb: %#v", want, got)
	}
	stored, err := store.QueryAllTransactions()
	if err != nil {
		t.Fatalf("QueryAllTransactions returned error: %v", err)
	}
	if len(stored) != 1 || stored[0].Completeness != model.TransactionPartialBoth || stored[0].FullReplaySpan == nil || stored[0].FullReplaySpan.BinlogPathEnd != "mysql-bin.000045" {
		t.Fatalf("DuckDB completeness/replay metadata lost: %+v", stored)
	}
}

func TestAnalyzerWindowPreservesXAAndLoadDataOnPartialTransaction(t *testing.T) {
	base := time.Date(2026, 8, 29, 13, 0, 0, 0, time.UTC)
	start := base.Add(2 * time.Second)
	end := base.Add(3 * time.Second)
	xid := "X'6276742d3537',X'',1"
	result, err := New(Options{Start: &start, End: &end, IncludeTables: []string{"slow"}}).Analyze([]model.NormalizedEvent{
		{Timestamp: base, EventType: "XA_START", XAXID: xid},
		{Timestamp: base.Add(time.Second), EventType: "ROWS_QUERY", Operation: "LOAD_DATA", QuerySQL: "LOAD DATA INFILE '/tmp/slow.csv' INTO TABLE dogfood_cut.slow"},
		{Timestamp: start, EventType: "ROWS", Schema: "dogfood_cut", Table: "slow", Operation: "INSERT", RowCount: 2},
		{Timestamp: end.Add(time.Second), EventType: "XA_PREPARE", XAXID: xid},
	})
	if err != nil {
		t.Fatalf("Analyze returned error: %v", err)
	}
	if len(result.Transactions) != 1 {
		t.Fatalf("transactions=%+v, want XA/LOAD DATA evidence", result.Transactions)
	}
	txn := result.Transactions[0]
	if txn.XAXID != xid || txn.Completeness != model.TransactionPartialBoth || txn.Operations["LOAD_DATA"] != 2 || txn.Tables["dogfood_cut.slow"] != 2 {
		t.Fatalf("XA/LOAD DATA semantics changed under windowing: %+v", txn)
	}
}

func TestAnalyzerNoWindowCompleteTransactionKeepsExistingConclusions(t *testing.T) {
	base := time.Date(2026, 8, 29, 14, 0, 0, 0, time.UTC)
	result, err := New(DefaultOptions()).Analyze([]model.NormalizedEvent{
		{Timestamp: base, EventType: "BEGIN"},
		{Timestamp: base.Add(time.Second), EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 2},
		{Timestamp: base.Add(2 * time.Second), EventType: "XID"},
	})
	if err != nil {
		t.Fatalf("Analyze returned error: %v", err)
	}
	if result.Summary.TotalRows != 2 || result.Summary.PartialTransactions != 0 || result.Summary.UnknownTransactions != 0 || len(result.Patterns) != 1 || len(result.Diagnostics.LargestTransactions) != 1 {
		t.Fatalf("no-window compatibility changed: %+v", result)
	}
	if result.Transactions[0].Completeness != model.TransactionComplete {
		t.Fatalf("complete transaction marked %q", result.Transactions[0].Completeness)
	}
}

func TestAnalyzerWindowKeepsQueryContextForRetainedRows(t *testing.T) {
	base := time.Date(2026, 8, 29, 15, 0, 0, 0, time.UTC)
	end := base.Add(2 * time.Second)
	result, err := New(Options{End: &end}).Analyze([]model.NormalizedEvent{
		{Timestamp: base, EventType: "BEGIN"},
		{Timestamp: base.Add(time.Second), EventType: "ROWS_QUERY", QuerySQL: "INSERT INTO shop.orders VALUES (1)"},
		{Timestamp: end, EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 1},
		{Timestamp: end.Add(time.Second), EventType: "ROWS_QUERY", QuerySQL: "DELETE FROM shop.orders WHERE id = 2"},
		{Timestamp: end.Add(2 * time.Second), EventType: "XID"},
	})
	if err != nil {
		t.Fatalf("Analyze returned error: %v", err)
	}
	if len(result.Transactions) != 1 || result.Transactions[0].QuerySummary != "INSERT INTO shop.orders VALUES (1)" {
		t.Fatalf("out-of-window query context replaced retained evidence: %+v", result.Transactions)
	}
}
