// Package analyzer validates analyzer orchestration and streaming result semantics.
// input: analyzer test fixtures expressed as model.NormalizedEvent sequences and analyzer.Options values.
// output: regression coverage for slice-wrapper compatibility, streaming finalization, window filtering, and failure handling.
// pos: module-level behavioral test suite for the analyzer entrypoint and its external contracts.
// note: if this file changes, update this header and module README.md.
package analyzer

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"binlogviz/internal/model"
)

var errAlertsRoundtripUnexpected = errors.New("alerts roundtrip should not be required")

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

func TestAnalyzerFinalizeDoesNotDependOnAlertStoreRoundtrip(t *testing.T) {
	baseStore := newInMemoryStore().(*inMemoryStore)
	a := &Analyzer{
		opts: Options{
			LargeTxnRows: 1,
		},
		store: &alertsRoundtripFailStore{inMemoryStore: baseStore},
	}
	a.reset()

	events := []model.NormalizedEvent{
		{Timestamp: time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC), EventType: "BEGIN", TxnKey: "t1"},
		{Timestamp: time.Date(2026, 3, 9, 10, 0, 1, 0, time.UTC), EventType: "ROWS", TxnKey: "t1", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 5},
		{Timestamp: time.Date(2026, 3, 9, 10, 0, 2, 0, time.UTC), EventType: "XID", TxnKey: "t1"},
	}
	for _, ev := range events {
		if err := a.Consume(ev); err != nil {
			t.Fatalf("Consume returned error: %v", err)
		}
	}

	result, err := a.Finalize()
	if err != nil {
		t.Fatalf("Finalize returned error: %v", err)
	}
	if len(result.Alerts) != 1 {
		t.Fatalf("expected 1 alert, got %d", len(result.Alerts))
	}
	if result.Alerts[0].Type != "large_transaction" {
		t.Fatalf("expected large_transaction alert, got %s", result.Alerts[0].Type)
	}
	if len(result.Diagnostics.Findings) == 0 {
		t.Fatal("expected findings built from in-memory alerts")
	}
}

func TestAnalyzerCountsTruncatedQueryContextsAsWarnings(t *testing.T) {
	a := New(Options{})
	base := time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC)
	longSQL := "UPDATE orders SET notes = '" + strings.Repeat("x", model.MaxStoredSQLBytes+128) + "' WHERE id = 7"

	events := []model.NormalizedEvent{
		{Timestamp: base, EventType: "BEGIN", TxnKey: "t1"},
		{
			Timestamp:          base.Add(time.Second),
			EventType:          "ROWS_QUERY",
			TxnKey:             "t1",
			QuerySQL:           longSQL[:model.MaxStoredSQLBytes],
			QueryTruncated:     true,
			QueryOriginalBytes: len(longSQL),
		},
		{Timestamp: base.Add(2 * time.Second), EventType: "ROWS", TxnKey: "t1", Schema: "shop", Table: "orders", Operation: "UPDATE", RowCount: 3},
		{Timestamp: base.Add(3 * time.Second), EventType: "XID", TxnKey: "t1"},
		{Timestamp: base.Add(time.Minute), EventType: "BEGIN", TxnKey: "t2"},
		{Timestamp: base.Add(time.Minute + time.Second), EventType: "ROWS", TxnKey: "t2", Schema: "shop", Table: "users", Operation: "INSERT", RowCount: 1},
		{Timestamp: base.Add(time.Minute + 2*time.Second), EventType: "XID", TxnKey: "t2"},
	}

	result, err := a.Analyze(events)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Warnings != 1 {
		t.Fatalf("expected 1 warning for truncated query context, got %d", result.Warnings)
	}
}

type alertsRoundtripFailStore struct {
	*inMemoryStore
}

func (s *alertsRoundtripFailStore) RecordAlerts([]model.Alert) error {
	return errAlertsRoundtripUnexpected
}

func (s *alertsRoundtripFailStore) QueryAlerts() ([]model.Alert, error) {
	return nil, errAlertsRoundtripUnexpected
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

func TestAnalyzerFinalizesTimeseriesAndDDLDiagnostics(t *testing.T) {
	a := New(Options{})
	base := time.Date(2026, 4, 15, 10, 0, 0, 0, time.UTC)

	events := []model.NormalizedEvent{
		{
			Timestamp:     base,
			EventType:     "DDL",
			Schema:        "shop",
			Table:         "orders",
			QuerySQL:      "ALTER TABLE shop.orders ADD COLUMN note TEXT",
			BinlogPath:    "mysql-bin.000001",
			PositionStart: 40,
			PositionEnd:   90,
			BinlogBytes:   50,
		},
		{Timestamp: base.Add(time.Second), EventType: "BEGIN", TxnKey: "t1", BinlogBytes: 10},
		{
			Timestamp:     base.Add(2 * time.Second),
			EventType:     "ROWS",
			TxnKey:        "t1",
			Schema:        "shop",
			Table:         "orders",
			Operation:     "INSERT",
			RowCount:      5,
			BinlogPath:    "mysql-bin.000001",
			PositionStart: 90,
			PositionEnd:   150,
			BinlogBytes:   60,
		},
		{Timestamp: base.Add(3 * time.Second), EventType: "XID", TxnKey: "t1", BinlogBytes: 12},
	}

	result, err := a.Analyze(events)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.Diagnostics.DDLEvents) != 1 {
		t.Fatalf("expected 1 ddl event, got %d", len(result.Diagnostics.DDLEvents))
	}
	if result.Diagnostics.DDLEvents[0].Operation != "ALTER TABLE" {
		t.Fatalf("expected ALTER TABLE ddl event, got %+v", result.Diagnostics.DDLEvents[0])
	}
	if len(result.Timeseries.TPSSeries) != 1 || result.Timeseries.TPSSeries[0].Value != 1.0/60.0 {
		t.Fatalf("expected tps series with one point=1/60, got %+v", result.Timeseries.TPSSeries)
	}
	if len(result.Timeseries.InsertEventSeries) != 1 || result.Timeseries.InsertEventSeries[0].Value != 1 {
		t.Fatalf("expected insert-event series with one point=1, got %+v", result.Timeseries.InsertEventSeries)
	}
	if len(result.Timeseries.DDLEventSeries) != 1 || result.Timeseries.DDLEventSeries[0].Value != 1 {
		t.Fatalf("expected ddl-event series with one point=1, got %+v", result.Timeseries.DDLEventSeries)
	}
	if len(result.Timeseries.BinlogBytesSeries) != 1 || result.Timeseries.BinlogBytesSeries[0].Value != 110 {
		t.Fatalf("expected binlog-bytes series with one point=110, got %+v", result.Timeseries.BinlogBytesSeries)
	}
	if len(result.Diagnostics.LargestTransactions) != 1 || result.Diagnostics.LargestTransactions[0].TxnKey != "txn-1" {
		t.Fatalf("expected largest transaction diagnostics for txn-1, got %+v", result.Diagnostics.LargestTransactions)
	}
	if len(result.Diagnostics.LongestTransactions) != 1 || result.Diagnostics.LongestTransactions[0].TxnKey != "txn-1" {
		t.Fatalf("expected longest transaction diagnostics for txn-1, got %+v", result.Diagnostics.LongestTransactions)
	}
	if len(result.Diagnostics.HotIntervals) != 1 || result.Diagnostics.HotIntervals[0].TotalRows != 5 {
		t.Fatalf("expected one hot interval with 5 rows, got %+v", result.Diagnostics.HotIntervals)
	}
}

func TestAnalyzerBuildsDiagnosticFindingsFromAlerts(t *testing.T) {
	opts := DefaultOptions()
	opts.LargeTxnRows = 2
	opts.LargeTxnDuration = 0
	opts.DetectSpikes = true
	opts.SpikeWindow = 1
	opts.SpikeFactor = 1.0
	opts.SpikeMinRows = 1

	a := New(opts)
	base := time.Date(2026, 4, 15, 11, 0, 0, 0, time.UTC)
	events := []model.NormalizedEvent{
		{Timestamp: base.Add(-time.Minute), EventType: "BEGIN", TxnKey: "txn-0"},
		{
			Timestamp:     base.Add(-time.Minute + time.Second),
			EventType:     "ROWS",
			TxnKey:        "txn-0",
			Schema:        "shop",
			Table:         "orders",
			Operation:     "INSERT",
			RowCount:      1,
			BinlogPath:    "mysql-bin.000009",
			PositionStart: 10,
			PositionEnd:   20,
			BinlogBytes:   10,
		},
		{Timestamp: base.Add(-time.Minute + 2*time.Second), EventType: "XID", TxnKey: "txn-0", BinlogBytes: 5},
		{
			Timestamp:  base,
			EventType:  "DDL",
			Schema:     "shop",
			Table:      "orders",
			QuerySQL:   "ALTER TABLE shop.orders ADD COLUMN flag TINYINT",
			BinlogPath: "mysql-bin.000010",
		},
		{Timestamp: base.Add(time.Second), EventType: "BEGIN", TxnKey: "txn-1", BinlogBytes: 10},
		{
			Timestamp:     base.Add(2 * time.Second),
			EventType:     "ROWS",
			TxnKey:        "txn-1",
			Schema:        "shop",
			Table:         "orders",
			Operation:     "INSERT",
			RowCount:      5,
			BinlogPath:    "mysql-bin.000010",
			PositionStart: 100,
			PositionEnd:   150,
			BinlogBytes:   50,
		},
		{
			Timestamp:     base.Add(3 * time.Second),
			EventType:     "XID",
			TxnKey:        "txn-1",
			BinlogPath:    "mysql-bin.000010",
			PositionStart: 150,
			PositionEnd:   170,
			BinlogBytes:   20,
		},
	}

	result, err := a.Analyze(events)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Diagnostics.Findings) == 0 {
		t.Fatal("expected diagnostic findings from alerts")
	}

	var sawTxnFinding bool
	var sawMinuteFinding bool
	var sawDDLEvidence bool
	for _, finding := range result.Diagnostics.Findings {
		if finding.TxnKey != "" {
			sawTxnFinding = true
		}
		if !finding.Minute.IsZero() && finding.Minute.Equal(base) {
			sawMinuteFinding = true
		}
		for _, evidence := range finding.EvidenceRefs {
			if evidence == "ddl=ALTER TABLE shop.orders @ mysql-bin.000010" {
				sawDDLEvidence = true
			}
		}
	}

	if !sawTxnFinding {
		t.Fatalf("expected at least one transaction-scoped finding, got %+v", result.Diagnostics.Findings)
	}
	if !sawMinuteFinding {
		t.Fatalf("expected at least one minute-based finding at %v, got %+v", base, result.Diagnostics.Findings)
	}
	if !sawDDLEvidence {
		t.Fatalf("expected DDL evidence in findings, got %+v", result.Diagnostics.Findings)
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

func TestAnalyzerStreamingMatchesAnalyze(t *testing.T) {
	base := time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC)
	opts := Options{
		DetectSpikes: true,
		SpikeWindow:  3,
		SpikeFactor:  3,
		SpikeMinRows: 5,
	}
	events := []model.NormalizedEvent{
		{Timestamp: base, EventType: "BEGIN", TxnKey: "t1"},
		{Timestamp: base.Add(time.Second), EventType: "ROWS_QUERY", TxnKey: "t1", QuerySQL: "UPDATE orders SET status='done' WHERE id IN (1,2,3)"},
		{Timestamp: base.Add(2 * time.Second), EventType: "ROWS", TxnKey: "t1", Schema: "shop", Table: "orders", Operation: "UPDATE", RowCount: 7},
		{Timestamp: base.Add(3 * time.Second), EventType: "XID", TxnKey: "t1"},
		{Timestamp: base.Add(time.Minute), EventType: "BEGIN", TxnKey: "t2"},
		{Timestamp: base.Add(time.Minute + time.Second), EventType: "ROWS", TxnKey: "t2", Schema: "shop", Table: "users", Operation: "INSERT", RowCount: 3},
		{Timestamp: base.Add(time.Minute + 2*time.Second), EventType: "XID", TxnKey: "t2"},
		{Timestamp: base.Add(2 * time.Minute), EventType: "ROWS", TxnKey: "t3", Schema: "shop", Table: "orders", Operation: "DELETE", RowCount: 20},
	}

	sliceAnalyzer := New(opts)
	want, err := sliceAnalyzer.Analyze(events)
	if err != nil {
		t.Fatalf("Analyze returned error: %v", err)
	}

	streamingAnalyzer := New(opts)
	for _, ev := range events {
		if err := streamingAnalyzer.Consume(ev); err != nil {
			t.Fatalf("Consume returned error: %v", err)
		}
	}
	got, err := streamingAnalyzer.Finalize()
	if err != nil {
		t.Fatalf("Finalize returned error: %v", err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("streaming result mismatch\nwant: %#v\ngot: %#v", want, got)
	}
}

func TestAnalyzerStreamingHandlesEmptyInput(t *testing.T) {
	a := New(Options{})

	result, err := a.Finalize()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Summary.TotalTransactions != 0 {
		t.Fatalf("expected 0 transactions, got %d", result.Summary.TotalTransactions)
	}
	if result.Summary.TotalRows != 0 {
		t.Fatalf("expected 0 total rows, got %d", result.Summary.TotalRows)
	}
	if result.Summary.TotalEvents != 0 {
		t.Fatalf("expected 0 total events, got %d", result.Summary.TotalEvents)
	}
	if len(result.Tables) != 0 || len(result.Transactions) != 0 || len(result.Minutes) != 0 || len(result.Alerts) != 0 {
		t.Fatalf("expected empty aggregates, got %#v", result)
	}
}

func TestAnalyzerStreamingTracksMultipleTransactions(t *testing.T) {
	a := New(Options{})
	base := time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC)
	events := []model.NormalizedEvent{
		{Timestamp: base, EventType: "BEGIN", TxnKey: "t1"},
		{Timestamp: base.Add(time.Second), EventType: "ROWS", TxnKey: "t1", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 5},
		{Timestamp: base.Add(2 * time.Second), EventType: "XID", TxnKey: "t1"},
		{Timestamp: base.Add(3 * time.Second), EventType: "BEGIN", TxnKey: "t2"},
		{Timestamp: base.Add(4 * time.Second), EventType: "ROWS", TxnKey: "t2", Schema: "shop", Table: "users", Operation: "INSERT", RowCount: 3},
		{Timestamp: base.Add(5 * time.Second), EventType: "XID", TxnKey: "t2"},
	}

	for _, ev := range events {
		if err := a.Consume(ev); err != nil {
			t.Fatalf("Consume returned error: %v", err)
		}
	}
	result, err := a.Finalize()
	if err != nil {
		t.Fatalf("Finalize returned error: %v", err)
	}

	if result.Summary.TotalTransactions != 2 {
		t.Fatalf("expected 2 transactions, got %d", result.Summary.TotalTransactions)
	}
	if result.Summary.TotalRows != 8 {
		t.Fatalf("expected 8 total rows, got %d", result.Summary.TotalRows)
	}
}

func TestFinalizeIncludesPatternsDerivedFromTransactions(t *testing.T) {
	a := New(Options{})
	base := time.Date(2026, 4, 9, 10, 0, 0, 0, time.UTC)
	events := []model.NormalizedEvent{
		{Timestamp: base, EventType: "BEGIN", TxnKey: "t1"},
		{Timestamp: base.Add(time.Second), EventType: "ROWS", TxnKey: "t1", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 4},
		{Timestamp: base.Add(2 * time.Second), EventType: "COMMIT", TxnKey: "t1"},
	}
	for _, ev := range events {
		if err := a.Consume(ev); err != nil {
			t.Fatalf("consume: %v", err)
		}
	}

	result, err := a.Finalize()
	if err != nil {
		t.Fatalf("finalize: %v", err)
	}
	if len(result.Patterns) != 1 {
		t.Fatalf("expected 1 pattern, got %d", len(result.Patterns))
	}
}

func TestAnalyzerStreamingFiltersTimeWindow(t *testing.T) {
	start := time.Date(2026, 3, 9, 10, 0, 30, 0, time.UTC)
	end := time.Date(2026, 3, 9, 10, 1, 30, 0, time.UTC)
	a := New(Options{Start: &start, End: &end})
	base := time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC)
	events := []model.NormalizedEvent{
		{Timestamp: base, EventType: "BEGIN", TxnKey: "t1"},
		{Timestamp: base.Add(15 * time.Second), EventType: "ROWS", TxnKey: "t1", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 5},
		{Timestamp: base.Add(45 * time.Second), EventType: "ROWS", TxnKey: "t1", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 3},
		{Timestamp: base.Add(90 * time.Second), EventType: "XID", TxnKey: "t1"},
		{Timestamp: base.Add(2 * time.Minute), EventType: "ROWS", TxnKey: "t2", Schema: "shop", Table: "users", Operation: "INSERT", RowCount: 9},
	}

	for _, ev := range events {
		if err := a.Consume(ev); err != nil {
			t.Fatalf("Consume returned error: %v", err)
		}
	}
	result, err := a.Finalize()
	if err != nil {
		t.Fatalf("Finalize returned error: %v", err)
	}

	if result.Summary.TotalRows != 3 {
		t.Fatalf("expected 3 total rows inside window, got %d", result.Summary.TotalRows)
	}
	if result.Summary.TotalEvents != 2 {
		t.Fatalf("expected 2 events inside window, got %d", result.Summary.TotalEvents)
	}
	if len(result.Transactions) != 1 {
		t.Fatalf("expected 1 transaction, got %d", len(result.Transactions))
	}
	if result.Transactions[0].TotalRows != 3 {
		t.Fatalf("expected transaction rows 3, got %d", result.Transactions[0].TotalRows)
	}
}

func TestAnalyzerStreamingMatchesAnalyzeWithCrossFileTransaction(t *testing.T) {
	base := time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC)
	opts := Options{TopTransactions: 5}
	events := []model.NormalizedEvent{
		{Timestamp: base, EventType: "BEGIN", TxnKey: "cross-1", BinlogPath: "mysql-bin.000044"},
		{Timestamp: base.Add(time.Second), EventType: "ROWS_QUERY", TxnKey: "cross-1", QuerySQL: "UPDATE orders SET status='done' WHERE id IN (1,2,3)", BinlogPath: "mysql-bin.000044"},
		{Timestamp: base.Add(2 * time.Second), EventType: "ROWS", TxnKey: "cross-1", Schema: "shop", Table: "orders", Operation: "UPDATE", RowCount: 7, BinlogPath: "mysql-bin.000044"},
		{Timestamp: base.Add(3 * time.Second), EventType: "ROWS", TxnKey: "cross-1", Schema: "shop", Table: "orders", Operation: "UPDATE", RowCount: 9, BinlogPath: "mysql-bin.000045"},
		{Timestamp: base.Add(4 * time.Second), EventType: "XID", TxnKey: "cross-1", BinlogPath: "mysql-bin.000045"},
		{Timestamp: base.Add(time.Minute), EventType: "BEGIN", TxnKey: "local-2", BinlogPath: "mysql-bin.000045"},
		{Timestamp: base.Add(time.Minute + time.Second), EventType: "ROWS", TxnKey: "local-2", Schema: "shop", Table: "users", Operation: "INSERT", RowCount: 3, BinlogPath: "mysql-bin.000045"},
		{Timestamp: base.Add(time.Minute + 2*time.Second), EventType: "XID", TxnKey: "local-2", BinlogPath: "mysql-bin.000045"},
	}

	sliceAnalyzer := New(opts)
	want, err := sliceAnalyzer.Analyze(events)
	if err != nil {
		t.Fatalf("Analyze returned error: %v", err)
	}

	streamingAnalyzer := New(opts)
	for _, ev := range events {
		if err := streamingAnalyzer.Consume(ev); err != nil {
			t.Fatalf("Consume returned error: %v", err)
		}
	}
	got, err := streamingAnalyzer.Finalize()
	if err != nil {
		t.Fatalf("Finalize returned error: %v", err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("cross-file streaming result mismatch\nwant: %#v\ngot: %#v", want, got)
	}
	if got.Summary.TotalTransactions != 2 {
		t.Fatalf("expected 2 transactions, got %d", got.Summary.TotalTransactions)
	}
	if got.Summary.TotalRows != 19 {
		t.Fatalf("expected 19 total rows, got %d", got.Summary.TotalRows)
	}
	var crossFileTxn *model.Transaction
	for i := range got.Transactions {
		if got.Transactions[i].TotalRows == 16 && got.Transactions[i].BinlogPathStart == "mysql-bin.000044" && got.Transactions[i].BinlogPathEnd == "mysql-bin.000045" {
			crossFileTxn = &got.Transactions[i]
			break
		}
	}
	if crossFileTxn == nil {
		t.Fatalf("expected cross-file txn to stay intact, got %#v", got.Transactions)
	}
}

func TestAnalyzerStreamingPreservesStateOnTransactionBuilderError(t *testing.T) {
	a := New(Options{})
	base := time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC)

	validEvents := []model.NormalizedEvent{
		{Timestamp: base, EventType: "BEGIN", TxnKey: "t1"},
		{Timestamp: base.Add(time.Second), EventType: "ROWS", TxnKey: "t1", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 5},
		{Timestamp: base.Add(2 * time.Second), EventType: "XID", TxnKey: "t1"},
		{Timestamp: base.Add(3 * time.Second), EventType: "BEGIN", TxnKey: "t2"},
		{Timestamp: base.Add(4 * time.Second), EventType: "ROWS", TxnKey: "t2", Schema: "shop", Table: "users", Operation: "INSERT", RowCount: 3},
	}
	for _, ev := range validEvents {
		if err := a.Consume(ev); err != nil {
			t.Fatalf("Consume returned error: %v", err)
		}
	}

	err := a.Consume(model.NormalizedEvent{
		Timestamp: base.Add(5 * time.Second),
		EventType: "BEGIN",
		TxnKey:    "t3",
	})
	if err == nil {
		t.Fatal("expected boundary error, got nil")
	}

	result, finalizeErr := a.Finalize()
	if finalizeErr == nil {
		t.Fatal("expected Finalize to return the stored error")
	}
	if result != nil {
		t.Fatalf("expected nil result after error, got %#v", result)
	}

	err = a.Consume(model.NormalizedEvent{
		Timestamp: base.Add(6 * time.Second),
		EventType: "ROWS",
		TxnKey:    "t3",
		Schema:    "shop",
		Table:     "products",
		Operation: "INSERT",
		RowCount:  10,
	})
	if err == nil {
		t.Fatal("expected analyzer to remain failed after error")
	}

	expected := New(Options{})
	for _, ev := range validEvents {
		if consumeErr := expected.Consume(ev); consumeErr != nil {
			t.Fatalf("expected analyzer setup failed: %v", consumeErr)
		}
	}
	want, err := expected.Finalize()
	if err != nil {
		t.Fatalf("expected Finalize to succeed: %v", err)
	}
	if want.Summary.TotalTransactions != 2 {
		t.Fatalf("expected reference total transactions 2, got %d", want.Summary.TotalTransactions)
	}
	if want.Summary.TotalRows != 8 {
		t.Fatalf("expected reference total rows 8, got %d", want.Summary.TotalRows)
	}
}

func TestAnalyzerAnalyzeRecoversAfterPreviousError(t *testing.T) {
	a := New(Options{})
	base := time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC)

	broken := []model.NormalizedEvent{
		{Timestamp: base, EventType: "BEGIN", TxnKey: "t1"},
		{Timestamp: base.Add(time.Second), EventType: "ROWS", TxnKey: "t1", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 5},
		{Timestamp: base.Add(2 * time.Second), EventType: "BEGIN", TxnKey: "t2"},
	}
	if _, err := a.Analyze(broken); err == nil {
		t.Fatal("expected boundary error from broken input")
	}

	healthy := []model.NormalizedEvent{
		{Timestamp: base.Add(3 * time.Second), EventType: "BEGIN", TxnKey: "t3"},
		{Timestamp: base.Add(4 * time.Second), EventType: "ROWS", TxnKey: "t3", Schema: "shop", Table: "users", Operation: "INSERT", RowCount: 4},
		{Timestamp: base.Add(5 * time.Second), EventType: "XID", TxnKey: "t3"},
	}
	result, err := a.Analyze(healthy)
	if err != nil {
		t.Fatalf("expected Analyze to recover on reuse, got error: %v", err)
	}
	if result.Summary.TotalTransactions != 1 {
		t.Fatalf("expected 1 transaction after recovery, got %d", result.Summary.TotalTransactions)
	}
	if result.Summary.TotalRows != 4 {
		t.Fatalf("expected 4 rows after recovery, got %d", result.Summary.TotalRows)
	}
}

func TestAnalyzerNewUsesInMemoryStoreByDefault(t *testing.T) {
	a := New(Options{})

	if a.store == nil {
		t.Fatal("expected default analyzer store to be initialized")
	}
	if _, ok := a.store.(*DuckDBStore); ok {
		t.Fatal("expected New to avoid implicit DuckDB temp store ownership")
	}
}

type queryAllPanicStore struct {
	*inMemoryStore
}

func (s *queryAllPanicStore) QueryAllTransactions() ([]model.Transaction, error) {
	panic("QueryAllTransactions should not be called")
}

func TestAnalyzerFinalizeDoesNotRequireQueryAllTransactions(t *testing.T) {
	baseStore := newInMemoryStore().(*inMemoryStore)
	a := &Analyzer{
		opts:  Options{TopTransactions: 10},
		store: &queryAllPanicStore{inMemoryStore: baseStore},
	}
	a.reset()

	base := time.Date(2026, 4, 19, 10, 0, 0, 0, time.UTC)
	events := []model.NormalizedEvent{
		{Timestamp: base, EventType: "BEGIN", TxnKey: "t1"},
		{Timestamp: base.Add(time.Second), EventType: "ROWS", TxnKey: "t1", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 5},
		{Timestamp: base.Add(2 * time.Second), EventType: "XID", TxnKey: "t1"},
	}
	for _, ev := range events {
		if err := a.Consume(ev); err != nil {
			t.Fatalf("Consume: %v", err)
		}
	}

	result, err := a.Finalize()
	if err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	if result.Summary.TotalTransactions != 1 {
		t.Fatalf("total transactions = %d, want 1", result.Summary.TotalTransactions)
	}
	if result.Summary.TotalRows != 5 {
		t.Fatalf("total rows = %d, want 5", result.Summary.TotalRows)
	}
}

func TestAnalyzerStreamingReportPreservesOperationTimeseries(t *testing.T) {
	opts := DefaultOptions()
	a := New(opts)
	base := time.Date(2026, 4, 19, 10, 0, 0, 0, time.UTC)

	events := []model.NormalizedEvent{
		{Timestamp: base, EventType: "BEGIN", TxnKey: "t1"},
		{Timestamp: base.Add(time.Second), EventType: "ROWS", TxnKey: "t1", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 5},
		{Timestamp: base.Add(2 * time.Second), EventType: "XID", TxnKey: "t1"},
		{Timestamp: base.Add(time.Minute), EventType: "BEGIN", TxnKey: "t2"},
		{Timestamp: base.Add(time.Minute + time.Second), EventType: "ROWS", TxnKey: "t2", Schema: "shop", Table: "orders", Operation: "UPDATE", RowCount: 3},
		{Timestamp: base.Add(time.Minute + 2*time.Second), EventType: "ROWS", TxnKey: "t2", Schema: "shop", Table: "orders", Operation: "DELETE", RowCount: 1},
		{Timestamp: base.Add(time.Minute + 3*time.Second), EventType: "XID", TxnKey: "t2"},
	}
	for _, ev := range events {
		if err := a.Consume(ev); err != nil {
			t.Fatalf("Consume: %v", err)
		}
	}
	result, err := a.Finalize()
	if err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	if len(result.Timeseries.InsertEventSeries) == 0 || result.Timeseries.InsertEventSeries[0].Value != 1 {
		t.Fatalf("expected insert-event series with point=1 in first minute, got %v", result.Timeseries.InsertEventSeries)
	}
	if len(result.Timeseries.UpdateEventSeries) < 2 || result.Timeseries.UpdateEventSeries[1].Value != 1 {
		t.Fatalf("expected update-event series with point=1 in second minute, got %v", result.Timeseries.UpdateEventSeries)
	}
	if len(result.Timeseries.DeleteEventSeries) < 2 || result.Timeseries.DeleteEventSeries[1].Value != 1 {
		t.Fatalf("expected delete-event series with point=1 in second minute, got %v", result.Timeseries.DeleteEventSeries)
	}
}

func TestAnalyzerStreamingReportOperationTimeseriesRespectsFilters(t *testing.T) {
	opts := DefaultOptions()
	opts.IncludeSchemas = []string{"shop"}
	a := New(opts)
	base := time.Date(2026, 4, 19, 10, 0, 0, 0, time.UTC)

	events := []model.NormalizedEvent{
		{Timestamp: base, EventType: "BEGIN", TxnKey: "t1"},
		{Timestamp: base.Add(time.Second), EventType: "ROWS", TxnKey: "t1", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 5},
		{Timestamp: base.Add(2 * time.Second), EventType: "ROWS", TxnKey: "t1", Schema: "audit", Table: "noise", Operation: "UPDATE", RowCount: 99},
		{Timestamp: base.Add(3 * time.Second), EventType: "XID", TxnKey: "t1"},
	}
	for _, ev := range events {
		if err := a.Consume(ev); err != nil {
			t.Fatalf("Consume: %v", err)
		}
	}
	result, err := a.Finalize()
	if err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	if len(result.Timeseries.InsertEventSeries) == 0 || result.Timeseries.InsertEventSeries[0].Value != 1 {
		t.Fatalf("expected one included insert event, got %v", result.Timeseries.InsertEventSeries)
	}
	if len(result.Timeseries.UpdateEventSeries) == 0 || result.Timeseries.UpdateEventSeries[0].Value != 0 {
		t.Fatalf("expected filtered update event to be excluded, got %v", result.Timeseries.UpdateEventSeries)
	}
	if result.Summary.TotalEvents != 4 {
		t.Fatalf("summary total events = %d, want 4 to preserve existing event-count semantics", result.Summary.TotalEvents)
	}
}

func TestAnalyzerStreamingReportKeepsAlertReferencedEvidence(t *testing.T) {
	opts := DefaultOptions()
	opts.LargeTxnRows = 5
	a := New(opts)
	base := time.Date(2026, 4, 19, 10, 0, 0, 0, time.UTC)

	events := []model.NormalizedEvent{
		// small txn: won't trigger large alert
		{Timestamp: base, EventType: "BEGIN", TxnKey: "t1"},
		{Timestamp: base.Add(time.Second), EventType: "ROWS", TxnKey: "t1", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 2},
		{Timestamp: base.Add(2 * time.Second), EventType: "XID", TxnKey: "t1"},
		// big txn: triggers large alert
		{Timestamp: base.Add(time.Minute), EventType: "BEGIN", TxnKey: "t2"},
		{Timestamp: base.Add(time.Minute + time.Second), EventType: "ROWS", TxnKey: "t2", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 100},
		{Timestamp: base.Add(time.Minute + 2*time.Second), EventType: "XID", TxnKey: "t2"},
	}
	for _, ev := range events {
		if err := a.Consume(ev); err != nil {
			t.Fatalf("Consume: %v", err)
		}
	}
	result, err := a.Finalize()
	if err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	// txn-2 has 100 rows and should trigger a large_transaction alert
	var bigAlertFound bool
	var bigAlertTxnKey string
	for _, alert := range result.Alerts {
		if alert.Type == "large_transaction" && alert.TxnKey == "txn-2" {
			bigAlertFound = true
			bigAlertTxnKey = alert.TxnKey
		}
	}
	if !bigAlertFound {
		t.Fatalf("expected large_transaction alert for txn-2, got %v", result.Alerts)
	}

	var bigFindingHasEvidence bool
	for _, f := range result.Diagnostics.Findings {
		if f.TxnKey == bigAlertTxnKey && len(f.EvidenceRefs) > 0 {
			bigFindingHasEvidence = true
		}
	}
	if !bigFindingHasEvidence {
		t.Fatalf("expected evidence for %q in findings, got %v", bigAlertTxnKey, result.Diagnostics.Findings)
	}
}

func TestAnalyzerStreamingReportPatternDrilldownsBoundedDeterministic(t *testing.T) {
	opts := DefaultOptions()
	opts.LargeTxnRows = 2
	a := New(opts)
	base := time.Date(2026, 4, 19, 10, 0, 0, 0, time.UTC)

	// Build enough transactions to exercise pattern drilldown selection.
	for i := 0; i < 10; i++ {
		txnKey := fmt.Sprintf("txn-%d", i)
		events := []model.NormalizedEvent{
			{Timestamp: base.Add(time.Duration(i) * time.Minute), EventType: "BEGIN", TxnKey: txnKey},
			{Timestamp: base.Add(time.Duration(i)*time.Minute + time.Second), EventType: "ROWS", TxnKey: txnKey, Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 50 + i},
			{Timestamp: base.Add(time.Duration(i)*time.Minute + 2*time.Second), EventType: "XID", TxnKey: txnKey},
		}
		for _, ev := range events {
			if err := a.Consume(ev); err != nil {
				t.Fatalf("Consume: %v", err)
			}
		}
	}

	result, err := a.Finalize()
	if err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	if len(result.PatternDrilldowns) > 2 {
		t.Fatalf("pattern drilldowns = %d, want <= 2", len(result.PatternDrilldowns))
	}
	for _, d := range result.PatternDrilldowns {
		if len(d.RepresentativeTransactions) > 2 {
			t.Fatalf("representative txns = %d, want <= 2", len(d.RepresentativeTransactions))
		}
	}
}
