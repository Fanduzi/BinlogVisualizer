package compare

import (
	"strings"
	"testing"
)

func TestBuildCompareResultProducesDDLDelta(t *testing.T) {
	current := InputReport{
		Summary: InputSummary{TotalRows: 1000, TotalTransactions: 50, TotalEvents: 1200},
		Diagnostics: InputDiagnostics{
			DDLEvents: []InputDDLEvent{
				{Timestamp: "2026-03-20T10:05:00Z", Schema: "shop", Table: "orders", Operation: "ALTER TABLE", Statement: "ALTER TABLE orders ADD COLUMN status INT"},
				{Timestamp: "2026-03-20T10:10:00Z", Schema: "shop", Table: "payments", Operation: "CREATE INDEX", Statement: "CREATE INDEX idx_status ON payments(status)"},
			},
		},
	}
	baseline := InputReport{
		Summary: InputSummary{TotalRows: 800, TotalTransactions: 40, TotalEvents: 1000},
		Diagnostics: InputDiagnostics{
			DDLEvents: []InputDDLEvent{
				{Timestamp: "2026-03-13T10:05:00Z", Schema: "shop", Table: "orders", Operation: "CREATE TABLE", Statement: "CREATE TABLE orders (id INT)"},
			},
		},
	}

	result := BuildCompareResult(current, baseline)

	if result.DiagnosticsDelta.DDLChanges.BaselineCount != 1 {
		t.Fatalf("expected baseline DDL count 1, got %d", result.DiagnosticsDelta.DDLChanges.BaselineCount)
	}
	if result.DiagnosticsDelta.DDLChanges.CurrentCount != 2 {
		t.Fatalf("expected current DDL count 2, got %d", result.DiagnosticsDelta.DDLChanges.CurrentCount)
	}
	if result.DiagnosticsDelta.DDLChanges.Delta != 1 {
		t.Fatalf("expected DDL delta 1, got %d", result.DiagnosticsDelta.DDLChanges.Delta)
	}
}

func TestBuildCompareResultProducesTxnDiagnosticDelta(t *testing.T) {
	current := InputReport{
		Summary: InputSummary{TotalRows: 1000, TotalTransactions: 50, TotalEvents: 1200},
		Diagnostics: InputDiagnostics{
			LargestTransactions: []InputTransaction{
				{TxnKey: "txn-c1", TotalRows: 500, Duration: "2s"},
			},
			LongestTransactions: []InputTransaction{
				{TxnKey: "txn-c2", TotalRows: 100, Duration: "30s"},
			},
		},
	}
	baseline := InputReport{
		Summary: InputSummary{TotalRows: 800, TotalTransactions: 40, TotalEvents: 1000},
		Diagnostics: InputDiagnostics{
			LargestTransactions: []InputTransaction{
				{TxnKey: "txn-b1", TotalRows: 300, Duration: "1s"},
			},
			LongestTransactions: []InputTransaction{
				{TxnKey: "txn-b2", TotalRows: 50, Duration: "15s"},
			},
		},
	}

	result := BuildCompareResult(current, baseline)

	// Largest transaction delta
	got := result.DiagnosticsDelta.TxnDiagnostics.LargestTxnDelta
	if got.BaselineRows != 300 {
		t.Fatalf("expected largest baseline rows 300, got %d", got.BaselineRows)
	}
	if got.CurrentRows != 500 {
		t.Fatalf("expected largest current rows 500, got %d", got.CurrentRows)
	}
	if got.DeltaRows != 200 {
		t.Fatalf("expected largest delta rows 200, got %d", got.DeltaRows)
	}
	if got.BaselineKey != "txn-b1" {
		t.Fatalf("expected largest baseline key txn-b1, got %q", got.BaselineKey)
	}
	if got.CurrentKey != "txn-c1" {
		t.Fatalf("expected largest current key txn-c1, got %q", got.CurrentKey)
	}

	// Longest transaction delta (duration-based)
	gotLong := result.DiagnosticsDelta.TxnDiagnostics.LongestTxnDelta
	if gotLong.BaselineKey != "txn-b2" {
		t.Fatalf("expected longest baseline key txn-b2, got %q", gotLong.BaselineKey)
	}
	if gotLong.CurrentKey != "txn-c2" {
		t.Fatalf("expected longest current key txn-c2, got %q", gotLong.CurrentKey)
	}
	if gotLong.BaselineDuration != "15s" {
		t.Fatalf("expected longest baseline duration 15s, got %q", gotLong.BaselineDuration)
	}
	if gotLong.CurrentDuration != "30s" {
		t.Fatalf("expected longest current duration 30s, got %q", gotLong.CurrentDuration)
	}
}

func TestBuildCompareResultProducesHotIntervalDelta(t *testing.T) {
	current := InputReport{
		Summary: InputSummary{TotalRows: 1000, TotalTransactions: 50, TotalEvents: 1200},
		Diagnostics: InputDiagnostics{
			HotIntervals: []InputHotInterval{
				{Minute: "2026-03-20T10:05:00Z", TotalRows: 600, TxnCount: 10, BinlogBytes: 10240},
				{Minute: "2026-03-20T10:10:00Z", TotalRows: 300, TxnCount: 5, BinlogBytes: 5120},
			},
		},
	}
	baseline := InputReport{
		Summary: InputSummary{TotalRows: 800, TotalTransactions: 40, TotalEvents: 1000},
		Diagnostics: InputDiagnostics{
			HotIntervals: []InputHotInterval{
				{Minute: "2026-03-13T10:05:00Z", TotalRows: 400, TxnCount: 8, BinlogBytes: 8192},
			},
		},
	}

	result := BuildCompareResult(current, baseline)

	got := result.DiagnosticsDelta.HotIntervalDelta
	if got.BaselineTopRows != 400 {
		t.Fatalf("expected baseline top rows 400, got %d", got.BaselineTopRows)
	}
	if got.CurrentTopRows != 600 {
		t.Fatalf("expected current top rows 600, got %d", got.CurrentTopRows)
	}
	if got.DeltaTopRows != 200 {
		t.Fatalf("expected delta top rows 200, got %d", got.DeltaTopRows)
	}
	if got.BaselineCount != 1 {
		t.Fatalf("expected baseline hot interval count 1, got %d", got.BaselineCount)
	}
	if got.CurrentCount != 2 {
		t.Fatalf("expected current hot interval count 2, got %d", got.CurrentCount)
	}
}

func TestBuildCompareResultProducesEventMixDelta(t *testing.T) {
	current := InputReport{
		Summary: InputSummary{TotalRows: 1000, TotalTransactions: 50, TotalEvents: 1200},
		Timeseries: InputTimeseries{
			InsertEventSeries:    []InputTimeseriesPoint{{Minute: "2026-03-20T10:00:00Z", Value: 100}},
			UpdateEventSeries:    []InputTimeseriesPoint{{Minute: "2026-03-20T10:00:00Z", Value: 200}},
			DeleteEventSeries:    []InputTimeseriesPoint{{Minute: "2026-03-20T10:00:00Z", Value: 50}},
			DDLEventSeries:       []InputTimeseriesPoint{{Minute: "2026-03-20T10:00:00Z", Value: 5}},
		},
	}
	baseline := InputReport{
		Summary: InputSummary{TotalRows: 800, TotalTransactions: 40, TotalEvents: 1000},
		Timeseries: InputTimeseries{
			InsertEventSeries:    []InputTimeseriesPoint{{Minute: "2026-03-13T10:00:00Z", Value: 80}},
			UpdateEventSeries:    []InputTimeseriesPoint{{Minute: "2026-03-13T10:00:00Z", Value: 150}},
			DeleteEventSeries:    []InputTimeseriesPoint{{Minute: "2026-03-13T10:00:00Z", Value: 40}},
			DDLEventSeries:       []InputTimeseriesPoint{{Minute: "2026-03-13T10:00:00Z", Value: 2}},
		},
	}

	result := BuildCompareResult(current, baseline)

	got := result.DiagnosticsDelta.EventMixDelta
	if got.InsertDelta != 20 {
		t.Fatalf("expected insert delta 20, got %d", got.InsertDelta)
	}
	if got.UpdateDelta != 50 {
		t.Fatalf("expected update delta 50, got %d", got.UpdateDelta)
	}
	if got.DeleteDelta != 10 {
		t.Fatalf("expected delete delta 10, got %d", got.DeleteDelta)
	}
	if got.DDLDelta != 3 {
		t.Fatalf("expected DDL delta 3, got %d", got.DDLDelta)
	}
}

func TestBuildCompareResultHandlesEmptyDiagnostics(t *testing.T) {
	current := InputReport{
		Summary: InputSummary{TotalRows: 1000, TotalTransactions: 50, TotalEvents: 1200},
	}
	baseline := InputReport{
		Summary: InputSummary{TotalRows: 800, TotalTransactions: 40, TotalEvents: 1000},
	}

	result := BuildCompareResult(current, baseline)

	// All delta fields should be zero-valued, not panic
	if result.DiagnosticsDelta.DDLChanges.Delta != 0 {
		t.Fatalf("expected zero DDL delta for empty diagnostics, got %d", result.DiagnosticsDelta.DDLChanges.Delta)
	}
	if result.DiagnosticsDelta.HotIntervalDelta.DeltaTopRows != 0 {
		t.Fatalf("expected zero hot interval delta for empty diagnostics")
	}
}

func TestBuildCompareResultDDLChangesIdentifiesAddedAndRemoved(t *testing.T) {
	current := InputReport{
		Summary: InputSummary{TotalRows: 1000, TotalTransactions: 50, TotalEvents: 1200},
		Diagnostics: InputDiagnostics{
			DDLEvents: []InputDDLEvent{
				{Timestamp: "2026-03-20T10:05:00Z", Schema: "shop", Table: "orders", Operation: "ALTER TABLE", Statement: "ALTER TABLE orders ADD status INT"},
				{Timestamp: "2026-03-20T10:10:00Z", Schema: "shop", Table: "payments", Operation: "CREATE INDEX", Statement: "CREATE INDEX idx ON payments(id)"},
			},
		},
	}
	baseline := InputReport{
		Summary: InputSummary{TotalRows: 800, TotalTransactions: 40, TotalEvents: 1000},
		Diagnostics: InputDiagnostics{
			DDLEvents: []InputDDLEvent{
				{Timestamp: "2026-03-13T10:05:00Z", Schema: "shop", Table: "legacy", Operation: "DROP TABLE", Statement: "DROP TABLE legacy"},
			},
		},
	}

	result := BuildCompareResult(current, baseline)

	if len(result.DiagnosticsDelta.DDLChanges.Added) != 2 {
		t.Fatalf("expected 2 added DDL events, got %d", len(result.DiagnosticsDelta.DDLChanges.Added))
	}
	if len(result.DiagnosticsDelta.DDLChanges.Removed) != 1 {
		t.Fatalf("expected 1 removed DDL event, got %d", len(result.DiagnosticsDelta.DDLChanges.Removed))
	}

	removed := result.DiagnosticsDelta.DDLChanges.Removed[0]
	if !strings.Contains(removed.Operation, "DROP") {
		t.Fatalf("expected removed DDL to be DROP TABLE, got %q", removed.Operation)
	}
}
