// Package compare verifies compare diagnostic deltas and replay evidence.
// input: synthetic current and baseline diagnostic reports.
// output: assertions for diagnostic metrics, spans, and command degradation.
// pos: regression coverage for compare diagnostic result construction.
// note: if this file changes, update this header and internal/compare/README.md.
package compare

import (
	"strings"
	"testing"

	"binlogviz/internal/model"
	"binlogviz/internal/report"
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

func TestBuildCompareResultPreservesCurrentTxnReplayEvidence(t *testing.T) {
	replayAvailable := true
	current := InputReport{
		Diagnostics: InputDiagnostics{
			LargestTransactions: []InputTransaction{{
				TxnKey:          "txn-current",
				TotalRows:       2000,
				EventCount:      8,
				BinlogBytes:     224,
				BinlogFileStart: "minimal.binlog",
				BinlogFileEnd:   "minimal.binlog",
				PosStart:        962,
				PosEnd:          1186,
				Completeness:    "partial_end",
				ReplayAvailable: &replayAvailable,
				ReplayScope:     "full_transaction",
				MysqlbinlogCmd:  "mysqlbinlog --base64-output=DECODE-ROWS -v --start-position=962 --stop-position=1186 /tmp/minimal.binlog",
			}},
			LongestTransactions: []InputTransaction{{
				TxnKey:          "txn-long",
				Duration:        "30s",
				EventCount:      4,
				BinlogBytes:     4096,
				BinlogFileStart: "mysql-bin.000044",
				BinlogFileEnd:   "mysql-bin.000045",
				PosStart:        300,
				PosEnd:          520,
				Completeness:    "complete",
				ReplayAvailable: &replayAvailable,
				ReplayScope:     "full_transaction",
				MysqlbinlogCmd:  "mariadb-binlog --base64-output=DECODE-ROWS -v --start-position=300 /tmp/mysql-bin.000044\nmariadb-binlog --base64-output=DECODE-ROWS -v --stop-position=520 /tmp/mysql-bin.000045",
			}},
		},
	}

	result := BuildCompareResult(current, InputReport{})
	largest := result.DiagnosticsDelta.TxnDiagnostics.LargestTxnDelta.CurrentEvidence
	if largest == nil || largest.BinlogSpan != "minimal.binlog:962-1186" {
		t.Fatalf("expected current largest span, got %+v", largest)
	}
	if largest.MysqlbinlogCmd != current.Diagnostics.LargestTransactions[0].MysqlbinlogCmd {
		t.Fatalf("expected current largest command to be preserved, got %q", largest.MysqlbinlogCmd)
	}
	if largest.Completeness != "partial_end" || largest.ReplayAvailable == nil || !*largest.ReplayAvailable || largest.ReplayScope != "full_transaction" {
		t.Fatalf("expected current largest replay contract to be preserved, got %+v", largest)
	}

	longest := result.DiagnosticsDelta.TxnDiagnostics.LongestTxnDelta.CurrentEvidence
	if longest == nil || longest.BinlogSpan != "mysql-bin.000044:300-mysql-bin.000045:520" {
		t.Fatalf("expected current longest cross-file span, got %+v", longest)
	}
	if longest.MysqlbinlogCmd != current.Diagnostics.LongestTransactions[0].MysqlbinlogCmd {
		t.Fatalf("expected current longest command to be preserved, got %q", longest.MysqlbinlogCmd)
	}
}

func TestTransactionEvidenceForDoesNotTrustRetainedSpanWithoutFullReplayContract(t *testing.T) {
	replayAvailable := false
	evidence := TransactionEvidenceFor(InputTransaction{
		TxnKey:          "txn-xid",
		TotalRows:       2000,
		EventCount:      8,
		BinlogBytes:     31,
		BinlogFileStart: "minimal.binlog",
		BinlogFileEnd:   "minimal.binlog",
		PosStart:        1155,
		PosEnd:          1186,
		Completeness:    "partial_start",
		ReplayAvailable: &replayAvailable,
		MysqlbinlogCmd:  "mysqlbinlog --start-position=1155 --stop-position=1186 /tmp/minimal.binlog",
	})
	if evidence == nil {
		t.Fatal("expected unusable span to retain transaction evidence")
	}
	if evidence.MysqlbinlogCmd != "" {
		t.Fatalf("retained span without full replay evidence must not retain a command, got %q", evidence.MysqlbinlogCmd)
	}
	if evidence.Completeness != "partial_start" || evidence.ReplayAvailable == nil || *evidence.ReplayAvailable {
		t.Fatalf("evidence contract was not preserved: %+v", evidence)
	}
}

func TestReportV3ReplayEvidencePreservesFullSpanCommandWithoutTrustingRetainedLegacySpan(t *testing.T) {
	payload, err := report.RenderJSON(model.AnalysisResult{
		Summary: model.WorkloadSummary{TotalTransactions: 1, PartialTransactions: 1},
		Transactions: []model.Transaction{{
			TxnKey:          "txn-53",
			Completeness:    model.TransactionPartialEnd,
			TotalRows:       1,
			EventCount:      2,
			BinlogPathStart: "mysql-bin.000008",
			BinlogPathEnd:   "mysql-bin.000008",
			PositionStart:   3200,
			PositionEnd:     3234,
			FullReplaySpan: &model.TransactionReplaySpan{
				BinlogPathStart: "mysql-bin.000008",
				BinlogPathEnd:   "mysql-bin.000008",
				PositionStart:   3183,
				PositionEnd:     3449,
			},
		}},
	})
	if err != nil {
		t.Fatalf("RenderJSON: %v", err)
	}
	decoded, err := DecodeReportJSON([]byte(payload))
	if err != nil {
		t.Fatalf("DecodeReportJSON: %v", err)
	}

	evidence := TransactionEvidenceFor(decoded.Transactions[0])
	if evidence == nil || evidence.BinlogSpan != "mysql-bin.000008:3200-3234" || evidence.Completeness != "partial_end" || evidence.ReplayAvailable == nil || !*evidence.ReplayAvailable || evidence.ReplayScope != "full_transaction" {
		t.Fatalf("report-v3 evidence contract was not preserved: %+v", evidence)
	}
	if !strings.Contains(evidence.MysqlbinlogCmd, "--start-position=3183 --stop-position=3449") || strings.Contains(evidence.MysqlbinlogCmd, "--start-position=3200") {
		t.Fatalf("replay command did not preserve the trusted full span: %q", evidence.MysqlbinlogCmd)
	}

	legacy := decoded.Transactions[0]
	legacy.ReplayAvailable = nil
	legacy.ReplayScope = ""
	if got := TransactionEvidenceFor(legacy); got == nil || got.BinlogSpan == "" || got.MysqlbinlogCmd != "" {
		t.Fatalf("legacy retained span should remain evidence without authorizing replay: %+v", got)
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
			InsertEventSeries: []InputTimeseriesPoint{{Minute: "2026-03-20T10:00:00Z", Value: 100}},
			UpdateEventSeries: []InputTimeseriesPoint{{Minute: "2026-03-20T10:00:00Z", Value: 200}},
			DeleteEventSeries: []InputTimeseriesPoint{{Minute: "2026-03-20T10:00:00Z", Value: 50}},
			DDLEventSeries:    []InputTimeseriesPoint{{Minute: "2026-03-20T10:00:00Z", Value: 5}},
		},
	}
	baseline := InputReport{
		Summary: InputSummary{TotalRows: 800, TotalTransactions: 40, TotalEvents: 1000},
		Timeseries: InputTimeseries{
			InsertEventSeries: []InputTimeseriesPoint{{Minute: "2026-03-13T10:00:00Z", Value: 80}},
			UpdateEventSeries: []InputTimeseriesPoint{{Minute: "2026-03-13T10:00:00Z", Value: 150}},
			DeleteEventSeries: []InputTimeseriesPoint{{Minute: "2026-03-13T10:00:00Z", Value: 40}},
			DDLEventSeries:    []InputTimeseriesPoint{{Minute: "2026-03-13T10:00:00Z", Value: 2}},
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
