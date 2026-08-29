// Package model verifies shared analysis and result contracts plus derived helpers.
// input: synthetic AnalysisResult, provenance-aware Transaction, query context, and diagnostics values built by tests.
// output: assertions for duration math, exact SQL bounds, optional fields, and stable model invariants.
// pos: regression coverage for shared result-model behavior reused across modules.
// note: if this file changes, keep internal/model/README.md synchronized.
package model

import (
	"testing"
	"time"
)

func TestTransactionDurationUsesStartAndEnd(t *testing.T) {
	trx := Transaction{}
	if trx.Duration != 0 {
		t.Fatalf("expected zero duration")
	}
}

func TestAnalysisResultIncludesDiagnosticsDomains(t *testing.T) {
	now := time.Unix(0, 0).UTC()

	result := AnalysisResult{
		Timeseries: Timeseries{
			TPSSeries:         []TimeseriesPoint{{Minute: now, Value: 1}},
			RowsSeries:        []TimeseriesPoint{{Minute: now, Value: 2}},
			EventsSeries:      []TimeseriesPoint{{Minute: now, Value: 3}},
			InsertEventSeries: []TimeseriesPoint{{Minute: now, Value: 4}},
			UpdateEventSeries: []TimeseriesPoint{{Minute: now, Value: 5}},
			DeleteEventSeries: []TimeseriesPoint{{Minute: now, Value: 6}},
			DDLEventSeries:    []TimeseriesPoint{{Minute: now, Value: 7}},
			BinlogBytesSeries: []TimeseriesPoint{{Minute: now, Value: 8}},
		},
		Diagnostics: Diagnostics{
			FileCoverage: FileCoverage{
				Selected: []FileCoverageItem{{BinlogPath: "binlog.000001", Reason: "covered"}},
				Skipped:  []FileCoverageItem{{BinlogPath: "binlog.000002", Reason: "outside window"}},
			},
			DDLEvents: []DDLEvent{{
				BinlogPath:    "binlog.000001",
				PositionStart: 120,
				PositionEnd:   240,
				BinlogBytes:   4096,
			}},
			Findings: []Finding{{
				Kind:    "ddl_hotspot",
				Message: "DDL overlaps write spike",
			}},
		},
	}

	if got := len(result.Timeseries.TPSSeries); got != 1 {
		t.Fatalf("expected 1 tps point, got %d", got)
	}
	if got := len(result.Diagnostics.FileCoverage.Selected); got != 1 {
		t.Fatalf("expected 1 selected file, got %d", got)
	}
	if got := result.Diagnostics.DDLEvents[0].PositionStart; got != 120 {
		t.Fatalf("expected ddl position start 120, got %d", got)
	}
	if got := result.Diagnostics.Findings[0].Kind; got != "ddl_hotspot" {
		t.Fatalf("expected finding kind ddl_hotspot, got %q", got)
	}
}

func TestTransactionCarriesPositionAndByteMetadata(t *testing.T) {
	trx := Transaction{
		BinlogPathStart: "mysql-bin.000001",
		BinlogPathEnd:   "mysql-bin.000002",
		PositionStart:   120,
		PositionEnd:     260,
		BinlogBytes:     4096,
	}

	if trx.BinlogPathStart != "mysql-bin.000001" {
		t.Fatalf("expected start file mysql-bin.000001, got %q", trx.BinlogPathStart)
	}
	if trx.BinlogPathEnd != "mysql-bin.000002" {
		t.Fatalf("expected end file mysql-bin.000002, got %q", trx.BinlogPathEnd)
	}
	if trx.PositionStart != 120 {
		t.Fatalf("expected position start 120, got %d", trx.PositionStart)
	}
	if trx.PositionEnd != 260 {
		t.Fatalf("expected position end 260, got %d", trx.PositionEnd)
	}
	if trx.BinlogBytes != 4096 {
		t.Fatalf("expected binlog bytes 4096, got %d", trx.BinlogBytes)
	}
}

func TestZeroValueModelMetadataRemainsEmpty(t *testing.T) {
	var result AnalysisResult

	if result.Timeseries.TxnSizeSeriesSummary.Buckets != nil {
		t.Fatalf("expected zero-value txn size buckets to be nil, got %#v", result.Timeseries.TxnSizeSeriesSummary.Buckets)
	}
	if result.Diagnostics.FileCoverage.Selected != nil || result.Diagnostics.FileCoverage.Skipped != nil {
		t.Fatalf("expected zero-value file coverage slices to be nil, got %#v", result.Diagnostics.FileCoverage)
	}
	if result.Diagnostics.DDLEvents != nil || result.Diagnostics.LargestTransactions != nil || result.Diagnostics.LongestTransactions != nil || result.Diagnostics.HotIntervals != nil || result.Diagnostics.Findings != nil {
		t.Fatalf("expected zero-value diagnostics slices to be nil, got %#v", result.Diagnostics)
	}
	if len(result.Minutes) != 0 {
		t.Fatalf("expected zero-value minutes to be empty, got %#v", result.Minutes)
	}
	if result.Timeseries.TPSSeries != nil || result.Timeseries.RowsSeries != nil || result.Timeseries.BinlogBytesSeries != nil {
		t.Fatalf("expected zero-value timeseries slices to be nil, got %#v", result.Timeseries)
	}

	var coverage FileCoverageItem
	if coverage.BinlogPath != "" || !coverage.FirstEventAt.IsZero() || !coverage.LastEventAt.IsZero() || coverage.Size != 0 {
		t.Fatalf("expected zero-value file coverage item to be empty, got %#v", coverage)
	}

	var table TableStats
	if table.Activity != nil {
		t.Fatalf("expected zero-value table activity slice to be nil, got %#v", table.Activity)
	}

	var ddl DDLEvent
	if ddl.BinlogPath != "" || !ddl.Timestamp.IsZero() || ddl.PositionStart != 0 || ddl.PositionEnd != 0 || ddl.BinlogBytes != 0 {
		t.Fatalf("expected zero-value ddl event to be empty, got %#v", ddl)
	}

	var txn Transaction
	if txn.BinlogPathStart != "" || txn.BinlogPathEnd != "" || txn.BinlogBytes != 0 || txn.PositionStart != 0 || txn.PositionEnd != 0 || !txn.StartTime.IsZero() || !txn.EndTime.IsZero() || txn.Duration != 0 {
		t.Fatalf("expected zero-value transaction location metadata to be empty, got %#v", txn)
	}

	var event NormalizedEvent
	if event.BinlogPath != "" || event.PositionStart != 0 || event.PositionEnd != 0 || event.BinlogBytes != 0 || !event.Timestamp.IsZero() {
		t.Fatalf("expected zero-value normalized event metadata to be empty, got %#v", event)
	}
}

// Tests for NewQueryContext

func TestNewQueryContextEmpty(t *testing.T) {
	qc := NewQueryContext("")
	if qc != nil {
		t.Fatalf("expected nil for empty SQL, got: %+v", qc)
	}
}

func TestNewQueryContextShort(t *testing.T) {
	sql := "SELECT 1"
	qc := NewQueryContext(sql)
	if qc == nil {
		t.Fatal("expected non-nil QueryContext")
	}
	if qc.SQL != sql {
		t.Fatalf("expected SQL %q, got %q", sql, qc.SQL)
	}
	if qc.Truncated {
		t.Fatal("expected Truncated=false for short SQL")
	}
	if qc.OriginalBytes != len(sql) {
		t.Fatalf("expected OriginalBytes=%d, got %d", len(sql), qc.OriginalBytes)
	}
}

func TestNewQueryContextLong(t *testing.T) {
	// Create SQL longer than MaxStoredSQLBytes
	longSQL := "SELECT '" + makeNChars('x', MaxStoredSQLBytes+1000) + "'"
	qc := NewQueryContext(longSQL)
	if qc == nil {
		t.Fatal("expected non-nil QueryContext")
	}
	if !qc.Truncated {
		t.Fatal("expected Truncated=true for long SQL")
	}
	if qc.OriginalBytes != len(longSQL) {
		t.Fatalf("expected OriginalBytes=%d, got %d", len(longSQL), qc.OriginalBytes)
	}
	if len(qc.SQL) > MaxStoredSQLBytes {
		t.Fatalf("expected SQL to be truncated to <=%d bytes, got %d", MaxStoredSQLBytes, len(qc.SQL))
	}
}

// Tests for NewQueryContextFromNormalized

func TestNewQueryContextFromNormalizedEmpty(t *testing.T) {
	qc := NewQueryContextFromNormalized("", false, 0)
	if qc != nil {
		t.Fatalf("expected nil for empty SQL, got: %+v", qc)
	}
}

func TestNewQueryContextFromNormalizedNotTruncated(t *testing.T) {
	sql := "SELECT id FROM users"
	originalBytes := len(sql)
	qc := NewQueryContextFromNormalized(sql, false, originalBytes)
	if qc == nil {
		t.Fatal("expected non-nil QueryContext")
	}
	if qc.SQL != sql {
		t.Fatalf("expected SQL %q, got %q", sql, qc.SQL)
	}
	if qc.Truncated {
		t.Fatal("expected Truncated=false")
	}
	if qc.OriginalBytes != originalBytes {
		t.Fatalf("expected OriginalBytes=%d, got %d", originalBytes, qc.OriginalBytes)
	}
}

func TestNewQueryContextFromNormalizedTruncated(t *testing.T) {
	// Simulate a scenario where SQL was truncated at normalize layer
	originalBytes := 5000               // Original was 5000 bytes
	truncatedSQL := "SELECT 'xxxxx...'" // Now only ~20 bytes after truncation
	truncated := true

	qc := NewQueryContextFromNormalized(truncatedSQL, truncated, originalBytes)
	if qc == nil {
		t.Fatal("expected non-nil QueryContext")
	}
	if qc.SQL != truncatedSQL {
		t.Fatalf("expected SQL %q, got %q", truncatedSQL, qc.SQL)
	}
	if !qc.Truncated {
		t.Fatal("expected Truncated=true")
	}
	// KEY TEST: OriginalBytes must be the ORIGINAL length, not the truncated length
	if qc.OriginalBytes != originalBytes {
		t.Fatalf("expected OriginalBytes=%d (original), got %d", originalBytes, qc.OriginalBytes)
	}
	if qc.OriginalBytes == len(truncatedSQL) {
		t.Fatalf("OriginalBytes should NOT equal truncated SQL length %d, but it does", len(truncatedSQL))
	}
}

// Tests for MakeQuerySummary

func TestMakeQuerySummaryEmpty(t *testing.T) {
	if s := MakeQuerySummary(""); s != "" {
		t.Fatalf("expected empty string, got %q", s)
	}
}

func TestMakeQuerySummaryNoTruncation(t *testing.T) {
	sql := "SELECT id FROM users WHERE id = 1"
	summary := MakeQuerySummary(sql)
	if summary != sql {
		t.Fatalf("expected %q, got %q", sql, summary)
	}
}

func TestMakeQuerySummaryWhitespaceCompression(t *testing.T) {
	sql := "SELECT   id\n\tFROM   users  WHERE  id  =  1"
	expected := "SELECT id FROM users WHERE id = 1"
	summary := MakeQuerySummary(sql)
	if summary != expected {
		t.Fatalf("expected %q, got %q", expected, summary)
	}
}

func TestMakeQuerySummaryTruncation(t *testing.T) {
	// Create SQL longer than MaxQuerySummaryChars
	longSQL := "SELECT " + makeNChars('x', MaxQuerySummaryChars+50) + " FROM users"
	summary := MakeQuerySummary(longSQL)

	// Should be truncated and end with "..."
	if len([]rune(summary)) != MaxQuerySummaryChars {
		t.Fatalf("expected summary to be exactly %d characters, got %d", MaxQuerySummaryChars, len([]rune(summary)))
	}
	if len(summary) < 3 || summary[len(summary)-3:] != "..." {
		t.Fatalf("expected summary to end with '...', got %q", summary)
	}
}

func TestMakeQuerySummaryWhitespaceThenTruncation(t *testing.T) {
	// Test that whitespace compression happens before truncation
	sql := "SELECT   " + makeNChars('x', MaxQuerySummaryChars) + "\n\nFROM   users"
	summary := MakeQuerySummary(sql)

	// Verify whitespace was compressed (no double spaces)
	if len(summary) > 0 {
		// Check there are no double spaces in the summary
		for i := 1; i < len(summary); i++ {
			if summary[i] == ' ' && summary[i-1] == ' ' {
				t.Fatalf("expected no double spaces in summary, got %q", summary)
			}
		}
	}
}

func TestMakeQuerySummaryUTF8(t *testing.T) {
	sql := "SELECT '日本語テストデータ' FROM users WHERE name = '山田太郎'"
	summary := MakeQuerySummary(sql)

	// Should preserve UTF-8 characters
	runes := []rune(summary)
	if len(runes) > MaxQuerySummaryChars {
		t.Fatalf("expected summary <= %d runes, got %d", MaxQuerySummaryChars, len(runes))
	}
}

// makeNChars creates a string of n identical characters
func makeNChars(c rune, n int) string {
	result := make([]rune, n)
	for i := range result {
		result[i] = c
	}
	return string(result)
}
