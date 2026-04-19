// Package analyzer verifies streaming report aggregation parity with finalize-time builders.
// input: synthetic normalized events, completed transactions, and minute buckets.
// output: assertions that ReportAggregator snapshots match existing analyzer builder semantics.
// pos: regression coverage for replacing QueryAllTransactions-based report finalization.
// note: if this file changes, keep internal/analyzer/README.md synchronized.
package analyzer

import (
	"fmt"
	"testing"
	"time"

	"binlogviz/internal/model"
)

func TestReportAggregatorMatchesExistingTransactionDerivedOutputs(t *testing.T) {
	base := time.Date(2026, 4, 19, 10, 0, 0, 0, time.UTC)
	txns := []model.Transaction{
		{
			TxnKey:       "txn-1",
			StartTime:    base,
			EndTime:      base.Add(2 * time.Second),
			Duration:     2 * time.Second,
			TotalRows:    10,
			EventCount:   3,
			BinlogBytes:  1000,
			Tables:       map[string]int{"shop.orders": 10},
			Operations:   map[string]int{"INSERT": 10},
			QuerySummary: "insert orders",
		},
		{
			TxnKey:       "txn-2",
			StartTime:    base.Add(time.Minute),
			EndTime:      base.Add(time.Minute + 45*time.Second),
			Duration:     45 * time.Second,
			TotalRows:    2000,
			EventCount:   5,
			BinlogBytes:  9000,
			Tables:       map[string]int{"shop.orders": 1500, "shop.items": 500},
			Operations:   map[string]int{"UPDATE": 2000},
			QuerySummary: "update orders",
			QueryContext: &model.QueryContext{SQL: "update orders", Truncated: true},
		},
	}
	minutes := []model.MinuteBucket{
		{Minute: base, TotalRows: 10, TxnCount: 1, EventCount: 3, BinlogBytes: 1000},
		{Minute: base.Add(time.Minute), TotalRows: 2000, TxnCount: 1, EventCount: 5, BinlogBytes: 9000},
	}

	agg := NewReportAggregator(DefaultOptions())
	for _, txn := range txns {
		agg.ConsumeTransaction(txn)
	}
	for _, bucket := range minutes {
		agg.ConsumeMinuteBucket(bucket)
	}
	snapshot := agg.Snapshot()

	expectedPatterns := BuildPatterns(txns)
	expectedLargest, expectedLongest := SelectDiagnosticTransactions(txns, 5)

	if snapshot.Summary.TotalTransactions != len(txns) {
		t.Fatalf("total transactions = %d, want %d", snapshot.Summary.TotalTransactions, len(txns))
	}
	if snapshot.Summary.TotalRows != 2010 {
		t.Fatalf("total rows = %d, want 2010", snapshot.Summary.TotalRows)
	}
	if snapshot.Warnings != 1 {
		t.Fatalf("warnings = %d, want 1", snapshot.Warnings)
	}
	if len(snapshot.Patterns) != len(expectedPatterns) {
		t.Fatalf("patterns = %d, want %d", len(snapshot.Patterns), len(expectedPatterns))
	}
	if snapshot.Patterns[0].PatternKey != expectedPatterns[0].PatternKey {
		t.Fatalf("top pattern = %q, want %q", snapshot.Patterns[0].PatternKey, expectedPatterns[0].PatternKey)
	}
	if snapshot.Diagnostics.LargestTransactions[0].TxnKey != expectedLargest[0].TxnKey {
		t.Fatalf("largest txn = %q, want %q", snapshot.Diagnostics.LargestTransactions[0].TxnKey, expectedLargest[0].TxnKey)
	}
	if snapshot.Diagnostics.LongestTransactions[0].TxnKey != expectedLongest[0].TxnKey {
		t.Fatalf("longest txn = %q, want %q", snapshot.Diagnostics.LongestTransactions[0].TxnKey, expectedLongest[0].TxnKey)
	}
	if len(snapshot.Timeseries.TxnSizeSeriesSummary.Buckets) == 0 {
		t.Fatal("expected transaction size histogram buckets")
	}
}

func TestReportAggregatorDoesNotRetainAllTransactions(t *testing.T) {
	agg := NewReportAggregator(DefaultOptions())
	base := time.Date(2026, 4, 19, 10, 0, 0, 0, time.UTC)
	for i := 0; i < 1000; i++ {
		agg.ConsumeTransaction(model.Transaction{
			TxnKey:     fmt.Sprintf("txn-%d", i+1),
			StartTime:  base,
			EndTime:    base,
			TotalRows:  i + 1,
			EventCount: 1,
			Tables:     map[string]int{"shop.orders": i + 1},
			Operations: map[string]int{"UPDATE": i + 1},
		})
	}

	snapshot := agg.Snapshot()
	if len(snapshot.Transactions) > DefaultOptions().TopTransactions {
		t.Fatalf("snapshot retained %d top transactions, want <= %d", len(snapshot.Transactions), DefaultOptions().TopTransactions)
	}
	if len(snapshot.Diagnostics.LargestTransactions) > 5 {
		t.Fatalf("largest transactions retained %d, want <= 5", len(snapshot.Diagnostics.LargestTransactions))
	}
}

func TestReportAggregatorPatternParity(t *testing.T) {
	base := time.Date(2026, 4, 19, 10, 0, 0, 0, time.UTC)
	txns := []model.Transaction{
		{
			TxnKey:       "txn-1",
			StartTime:    base,
			EndTime:      base,
			TotalRows:    50,
			EventCount:   2,
			Tables:       map[string]int{"shop.orders": 50},
			Operations:   map[string]int{"INSERT": 50},
			QuerySummary: "batch insert",
		},
		{
			TxnKey:       "txn-2",
			StartTime:    base.Add(time.Minute),
			EndTime:      base.Add(time.Minute),
			TotalRows:    30,
			EventCount:   1,
			Tables:       map[string]int{"shop.orders": 30},
			Operations:   map[string]int{"INSERT": 30},
			QuerySummary: "single insert",
		},
		{
			TxnKey:      "txn-3",
			StartTime:   base.Add(2 * time.Minute),
			EndTime:     base.Add(2 * time.Minute),
			TotalRows:   100,
			EventCount:  4,
			Tables:      map[string]int{"shop.items": 100},
			Operations:  map[string]int{"UPDATE": 100},
		},
		{
			TxnKey:      "txn-4",
			StartTime:   base.Add(3 * time.Minute),
			EndTime:     base.Add(3 * time.Minute),
			TotalRows:   80,
			EventCount:  3,
			Tables:      map[string]int{"shop.orders": 80},
			Operations:  map[string]int{"UPDATE": 80},
		},
	}

	agg := NewReportAggregator(DefaultOptions())
	for _, txn := range txns {
		agg.ConsumeTransaction(txn)
	}
	snapshot := agg.Snapshot()

	expected := BuildPatterns(txns)

	if len(snapshot.Patterns) != len(expected) {
		t.Fatalf("pattern count = %d, want %d", len(snapshot.Patterns), len(expected))
	}
	for i, got := range snapshot.Patterns {
		want := expected[i]
		if got.PatternKey != want.PatternKey {
			t.Errorf("pattern[%d] key = %q, want %q", i, got.PatternKey, want.PatternKey)
		}
		if got.TotalRows != want.TotalRows {
			t.Errorf("pattern[%d] TotalRows = %d, want %d", i, got.TotalRows, want.TotalRows)
		}
		if got.TxnCount != want.TxnCount {
			t.Errorf("pattern[%d] TxnCount = %d, want %d", i, got.TxnCount, want.TxnCount)
		}
		if got.AvgRowsPerTxn != want.AvgRowsPerTxn {
			t.Errorf("pattern[%d] AvgRowsPerTxn = %f, want %f", i, got.AvgRowsPerTxn, want.AvgRowsPerTxn)
		}
		if got.ShareOfRows != want.ShareOfRows {
			t.Errorf("pattern[%d] ShareOfRows = %f, want %f", i, got.ShareOfRows, want.ShareOfRows)
		}
		if got.ShareOfTransactions != want.ShareOfTransactions {
			t.Errorf("pattern[%d] ShareOfTransactions = %f, want %f", i, got.ShareOfTransactions, want.ShareOfTransactions)
		}
	}
}

func TestReportAggregatorTopSelectorParity(t *testing.T) {
	base := time.Date(2026, 4, 19, 10, 0, 0, 0, time.UTC)
	txns := []model.Transaction{
		{
			TxnKey: "txn-a", StartTime: base, EndTime: base,
			Duration: 1 * time.Second, TotalRows: 500, BinlogBytes: 5000,
			Tables: map[string]int{"t1": 500}, Operations: map[string]int{"INSERT": 500},
		},
		{
			TxnKey: "txn-b", StartTime: base, EndTime: base.Add(60 * time.Second),
			Duration: 60 * time.Second, TotalRows: 100, BinlogBytes: 1000,
			Tables: map[string]int{"t1": 50, "t2": 30, "t3": 20}, Operations: map[string]int{"UPDATE": 100},
		},
		{
			TxnKey: "txn-c", StartTime: base, EndTime: base.Add(30 * time.Second),
			Duration: 30 * time.Second, TotalRows: 1000, BinlogBytes: 10000,
			Tables: map[string]int{"t1": 1000}, Operations: map[string]int{"DELETE": 1000},
		},
		{
			TxnKey: "txn-d", StartTime: base, EndTime: base,
			Duration: 0, TotalRows: 50, BinlogBytes: 500,
			Tables: map[string]int{"t4": 50}, Operations: map[string]int{"INSERT": 50},
		},
	}

	agg := NewReportAggregator(DefaultOptions())
	for _, txn := range txns {
		agg.ConsumeTransaction(txn)
	}
	snapshot := agg.Snapshot()

	expectedLargest, expectedLongest := SelectDiagnosticTransactions(txns, 5)
	expectedWidest := SelectWidestTransactions(txns, 5)

	assertTxnKeys := func(t *testing.T, name string, got, want []model.Transaction) {
		t.Helper()
		if len(got) != len(want) {
			t.Fatalf("%s count = %d, want %d", name, len(got), len(want))
		}
		for i := range got {
			if got[i].TxnKey != want[i].TxnKey {
				t.Errorf("%s[%d] = %q, want %q", name, i, got[i].TxnKey, want[i].TxnKey)
			}
		}
	}
	assertTxnKeys(t, "largest", snapshot.Diagnostics.LargestTransactions, expectedLargest)
	assertTxnKeys(t, "longest", snapshot.Diagnostics.LongestTransactions, expectedLongest)
	assertTxnKeys(t, "widest", snapshot.Diagnostics.WidestTransactions, expectedWidest)
}

func TestReportAggregatorWarningsCount(t *testing.T) {
	base := time.Date(2026, 4, 19, 10, 0, 0, 0, time.UTC)
	txns := []model.Transaction{
		{
			TxnKey:    "txn-1", StartTime: base, EndTime: base,
			TotalRows: 10, EventCount: 1,
			Tables:    map[string]int{"t1": 10},
			Operations: map[string]int{"INSERT": 10},
		},
		{
			TxnKey: "txn-2", StartTime: base, EndTime: base,
			TotalRows: 20, EventCount: 1,
			Tables:    map[string]int{"t1": 20},
			Operations: map[string]int{"INSERT": 20},
			QueryContext: &model.QueryContext{SQL: "long query...", Truncated: true},
		},
		{
			TxnKey: "txn-3", StartTime: base, EndTime: base,
			TotalRows: 30, EventCount: 1,
			Tables:    map[string]int{"t1": 30},
			Operations: map[string]int{"INSERT": 30},
			QueryContext: &model.QueryContext{SQL: "another long query...", Truncated: true},
		},
		{
			TxnKey: "txn-4", StartTime: base, EndTime: base,
			TotalRows: 40, EventCount: 1,
			Tables:    map[string]int{"t1": 40},
			Operations: map[string]int{"INSERT": 40},
			QueryContext: &model.QueryContext{SQL: "short query", Truncated: false},
		},
	}

	agg := NewReportAggregator(DefaultOptions())
	for _, txn := range txns {
		agg.ConsumeTransaction(txn)
	}
	snapshot := agg.Snapshot()

	wantWarnings := 2 // txn-2 and txn-3 have Truncated=true
	if snapshot.Warnings != wantWarnings {
		t.Fatalf("warnings = %d, want %d", snapshot.Warnings, wantWarnings)
	}
}
