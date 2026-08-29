// Package analyzer verifies streaming report aggregation parity with finalize-time builders.
// input: synthetic normalized events, completed transactions, and minute buckets.
// output: assertions that ReportAggregator snapshots match existing analyzer builder semantics.
// pos: regression coverage for replacing QueryAllTransactions-based report finalization.
// note: if this file changes, keep internal/analyzer/README.md synchronized.
package analyzer

import (
	"fmt"
	"reflect"
	"strings"
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
	for _, reps := range agg.patternRepTxns {
		if len(reps) > maxRepresentativeTxns {
			t.Fatalf("pattern reps retained %d, want <= %d", len(reps), maxRepresentativeTxns)
		}
	}
}

func TestReportAggregatorDeletePatternDoesNotCiteInsertTxns(t *testing.T) {
	agg := NewReportAggregator(DefaultOptions())
	base := time.Date(2026, 4, 19, 10, 0, 0, 0, time.UTC)
	for i := 0; i < 6; i++ {
		agg.ConsumeTransaction(model.Transaction{
			TxnKey:     fmt.Sprintf("txn-insert-%d", i+1),
			StartTime:  base,
			EndTime:    base,
			TotalRows:  400000,
			EventCount: 1,
			Tables:     map[string]int{"dogfood_big.t": 400000},
			Operations: map[string]int{"INSERT": 400000},
		})
	}
	for i := 0; i < 20; i++ {
		agg.ConsumeTransaction(model.Transaction{
			TxnKey:     fmt.Sprintf("txn-del-%d", i+1),
			StartTime:  base,
			EndTime:    base,
			TotalRows:  80 - i,
			EventCount: 1,
			Tables:     map[string]int{"dogfood_big.t": 80 - i},
			Operations: map[string]int{"DELETE": 80 - i},
		})
	}

	snapshot := agg.Snapshot()
	var deleteKey string
	for _, p := range snapshot.Patterns {
		if _, ok := p.Operations["DELETE"]; ok && p.Operations["INSERT"] == 0 {
			deleteKey = p.PatternKey
			if got := formatSharePercent(p.ShareOfTransactions); got == "0%" && p.ShareOfTransactions > 0 {
				t.Fatalf("DELETE txn share %f formatted as 0%%", p.ShareOfTransactions)
			}
		}
	}
	if deleteKey == "" {
		t.Fatal("expected a DELETE-only pattern")
	}
	foundDelete := false
	for _, d := range snapshot.PatternDrilldowns {
		if d.PatternKey != deleteKey {
			continue
		}
		foundDelete = true
		if len(d.RepresentativeTransactions) == 0 {
			t.Fatal("DELETE pattern had no representative transactions")
		}
		for _, txn := range d.RepresentativeTransactions {
			if txn.TotalRows >= 400000 || strings.HasPrefix(txn.TxnKey, "txn-insert-") {
				t.Fatalf("DELETE pattern cited INSERT txn %s rows=%d", txn.TxnKey, txn.TotalRows)
			}
		}
	}
	if !foundDelete {
		t.Fatal("expected DELETE pattern to be selected for drilldown")
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
			TxnKey:     "txn-3",
			StartTime:  base.Add(2 * time.Minute),
			EndTime:    base.Add(2 * time.Minute),
			TotalRows:  100,
			EventCount: 4,
			Tables:     map[string]int{"shop.items": 100},
			Operations: map[string]int{"UPDATE": 100},
		},
		{
			TxnKey:     "txn-4",
			StartTime:  base.Add(3 * time.Minute),
			EndTime:    base.Add(3 * time.Minute),
			TotalRows:  80,
			EventCount: 3,
			Tables:     map[string]int{"shop.orders": 80},
			Operations: map[string]int{"UPDATE": 80},
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
			TxnKey: "txn-1", StartTime: base, EndTime: base,
			TotalRows: 10, EventCount: 1,
			Tables:     map[string]int{"t1": 10},
			Operations: map[string]int{"INSERT": 10},
		},
		{
			TxnKey: "txn-2", StartTime: base, EndTime: base,
			TotalRows: 20, EventCount: 1,
			Tables:       map[string]int{"t1": 20},
			Operations:   map[string]int{"INSERT": 20},
			QueryContext: &model.QueryContext{SQL: "long query...", Truncated: true},
		},
		{
			TxnKey: "txn-3", StartTime: base, EndTime: base,
			TotalRows: 30, EventCount: 1,
			Tables:       map[string]int{"t1": 30},
			Operations:   map[string]int{"INSERT": 30},
			QueryContext: &model.QueryContext{SQL: "another long query...", Truncated: true},
		},
		{
			TxnKey: "txn-4", StartTime: base, EndTime: base,
			TotalRows: 40, EventCount: 1,
			Tables:       map[string]int{"t1": 40},
			Operations:   map[string]int{"INSERT": 40},
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

func TestReportAggregatorOperationTimeseries(t *testing.T) {
	base := time.Date(2026, 4, 19, 10, 0, 0, 0, time.UTC)
	agg := NewReportAggregator(DefaultOptions())

	agg.ConsumeEvent(model.NormalizedEvent{Timestamp: base, Operation: "INSERT"})
	agg.ConsumeOperationEvent(model.NormalizedEvent{Timestamp: base, Operation: "INSERT"})
	agg.ConsumeOperationEvent(model.NormalizedEvent{Timestamp: base.Add(time.Second), Operation: "INSERT"})
	agg.ConsumeOperationEvent(model.NormalizedEvent{Timestamp: base.Add(2 * time.Second), Operation: "UPDATE"})
	agg.ConsumeOperationEvent(model.NormalizedEvent{Timestamp: base.Add(time.Minute), Operation: "DELETE"})

	agg.ConsumeMinuteBucket(model.MinuteBucket{Minute: base, TotalRows: 10, TxnCount: 1})
	agg.ConsumeMinuteBucket(model.MinuteBucket{Minute: base.Add(time.Minute), TotalRows: 5, TxnCount: 1})

	snapshot := agg.Snapshot()
	if len(snapshot.Timeseries.InsertEventSeries) == 0 {
		t.Fatal("expected insert event series points")
	}
	if snapshot.Timeseries.InsertEventSeries[0].Value != 2 {
		t.Fatalf("first minute inserts = %v, want 2", snapshot.Timeseries.InsertEventSeries[0].Value)
	}
	if snapshot.Timeseries.UpdateEventSeries[0].Value != 1 {
		t.Fatalf("first minute updates = %v, want 1", snapshot.Timeseries.UpdateEventSeries[0].Value)
	}
	if len(snapshot.Timeseries.DeleteEventSeries) < 2 {
		t.Fatalf("expected 2 minute points, got %d", len(snapshot.Timeseries.DeleteEventSeries))
	}
	if snapshot.Timeseries.DeleteEventSeries[1].Value != 1 {
		t.Fatalf("second minute deletes = %v, want 1", snapshot.Timeseries.DeleteEventSeries[1].Value)
	}
}

func TestReportAggregatorPreservesEventOnlyProvenance(t *testing.T) {
	agg := NewReportAggregator(DefaultOptions())
	agg.ConsumeEvent(model.NormalizedEvent{
		Timestamp:     time.Date(2026, 8, 30, 10, 0, 0, 0, time.UTC),
		EventType:     "DDL",
		ServerID:      9,
		ServerVersion: "8.4.6",
		ServerFlavor:  "mysql",
	})

	want := model.ReportProvenance{
		ServerIDs:      []uint32{9},
		ServerVersions: []string{"8.4.6"},
		ServerFlavors:  []string{"mysql"},
	}
	if got := agg.Snapshot().Provenance; !reflect.DeepEqual(got, want) {
		t.Fatalf("event-only provenance\ngot:  %#v\nwant: %#v", got, want)
	}
}

func TestReportAggregatorAlertReferencedEvidence(t *testing.T) {
	base := time.Date(2026, 4, 19, 10, 0, 0, 0, time.UTC)
	opts := DefaultOptions()
	opts.LargeTxnRows = 50

	// txn-small: 10 rows, won't trigger large alert, but is in largest-5
	smallTxn := model.Transaction{
		TxnKey: "txn-small", StartTime: base, EndTime: base,
		TotalRows: 10, EventCount: 1,
		Tables: map[string]int{"t1": 10}, Operations: map[string]int{"INSERT": 10},
	}
	// txn-big: 100 rows, triggers large alert
	bigTxn := model.Transaction{
		TxnKey: "txn-big", StartTime: base, EndTime: base,
		TotalRows: 100, EventCount: 1,
		Tables: map[string]int{"t1": 100}, Operations: map[string]int{"UPDATE": 100},
	}

	agg := NewReportAggregator(opts)
	agg.ConsumeTransaction(smallTxn)
	agg.ConsumeTransaction(bigTxn)
	snapshot := agg.Snapshot()

	// Should have a large_transaction alert for txn-big
	var foundAlert bool
	for _, a := range snapshot.Alerts {
		if a.TxnKey == "txn-big" && a.Type == "large_transaction" {
			foundAlert = true
		}
	}
	if !foundAlert {
		t.Fatalf("expected large_transaction alert for txn-big, got %v", snapshot.Alerts)
	}

	// Findings should have evidence for txn-big even though it may not be first in largest
	var foundEvidence bool
	for _, f := range snapshot.Diagnostics.Findings {
		if f.TxnKey == "txn-big" && len(f.EvidenceRefs) > 0 {
			foundEvidence = true
		}
	}
	if !foundEvidence {
		t.Fatalf("expected evidence refs for txn-big in findings, got %v", snapshot.Diagnostics.Findings)
	}
}

func TestReportAggregatorSnapshotPatternsNonNilMaps(t *testing.T) {
	base := time.Date(2026, 4, 19, 10, 0, 0, 0, time.UTC)
	agg := NewReportAggregator(DefaultOptions())
	agg.ConsumeTransaction(model.Transaction{
		TxnKey: "txn-1", StartTime: base, EndTime: base,
		TotalRows: 5, EventCount: 1,
		Tables:     map[string]int{"shop.orders": 5},
		Operations: map[string]int{"INSERT": 5},
	})
	snapshot := agg.Snapshot()
	if len(snapshot.Patterns) == 0 {
		t.Fatal("expected at least one pattern")
	}
	if snapshot.Patterns[0].Tables == nil {
		t.Fatal("pattern Tables map should not be nil")
	}
	if snapshot.Patterns[0].Operations == nil {
		t.Fatal("pattern Operations map should not be nil")
	}
}
