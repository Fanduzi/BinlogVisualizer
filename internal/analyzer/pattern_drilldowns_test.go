package analyzer

import (
	"testing"
	"time"

	"binlogviz/internal/model"
)

func ts(hour, minute int) time.Time {
	return time.Date(2026, 4, 12, hour, minute, 0, 0, time.UTC)
}

func TestBuildPatternDrilldowns_LowSignal(t *testing.T) {
	patterns := []model.PatternStats{
		{PatternKey: "p1", Label: "small batch", TotalRows: 10, TxnCount: 5, ShareOfRows: 0.10, ShareOfTransactions: 0.10, AvgRowsPerTxn: 2},
	}
	minutes := []model.MinuteBucket{
		{Minute: ts(10, 0), TotalRows: 10, TxnCount: 5},
	}
	txns := []model.Transaction{
		{TxnKey: "t1", TotalRows: 2, Tables: map[string]int{"users": 2}},
	}
	alerts := []model.Alert{}

	result := BuildPatternDrilldowns(patterns, minutes, txns, alerts)
	if len(result) != 0 {
		t.Fatalf("expected no drilldowns for low-signal workload, got %d", len(result))
	}
}

func TestBuildPatternDrilldowns_DominanceAndAnomaly(t *testing.T) {
	patterns := []model.PatternStats{
		{PatternKey: "p1", Label: "large insert batch", TotalRows: 50000, TxnCount: 100, ShareOfRows: 0.80, ShareOfTransactions: 0.70, AvgRowsPerTxn: 500},
		{PatternKey: "p2", Label: "small update", TotalRows: 1000, TxnCount: 20, ShareOfRows: 0.02, ShareOfTransactions: 0.15, AvgRowsPerTxn: 50},
	}
	// Concentrated minutes: top 2 cover 90% of rows → concentration anomaly
	minutes := []model.MinuteBucket{
		{Minute: ts(10, 0), TotalRows: 30000, TxnCount: 60},
		{Minute: ts(10, 1), TotalRows: 15000, TxnCount: 30},
		{Minute: ts(10, 2), TotalRows: 5000, TxnCount: 10},
	}
	txns := []model.Transaction{
		{TxnKey: "t1", TotalRows: 500, Tables: map[string]int{"orders": 500}, Operations: map[string]int{"INSERT": 500}},
		{TxnKey: "t2", TotalRows: 480, Tables: map[string]int{"orders": 480}, Operations: map[string]int{"INSERT": 480}},
	}
	// Spike alert is present but should NOT be the anomaly signal source.
	// The anomaly comes from concentration, not from global alert overlap.
	alerts := []model.Alert{
		{Type: "spike", Severity: "warning", Minute: ts(10, 0)},
	}

	result := BuildPatternDrilldowns(patterns, minutes, txns, alerts)
	if len(result) == 0 {
		t.Fatal("expected drilldown for dominance+anomaly pattern, got none")
	}
	found := false
	for _, d := range result {
		if d.PatternKey == "p1" {
			found = true
			if !d.SignalFlags.Dominance {
				t.Error("expected dominance flag for p1")
			}
			if !d.SignalFlags.Anomaly {
				t.Error("expected anomaly flag for p1 (from concentration)")
			}
			if d.WhySelected == "" {
				t.Error("expected why_selected to be populated")
			}
		}
	}
	if !found {
		t.Error("expected p1 to be selected for drilldown")
	}
}

func TestBuildPatternDrilldowns_DominanceExtreme(t *testing.T) {
	patterns := []model.PatternStats{
		{PatternKey: "p1", Label: "dominant batch", TotalRows: 90000, TxnCount: 200, ShareOfRows: 0.95, ShareOfTransactions: 0.90, AvgRowsPerTxn: 450},
	}
	minutes := []model.MinuteBucket{
		{Minute: ts(10, 0), TotalRows: 90000, TxnCount: 200},
	}
	txns := []model.Transaction{}
	alerts := []model.Alert{}

	result := BuildPatternDrilldowns(patterns, minutes, txns, alerts)
	if len(result) == 0 {
		t.Fatal("expected drilldown for extreme dominance, got none")
	}
	if result[0].PatternKey != "p1" {
		t.Errorf("expected p1, got %s", result[0].PatternKey)
	}
	if !result[0].SignalFlags.Dominance {
		t.Error("expected dominance flag")
	}
}

func TestBuildPatternDrilldowns_AnomalyExtreme(t *testing.T) {
	// Pattern p2 is extreme dominant. Pattern p1 has anomaly via high rows-per-txn
	// AND table-aligned large_transaction alert (pattern-local signal).
	patterns := []model.PatternStats{
		{PatternKey: "p1", Label: "spike pattern", TotalRows: 5000, TxnCount: 2, ShareOfRows: 0.05, ShareOfTransactions: 0.01, AvgRowsPerTxn: 2500, Tables: map[string]int{"cache": 5000}},
		{PatternKey: "p2", Label: "steady background", TotalRows: 95000, TxnCount: 800, ShareOfRows: 0.95, ShareOfTransactions: 0.99, AvgRowsPerTxn: 119},
	}
	minutes := []model.MinuteBucket{
		{Minute: ts(10, 0), TotalRows: 100000, TxnCount: 800},
		{Minute: ts(10, 1), TotalRows: 200000, TxnCount: 2},
		{Minute: ts(10, 2), TotalRows: 50000, TxnCount: 100},
	}
	txns := []model.Transaction{
		{TxnKey: "big1", TotalRows: 2500, Tables: map[string]int{"cache": 2500}},
	}
	alerts := []model.Alert{
		{Type: "large_transaction", Severity: "warning", TxnKey: "big1"},
	}

	result := BuildPatternDrilldowns(patterns, minutes, txns, alerts)
	if len(result) == 0 {
		t.Fatal("expected at least one drilldown")
	}

	// p2 should be selected via extreme dominance
	foundP2 := false
	for _, d := range result {
		if d.PatternKey == "p2" {
			foundP2 = true
			if !d.SignalFlags.Dominance {
				t.Error("expected dominance flag for p2")
			}
		}
	}
	if !foundP2 {
		t.Error("expected p2 to be selected for extreme dominance")
	}
}

func TestBuildPatternDrilldowns_CapAtTwo(t *testing.T) {
	patterns := []model.PatternStats{
		{PatternKey: "p1", Label: "dominant 1", TotalRows: 40000, TxnCount: 100, ShareOfRows: 0.40, ShareOfTransactions: 0.50, AvgRowsPerTxn: 400},
		{PatternKey: "p2", Label: "dominant 2", TotalRows: 35000, TxnCount: 80, ShareOfRows: 0.35, ShareOfTransactions: 0.40, AvgRowsPerTxn: 437},
		{PatternKey: "p3", Label: "dominant 3", TotalRows: 20000, TxnCount: 20, ShareOfRows: 0.20, ShareOfTransactions: 0.10, AvgRowsPerTxn: 1000},
	}
	minutes := []model.MinuteBucket{
		{Minute: ts(10, 0), TotalRows: 95000, TxnCount: 200},
	}
	txns := []model.Transaction{}
	alerts := []model.Alert{
		{Type: "spike", Severity: "warning", Minute: ts(10, 0)},
	}

	result := BuildPatternDrilldowns(patterns, minutes, txns, alerts)
	if len(result) > 2 {
		t.Fatalf("expected at most 2 drilldowns, got %d", len(result))
	}
}

func TestBuildPatternDrilldowns_NestedCaps(t *testing.T) {
	patterns := []model.PatternStats{
		{PatternKey: "p1", Label: "big pattern", TotalRows: 80000, TxnCount: 100, ShareOfRows: 0.80, ShareOfTransactions: 0.80, AvgRowsPerTxn: 800},
	}
	minutes := []model.MinuteBucket{
		{Minute: ts(10, 0), TotalRows: 40000, TxnCount: 50},
		{Minute: ts(10, 1), TotalRows: 25000, TxnCount: 30},
		{Minute: ts(10, 2), TotalRows: 10000, TxnCount: 15},
		{Minute: ts(10, 3), TotalRows: 5000, TxnCount: 5},
	}
	txns := []model.Transaction{
		{TxnKey: "t1", TotalRows: 800, EventCount: 1, Tables: map[string]int{"a": 800}, Operations: map[string]int{"INSERT": 800}},
		{TxnKey: "t2", TotalRows: 780, EventCount: 1, Tables: map[string]int{"a": 780}, Operations: map[string]int{"INSERT": 780}},
		{TxnKey: "t3", TotalRows: 760, EventCount: 1, Tables: map[string]int{"a": 760}, Operations: map[string]int{"INSERT": 760}},
		{TxnKey: "t4", TotalRows: 750, EventCount: 1, Tables: map[string]int{"a": 750}, Operations: map[string]int{"INSERT": 750}},
	}
	patterns[0].PatternKey, _ = patternIdentity(txns[0])
	alerts := []model.Alert{}

	result := BuildPatternDrilldowns(patterns, minutes, txns, alerts)
	if len(result) == 0 {
		t.Fatal("expected drilldown")
	}
	d := result[0]
	if len(d.BusiestMinutes) > 2 {
		t.Errorf("expected at most 2 busiest minutes, got %d", len(d.BusiestMinutes))
	}
	if len(d.RepresentativeTransactions) > 2 {
		t.Errorf("expected at most 2 representative txns, got %d", len(d.RepresentativeTransactions))
	}
}

func TestBuildPatternDrilldowns_DeterministicOrdering(t *testing.T) {
	patterns := []model.PatternStats{
		{PatternKey: "alpha", Label: "pattern A", TotalRows: 50000, TxnCount: 100, ShareOfRows: 0.50, ShareOfTransactions: 0.60, AvgRowsPerTxn: 500},
		{PatternKey: "beta", Label: "pattern B", TotalRows: 45000, TxnCount: 90, ShareOfRows: 0.45, ShareOfTransactions: 0.35, AvgRowsPerTxn: 500},
	}
	minutes := []model.MinuteBucket{
		{Minute: ts(10, 0), TotalRows: 95000, TxnCount: 190},
	}
	txns := []model.Transaction{}
	alerts := []model.Alert{}

	result1 := BuildPatternDrilldowns(patterns, minutes, txns, alerts)
	result2 := BuildPatternDrilldowns(patterns, minutes, txns, alerts)

	if len(result1) != len(result2) {
		t.Fatalf("non-deterministic count: %d vs %d", len(result1), len(result2))
	}
	for i := range result1 {
		if result1[i].PatternKey != result2[i].PatternKey {
			t.Errorf("non-deterministic order at %d: %s vs %s", i, result1[i].PatternKey, result2[i].PatternKey)
		}
	}
}

func TestBuildPatternDrilldowns_SharesAndWhyPopulated(t *testing.T) {
	patterns := []model.PatternStats{
		{PatternKey: "p1", Label: "dominant", TotalRows: 70000, TxnCount: 100, ShareOfRows: 0.70, ShareOfTransactions: 0.80, AvgRowsPerTxn: 700},
	}
	minutes := []model.MinuteBucket{
		{Minute: ts(10, 0), TotalRows: 70000, TxnCount: 100},
	}
	txns := []model.Transaction{}
	alerts := []model.Alert{}

	result := BuildPatternDrilldowns(patterns, minutes, txns, alerts)
	if len(result) == 0 {
		t.Fatal("expected drilldown")
	}
	d := result[0]
	if d.ShareOfRows != 0.70 {
		t.Errorf("expected ShareOfRows=0.70, got %f", d.ShareOfRows)
	}
	if d.ShareOfTxns != 0.80 {
		t.Errorf("expected ShareOfTxns=0.80, got %f", d.ShareOfTxns)
	}
	if d.WhySelected == "" {
		t.Error("expected why_selected to be populated")
	}
}

// TestBuildPatternDrilldowns_GlobalSpikeDoesNotFlagUnrelatedPattern verifies that
// a global spike alert in a busy minute does NOT cause every pattern to get
// anomaly=true. Only table-aligned txn-key alerts are a valid anomaly signal.
func TestBuildPatternDrilldowns_GlobalSpikeDoesNotFlagUnrelatedPattern(t *testing.T) {
	patterns := []model.PatternStats{
		// p1 is a small, low-share pattern that does NOT overlap with the spike's tables
		{PatternKey: "p1", Label: "small config writes", TotalRows: 500, TxnCount: 50, ShareOfRows: 0.01, ShareOfTransactions: 0.10, AvgRowsPerTxn: 10, Tables: map[string]int{"config": 500}},
		// p2 is a dominant pattern with high share
		{PatternKey: "p2", Label: "heavy inserts", TotalRows: 49500, TxnCount: 450, ShareOfRows: 0.99, ShareOfTransactions: 0.90, AvgRowsPerTxn: 110, Tables: map[string]int{"orders": 49500}},
	}
	minutes := []model.MinuteBucket{
		{Minute: ts(10, 0), TotalRows: 40000, TxnCount: 400},
		{Minute: ts(10, 1), TotalRows: 8000, TxnCount: 80},
		{Minute: ts(10, 2), TotalRows: 2000, TxnCount: 20},
	}
	txns := []model.Transaction{
		{TxnKey: "big1", TotalRows: 5000, Tables: map[string]int{"orders": 5000}},
	}
	// Spike alert at 10:00 and large-txn alert for an orders txn
	alerts := []model.Alert{
		{Type: "spike", Severity: "warning", Minute: ts(10, 0)},
		{Type: "large_transaction", Severity: "warning", TxnKey: "big1"},
	}

	result := BuildPatternDrilldowns(patterns, minutes, txns, alerts)

	// p1 should NOT be selected — it has no dominance, no anomaly signals
	for _, d := range result {
		if d.PatternKey == "p1" && d.SignalFlags.Anomaly {
			t.Error("p1 should not get anomaly flag from a global spike alert it has no table overlap with")
		}
	}

	// p2 should be selected via dominance (0.99 share)
	foundP2 := false
	for _, d := range result {
		if d.PatternKey == "p2" {
			foundP2 = true
			if !d.SignalFlags.Dominance {
				t.Error("expected dominance flag for p2")
			}
		}
	}
	if !foundP2 {
		t.Error("expected p2 to be selected for dominance")
	}
}

func TestSelectRepresentativeTxnsBelongToSamePattern(t *testing.T) {
	insertLarge := model.Transaction{
		TxnKey:     "txn-1",
		TotalRows:  400000,
		EventCount: 1,
		Tables:     map[string]int{"dogfood_big.t": 400000},
		Operations: map[string]int{"INSERT": 400000},
	}
	insertLarge2 := model.Transaction{
		TxnKey:     "txn-2",
		TotalRows:  400000,
		EventCount: 1,
		Tables:     map[string]int{"dogfood_big.t": 400000},
		Operations: map[string]int{"INSERT": 400000},
	}
	deleteLarge := model.Transaction{
		TxnKey:     "txn-del-1",
		TotalRows:  80,
		EventCount: 1,
		Tables:     map[string]int{"dogfood_big.t": 80},
		Operations: map[string]int{"DELETE": 80},
	}
	deleteLarge2 := model.Transaction{
		TxnKey:     "txn-del-2",
		TotalRows:  60,
		EventCount: 1,
		Tables:     map[string]int{"dogfood_big.t": 60},
		Operations: map[string]int{"DELETE": 60},
	}
	deleteKey, _ := patternIdentity(deleteLarge)
	insertKey, _ := patternIdentity(insertLarge)
	if deleteKey == insertKey {
		t.Fatal("expected INSERT and DELETE large-batch identities to differ")
	}

	got := selectRepresentativeTxns(
		[]model.Transaction{insertLarge, insertLarge2, deleteLarge, deleteLarge2},
		deleteKey,
		2,
	)
	if len(got) != 2 {
		t.Fatalf("expected 2 DELETE representatives, got %d", len(got))
	}
	for _, txn := range got {
		if txn.TxnKey == "txn-1" || txn.TxnKey == "txn-2" {
			t.Fatalf("DELETE pattern cited INSERT txn %s", txn.TxnKey)
		}
		if txn.TotalRows >= 400000 {
			t.Fatalf("DELETE representative has INSERT-sized rows=%d", txn.TotalRows)
		}
	}
}

func TestFormatSharePercentKeepsSubOnePercent(t *testing.T) {
	if got := formatSharePercent(0.003); got != "0.3%" {
		t.Fatalf("0.3%% share formatted as %q, want 0.3%%", got)
	}
	if got := formatSharePercent(0.66); got != "66%" {
		t.Fatalf("66%% share formatted as %q, want 66%%", got)
	}
	if got := formatSharePercent(0); got != "0%" {
		t.Fatalf("zero share formatted as %q, want 0%%", got)
	}
}
