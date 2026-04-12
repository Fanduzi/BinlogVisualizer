package compare

import (
	"testing"
)

func TestBuildComparePatternDrilldownsReturnsEmptyForLowSignal(t *testing.T) {
	result := CompareResult{
		Summary: SummaryDelta{TotalRowsDelta: 100},
		PatternChanges: []PatternChange{
			{PatternKey: "a", Label: "a", CurrentRows: 120, BaselineRows: 100, DeltaRows: 20, CurrentTxnCount: 10, BaselineTxnCount: 9, DeltaTxnCount: 1},
			{PatternKey: "b", Label: "b", CurrentRows: 110, BaselineRows: 100, DeltaRows: 10, CurrentTxnCount: 8, BaselineTxnCount: 8, DeltaTxnCount: 0},
		},
	}

	got := buildComparePatternDrilldowns(result)
	if got == nil {
		t.Fatal("drilldowns should be an empty slice, not nil")
	}
	if len(got) != 0 {
		t.Fatalf("len(drilldowns) = %d, want 0", len(got))
	}
}

func TestBuildComparePatternDrilldownsSelectsDominantDriver(t *testing.T) {
	result := CompareResult{
		Summary: SummaryDelta{TotalRowsDelta: 1000},
		PatternChanges: []PatternChange{
			{PatternKey: "orders.insert", Label: "orders.insert", CurrentRows: 1200, BaselineRows: 200, DeltaRows: 1000, CurrentTxnCount: 140, BaselineTxnCount: 20, DeltaTxnCount: 120},
			{PatternKey: "other", Label: "other", CurrentRows: 220, BaselineRows: 210, DeltaRows: 10, CurrentTxnCount: 20, BaselineTxnCount: 20, DeltaTxnCount: 0},
		},
	}

	got := buildComparePatternDrilldowns(result)
	if len(got) != 1 {
		t.Fatalf("len(drilldowns) = %d, want 1", len(got))
	}
	if got[0].PatternKey != "orders.insert" {
		t.Fatalf("pattern_key = %q, want orders.insert", got[0].PatternKey)
	}
	if got[0].WhySelected == "" {
		t.Fatal("expected why_selected")
	}
	if len(got[0].KeyPoints) == 0 || len(got[0].KeyPoints) > 2 {
		t.Fatalf("key_points len = %d, want 1..2", len(got[0].KeyPoints))
	}
}

func TestBuildComparePatternDrilldownsSelectsNewPattern(t *testing.T) {
	result := CompareResult{
		Summary: SummaryDelta{TotalRowsDelta: 500},
		PatternChanges: []PatternChange{
			{PatternKey: "orders.insert", Label: "orders.insert", CurrentRows: 500, BaselineRows: 0, DeltaRows: 500, CurrentTxnCount: 50, BaselineTxnCount: 0, DeltaTxnCount: 50},
			{PatternKey: "other", Label: "other", CurrentRows: 220, BaselineRows: 210, DeltaRows: 10, CurrentTxnCount: 20, BaselineTxnCount: 20, DeltaTxnCount: 0},
		},
	}

	got := buildComparePatternDrilldowns(result)
	if len(got) < 1 {
		t.Fatalf("len(drilldowns) = %d, want >= 1", len(got))
	}
	found := false
	for _, d := range got {
		if d.PatternKey == "orders.insert" {
			found = true
			if !d.SignalFlags.NewPattern {
				t.Error("expected new_pattern signal flag")
			}
			if len(d.KeyPoints) == 0 || len(d.KeyPoints) > 2 {
				t.Fatalf("key_points len = %d, want 1..2", len(d.KeyPoints))
			}
		}
	}
	if !found {
		t.Fatal("expected orders.insert drilldown")
	}
}

func TestBuildComparePatternDrilldownsSelectionIsDeterministic(t *testing.T) {
	result := CompareResult{
		Summary: SummaryDelta{TotalRowsDelta: 3000},
		PatternChanges: []PatternChange{
			{PatternKey: "alpha", Label: "alpha", CurrentRows: 2200, BaselineRows: 200, DeltaRows: 2000, CurrentTxnCount: 200, BaselineTxnCount: 20, DeltaTxnCount: 180},
			{PatternKey: "beta", Label: "beta", CurrentRows: 1200, BaselineRows: 200, DeltaRows: 1000, CurrentTxnCount: 100, BaselineTxnCount: 20, DeltaTxnCount: 80},
			{PatternKey: "gamma", Label: "gamma", CurrentRows: 220, BaselineRows: 210, DeltaRows: 10, CurrentTxnCount: 20, BaselineTxnCount: 20, DeltaTxnCount: 0},
		},
	}

	got1 := buildComparePatternDrilldowns(result)
	got2 := buildComparePatternDrilldowns(result)

	if len(got1) != len(got2) {
		t.Fatalf("non-deterministic count: %d vs %d", len(got1), len(got2))
	}
	for i := range got1 {
		if got1[i].PatternKey != got2[i].PatternKey {
			t.Errorf("order mismatch at %d: %q vs %q", i, got1[i].PatternKey, got2[i].PatternKey)
		}
	}
}

func TestBuildComparePatternDrilldownsCapsAtTwo(t *testing.T) {
	result := CompareResult{
		Summary: SummaryDelta{TotalRowsDelta: 6000},
		PatternChanges: []PatternChange{
			{PatternKey: "a", Label: "a", CurrentRows: 3200, BaselineRows: 200, DeltaRows: 3000, CurrentTxnCount: 300, BaselineTxnCount: 20, DeltaTxnCount: 280},
			{PatternKey: "b", Label: "b", CurrentRows: 2200, BaselineRows: 200, DeltaRows: 2000, CurrentTxnCount: 200, BaselineTxnCount: 20, DeltaTxnCount: 180},
			{PatternKey: "c", Label: "c", CurrentRows: 1200, BaselineRows: 200, DeltaRows: 1000, CurrentTxnCount: 100, BaselineTxnCount: 20, DeltaTxnCount: 80},
		},
	}

	got := buildComparePatternDrilldowns(result)
	if len(got) > 2 {
		t.Fatalf("len(drilldowns) = %d, want at most 2", len(got))
	}
}

func TestBuildComparePatternDrilldownsCapsKeyPointsAtTwo(t *testing.T) {
	result := CompareResult{
		Summary: SummaryDelta{TotalRowsDelta: 2000},
		PatternChanges: []PatternChange{
			{PatternKey: "orders.insert", Label: "orders.insert", CurrentRows: 2200, BaselineRows: 200, DeltaRows: 2000, CurrentTxnCount: 200, BaselineTxnCount: 20, DeltaTxnCount: 180},
		},
	}

	got := buildComparePatternDrilldowns(result)
	for _, d := range got {
		if len(d.KeyPoints) > 2 {
			t.Fatalf("key_points len = %d for %q, want at most 2", len(d.KeyPoints), d.PatternKey)
		}
	}
}

func TestBuildComparePatternDrilldownsUsesCompareScopedWording(t *testing.T) {
	result := CompareResult{
		Summary: SummaryDelta{TotalRowsDelta: 2000},
		PatternChanges: []PatternChange{
			{PatternKey: "orders.insert", Label: "orders.insert", CurrentRows: 2200, BaselineRows: 200, DeltaRows: 2000, CurrentTxnCount: 200, BaselineTxnCount: 20, DeltaTxnCount: 180},
		},
	}

	got := buildComparePatternDrilldowns(result)
	if len(got) == 0 {
		t.Fatal("expected at least one drilldown")
	}
	// WhySelected should reference compare context, not raw pattern ownership
	if got[0].WhySelected == "" {
		t.Fatal("why_selected must not be empty")
	}
	for _, kp := range got[0].KeyPoints {
		if kp.Label == "" || kp.Summary == "" {
			t.Errorf("key_point has empty label or summary: %+v", kp)
		}
	}
}

func TestBuildComparePatternDrilldownsReturnsEmptySliceWhenNoPatternChanges(t *testing.T) {
	result := CompareResult{
		Summary:        SummaryDelta{TotalRowsDelta: 0},
		PatternChanges: nil,
	}

	got := buildComparePatternDrilldowns(result)
	if got == nil {
		t.Fatal("drilldowns should be an empty slice, not nil")
	}
	if len(got) != 0 {
		t.Fatalf("len(drilldowns) = %d, want 0", len(got))
	}
}
