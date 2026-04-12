package trend

import (
	"testing"
)

func TestBuildTrendPatternDrilldownsReturnsEmptyForLowSignal(t *testing.T) {
	result := Result{
		PatternTrends: []PatternTrend{
			{
				PatternKey:       "a",
				Label:            "a",
				FirstRows:        100,
				LastRows:         110,
				DeltaRows:        10,
				FirstShareOfRows: 0.10,
				LastShareOfRows:  0.11,
				DeltaShareOfRows: 0.01,
			},
		},
	}

	got := buildTrendPatternDrilldowns(result)
	if got == nil {
		t.Fatal("drilldowns should be an empty slice, not nil")
	}
	if len(got) != 0 {
		t.Fatalf("len(drilldowns) = %d, want 0", len(got))
	}
}

func TestBuildTrendPatternDrilldownsSelectsRisingPattern(t *testing.T) {
	result := Result{
		PatternTrends: []PatternTrend{
			{
				PatternKey:       "orders.insert",
				Label:            "orders.insert",
				FirstRows:        100,
				LastRows:         900,
				DeltaRows:        800,
				FirstShareOfRows: 0.10,
				LastShareOfRows:  0.55,
				DeltaShareOfRows: 0.45,
				ShareOfRowsSeries: []PatternTrendSharePoint{
					{SnapshotName: "s1", ShareOfRows: 0.10},
					{SnapshotName: "s2", ShareOfRows: 0.15},
					{SnapshotName: "s3", ShareOfRows: 0.55},
				},
			},
		},
	}

	got := buildTrendPatternDrilldowns(result)
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

func TestBuildTrendPatternDrilldownsSelectsFallingPattern(t *testing.T) {
	result := Result{
		PatternTrends: []PatternTrend{
			{
				PatternKey:       "orders.delete",
				Label:            "orders.delete",
				FirstRows:        800,
				LastRows:         100,
				DeltaRows:        -700,
				FirstShareOfRows: 0.60,
				LastShareOfRows:  0.08,
				DeltaShareOfRows: -0.52,
				ShareOfRowsSeries: []PatternTrendSharePoint{
					{SnapshotName: "s1", ShareOfRows: 0.60},
					{SnapshotName: "s2", ShareOfRows: 0.30},
					{SnapshotName: "s3", ShareOfRows: 0.08},
				},
			},
		},
	}

	got := buildTrendPatternDrilldowns(result)
	if len(got) < 1 {
		t.Fatalf("len(drilldowns) = %d, want >= 1", len(got))
	}
	if got[0].PatternKey != "orders.delete" {
		t.Fatalf("pattern_key = %q, want orders.delete", got[0].PatternKey)
	}
}

func TestBuildTrendPatternDrilldownsSelectionIsDeterministic(t *testing.T) {
	result := Result{
		PatternTrends: []PatternTrend{
			{
				PatternKey:       "alpha",
				Label:            "alpha",
				FirstRows:        100,
				LastRows:         900,
				DeltaRows:        800,
				FirstShareOfRows: 0.05,
				LastShareOfRows:  0.60,
				DeltaShareOfRows: 0.55,
				ShareOfRowsSeries: []PatternTrendSharePoint{
					{SnapshotName: "s1", ShareOfRows: 0.05},
					{SnapshotName: "s2", ShareOfRows: 0.30},
					{SnapshotName: "s3", ShareOfRows: 0.60},
				},
			},
			{
				PatternKey:       "beta",
				Label:            "beta",
				FirstRows:        200,
				LastRows:         600,
				DeltaRows:        400,
				FirstShareOfRows: 0.10,
				LastShareOfRows:  0.40,
				DeltaShareOfRows: 0.30,
				ShareOfRowsSeries: []PatternTrendSharePoint{
					{SnapshotName: "s1", ShareOfRows: 0.10},
					{SnapshotName: "s2", ShareOfRows: 0.25},
					{SnapshotName: "s3", ShareOfRows: 0.40},
				},
			},
		},
	}

	got1 := buildTrendPatternDrilldowns(result)
	got2 := buildTrendPatternDrilldowns(result)

	if len(got1) != len(got2) {
		t.Fatalf("non-deterministic count: %d vs %d", len(got1), len(got2))
	}
	for i := range got1 {
		if got1[i].PatternKey != got2[i].PatternKey {
			t.Errorf("order mismatch at %d: %q vs %q", i, got1[i].PatternKey, got2[i].PatternKey)
		}
	}
}

func TestBuildTrendPatternDrilldownsCapsAtTwo(t *testing.T) {
	result := Result{
		PatternTrends: []PatternTrend{
			{
				PatternKey: "a", Label: "a",
				FirstRows: 100, LastRows: 900, DeltaRows: 800,
				FirstShareOfRows: 0.05, LastShareOfRows: 0.60, DeltaShareOfRows: 0.55,
				ShareOfRowsSeries: []PatternTrendSharePoint{
					{SnapshotName: "s1", ShareOfRows: 0.05}, {SnapshotName: "s2", ShareOfRows: 0.60},
				},
			},
			{
				PatternKey: "b", Label: "b",
				FirstRows: 100, LastRows: 700, DeltaRows: 600,
				FirstShareOfRows: 0.05, LastShareOfRows: 0.50, DeltaShareOfRows: 0.45,
				ShareOfRowsSeries: []PatternTrendSharePoint{
					{SnapshotName: "s1", ShareOfRows: 0.05}, {SnapshotName: "s2", ShareOfRows: 0.50},
				},
			},
			{
				PatternKey: "c", Label: "c",
				FirstRows: 100, LastRows: 500, DeltaRows: 400,
				FirstShareOfRows: 0.05, LastShareOfRows: 0.40, DeltaShareOfRows: 0.35,
				ShareOfRowsSeries: []PatternTrendSharePoint{
					{SnapshotName: "s1", ShareOfRows: 0.05}, {SnapshotName: "s2", ShareOfRows: 0.40},
				},
			},
		},
	}

	got := buildTrendPatternDrilldowns(result)
	if len(got) > 2 {
		t.Fatalf("len(drilldowns) = %d, want at most 2", len(got))
	}
}

func TestBuildTrendPatternDrilldownsCapsKeyPointsAtTwo(t *testing.T) {
	result := Result{
		PatternTrends: []PatternTrend{
			{
				PatternKey: "orders.insert", Label: "orders.insert",
				FirstRows: 100, LastRows: 900, DeltaRows: 800,
				FirstShareOfRows: 0.05, LastShareOfRows: 0.60, DeltaShareOfRows: 0.55,
				ShareOfRowsSeries: []PatternTrendSharePoint{
					{SnapshotName: "s1", ShareOfRows: 0.05}, {SnapshotName: "s2", ShareOfRows: 0.60},
				},
			},
		},
	}

	got := buildTrendPatternDrilldowns(result)
	for _, d := range got {
		if len(d.KeyPoints) > 2 {
			t.Fatalf("key_points len = %d for %q, want at most 2", len(d.KeyPoints), d.PatternKey)
		}
	}
}

func TestBuildTrendPatternDrilldownsReturnsEmptyWhenNoPatternTrends(t *testing.T) {
	result := Result{
		PatternTrends: nil,
	}

	got := buildTrendPatternDrilldowns(result)
	if got == nil {
		t.Fatal("drilldowns should be an empty slice, not nil")
	}
	if len(got) != 0 {
		t.Fatalf("len(drilldowns) = %d, want 0", len(got))
	}
}
