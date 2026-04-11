package trend

import "testing"

func TestBuildTrendRecommendationsRisingPatternHighPriority(t *testing.T) {
	result := Result{
		TrendSummary: []TrendFinding{{
			Kind:    "rising_pattern",
			Summary: "orders.insert_batch grew by +1200 rows across the series",
			Evidence: map[string]any{
				"pattern_key": "orders.insert_batch",
				"delta_rows":  1200,
				"share_delta": 0.25,
			},
			EvidenceRefs: []EvidenceRef{{
				Section: "pattern_trends",
				Key:     "orders.insert_batch",
				Label:   "orders.insert_batch",
				Anchor:  "pattern-0",
			}},
		}},
	}

	got := buildTrendRecommendations(result)
	if len(got) != 1 {
		t.Fatalf("recommendations len = %d, want 1", len(got))
	}
	if got[0].Kind != "track_rising_pattern" {
		t.Fatalf("kind = %q, want track_rising_pattern", got[0].Kind)
	}
	if got[0].Priority != "high" {
		t.Fatalf("priority = %q, want high", got[0].Priority)
	}
	if len(got[0].EvidenceRefs) != 1 || got[0].EvidenceRefs[0].Anchor != "pattern-0" {
		t.Fatalf("evidence refs = %#v, want inherited pattern ref", got[0].EvidenceRefs)
	}
}

func TestBuildTrendRecommendationsSpikeOutlierHighPriority(t *testing.T) {
	result := Result{
		TrendSummary: []TrendFinding{{
			Kind:    "spike_outlier",
			Summary: "snapshot week3 is a clear volume spike",
			Evidence: map[string]any{
				"snapshot_name": "week3",
				"rows":          9000,
				"median_rows":   3000,
			},
			EvidenceRefs: []EvidenceRef{{
				Section: "ordered_points",
				Key:     "week3",
				Label:   "week3",
				Anchor:  "point-2",
			}},
		}},
	}

	got := buildTrendRecommendations(result)
	if len(got) != 1 {
		t.Fatalf("recommendations len = %d, want 1", len(got))
	}
	if got[0].Kind != "capture_followup_snapshot" {
		t.Fatalf("kind = %q, want capture_followup_snapshot", got[0].Kind)
	}
	if got[0].Priority != "high" {
		t.Fatalf("priority = %q, want high", got[0].Priority)
	}
}

func TestBuildTrendRecommendationsOrderingAndDedupe(t *testing.T) {
	result := Result{
		TrendSummary: []TrendFinding{
			{Kind: "falling_pattern", Summary: "p1 declined", Evidence: map[string]any{"pattern_key": "p1", "share_delta": -0.05}},
			{Kind: "concentration_shift", Summary: "p2 became dominant", Evidence: map[string]any{"pattern_key": "p2", "share_delta": 0.30}},
			{Kind: "spike_outlier", Summary: "week3 spiked", Evidence: map[string]any{"snapshot_name": "week3"}},
			{Kind: "concentration_shift", Summary: "duplicate concentration", Evidence: map[string]any{"pattern_key": "p3", "share_delta": 0.25}},
		},
	}

	got := buildTrendRecommendations(result)
	wantKinds := []string{"watch_workload_concentration", "capture_followup_snapshot", "confirm_declining_pattern"}
	if len(got) != len(wantKinds) {
		t.Fatalf("recommendations len = %d, want %d: %#v", len(got), len(wantKinds), got)
	}
	for i, want := range wantKinds {
		if got[i].Kind != want {
			t.Fatalf("recommendation[%d] kind = %q, want %q", i, got[i].Kind, want)
		}
	}
}

func TestBuildTrendRecommendationsReturnsEmptyArrayForNoFindings(t *testing.T) {
	got := buildTrendRecommendations(Result{})
	if got == nil {
		t.Fatal("recommendations should be an empty array, not nil")
	}
	if len(got) != 0 {
		t.Fatalf("recommendations len = %d, want 0", len(got))
	}
}
