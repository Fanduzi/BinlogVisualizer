package compare

import "testing"

func TestBuildCompareRecommendationsPatternDriverHighPriority(t *testing.T) {
	result := CompareResult{
		Summary: SummaryDelta{
			CurrentTotalRows:  2000,
			BaselineTotalRows: 1000,
			TotalRowsDelta:   1000,
		},
		KeyFindings: []CompareFinding{{
			Kind:    "pattern_driver",
			Title:   "Top pattern driver",
			Summary: "orders.insert_batch drove most row growth",
			Evidence: map[string]any{
				"pattern_key":          "orders.insert_batch",
				"delta_rows":           750,
				"share_of_total_delta": 0.75,
			},
			EvidenceRefs: []EvidenceRef{{
				Section: "pattern_changes",
				Key:     "orders.insert_batch",
				Label:   "orders.insert_batch",
				Anchor:  "section-pattern-changes",
			}},
		}},
	}

	got := buildCompareRecommendations(result)
	if len(got) != 1 {
		t.Fatalf("recommendations len = %d, want 1", len(got))
	}
	if got[0].Kind != "check_pattern_driver" {
		t.Fatalf("kind = %q, want check_pattern_driver", got[0].Kind)
	}
	if got[0].Priority != "high" {
		t.Fatalf("priority = %q, want high", got[0].Priority)
	}
	if got[0].Title == "" || got[0].Summary == "" || got[0].Rationale == "" {
		t.Fatalf("recommendation should include title, summary, and rationale: %+v", got[0])
	}
	if len(got[0].RelatedFindingKinds) != 1 || got[0].RelatedFindingKinds[0] != "pattern_driver" {
		t.Fatalf("related finding kinds = %#v, want [pattern_driver]", got[0].RelatedFindingKinds)
	}
	if len(got[0].EvidenceRefs) != 1 || got[0].EvidenceRefs[0].Anchor != "section-pattern-changes" {
		t.Fatalf("evidence refs = %#v, want inherited pattern ref", got[0].EvidenceRefs)
	}
}

func TestBuildCompareRecommendationsSuppressesGenericGrowthWhenSpecificDriverExists(t *testing.T) {
	result := CompareResult{
		Summary: SummaryDelta{
			CurrentTotalRows:  3000,
			BaselineTotalRows: 1000,
			TotalRowsDelta:   2000,
		},
		KeyFindings: []CompareFinding{
			{
				Kind:    "volume_change",
				Summary: "rows more than doubled",
				Evidence: map[string]any{
					"delta_rows":    2000,
					"baseline_rows": 1000,
					"current_rows":  3000,
				},
			},
			{
				Kind:    "pattern_driver",
				Summary: "orders.insert_batch drove most row growth",
				Evidence: map[string]any{
					"pattern_key":          "orders.insert_batch",
					"delta_rows":           1600,
					"share_of_total_delta": 0.80,
				},
			},
		},
	}

	got := buildCompareRecommendations(result)
	if len(got) != 1 {
		t.Fatalf("recommendations len = %d, want 1: %#v", len(got), got)
	}
	if got[0].Kind != "check_pattern_driver" {
		t.Fatalf("kind = %q, want check_pattern_driver", got[0].Kind)
	}
}

func TestBuildCompareRecommendationsVolumeDecline(t *testing.T) {
	result := CompareResult{
		Summary: SummaryDelta{
			CurrentTotalRows:  200,
			BaselineTotalRows: 1000,
			TotalRowsDelta:   -800,
		},
		KeyFindings: []CompareFinding{{
			Kind:    "volume_change",
			Summary: "rows more than halved",
			Evidence: map[string]any{
				"delta_rows":    -800,
				"baseline_rows": 1000,
				"current_rows":  200,
			},
		}},
	}

	got := buildCompareRecommendations(result)
	if len(got) != 1 {
		t.Fatalf("recommendations len = %d, want 1", len(got))
	}
	if got[0].Kind != "check_volume_drop_source" {
		t.Fatalf("kind = %q, want check_volume_drop_source", got[0].Kind)
	}
	if got[0].Priority != "high" {
		t.Fatalf("priority = %q, want high", got[0].Priority)
	}
}

func TestBuildCompareRecommendationsReturnsEmptyArrayForNoFindings(t *testing.T) {
	got := buildCompareRecommendations(CompareResult{})
	if got == nil {
		t.Fatal("recommendations should be an empty array, not nil")
	}
	if len(got) != 0 {
		t.Fatalf("recommendations len = %d, want 0", len(got))
	}
}
