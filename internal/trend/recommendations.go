package trend

import "sort"

const maxTrendRecommendations = 5

func buildTrendRecommendations(result Result) []Recommendation {
	candidates := make([]Recommendation, 0, len(result.TrendSummary))
	for _, f := range result.TrendSummary {
		switch f.Kind {
		case "rising_pattern":
			candidates = append(candidates, trendRisingPatternRecommendation(f))
		case "falling_pattern":
			candidates = append(candidates, trendFallingPatternRecommendation(f))
		case "table_trend":
			candidates = append(candidates, trendTableRecommendation(f))
		case "concentration_shift":
			candidates = append(candidates, trendConcentrationRecommendation(f))
		case "spike_outlier":
			candidates = append(candidates, trendSpikeRecommendation(f))
		}
	}

	sort.SliceStable(candidates, func(i, j int) bool {
		if trendPriorityRank(candidates[i].Priority) != trendPriorityRank(candidates[j].Priority) {
			return trendPriorityRank(candidates[i].Priority) < trendPriorityRank(candidates[j].Priority)
		}
		if trendRecommendationKindRank(candidates[i].Kind) != trendRecommendationKindRank(candidates[j].Kind) {
			return trendRecommendationKindRank(candidates[i].Kind) < trendRecommendationKindRank(candidates[j].Kind)
		}
		return candidates[i].Kind < candidates[j].Kind
	})

	candidates = dedupeTrendRecommendations(candidates)
	if len(candidates) > maxTrendRecommendations {
		candidates = candidates[:maxTrendRecommendations]
	}
	if len(candidates) == 0 {
		return []Recommendation{}
	}
	return candidates
}

func trendRisingPatternRecommendation(f TrendFinding) Recommendation {
	priority := "medium"
	if absShareDeltaFromTrendEvidence(f) >= 0.20 {
		priority = "high"
	}
	return Recommendation{
		Kind:                "track_rising_pattern",
		Priority:            priority,
		Title:               "Track rising pattern",
		Summary:             f.Summary + "; capture or compare the next window to confirm whether this is becoming a persistent workload driver.",
		Rationale:           "A repeated write pattern is rising across the ordered snapshot series.",
		RelatedFindingKinds: []string{f.Kind},
		EvidenceRefs:        cloneTrendRecommendationEvidenceRefs(f.EvidenceRefs),
	}
}

func trendFallingPatternRecommendation(f TrendFinding) Recommendation {
	return Recommendation{
		Kind:                "confirm_declining_pattern",
		Priority:            "medium",
		Title:               "Confirm declining pattern",
		Summary:             f.Summary + "; confirm whether the decline is expected or indicates missing workload, filters, or input coverage.",
		Rationale:           "A repeated write pattern declined enough to be selected as a trend finding.",
		RelatedFindingKinds: []string{f.Kind},
		EvidenceRefs:        cloneTrendRecommendationEvidenceRefs(f.EvidenceRefs),
	}
}

func trendTableRecommendation(f TrendFinding) Recommendation {
	return Recommendation{
		Kind:                "review_growing_table",
		Priority:            "medium",
		Title:               "Review growing table",
		Summary:             f.Summary + "; review table owners, scheduled jobs, and whether the table growth matches expectations.",
		Rationale:           "A table-level trend was large enough to be highlighted in the trend summary.",
		RelatedFindingKinds: []string{f.Kind},
		EvidenceRefs:        cloneTrendRecommendationEvidenceRefs(f.EvidenceRefs),
	}
}

func trendConcentrationRecommendation(f TrendFinding) Recommendation {
	return Recommendation{
		Kind:                "watch_workload_concentration",
		Priority:            "high",
		Title:               "Watch workload concentration",
		Summary:             f.Summary + "; check whether one workload shape is becoming dominant enough to increase operational risk.",
		Rationale:           "One pattern crossed the concentration threshold in the trend summary.",
		RelatedFindingKinds: []string{f.Kind},
		EvidenceRefs:        cloneTrendRecommendationEvidenceRefs(f.EvidenceRefs),
	}
}

func trendSpikeRecommendation(f TrendFinding) Recommendation {
	return Recommendation{
		Kind:                "capture_followup_snapshot",
		Priority:            "high",
		Title:               "Capture follow-up snapshot",
		Summary:             f.Summary + "; capture or compare a neighboring window to distinguish a one-off spike from a persistent shift.",
		Rationale:           "One ordered point is a clear row-volume outlier relative to the series median.",
		RelatedFindingKinds: []string{f.Kind},
		EvidenceRefs:        cloneTrendRecommendationEvidenceRefs(f.EvidenceRefs),
	}
}

func absShareDeltaFromTrendEvidence(f TrendFinding) float64 {
	share, _ := f.Evidence["share_delta"].(float64)
	if share < 0 {
		return -share
	}
	return share
}

func trendPriorityRank(priority string) int {
	switch priority {
	case "high":
		return 0
	case "medium":
		return 1
	case "low":
		return 2
	default:
		return 3
	}
}

func trendRecommendationKindRank(kind string) int {
	switch kind {
	case "watch_workload_concentration":
		return 0
	case "capture_followup_snapshot":
		return 1
	case "track_rising_pattern":
		return 2
	case "review_growing_table":
		return 3
	case "confirm_declining_pattern":
		return 4
	default:
		return 9
	}
}

func dedupeTrendRecommendations(in []Recommendation) []Recommendation {
	seen := map[string]bool{}
	out := make([]Recommendation, 0, len(in))
	for _, rec := range in {
		if seen[rec.Kind] {
			continue
		}
		seen[rec.Kind] = true
		out = append(out, rec)
	}
	return out
}

func cloneTrendRecommendationEvidenceRefs(in []EvidenceRef) []EvidenceRef {
	if len(in) == 0 {
		return nil
	}
	out := make([]EvidenceRef, len(in))
	copy(out, in)
	return out
}
