package compare

import "sort"

const maxRecommendations = 5

func buildCompareRecommendations(result CompareResult) []Recommendation {
	candidates := make([]Recommendation, 0, len(result.KeyFindings))
	hasSpecificGrowthDriver := false

	for _, f := range result.KeyFindings {
		switch f.Kind {
		case "pattern_driver":
			rec := comparePatternDriverRecommendation(f)
			candidates = append(candidates, rec)
			if rec.Priority == "high" {
				hasSpecificGrowthDriver = true
			}
		case "table_driver":
			rec := compareTableDriverRecommendation(f)
			candidates = append(candidates, rec)
			if rec.Priority == "high" {
				hasSpecificGrowthDriver = true
			}
		case "new_pattern":
			rec := compareNewPatternRecommendation(f)
			candidates = append(candidates, rec)
			if rec.Priority == "high" {
				hasSpecificGrowthDriver = true
			}
		case "operation_mix_drift":
			candidates = append(candidates, compareOperationMixRecommendation(f))
		case "volume_change":
			candidates = append(candidates, compareVolumeRecommendation(f))
		}
	}

	if hasSpecificGrowthDriver {
		candidates = dropCompareRecommendationKind(candidates, "check_volume_growth_source")
	}

	sort.SliceStable(candidates, func(i, j int) bool {
		if priorityRank(candidates[i].Priority) != priorityRank(candidates[j].Priority) {
			return priorityRank(candidates[i].Priority) < priorityRank(candidates[j].Priority)
		}
		if compareRecommendationKindRank(candidates[i].Kind) != compareRecommendationKindRank(candidates[j].Kind) {
			return compareRecommendationKindRank(candidates[i].Kind) < compareRecommendationKindRank(candidates[j].Kind)
		}
		return candidates[i].Kind < candidates[j].Kind
	})

	candidates = dedupeCompareRecommendations(candidates)
	if len(candidates) > maxRecommendations {
		candidates = candidates[:maxRecommendations]
	}
	if len(candidates) == 0 {
		return []Recommendation{}
	}
	return candidates
}

func comparePatternDriverRecommendation(f CompareFinding) Recommendation {
	priority := "medium"
	if shareOfTotalDelta(f) >= 0.50 {
		priority = "high"
	}
	return Recommendation{
		Kind:                "check_pattern_driver",
		Priority:            priority,
		Title:               "Check pattern driver",
		Summary:             f.Summary + "; confirm whether a deploy, backfill, or scheduled batch changed during this window.",
		Rationale:           "A single write pattern explains a significant share of the row delta.",
		RelatedFindingKinds: []string{f.Kind},
		EvidenceRefs:        cloneRecommendationEvidenceRefs(f.EvidenceRefs),
	}
}

func compareTableDriverRecommendation(f CompareFinding) Recommendation {
	priority := "medium"
	if shareOfTotalDelta(f) >= 0.50 {
		priority = "high"
	}
	return Recommendation{
		Kind:                "check_table_hotspot",
		Priority:            priority,
		Title:               "Check table hotspot",
		Summary:             f.Summary + "; review the table owner, recent jobs, and whether this table-specific write movement is expected.",
		Rationale:           "A table-level delta is large enough to be an operator follow-up target.",
		RelatedFindingKinds: []string{f.Kind},
		EvidenceRefs:        cloneRecommendationEvidenceRefs(f.EvidenceRefs),
	}
}

func compareNewPatternRecommendation(f CompareFinding) Recommendation {
	priority := "medium"
	if share, ok := f.Evidence["share_of_rows"].(float64); ok && share >= 0.20 {
		priority = "high"
	}
	return Recommendation{
		Kind:                "check_new_write_pattern",
		Priority:            priority,
		Title:               "Check new write pattern",
		Summary:             f.Summary + "; identify whether this new shape came from a planned rollout, backfill, or unexpected workload.",
		Rationale:           "A pattern appears in the current window without baseline rows.",
		RelatedFindingKinds: []string{f.Kind},
		EvidenceRefs:        cloneRecommendationEvidenceRefs(f.EvidenceRefs),
	}
}

func compareOperationMixRecommendation(f CompareFinding) Recommendation {
	return Recommendation{
		Kind:                "check_operation_mix_shift",
		Priority:            "medium",
		Title:               "Check operation mix shift",
		Summary:             f.Summary + "; confirm whether insert, update, or delete behavior changed intentionally.",
		Rationale:           "The write operation mix shifted by more than the finding threshold.",
		RelatedFindingKinds: []string{f.Kind},
		EvidenceRefs:        cloneRecommendationEvidenceRefs(f.EvidenceRefs),
	}
}

func compareVolumeRecommendation(f CompareFinding) Recommendation {
	delta, _ := f.Evidence["delta_rows"].(int)
	kind := "check_volume_growth_source"
	title := "Check volume growth source"
	summary := f.Summary + "; confirm expected workload growth and identify the source of the larger write window."
	if delta < 0 {
		kind = "check_volume_drop_source"
		title = "Check volume drop source"
		summary = f.Summary + "; confirm whether ingestion, workload, filters, or upstream scheduling changed."
	}
	priority := "medium"
	switch f.Summary {
	case "rows more than doubled", "rows more than halved", "rows grew sharply", "rows declined sharply":
		priority = "high"
	}
	return Recommendation{
		Kind:                kind,
		Priority:            priority,
		Title:               title,
		Summary:             summary,
		Rationale:           "Overall row volume changed enough to require operator confirmation.",
		RelatedFindingKinds: []string{f.Kind},
		EvidenceRefs:        cloneRecommendationEvidenceRefs(f.EvidenceRefs),
	}
}

func shareOfTotalDelta(f CompareFinding) float64 {
	if share, ok := f.Evidence["share_of_total_delta"].(float64); ok {
		return share
	}
	return 0
}

func dropCompareRecommendationKind(in []Recommendation, kind string) []Recommendation {
	out := in[:0]
	for _, rec := range in {
		if rec.Kind == kind {
			continue
		}
		out = append(out, rec)
	}
	return out
}

func dedupeCompareRecommendations(in []Recommendation) []Recommendation {
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

func priorityRank(priority string) int {
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

func compareRecommendationKindRank(kind string) int {
	switch kind {
	case "check_pattern_driver":
		return 0
	case "check_new_write_pattern":
		return 1
	case "check_table_hotspot":
		return 2
	case "check_operation_mix_shift":
		return 3
	case "check_volume_growth_source", "check_volume_drop_source":
		return 4
	default:
		return 9
	}
}

func cloneRecommendationEvidenceRefs(in []EvidenceRef) []EvidenceRef {
	if len(in) == 0 {
		return nil
	}
	out := make([]EvidenceRef, len(in))
	copy(out, in)
	return out
}
