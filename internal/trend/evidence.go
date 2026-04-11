// Package trend builds evidence refs that link findings back to trend report sections.
// input: a trend Result with populated trend_summary and section data.
// output: deterministic evidence_refs attached to each TrendFinding.
// pos: evidence ref builder called after trend summary is constructed.
package trend

import "fmt"

// buildTrendEvidenceRefs attaches evidence refs to each finding in the result.
// It only emits refs when the finding's evidence data matches an existing item.
func buildTrendEvidenceRefs(result *Result) {
	for i := range result.TrendSummary {
		f := &result.TrendSummary[i]
		f.EvidenceRefs = buildTrendFindingRefs(*f, result)
	}
}

func buildTrendFindingRefs(f TrendFinding, result *Result) []EvidenceRef {
	var refs []EvidenceRef

	switch f.Kind {
	case "rising_pattern", "falling_pattern":
		if key, ok := f.Evidence["pattern_key"].(string); ok {
			idx := findPatternTrendIndex(result.PatternTrends, key)
			if idx >= 0 {
				refs = append(refs, EvidenceRef{
					Section: "pattern_trends",
					Key:     key,
					Label:   patternTrendDisplayLabel(result.PatternTrends[idx]),
					Anchor:  trendAnchor("pattern", idx),
				})
			}
		}

	case "table_trend":
		if table, ok := f.Evidence["table"].(string); ok {
			idx := findTableTrendIndex(result.TableTrends, table)
			if idx >= 0 {
				refs = append(refs, EvidenceRef{
					Section: "table_trends",
					Key:     table,
					Label:   table,
					Anchor:  trendAnchor("table", idx),
				})
			}
		}

	case "concentration_shift":
		if key, ok := f.Evidence["pattern_key"].(string); ok {
			idx := findPatternTrendIndex(result.PatternTrends, key)
			if idx >= 0 {
				refs = append(refs, EvidenceRef{
					Section: "pattern_trends",
					Key:     key,
					Label:   patternTrendDisplayLabel(result.PatternTrends[idx]),
					Anchor:  trendAnchor("pattern", idx),
				})
			}
		}

	case "spike_outlier":
		if name, ok := f.Evidence["snapshot_name"].(string); ok {
			idx := findPointIndex(result.Points, name)
			if idx >= 0 {
				refs = append(refs, EvidenceRef{
					Section: "ordered_points",
					Key:     name,
					Label:   name,
					Anchor:  trendAnchor("point", idx),
				})
			}
		}
	}

	if len(refs) == 0 {
		return nil
	}
	return refs
}

func findPatternTrendIndex(trends []PatternTrend, key string) int {
	for i, t := range trends {
		if t.PatternKey == key {
			return i
		}
	}
	return -1
}

func findTableTrendIndex(trends []TableTrend, table string) int {
	for i, t := range trends {
		if t.Schema+"."+t.Table == table {
			return i
		}
	}
	return -1
}

func findPointIndex(points []Point, name string) int {
	for i, p := range points {
		if p.Snapshot.Name == name {
			return i
		}
	}
	return -1
}

func trendAnchor(prefix string, idx int) string {
	return fmt.Sprintf("%s-%d", prefix, idx)
}
