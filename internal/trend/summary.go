// Package trend selects trend summary findings from multi-snapshot results.
// input: computed trend Result metrics (pattern trends, table trends, insights).
// output: deterministic list of TrendFinding values capped at 5.
// pos: trend summary builder between result construction and renderer output.
package trend

import (
	"fmt"
	"sort"
)

const maxTrendFindings = 5

// trendMinDelta is the minimum absolute row delta before a trend finding is emitted.
const trendMinDelta = 100

func buildTrendSummary(result Result) []TrendFinding {
	findings := make([]TrendFinding, 0, maxTrendFindings)

	// 1. Strongest rising pattern
	if f := risingPatternFinding(result.PatternTrends); f != nil {
		findings = append(findings, *f)
	}

	// 2. Strongest falling pattern
	if f := fallingPatternFinding(result.PatternTrends); f != nil {
		findings = append(findings, *f)
	}

	// 3. Top table increase
	if f := topTableIncreaseFinding(result.TableTrends); f != nil {
		findings = append(findings, *f)
	}

	// 4. Concentration shift
	if f := concentrationFinding(result.PatternTrends); f != nil {
		findings = append(findings, *f)
	}

	// 5. Spike vs persistent (if one point is a clear outlier)
	if f := spikePersistenceFinding(result.Points); f != nil {
		findings = append(findings, *f)
	}

	if len(findings) > maxTrendFindings {
		findings = findings[:maxTrendFindings]
	}

	return findings
}

func risingPatternFinding(trends []PatternTrend) *TrendFinding {
	var best *PatternTrend
	for i := range trends {
		t := &trends[i]
		if t.DeltaRows <= trendMinDelta {
			continue
		}
		if best == nil || t.DeltaRows > best.DeltaRows {
			best = t
		}
	}
	if best == nil {
		return nil
	}
	name := best.Label
	if name == "" {
		name = best.PatternKey
	}
	return &TrendFinding{
		Kind:    "rising_pattern",
		Title:   "Strongest rising pattern",
		Summary: fmt.Sprintf("%s grew by %+d rows across the series", name, best.DeltaRows),
		Evidence: map[string]any{
			"pattern_key":   best.PatternKey,
			"delta_rows":    best.DeltaRows,
			"first_rows":    best.FirstRows,
			"last_rows":     best.LastRows,
			"share_delta":   best.DeltaShareOfRows,
		},
	}
}

func fallingPatternFinding(trends []PatternTrend) *TrendFinding {
	var best *PatternTrend
	for i := range trends {
		t := &trends[i]
		if t.DeltaRows >= -trendMinDelta {
			continue
		}
		if best == nil || t.DeltaRows < best.DeltaRows {
			best = t
		}
	}
	if best == nil {
		return nil
	}
	name := best.Label
	if name == "" {
		name = best.PatternKey
	}
	return &TrendFinding{
		Kind:    "falling_pattern",
		Title:   "Strongest falling pattern",
		Summary: fmt.Sprintf("%s declined by %+d rows across the series", name, best.DeltaRows),
		Evidence: map[string]any{
			"pattern_key":   best.PatternKey,
			"delta_rows":    best.DeltaRows,
			"first_rows":    best.FirstRows,
			"last_rows":     best.LastRows,
			"share_delta":   best.DeltaShareOfRows,
		},
	}
}

func topTableIncreaseFinding(trends []TableTrend) *TrendFinding {
	if len(trends) == 0 {
		return nil
	}
	// Already sorted by abs(delta) desc from buildTableTrends
	top := trends[0]
	if top.DeltaRows < trendMinDelta {
		return nil
	}
	return &TrendFinding{
		Kind:    "table_trend",
		Title:   "Top table trend",
		Summary: fmt.Sprintf("%s.%s grew by %+d rows", top.Schema, top.Table, top.DeltaRows),
		Evidence: map[string]any{
			"table":      top.Schema + "." + top.Table,
			"delta_rows": top.DeltaRows,
			"first_rows": top.FirstRows,
			"last_rows":  top.LastRows,
		},
	}
}

func concentrationFinding(trends []PatternTrend) *TrendFinding {
	if len(trends) < 2 {
		return nil
	}
	// Check if any pattern's share crossed 60%
	for _, t := range trends {
		if t.FirstShareOfRows < 0.50 && t.LastShareOfRows >= 0.60 {
			name := t.Label
			if name == "" {
				name = t.PatternKey
			}
			return &TrendFinding{
				Kind:    "concentration_shift",
				Title:   "Concentration shift",
				Summary: fmt.Sprintf("%s became dominant at %.0f%% share", name, t.LastShareOfRows*100),
				Evidence: map[string]any{
					"pattern_key":     t.PatternKey,
					"first_share":     t.FirstShareOfRows,
					"last_share":      t.LastShareOfRows,
					"share_delta":     t.DeltaShareOfRows,
				},
			}
		}
	}
	return nil
}

func spikePersistenceFinding(points []Point) *TrendFinding {
	if len(points) < 3 {
		return nil
	}
	// Check if the largest point is a clear outlier (>2x median)
	rows := make([]int, len(points))
	for i, p := range points {
		rows[i] = p.Summary.TotalRows
	}
	sort.Ints(rows)
	median := rows[len(rows)/2]

	var maxIdx int
	var maxVal int
	for i, r := range rows {
		if r > maxVal {
			maxVal = r
			maxIdx = i
		}
	}
	_ = maxIdx

	if median > 0 && float64(maxVal) > 2.0*float64(median) {
		// Find which point has this max
		for _, p := range points {
			if p.Summary.TotalRows == maxVal {
				return &TrendFinding{
					Kind:    "spike_outlier",
					Title:   "Spike outlier",
					Summary: fmt.Sprintf("snapshot %s is a clear volume spike", p.Snapshot.Name),
					Evidence: map[string]any{
						"snapshot_name": p.Snapshot.Name,
						"rows":          p.Summary.TotalRows,
						"median_rows":   median,
					},
				}
			}
		}
	}

	_ = maxIdx
	return nil
}
