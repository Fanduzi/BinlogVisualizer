// Package compare selects key findings from compare results.
// input: computed CompareResult metrics (deltas, patterns, tables, operations).
// output: deterministic list of CompareFinding values capped at 5.
// pos: compare finding builder between diff computation and renderer output.
package compare

import "math"

const maxFindings = 5

// minShareThreshold is the minimum share of total delta a candidate must
// represent before it is included as a finding. This prevents low-signal noise.
const minShareThreshold = 0.15

// minAbsDelta is the minimum absolute row delta before a volume-change finding
// is emitted. Below this threshold the change is too small to be actionable.
const minAbsDelta = 100

func buildKeyFindings(result CompareResult) []CompareFinding {
	totalDelta := result.Summary.TotalRowsDelta
	absTotal := math.Abs(float64(totalDelta))
	findings := make([]CompareFinding, 0, maxFindings)

	// 1. Overall volume change severity
	if absTotal >= float64(minAbsDelta) {
		severity := volumeSeverity(totalDelta, result.Summary.BaselineTotalRows)
		findings = append(findings, CompareFinding{
			Kind:    "volume_change",
			Title:   "Total volume change",
			Summary: severity,
			Evidence: map[string]any{
				"delta_rows":    totalDelta,
				"baseline_rows": result.Summary.BaselineTotalRows,
				"current_rows":  result.Summary.CurrentTotalRows,
			},
		})
	}

	// 2. Top pattern driver
	if len(result.PatternChanges) > 0 && absTotal >= float64(minAbsDelta) {
		top := result.PatternChanges[0]
		if top.DeltaRows != 0 && shareOfAbs(float64(top.DeltaRows), absTotal) >= minShareThreshold {
			name := top.Label
			if name == "" {
				name = top.PatternKey
			}
			driverDir := "growth"
			if totalDelta < 0 {
				driverDir = "decline"
			}
			findings = append(findings, CompareFinding{
				Kind:    "pattern_driver",
				Title:   "Top pattern driver",
				Summary: name + " drove most row " + driverDir,
				Evidence: map[string]any{
					"pattern_key":         top.PatternKey,
					"delta_rows":          top.DeltaRows,
					"share_of_total_delta": shareOfAbs(float64(top.DeltaRows), absTotal),
				},
			})
		}
	}

	// 3. Top table driver
	if len(result.TableChanges) > 0 && absTotal >= float64(minAbsDelta) {
		top := result.TableChanges[0]
		if top.DeltaRows != 0 && shareOfAbs(float64(top.DeltaRows), absTotal) >= minShareThreshold {
			findings = append(findings, CompareFinding{
				Kind:    "table_driver",
				Title:   "Top table driver",
				Summary: top.Schema + "." + top.Table + " had the largest row change",
				Evidence: map[string]any{
					"table":               top.Schema + "." + top.Table,
					"delta_rows":          top.DeltaRows,
					"share_of_total_delta": shareOfAbs(float64(top.DeltaRows), absTotal),
				},
			})
		}
	}

	// 4. Operation mix drift
	if len(result.OperationMix) > 0 && absTotal >= float64(minAbsDelta) {
		drift := operationMixDrift(result.OperationMix, result.Summary.BaselineTotalRows)
		if drift != nil {
			findings = append(findings, *drift)
		}
	}

	// 5. New significant pattern (baseline had 0 rows, current has significant share)
	if len(result.PatternChanges) > 0 && len(findings) < maxFindings {
		if f := newPatternFinding(result.PatternChanges, result.Summary.CurrentTotalRows); f != nil {
			findings = append(findings, *f)
		}
	}

	if len(findings) > maxFindings {
		findings = findings[:maxFindings]
	}

	return findings
}

func volumeSeverity(delta, baseline int) string {
	if baseline == 0 {
		return "rows appeared in current window"
	}
	pct := math.Abs(float64(delta)/float64(baseline)) * 100
	dir := "grew"
	if delta < 0 {
		dir = "declined"
	}
	switch {
	case pct >= 100:
		if delta < 0 {
			return "rows more than halved"
		}
		return "rows more than doubled"
	case pct >= 50:
		return "rows " + dir + " sharply"
	case pct >= 20:
		return "rows " + dir + " moderately"
	case pct >= 5:
		return "rows " + dir + " slightly"
	default:
		return "rows changed minimally"
	}
}

func shareOfAbs(part, totalAbs float64) float64 {
	if totalAbs == 0 {
		return 0
	}
	return math.Abs(part) / totalAbs
}

func operationMixDrift(mix []OperationDelta, baselineRows int) *CompareFinding {
	// Check if any single operation's delta share shifted significantly
	var baselineTotal, currentTotal int
	for _, op := range mix {
		baselineTotal += op.Baseline
		currentTotal += op.Current
	}
	if baselineTotal == 0 || currentTotal == 0 {
		return nil
	}

	var dominantOp string
	var baselineShare, currentShare float64
	for _, op := range mix {
		bShare := float64(op.Baseline) / float64(baselineTotal)
		cShare := float64(op.Current) / float64(currentTotal)
		drift := math.Abs(cShare - bShare)
		if drift > 0.10 && drift > math.Abs(currentShare-baselineShare) {
			dominantOp = op.Operation
			baselineShare = bShare
			currentShare = cShare
		}
	}
	if dominantOp == "" {
		return nil
	}

	direction := "increased"
	if currentShare < baselineShare {
		direction = "decreased"
	}
	return &CompareFinding{
		Kind:    "operation_mix_drift",
		Title:   "Operation mix drift",
		Summary: dominantOp + " share " + direction + " significantly",
		Evidence: map[string]any{
			"operation":      dominantOp,
			"baseline_share": baselineShare,
			"current_share":  currentShare,
		},
	}
}

func newPatternFinding(patterns []PatternChange, currentTotalRows int) *CompareFinding {
	if currentTotalRows == 0 {
		return nil
	}
	for _, p := range patterns {
		if p.BaselineRows > 0 {
			continue
		}
		if p.CurrentRows == 0 {
			continue
		}
		share := float64(p.CurrentRows) / float64(currentTotalRows)
		if share >= 0.10 {
			name := p.Label
			if name == "" {
				name = p.PatternKey
			}
			return &CompareFinding{
				Kind:    "new_pattern",
				Title:   "New significant pattern",
				Summary: name + " appeared as a new write pattern",
				Evidence: map[string]any{
					"pattern_key":   p.PatternKey,
					"current_rows":  p.CurrentRows,
					"share_of_rows": share,
				},
			}
		}
	}
	return nil
}
