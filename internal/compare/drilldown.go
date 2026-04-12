package compare

import (
	"fmt"
	"sort"
)

const (
	// compareDrilldownCap is the maximum number of compare pattern drilldowns emitted.
	compareDrilldownCap = 2
	// compareKeyPointsCap is the maximum number of key points per drilldown.
	compareKeyPointsCap = 2
	// compareDominanceThreshold is the minimum share_of_total_delta for dominant selection.
	compareDominanceThreshold = 0.40
	// compareNewPatternRowThreshold is the minimum current_rows for a new pattern to qualify.
	compareNewPatternRowThreshold = 100
)

func buildComparePatternDrilldowns(result CompareResult) []PatternDrilldown {
	changes := result.PatternChanges
	if len(changes) == 0 {
		return []PatternDrilldown{}
	}

	totalDelta := result.Summary.TotalRowsDelta
	if totalDelta == 0 {
		totalDelta = 1 // avoid division by zero; no meaningful delta to rank against
	}

	type candidate struct {
		change   PatternChange
		score    float64
		flags    CompareDrilldownSignals
		keyPoints []CompareKeyPoint
	}

	var candidates []candidate
	for _, ch := range changes {
		score := compareDominanceScore(ch, totalDelta)
		flags := compareSignalFlags(ch, totalDelta)
		kps := compareKeyPoints(ch, flags)

		// Select only when both dominance and anomaly are present,
		// or one of them is extremely strong.
		dominant := flags.DominantDelta
		anomaly := flags.NewPattern || flags.Disappeared || flags.TxnRowsDiverged

		if !dominant && !anomaly {
			continue
		}
		if !dominant && score < compareDominanceThreshold*2 {
			// anomaly alone must be very strong
			continue
		}

		candidates = append(candidates, candidate{
			change:    ch,
			score:     score,
			flags:     flags,
			keyPoints: kps,
		})
	}

	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].score != candidates[j].score {
			return candidates[i].score > candidates[j].score
		}
		return candidates[i].change.PatternKey < candidates[j].change.PatternKey
	})

	if len(candidates) > compareDrilldownCap {
		candidates = candidates[:compareDrilldownCap]
	}

	out := make([]PatternDrilldown, 0, len(candidates))
	for _, c := range candidates {
		out = append(out, PatternDrilldown{
			PatternKey:   c.change.PatternKey,
			Label:        c.change.Label,
			WhySelected:  formatCompareWhySelected(c.change, totalDelta, c.flags),
			BaselineRows: c.change.BaselineRows,
			CurrentRows:  c.change.CurrentRows,
			DeltaRows:    c.change.DeltaRows,
			BaselineTxns: c.change.BaselineTxnCount,
			CurrentTxns:  c.change.CurrentTxnCount,
			DeltaTxns:    c.change.DeltaTxnCount,
			SignalFlags:  c.flags,
			KeyPoints:    c.keyPoints,
		})
	}
	return out
}

func compareDominanceScore(change PatternChange, totalDelta int) float64 {
	if totalDelta == 0 {
		return 0
	}
	return float64(absInt(change.DeltaRows)) / float64(absInt(totalDelta))
}

func compareSignalFlags(change PatternChange, totalDelta int) CompareDrilldownSignals {
	share := compareDominanceScore(change, totalDelta)

	flags := CompareDrilldownSignals{
		DominantDelta: share >= compareDominanceThreshold,
		NewPattern:    change.BaselineRows == 0 && change.CurrentRows >= compareNewPatternRowThreshold,
		Disappeared:   change.CurrentRows == 0 && change.BaselineRows >= compareNewPatternRowThreshold,
	}

	// Detect txn/rows divergence: delta directions differ, or ratio shifts sharply.
	if change.DeltaRows != 0 && change.DeltaTxnCount != 0 {
		rowsSign := signInt(change.DeltaRows)
		txnsSign := signInt(change.DeltaTxnCount)
		if rowsSign != txnsSign {
			flags.TxnRowsDiverged = true
		}
	}

	return flags
}

func compareKeyPoints(change PatternChange, flags CompareDrilldownSignals) []CompareKeyPoint {
	var kps []CompareKeyPoint

	if flags.NewPattern {
		kps = append(kps, CompareKeyPoint{
			Label:   "new in current window",
			Summary: summarizeRowCounts(change),
		})
	} else if flags.Disappeared {
		kps = append(kps, CompareKeyPoint{
			Label:   "absent in current window",
			Summary: summarizeRowCounts(change),
		})
	} else {
		kps = append(kps, CompareKeyPoint{
			Label:   "baseline context",
			Summary: summarizeRowCounts(change),
		})
	}

	if flags.DominantDelta && len(kps) < compareKeyPointsCap {
		kps = append(kps, CompareKeyPoint{
			Label:   "current context",
			Summary: summarizeTxnContext(change),
		})
	}

	if len(kps) > compareKeyPointsCap {
		kps = kps[:compareKeyPointsCap]
	}

	return kps
}

func formatCompareWhySelected(change PatternChange, totalDelta int, flags CompareDrilldownSignals) string {
	switch {
	case flags.DominantDelta && flags.NewPattern:
		return "dominant new pattern driving most of the row delta"
	case flags.DominantDelta && flags.Disappeared:
		return "dominant pattern disappeared from the current window"
	case flags.DominantDelta:
		return "dominant driver of the row delta between windows"
	case flags.NewPattern:
		return "new pattern appeared at meaningful scale"
	case flags.Disappeared:
		return "pattern disappeared from the current window"
	case flags.TxnRowsDiverged:
		return "row and transaction deltas diverged sharply"
	default:
		return "high-signal pattern change"
	}
}

func summarizeRowCounts(change PatternChange) string {
	return formatRowTxnString(change.BaselineRows, change.CurrentRows, change.DeltaRows, change.BaselineTxnCount, change.CurrentTxnCount, change.DeltaTxnCount)
}

func summarizeTxnContext(change PatternChange) string {
	return formatRowTxnString(change.BaselineRows, change.CurrentRows, change.DeltaRows, change.BaselineTxnCount, change.CurrentTxnCount, change.DeltaTxnCount)
}

func formatRowTxnString(bRows, cRows, dRows, bTxns, cTxns, dTxns int) string {
	return fmt.Sprintf("rows %d→%d (%+d), txns %d→%d (%+d)", bRows, cRows, dRows, bTxns, cTxns, dTxns)
}

func absInt(v int) int {
	if v < 0 {
		return -v
	}
	return v
}

func signInt(v int) int {
	if v < 0 {
		return -1
	}
	if v > 0 {
		return 1
	}
	return 0
}
