package trend

import (
	"fmt"
	"math"
	"sort"
)

const (
	// trendDrilldownCap is the maximum number of trend pattern drilldowns emitted.
	trendDrilldownCap = 2
	// trendKeyPointsCap is the maximum number of key points per drilldown.
	trendKeyPointsCap = 2
	// trendShareThreshold is the minimum |delta_share_of_rows| for dominant selection.
	trendShareThreshold = 0.20
	// trendConcentratedJumpThreshold is the minimum absolute share step required
	// before a single interval jump is worth a drilldown on its own.
	trendConcentratedJumpThreshold = 0.15
)

func buildTrendPatternDrilldowns(result Result) []PatternDrilldown {
	trends := result.PatternTrends
	if len(trends) == 0 {
		return []PatternDrilldown{}
	}

	type candidate struct {
		trend     PatternTrend
		score     float64
		flags     TrendDrilldownSignals
		keyPoints []TrendKeyPoint
	}

	var candidates []candidate
	for _, pt := range trends {
		score := trendDominanceScore(pt)
		flags := trendSignalFlags(pt)

		dominant := flags.DominantShareShift
		anomaly := flags.SteadyRise || flags.SteadyFall || flags.ConcentratedJump

		if !dominant && !anomaly {
			continue
		}
		if dominant && !anomaly && score < trendShareThreshold*2 {
			continue
		}
		if anomaly && !dominant && score < trendShareThreshold/2 {
			continue
		}

		kps := trendKeyPoints(pt, flags)
		candidates = append(candidates, candidate{
			trend:     pt,
			score:     score,
			flags:     flags,
			keyPoints: kps,
		})
	}

	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].score != candidates[j].score {
			return candidates[i].score > candidates[j].score
		}
		return candidates[i].trend.PatternKey < candidates[j].trend.PatternKey
	})

	if len(candidates) > trendDrilldownCap {
		candidates = candidates[:trendDrilldownCap]
	}

	out := make([]PatternDrilldown, 0, len(candidates))
	for _, c := range candidates {
		out = append(out, PatternDrilldown{
			PatternKey:  c.trend.PatternKey,
			Label:       c.trend.Label,
			WhySelected: formatTrendWhySelected(c.trend, c.flags),
			StartShare:  c.trend.FirstShareOfRows,
			EndShare:    c.trend.LastShareOfRows,
			ShareDelta:  c.trend.DeltaShareOfRows,
			StartRows:   c.trend.FirstRows,
			EndRows:     c.trend.LastRows,
			RowsDelta:   c.trend.DeltaRows,
			SignalFlags: c.flags,
			KeyPoints:   c.keyPoints,
		})
	}
	return out
}

func trendDominanceScore(pattern PatternTrend) float64 {
	return math.Abs(pattern.DeltaShareOfRows)
}

func trendSignalFlags(pattern PatternTrend) TrendDrilldownSignals {
	flags := TrendDrilldownSignals{
		DominantShareShift: math.Abs(pattern.DeltaShareOfRows) >= trendShareThreshold,
	}

	series := pattern.ShareOfRowsSeries
	if len(series) >= 2 {
		// Check for steady rise: all consecutive steps go up
		steadyRise := true
		steadyFall := true
		for i := 1; i < len(series); i++ {
			if series[i].ShareOfRows <= series[i-1].ShareOfRows {
				steadyRise = false
			}
			if series[i].ShareOfRows >= series[i-1].ShareOfRows {
				steadyFall = false
			}
		}
		flags.SteadyRise = steadyRise && pattern.DeltaShareOfRows > 0 && pattern.DeltaRows > 0
		flags.SteadyFall = steadyFall && pattern.DeltaShareOfRows < 0 && pattern.DeltaRows < 0

		// Check for concentrated jump: one step accounts for most of the movement
		maxStep := 0.0
		for i := 1; i < len(series); i++ {
			step := math.Abs(series[i].ShareOfRows - series[i-1].ShareOfRows)
			if step > maxStep {
				maxStep = step
			}
		}
		totalMove := math.Abs(pattern.DeltaShareOfRows)
		if totalMove > 0 && maxStep >= trendConcentratedJumpThreshold && maxStep/totalMove > 0.70 {
			flags.ConcentratedJump = true
		}
	}

	return flags
}

func trendKeyPoints(pattern PatternTrend, flags TrendDrilldownSignals) []TrendKeyPoint {
	var kps []TrendKeyPoint

	direction := "rising"
	if pattern.DeltaRows < 0 && pattern.DeltaShareOfRows < 0 {
		direction = "falling"
	} else if pattern.DeltaRows > 0 && pattern.DeltaShareOfRows < 0 {
		direction = "rows grew while share fell"
	} else if pattern.DeltaRows < 0 && pattern.DeltaShareOfRows > 0 {
		direction = "rows fell while share rose"
	}

	kps = append(kps, TrendKeyPoint{
		Label:   "direction",
		Summary: fmt.Sprintf("%s across series: share %.2f -> %.2f, rows %d -> %d", direction, pattern.FirstShareOfRows, pattern.LastShareOfRows, pattern.FirstRows, pattern.LastRows),
	})

	if flags.ConcentratedJump && len(pattern.ShareOfRowsSeries) >= 2 && len(kps) < trendKeyPointsCap {
		// Find the biggest jump step
		maxStep := 0.0
		maxIdx := 0
		for i := 1; i < len(pattern.ShareOfRowsSeries); i++ {
			step := math.Abs(pattern.ShareOfRowsSeries[i].ShareOfRows - pattern.ShareOfRowsSeries[i-1].ShareOfRows)
			if step > maxStep {
				maxStep = step
				maxIdx = i
			}
		}
		from := pattern.ShareOfRowsSeries[maxIdx-1].SnapshotName
		to := pattern.ShareOfRowsSeries[maxIdx].SnapshotName
		kps = append(kps, TrendKeyPoint{
			Label:   "concentrated jump",
			Summary: fmt.Sprintf("largest share shift between %s and %s", from, to),
		})
	}

	if len(kps) > trendKeyPointsCap {
		kps = kps[:trendKeyPointsCap]
	}

	return kps
}

func formatTrendWhySelected(pattern PatternTrend, flags TrendDrilldownSignals) string {
	switch {
	case flags.DominantShareShift && flags.SteadyRise:
		return "dominant and steadily rising share across the series"
	case flags.DominantShareShift && flags.SteadyFall:
		return "dominant and steadily falling share across the series"
	case flags.DominantShareShift && flags.ConcentratedJump:
		return "dominant share shift concentrated in one interval"
	case flags.DominantShareShift:
		return "dominant share movement across the series"
	case flags.SteadyRise:
		return "steady share rise across the series"
	case flags.SteadyFall:
		return "steady share decline across the series"
	case flags.ConcentratedJump:
		return "concentrated share jump in one interval"
	default:
		return "high-signal share movement"
	}
}
