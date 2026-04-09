package trend

import (
	"math"
	"sort"
	"strings"

	comparepkg "binlogviz/internal/compare"
)

type patternTrendState struct {
	trend PatternTrend
}

func buildPatternTrends(points []resolvedPoint) []PatternTrend {
	if len(points) == 0 {
		return nil
	}

	pointPatterns := make([]map[string]comparepkg.InputPattern, len(points))
	aggregates := make(map[string]*patternTrendState)

	for idx, point := range points {
		patternMap := make(map[string]comparepkg.InputPattern, len(point.Report.Patterns))
		for _, pattern := range point.Report.Patterns {
			if strings.TrimSpace(pattern.PatternKey) == "" {
				continue
			}
			patternMap[pattern.PatternKey] = pattern
			state, ok := aggregates[pattern.PatternKey]
			if !ok {
				state = &patternTrendState{
					trend: PatternTrend{
						PatternKey: pattern.PatternKey,
					},
				}
				aggregates[pattern.PatternKey] = state
			}
			if state.trend.Label == "" && strings.TrimSpace(pattern.Label) != "" {
				state.trend.Label = pattern.Label
			}
			if state.trend.SampleQuerySummary == "" && strings.TrimSpace(pattern.SampleQuerySummary) != "" {
				state.trend.SampleQuerySummary = pattern.SampleQuerySummary
			}
		}
		pointPatterns[idx] = patternMap
	}

	result := make([]PatternTrend, 0, len(aggregates))
	for _, state := range aggregates {
		seriesRows := make([]PatternTrendRowsPoint, 0, len(points))
		seriesShares := make([]PatternTrendSharePoint, 0, len(points))
		for idx, point := range points {
			pattern, ok := pointPatterns[idx][state.trend.PatternKey]
			rows := 0
			share := 0.0
			if ok {
				rows = pattern.TotalRows
				share = shareOfRows(rows, point.Report.Summary.TotalRows)
			}
			seriesRows = append(seriesRows, PatternTrendRowsPoint{
				SnapshotName: point.Meta.Name,
				StartTime:    point.Meta.Window.StartTime,
				Rows:         rows,
			})
			seriesShares = append(seriesShares, PatternTrendSharePoint{
				SnapshotName: point.Meta.Name,
				StartTime:    point.Meta.Window.StartTime,
				ShareOfRows:  share,
			})
		}

		firstRows := 0
		lastRows := 0
		firstShare := 0.0
		lastShare := 0.0
		if len(seriesRows) > 0 {
			firstRows = seriesRows[0].Rows
			lastRows = seriesRows[len(seriesRows)-1].Rows
		}
		if len(seriesShares) > 0 {
			firstShare = seriesShares[0].ShareOfRows
			lastShare = seriesShares[len(seriesShares)-1].ShareOfRows
		}

		state.trend.FirstRows = firstRows
		state.trend.LastRows = lastRows
		state.trend.DeltaRows = lastRows - firstRows
		state.trend.FirstShareOfRows = firstShare
		state.trend.LastShareOfRows = lastShare
		state.trend.DeltaShareOfRows = lastShare - firstShare
		state.trend.RowsSeries = seriesRows
		state.trend.ShareOfRowsSeries = seriesShares

		result = append(result, state.trend)
	}

	sort.SliceStable(result, func(i, j int) bool {
		leftShare := math.Abs(result[i].DeltaShareOfRows)
		rightShare := math.Abs(result[j].DeltaShareOfRows)
		if leftShare != rightShare {
			return leftShare > rightShare
		}

		leftRows := math.Abs(float64(result[i].DeltaRows))
		rightRows := math.Abs(float64(result[j].DeltaRows))
		if leftRows != rightRows {
			return leftRows > rightRows
		}

		return result[i].PatternKey < result[j].PatternKey
	})

	return result
}

func shareOfRows(rows, totalRows int) float64 {
	if totalRows <= 0 {
		return 0
	}
	return float64(rows) / float64(totalRows)
}
