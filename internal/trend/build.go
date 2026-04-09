package trend

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

func BuildResult(opts BuildOptions) (Result, error) {
	if len(opts.Points) < 2 {
		return Result{}, fmt.Errorf("trend requires at least 2 snapshots")
	}
	topTables := opts.TopTables
	if topTables <= 0 {
		topTables = 10
	}

	sorted := make([]resolvedPoint, 0, len(opts.Points))
	for _, input := range opts.Points {
		resolved, err := resolvePoint(input)
		if err != nil {
			return Result{}, err
		}
		sorted = append(sorted, resolved)
	}
	sort.SliceStable(sorted, func(i, j int) bool {
		return sorted[i].Start.Before(sorted[j].Start)
	})

	var baseline *resolvedPoint
	if opts.Baseline != nil {
		resolved, err := resolvePoint(*opts.Baseline)
		if err != nil {
			return Result{}, err
		}
		baseline = &resolved
	}

	result := Result{
		InputMode:     opts.InputMode,
		SnapshotDir:   opts.SnapshotDir,
		Points:        make([]Point, 0, len(sorted)),
		TableTrends:   buildTableTrends(sorted, topTables),
		PatternTrends: buildPatternTrends(sorted),
	}
	if baseline != nil {
		meta := baseline.Meta
		result.BaselineSnapshot = &meta
	}
	for _, point := range sorted {
		built := point.Point
		if baseline != nil {
			delta := buildBaselineDelta(point, *baseline)
			built.BaselineDelta = &delta
		}
		result.Points = append(result.Points, built)
	}
	result.Insights = buildInsights(result.Points)
	return result, nil
}

type resolvedPoint struct {
	Start  time.Time
	Meta   SnapshotMeta
	Point  Point
	Report InputReport
}

func resolvePoint(input BuildInput) (resolvedPoint, error) {
	if input.Report.Snapshot == nil {
		return resolvedPoint{}, fmt.Errorf("snapshot %q missing snapshot metadata", input.Path)
	}
	window, startRaw := resolvedSnapshotWindow(input.Report)
	if startRaw == "" {
		return resolvedPoint{}, fmt.Errorf("snapshot %q missing snapshot.window.start_time", displayName(input.Report.Snapshot))
	}
	start, err := time.Parse(time.RFC3339, startRaw)
	if err != nil {
		return resolvedPoint{}, fmt.Errorf("snapshot %q has invalid snapshot.window.start_time: %w", displayName(input.Report.Snapshot), err)
	}

	meta := SnapshotMeta{
		Name:      input.Report.Snapshot.Name,
		Label:     input.Report.Snapshot.Label,
		Path:      input.Path,
		CreatedAt: input.Report.Snapshot.CreatedAt,
		InputMode: input.Report.Snapshot.InputMode,
		Input:     input.Report.Snapshot.Input,
		Window:    window,
		Filters:   input.Report.Snapshot.Filters,
	}

	ops := aggregateOperations(input.Report.Tables)
	point := Point{
		Snapshot: meta,
		Window:   window,
		Summary: PointSummary{
			TotalRows:         input.Report.Summary.TotalRows,
			TotalTransactions: input.Report.Summary.TotalTransactions,
			TotalEvents:       input.Report.Summary.TotalEvents,
			Warnings:          input.Report.Warnings,
		},
		Operations: ops,
		AlertCount: len(input.Report.Alerts),
	}

	return resolvedPoint{
		Start:  start,
		Meta:   meta,
		Point:  point,
		Report: input.Report,
	}, nil
}

func resolvedSnapshotWindow(report InputReport) (InputSnapshotWindow, string) {
	window := report.Snapshot.Window
	startRaw := strings.TrimSpace(window.StartTime)
	if startRaw == "" {
		startRaw = strings.TrimSpace(report.Summary.StartTime)
		if startRaw != "" {
			window.StartTime = startRaw
		}
	}
	if strings.TrimSpace(window.EndTime) == "" {
		if endRaw := strings.TrimSpace(report.Summary.EndTime); endRaw != "" {
			window.EndTime = endRaw
		}
	}
	return window, startRaw
}

func aggregateOperations(tables []InputTable) OperationBreakdown {
	var inserts, updates, deletes int
	for _, table := range tables {
		inserts += table.InsertRows
		updates += table.UpdateRows
		deletes += table.DeleteRows
	}
	total := inserts + updates + deletes
	return OperationBreakdown{
		Inserts:     inserts,
		Updates:     updates,
		Deletes:     deletes,
		InsertShare: percent(inserts, total),
		UpdateShare: percent(updates, total),
		DeleteShare: percent(deletes, total),
	}
}

func buildBaselineDelta(point, baseline resolvedPoint) BaselineDelta {
	return BaselineDelta{
		RowsDelta:         point.Report.Summary.TotalRows - baseline.Report.Summary.TotalRows,
		TransactionsDelta: point.Report.Summary.TotalTransactions - baseline.Report.Summary.TotalTransactions,
		EventsDelta:       point.Report.Summary.TotalEvents - baseline.Report.Summary.TotalEvents,
		AlertDelta:        len(point.Report.Alerts) - len(baseline.Report.Alerts),
		RowsPercent:       deltaPercent(point.Report.Summary.TotalRows, baseline.Report.Summary.TotalRows),
		TransactionsPct:   deltaPercent(point.Report.Summary.TotalTransactions, baseline.Report.Summary.TotalTransactions),
		EventsPercent:     deltaPercent(point.Report.Summary.TotalEvents, baseline.Report.Summary.TotalEvents),
		AlertsPercent:     deltaPercent(len(point.Report.Alerts), len(baseline.Report.Alerts)),
	}
}

func buildTableTrends(points []resolvedPoint, topTables int) []TableTrend {
	type key struct {
		Schema string
		Table  string
	}

	union := map[key]TableTrend{}
	for _, point := range points {
		tables := point.Report.Tables
		if len(tables) > topTables {
			tables = tables[:topTables]
		}
		for _, table := range tables {
			k := key{Schema: table.Schema, Table: table.Table}
			trend := union[k]
			trend.Schema = table.Schema
			trend.Table = table.Table
			union[k] = trend
		}
	}

	result := make([]TableTrend, 0, len(union))
	for _, trend := range union {
		series := make([]TableTrendItem, 0, len(points))
		var firstRows, lastRows int
		for idx, point := range points {
			rows := tableRowsForPoint(point.Report.Tables, trend.Schema, trend.Table)
			if idx == 0 {
				firstRows = rows
			}
			lastRows = rows
			series = append(series, TableTrendItem{
				SnapshotName: point.Meta.Name,
				StartTime:    point.Meta.Window.StartTime,
				Rows:         rows,
			})
		}
		trend.FirstRows = firstRows
		trend.LastRows = lastRows
		trend.DeltaRows = lastRows - firstRows
		trend.Series = series
		result = append(result, trend)
	}

	sort.SliceStable(result, func(i, j int) bool {
		left := math.Abs(float64(result[i].DeltaRows))
		right := math.Abs(float64(result[j].DeltaRows))
		if left == right {
			if result[i].Schema == result[j].Schema {
				return result[i].Table < result[j].Table
			}
			return result[i].Schema < result[j].Schema
		}
		return left > right
	})
	return result
}

func tableRowsForPoint(tables []InputTable, schema, table string) int {
	for _, entry := range tables {
		if entry.Schema == schema && entry.Table == table {
			return entry.TotalRows
		}
	}
	return 0
}

func buildInsights(points []Point) Insights {
	first := points[0]
	last := points[len(points)-1]
	return Insights{
		FirstSnapshot:   first.Snapshot.Name,
		LastSnapshot:    last.Snapshot.Name,
		RowsDelta:       last.Summary.TotalRows - first.Summary.TotalRows,
		TxnsDelta:       last.Summary.TotalTransactions - first.Summary.TotalTransactions,
		EventsDelta:     last.Summary.TotalEvents - first.Summary.TotalEvents,
		AlertCountDelta: last.AlertCount - first.AlertCount,
	}
}

func displayName(snapshot *InputSnapshot) string {
	if snapshot == nil {
		return ""
	}
	if strings.TrimSpace(snapshot.Name) != "" {
		return snapshot.Name
	}
	return snapshot.Label
}

func percent(value, total int) float64 {
	if total <= 0 {
		return 0
	}
	return float64(value) / float64(total) * 100
}

func deltaPercent(current, baseline int) float64 {
	if baseline == 0 {
		return 0
	}
	return float64(current-baseline) / float64(baseline) * 100
}
