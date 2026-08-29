// Package report defines user-facing report presentation contracts.
// input: analyzer results and CLI-selected report presentation options.
// output: stable defaults, labels, and byte-coverage formatting helpers shared by text, JSON, and HTML renderers.
// pos: report product contract layer used by renderer view-model builders.
// note: if this file changes, keep internal/report/README.md synchronized.
package report

import (
	"binlogviz/internal/i18n"
	"binlogviz/internal/model"
)

const (
	// DefaultTopN is the product-wide default number of ranked items shown in reports.
	DefaultTopN = 10
)

const (
	MetricAvgTPSPerMinute = "avg_tps_per_minute"
	MetricRows            = "rows"
	MetricDuration        = "duration"
	MetricTouchedTables   = "touched_tables"
)

func limitTablesForDisplay(tables []model.TableStats, limit int) ([]model.TableStats, int) {
	if limit <= 0 || len(tables) <= limit {
		return tables, 0
	}
	return tables[:limit], len(tables) - limit
}

func omittedTablesLabel(count int) string {
	return i18n.Tf("report.text.omittedTables", map[string]any{"Count": count})
}

func selectedInputFileBytes(coverage model.FileCoverage) (int64, bool) {
	if len(coverage.Selected) == 0 {
		return 0, false
	}

	var total int64
	for _, item := range coverage.Selected {
		if item.Size <= 0 {
			return 0, false
		}
		total += item.Size
	}
	return total, true
}

func countedEventBytes(result model.AnalysisResult) int64 {
	if result.Diagnostics.CountedEventBytes != 0 {
		return result.Diagnostics.CountedEventBytes
	}
	var total int64
	for _, segment := range result.Diagnostics.FileSegments {
		total += segment.BinlogBytes
	}
	return total
}
