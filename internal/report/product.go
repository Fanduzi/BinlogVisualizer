// Package report defines user-facing report presentation contracts.
// input: analyzer results and CLI-selected report presentation options.
// output: stable defaults and labels shared by text, JSON, and HTML renderers.
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
