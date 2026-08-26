// Package compare renders self-contained HTML compare reports.
// input: deterministic CompareResult values produced by the compare diff engine.
// output: compare-specific HTML pages with summary cards, charts, and detail tables.
// pos: compare renderer used by the compare command HTML output path.
// note: if this file changes, keep internal/compare/README.md synchronized.
package compare

import (
	"bytes"
	"encoding/json"
	"fmt"
	"html/template"
	"strings"
	"time"

	"binlogviz/internal/i18n"
	"binlogviz/internal/report"
)

type htmlCompareData struct {
	Result                CompareResult
	GeneratedAt           string
	EChartsJS             template.JS
	SummaryPairsJSON      template.JS
	TopTablesJSON         template.JS
	PatternChangesJSON    template.JS
	PatternDrilldownsJSON template.JS
	OpsMixJSON            template.JS
	AlertCountsJSON       template.JS
	KeyFindingsJSON       template.JS
	RecommendationsJSON   template.JS
	DDLChangesJSON        template.JS
	TxnDiagnosticsJSON    template.JS
	HotIntervalsJSON      template.JS
	EventMixDeltaJSON     template.JS
}

type htmlMetricDatum struct {
	Name     string `json:"name"`
	Baseline int    `json:"baseline"`
	Current  int    `json:"current"`
	Delta    int    `json:"delta"`
}

type htmlAlertDatum struct {
	Name  string `json:"name"`
	Value int    `json:"value"`
}

func RenderHTML(result CompareResult) (string, error) {
	echartsJS, err := report.ReadEmbeddedECharts()
	if err != nil {
		return "", err
	}

	tmpl, err := report.NewHTMLTemplate("compare", compareHTMLTemplate, template.FuncMap{
		"formatDelta":       formatHTMLDelta,
		"formatPercent":     formatHTMLPercent,
		"snapshotWindow":    formatSnapshotWindow,
		"snapshotInputMode": formatSnapshotInputMode,
		"snapshotSource":    formatSnapshotSource,
		"snapshotFilters":   formatSnapshotFilters,
		"compareLabel":      localizedCompareLabel,
		"compareSource":     localizedCompareSource,
	})
	if err != nil {
		return "", err
	}

	data := htmlCompareData{
		Result:                result,
		GeneratedAt:           time.Now().UTC().Format("2006-01-02 15:04:05 UTC"),
		EChartsJS:             template.JS(echartsJS), //nolint:gosec
		SummaryPairsJSON:      mustHTMLJSON(buildSummaryPairs(result)),
		TopTablesJSON:         mustHTMLJSON(buildTopTableSeries(result.TableChanges)),
		PatternChangesJSON:    mustHTMLJSON(buildPatternSeries(result.PatternChanges)),
		PatternDrilldownsJSON: mustHTMLJSON(result.PatternDrilldowns),
		OpsMixJSON:            mustHTMLJSON(buildOperationSeries(result.OperationMix)),
		AlertCountsJSON:       mustHTMLJSON(buildAlertCounts(result.AlertChanges)),
		KeyFindingsJSON:       mustHTMLJSON(result.KeyFindings),
		RecommendationsJSON:   mustHTMLJSON(result.Recommendations),
		DDLChangesJSON:        mustHTMLJSON(buildDDLChangeData(result.DiagnosticsDelta)),
		TxnDiagnosticsJSON:    mustHTMLJSON(buildTxnDiagnosticData(result.DiagnosticsDelta)),
		HotIntervalsJSON:      mustHTMLJSON(buildHotIntervalData(result.DiagnosticsDelta)),
		EventMixDeltaJSON:     mustHTMLJSON(result.DiagnosticsDelta.EventMixDelta),
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("render compare html template: %w", err)
	}
	return buf.String(), nil
}

func mustHTMLJSON(v any) template.JS {
	data, err := json.Marshal(v)
	if err != nil {
		return template.JS("[]") //nolint:gosec
	}
	return template.JS(data) //nolint:gosec
}

func buildSummaryPairs(result CompareResult) []htmlMetricDatum {
	return []htmlMetricDatum{
		{
			Name:     i18n.T("report.html.common.rows"),
			Baseline: result.Summary.BaselineTotalRows,
			Current:  result.Summary.CurrentTotalRows,
			Delta:    result.Summary.TotalRowsDelta,
		},
		{
			Name:     i18n.T("report.html.common.transactions"),
			Baseline: result.Summary.BaselineTotalTransactions,
			Current:  result.Summary.CurrentTotalTransactions,
			Delta:    result.Summary.TotalTransactionsDelta,
		},
		{
			Name:     i18n.T("report.html.compare.warnings"),
			Baseline: result.Summary.BaselineWarnings,
			Current:  result.Summary.CurrentWarnings,
			Delta:    result.Summary.CurrentWarnings - result.Summary.BaselineWarnings,
		},
	}
}

func buildTopTableSeries(changes []TableChange) []htmlMetricDatum {
	series := make([]htmlMetricDatum, 0, len(changes))
	for _, change := range changes {
		series = append(series, htmlMetricDatum{
			Name:     change.Schema + "." + change.Table,
			Baseline: change.BaselineRows,
			Current:  change.CurrentRows,
			Delta:    change.DeltaRows,
		})
	}
	return series
}

func buildOperationSeries(changes []OperationDelta) []htmlMetricDatum {
	series := make([]htmlMetricDatum, 0, len(changes))
	for _, change := range changes {
		series = append(series, htmlMetricDatum{
			Name:     change.Operation,
			Baseline: change.Baseline,
			Current:  change.Current,
			Delta:    change.Delta,
		})
	}
	return series
}

func buildPatternSeries(changes []PatternChange) []htmlMetricDatum {
	series := make([]htmlMetricDatum, 0, len(changes))
	for _, change := range changes {
		name := change.Label
		if name == "" {
			name = change.PatternKey
		}
		series = append(series, htmlMetricDatum{
			Name:     name,
			Baseline: change.BaselineRows,
			Current:  change.CurrentRows,
			Delta:    change.DeltaRows,
		})
	}
	return series
}

func buildAlertCounts(changes AlertDelta) []htmlAlertDatum {
	return []htmlAlertDatum{
		{Name: i18n.T("report.html.compare.addedAlerts"), Value: len(changes.Added)},
		{Name: i18n.T("report.html.compare.removedAlerts"), Value: len(changes.Removed)},
	}
}

func formatHTMLDelta(v int) string {
	return fmt.Sprintf("%+d", v)
}

func formatHTMLPercent(current, baseline int, v *float64) string {
	return formatDeltaPercent(current, baseline, v)
}

type htmlDDLChangeData struct {
	BaselineCount int            `json:"baseline_count"`
	CurrentCount  int            `json:"current_count"`
	Delta         int            `json:"delta"`
	Added         []htmlDDLEvent `json:"added"`
	Removed       []htmlDDLEvent `json:"removed"`
}

type htmlDDLEvent struct {
	Timestamp string `json:"timestamp"`
	Schema    string `json:"schema"`
	Table     string `json:"table"`
	Operation string `json:"operation"`
	Statement string `json:"statement"`
}

type htmlTxnDiagnosticData struct {
	LargestTxn htmlTxnCompare      `json:"largest_txn"`
	LongestTxn htmlDurationCompare `json:"longest_txn"`
}

type htmlTxnCompare struct {
	BaselineRows int    `json:"baseline_rows"`
	BaselineKey  string `json:"baseline_key"`
	CurrentRows  int    `json:"current_rows"`
	CurrentKey   string `json:"current_key"`
	DeltaRows    int    `json:"delta_rows"`
}

type htmlDurationCompare struct {
	BaselineDuration string `json:"baseline_duration"`
	BaselineKey      string `json:"baseline_key"`
	CurrentDuration  string `json:"current_duration"`
	CurrentKey       string `json:"current_key"`
}

type htmlHotIntervalData struct {
	BaselineCount int                   `json:"baseline_count"`
	CurrentCount  int                   `json:"current_count"`
	BaselineTop   int                   `json:"baseline_top_rows"`
	CurrentTop    int                   `json:"current_top_rows"`
	DeltaTop      int                   `json:"delta_top_rows"`
	TopItems      []htmlHotIntervalItem `json:"top_items"`
}

type htmlHotIntervalItem struct {
	Minute    string `json:"minute"`
	Source    string `json:"source"`
	TotalRows int    `json:"total_rows"`
	TxnCount  int    `json:"txn_count"`
}

func buildDDLChangeData(delta DiagnosticsDelta) htmlDDLChangeData {
	added := make([]htmlDDLEvent, 0, len(delta.DDLChanges.Added))
	for _, evt := range delta.DDLChanges.Added {
		added = append(added, htmlDDLEvent{
			Timestamp: evt.Timestamp,
			Schema:    evt.Schema,
			Table:     evt.Table,
			Operation: evt.Operation,
			Statement: evt.Statement,
		})
	}
	removed := make([]htmlDDLEvent, 0, len(delta.DDLChanges.Removed))
	for _, evt := range delta.DDLChanges.Removed {
		removed = append(removed, htmlDDLEvent{
			Timestamp: evt.Timestamp,
			Schema:    evt.Schema,
			Table:     evt.Table,
			Operation: evt.Operation,
			Statement: evt.Statement,
		})
	}
	return htmlDDLChangeData{
		BaselineCount: delta.DDLChanges.BaselineCount,
		CurrentCount:  delta.DDLChanges.CurrentCount,
		Delta:         delta.DDLChanges.Delta,
		Added:         added,
		Removed:       removed,
	}
}

func buildTxnDiagnosticData(delta DiagnosticsDelta) htmlTxnDiagnosticData {
	return htmlTxnDiagnosticData{
		LargestTxn: htmlTxnCompare{
			BaselineRows: delta.TxnDiagnostics.LargestTxnDelta.BaselineRows,
			BaselineKey:  delta.TxnDiagnostics.LargestTxnDelta.BaselineKey,
			CurrentRows:  delta.TxnDiagnostics.LargestTxnDelta.CurrentRows,
			CurrentKey:   delta.TxnDiagnostics.LargestTxnDelta.CurrentKey,
			DeltaRows:    delta.TxnDiagnostics.LargestTxnDelta.DeltaRows,
		},
		LongestTxn: htmlDurationCompare{
			BaselineDuration: delta.TxnDiagnostics.LongestTxnDelta.BaselineDuration,
			BaselineKey:      delta.TxnDiagnostics.LongestTxnDelta.BaselineKey,
			CurrentDuration:  delta.TxnDiagnostics.LongestTxnDelta.CurrentDuration,
			CurrentKey:       delta.TxnDiagnostics.LongestTxnDelta.CurrentKey,
		},
	}
}

func buildHotIntervalData(delta DiagnosticsDelta) htmlHotIntervalData {
	items := make([]htmlHotIntervalItem, 0, len(delta.HotIntervalDelta.TopItems))
	for _, item := range delta.HotIntervalDelta.TopItems {
		items = append(items, htmlHotIntervalItem{
			Minute:    item.Minute,
			Source:    localizedCompareSource(item.Source),
			TotalRows: item.TotalRows,
			TxnCount:  item.TxnCount,
		})
	}
	return htmlHotIntervalData{
		BaselineCount: delta.HotIntervalDelta.BaselineCount,
		CurrentCount:  delta.HotIntervalDelta.CurrentCount,
		BaselineTop:   delta.HotIntervalDelta.BaselineTopRows,
		CurrentTop:    delta.HotIntervalDelta.CurrentTopRows,
		DeltaTop:      delta.HotIntervalDelta.DeltaTopRows,
		TopItems:      items,
	}
}

func localizedCompareLabel(label string) string {
	switch strings.ToLower(strings.TrimSpace(label)) {
	case "current":
		return i18n.T("report.html.compare.current")
	case "baseline":
		return i18n.T("report.html.compare.baseline")
	default:
		return label
	}
}

func localizedCompareSource(source string) string {
	switch strings.ToLower(strings.TrimSpace(source)) {
	case "current":
		return i18n.T("report.html.compare.sourceCurrent")
	case "baseline":
		return i18n.T("report.html.compare.sourceBaseline")
	default:
		return source
	}
}

const compareHTMLTemplate = `<!DOCTYPE html>
<html lang="{{lang}}">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{{t "report.html.compare.title"}}</title>
<style>
  :root, [data-theme="nebula"] {
    --bg: #07090e;
    --bg-mesh: radial-gradient(ellipse 80% 50% at 50% -20%, rgba(129, 140, 248, 0.15), transparent 70%), radial-gradient(circle 40% at 90% 20%, rgba(34, 211, 238, 0.08), transparent 60%);
    --surface: #0e121e;
    --surface2: #141a2b;
    --surface-hover: #1b233a;
    --border: rgba(255, 255, 255, 0.08);
    --primary: #818cf8;
    --primary-rgb: 129, 140, 248;
    --accent: #22d3ee;
    --accent-rgb: 34, 211, 238;
    --text: #f1f5f9;
    --text-heading: #ffffff;
    --muted: #8899b0;
    --success: #34d399;
    --warn: #fbbf24;
    --danger: #f87171;
    --card-shadow: 0 10px 30px -10px rgba(0, 0, 0, 0.5), 0 0 1px 1px rgba(255, 255, 255, 0.05);
    --header-bg: rgba(14, 18, 30, 0.85);
  }
  [data-theme="forest"] {
    --bg: #050c07;
    --bg-mesh: radial-gradient(ellipse 80% 50% at 50% -20%, rgba(74, 222, 128, 0.15), transparent 70%);
    --surface: #0a160e;
    --surface2: #102016;
    --surface-hover: #162c1e;
    --border: rgba(74, 222, 128, 0.12);
    --primary: #4ade80;
    --primary-rgb: 74, 222, 128;
    --accent: #fbbf24;
    --accent-rgb: 251, 191, 36;
    --text: #f0fdf4;
    --text-heading: #ffffff;
    --muted: #7d9884;
    --card-shadow: 0 10px 30px -10px rgba(0, 0, 0, 0.5);
    --header-bg: rgba(10, 22, 14, 0.85);
  }
  [data-theme="navy"] {
    --bg: #040814;
    --bg-mesh: radial-gradient(ellipse 80% 50% at 50% -20%, rgba(96, 165, 250, 0.15), transparent 70%);
    --surface: #081024;
    --surface2: #0e1a38;
    --surface-hover: #14244d;
    --border: rgba(96, 165, 250, 0.12);
    --primary: #60a5fa;
    --primary-rgb: 96, 165, 250;
    --accent: #fcd34d;
    --accent-rgb: 252, 211, 77;
    --text: #e8f0ff;
    --text-heading: #ffffff;
    --muted: #7286a8;
    --card-shadow: 0 10px 30px -10px rgba(0, 0, 0, 0.5);
    --header-bg: rgba(8, 16, 36, 0.85);
  }
  [data-theme="ember"] {
    --bg: #0a0604;
    --bg-mesh: radial-gradient(ellipse 80% 50% at 50% -20%, rgba(251, 146, 60, 0.15), transparent 70%);
    --surface: #150d09;
    --surface2: #20140f;
    --surface-hover: #2d1d16;
    --border: rgba(251, 146, 60, 0.12);
    --primary: #fb923c;
    --primary-rgb: 251, 146, 60;
    --accent: #f43f5e;
    --accent-rgb: 244, 63, 94;
    --text: #fef3ee;
    --text-heading: #ffffff;
    --muted: #9a7f72;
    --card-shadow: 0 10px 30px -10px rgba(0, 0, 0, 0.5);
    --header-bg: rgba(21, 13, 9, 0.85);
  }
  [data-theme="light"] {
    --bg: #f8fafc;
    --bg-mesh: radial-gradient(ellipse 80% 50% at 50% -20%, rgba(79, 70, 229, 0.08), transparent 70%);
    --surface: #ffffff;
    --surface2: #f1f5f9;
    --surface-hover: #e2e8f0;
    --border: #e2e8f0;
    --primary: #4f46e5;
    --primary-rgb: 79, 70, 229;
    --accent: #0891b2;
    --accent-rgb: 8, 145, 178;
    --text: #1e293b;
    --text-heading: #0f172a;
    --muted: #64748b;
    --card-shadow: 0 4px 20px -4px rgba(0, 0, 0, 0.06);
    --header-bg: rgba(255, 255, 255, 0.9);
  }

  * { box-sizing: border-box; margin: 0; padding: 0; }
  html { scroll-behavior: smooth; }
  body {
    margin: 0;
    background: var(--bg);
    background-image: var(--bg-mesh);
    background-attachment: fixed;
    color: var(--text);
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
    font-size: 13.5px;
    line-height: 1.6;
    -webkit-font-smoothing: antialiased;
  }
  .page { max-width: 1320px; margin: 0 auto; padding: 16px 20px 60px; }
  
  .topbar {
    position: sticky;
    top: 12px;
    z-index: 100;
    margin-bottom: 20px;
    backdrop-filter: blur(16px);
    -webkit-backdrop-filter: blur(16px);
    background: var(--header-bg);
    border: 1px solid var(--border);
    border-radius: 12px;
    box-shadow: var(--card-shadow);
  }
  .header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 16px;
    padding: 14px 20px;
    flex-wrap: wrap;
  }
  .header-brand {
    display: flex;
    align-items: center;
    gap: 12px;
  }
  .header-logo {
    font-family: "JetBrains Mono", "Fira Code", monospace;
    font-size: 20px;
    font-weight: 800;
    color: var(--text-heading);
  }
  .header-logo span {
    background: linear-gradient(135deg, var(--primary), var(--accent));
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
  }
  .header-type-tag {
    font-family: "JetBrains Mono", monospace;
    font-size: 10px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.8px;
    padding: 2px 8px;
    border-radius: 999px;
    background: rgba(var(--primary-rgb), 0.12);
    color: var(--primary);
    border: 1px solid rgba(var(--primary-rgb), 0.2);
  }
  .header-context {
    font-size: 11.5px;
    color: var(--muted);
    margin-top: 2px;
  }
  .header-meta {
    text-align: right;
    color: var(--muted);
    font-size: 11.5px;
  }
  .header-meta strong {
    display: block;
    color: var(--text-heading);
    font-size: 12.5px;
    margin-bottom: 2px;
  }

  .theme-switcher {
    display: flex;
    gap: 6px;
    align-items: center;
    padding: 3px 6px;
    background: var(--surface2);
    border-radius: 999px;
    border: 1px solid var(--border);
  }
  .theme-btn {
    width: 18px;
    height: 18px;
    border-radius: 50%;
    border: 2px solid transparent;
    cursor: pointer;
    transition: transform 0.15s;
    padding: 0;
  }
  .theme-btn:hover { transform: scale(1.2); }
  .theme-btn.active { border-color: var(--text-heading); }
  .theme-btn[data-t="nebula"] { background: linear-gradient(135deg, #818cf8 50%, #22d3ee 50%); }
  .theme-btn[data-t="forest"] { background: linear-gradient(135deg, #4ade80 50%, #fbbf24 50%); }
  .theme-btn[data-t="navy"]   { background: linear-gradient(135deg, #60a5fa 50%, #fcd34d 50%); }
  .theme-btn[data-t="ember"]  { background: linear-gradient(135deg, #fb923c 50%, #f43f5e 50%); }
  .theme-btn[data-t="light"]  { background: linear-gradient(135deg, #f8fafc 50%, #4f46e5 50%); border-color: #cbd5e1; }

  .cards {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
    gap: 14px;
    margin-bottom: 22px;
  }
  .card {
    padding: 16px 18px;
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 12px;
    position: relative;
    overflow: hidden;
    box-shadow: var(--card-shadow);
    transition: transform 0.2s, border-color 0.2s;
  }
  .card:hover {
    transform: translateY(-2px);
    border-color: rgba(var(--primary-rgb), 0.35);
  }
  .card::before {
    content: "";
    position: absolute;
    left: 0;
    top: 0;
    width: 100%;
    height: 3px;
    background: linear-gradient(90deg, var(--primary), var(--accent));
  }
  .card.accent::before { background: linear-gradient(90deg, var(--accent), #38bdf8); }
  .card.success::before { background: linear-gradient(90deg, var(--success), #a7f3d0); }
  .card.warn::before { background: linear-gradient(90deg, var(--warn), #fde68a); }
  .card-label {
    font-size: 11px;
    font-weight: 700;
    letter-spacing: 0.7px;
    text-transform: uppercase;
    color: var(--muted);
    margin-bottom: 8px;
  }
  .card-value {
    font-family: "JetBrains Mono", "Fira Code", monospace;
    font-size: 26px;
    font-weight: 800;
    color: var(--text-heading);
    font-variant-numeric: tabular-nums;
  }
  .card-sub { margin-top: 6px; color: var(--muted); font-size: 11.5px; }

  .section {
    margin-bottom: 22px;
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 14px;
    overflow: hidden;
    box-shadow: var(--card-shadow);
  }
  .section-header {
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 14px 18px;
    border-bottom: 1px solid var(--border);
    font-size: 13.5px;
    font-weight: 700;
    color: var(--text-heading);
  }
  .section-header .dot {
    width: 8px;
    height: 8px;
    border-radius: 50%;
    background: var(--primary);
    box-shadow: 0 0 8px rgba(var(--primary-rgb), 0.6);
  }
  .section-body { padding: 18px; }
  .chart-box { width: 100%; height: 300px; }
  .chart-box-tall { height: 420px; }
  .two-col {
    display: grid;
    grid-template-columns: 1.1fr 0.9fr;
    gap: 18px;
  }
  .pattern-stack {
    display: flex;
    flex-direction: column;
    gap: 18px;
  }
  table {
    width: 100%;
    border-collapse: collapse;
    font-size: 13px;
  }
  thead th {
    padding: 10px 14px;
    text-align: left;
    background: var(--surface2);
    color: var(--muted);
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 0.6px;
    border-bottom: 1px solid var(--border);
  }
  thead th.num, tbody td.num { text-align: right; }
  tbody td {
    padding: 10px 14px;
    border-bottom: 1px solid var(--border);
    font-family: "JetBrains Mono", "Fira Code", monospace;
    font-size: 12.5px;
  }
  tbody tr:last-child td { border-bottom: none; }
  tbody tr:hover { background: var(--surface-hover); }

  .alerts-layout {
    display: grid;
    grid-template-columns: 320px 1fr;
    gap: 18px;
  }
  .alert-columns {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 18px;
  }
  .alert-column h3 {
    margin: 0 0 10px;
    font-size: 13px;
    font-weight: 700;
    color: var(--text-heading);
  }
  .alert-item, .empty-state {
    padding: 12px 14px;
    border: 1px solid var(--border);
    border-radius: 8px;
    background: var(--surface2);
    margin-bottom: 10px;
  }
  .alert-item strong {
    display: inline-block;
    margin-bottom: 4px;
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 0.6px;
    color: var(--muted);
  }
  .alert-item p, .empty-state {
    margin: 0;
    font-size: 13px;
  }
  .evidence-refs {
    font-size: 12px;
    color: var(--muted);
    margin-left: 6px;
    display: inline-flex;
    align-items: center;
    flex-wrap: wrap;
    gap: 4px;
  }
  .evidence-refs a {
    color: var(--primary);
    text-decoration: none;
    background: rgba(var(--primary-rgb), 0.1);
    padding: 1px 7px;
    border-radius: 4px;
    border: 1px solid rgba(var(--primary-rgb), 0.22);
    font-family: "JetBrains Mono", "Fira Code", monospace;
    font-size: 11px;
    transition: all 0.15s ease;
  }
  .evidence-refs a:hover {
    background: rgba(var(--primary-rgb), 0.22);
    border-color: rgba(var(--primary-rgb), 0.45);
    text-decoration: none;
  }
  #compare-findings-list ol,
  #compare-recommendations-list ol {
    list-style: none;
    margin: 0;
    padding: 0;
    display: flex;
    flex-direction: column;
    gap: 10px;
  }
  #compare-findings-list li {
    display: block;
    padding: 14px 18px;
    border: 1px solid var(--border);
    border-radius: 8px;
    background: var(--surface2);
    font-size: 13px;
    line-height: 1.6;
    color: var(--text);
    transition: background 0.15s ease, border-color 0.15s ease, transform 0.15s ease;
  }
  #compare-findings-list li:hover {
    background: var(--surface-hover);
    border-color: rgba(var(--primary-rgb), 0.35);
    transform: translateX(2px);
  }
  #compare-findings-list li strong {
    display: inline-block;
    font-family: "JetBrains Mono", "Fira Code", monospace;
    font-size: 11px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.6px;
    color: var(--primary);
    background: rgba(var(--primary-rgb), 0.12);
    padding: 2px 8px;
    border-radius: 4px;
    border: 1px solid rgba(var(--primary-rgb), 0.25);
    margin-right: 6px;
    vertical-align: middle;
  }
  .rec-item {
    display: block;
    padding: 14px 18px;
    border: 1px solid var(--border);
    border-radius: 8px;
    background: var(--surface2);
    margin-bottom: 0;
    font-size: 13px;
    line-height: 1.6;
    transition: background 0.15s ease, border-color 0.15s ease, transform 0.15s ease;
  }
  .rec-item:hover {
    background: var(--surface-hover);
    border-color: rgba(var(--primary-rgb), 0.35);
    transform: translateX(2px);
  }
  .rec-item strong {
    font-size: 13.5px;
    font-weight: 700;
    color: var(--text-heading);
    margin-left: 6px;
    vertical-align: middle;
  }
  .rec-badge {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    padding: 2px 8px;
    border-radius: 4px;
    font-family: "JetBrains Mono", monospace;
    font-size: 10.5px;
    font-weight: 800;
    text-transform: uppercase;
    letter-spacing: 0.6px;
    vertical-align: middle;
  }
  .rec-badge.high { background: rgba(248,113,113,0.18); color: #f87171; border: 1px solid rgba(248,113,113,0.3); }
  .rec-badge.medium { background: rgba(251,191,36,0.18); color: #fbbf24; border: 1px solid rgba(251,191,36,0.3); }
  .rec-badge.low { background: rgba(52,211,153,0.18); color: #34d399; border: 1px solid rgba(52,211,153,0.3); }
  .rec-summary { font-size: 13px; color: var(--text); line-height: 1.6; margin-top: 2px; }
  @media (max-width: 900px) {
    .two-col, .alerts-layout, .alert-columns { grid-template-columns: 1fr; }
  }

  /* ── Back to Top Floating Button ── */
  .back-to-top {
    position: fixed;
    bottom: 28px;
    right: 28px;
    z-index: 999;
    width: 44px;
    height: 44px;
    border-radius: 50%;
    background: var(--surface2);
    border: 1px solid var(--border);
    color: var(--text-heading);
    display: flex;
    align-items: center;
    justify-content: center;
    cursor: pointer;
    box-shadow: var(--card-shadow);
    opacity: 0;
    pointer-events: none;
    transform: translateY(14px);
    transition: opacity 0.25s ease, transform 0.25s ease, background 0.15s ease, border-color 0.15s ease, color 0.15s ease;
  }
  .back-to-top.visible {
    opacity: 1;
    pointer-events: auto;
    transform: translateY(0);
  }
  .back-to-top:hover {
    background: var(--surface-hover);
    border-color: rgba(var(--primary-rgb), 0.5);
    color: var(--primary);
    transform: translateY(-2px);
  }
</style>
</head>
<body>
  <div class="page">
    <div class="topbar">
      <header class="header">
        <div>
          <div class="header-brand">
            <div class="header-logo"><span>Binlog</span>Viz</div>
            <div class="header-type-tag">{{t "report.html.compare.logoSuffix"}}</div>
          </div>
          <div style="margin-top:4px;font-weight:600;font-size:13px">{{.Result.CurrentLabel}} vs {{.Result.BaselineLabel}}</div>
          {{if snapshotWindow .Result.CurrentSnapshot}}<div class="header-context">{{t "report.html.compare.currentWindow"}}: {{snapshotWindow .Result.CurrentSnapshot}}</div>{{end}}
          {{if snapshotWindow .Result.BaselineSnapshot}}<div class="header-context">{{t "report.html.compare.baselineWindow"}}: {{snapshotWindow .Result.BaselineSnapshot}}</div>{{end}}
          {{if snapshotInputMode .Result.CurrentSnapshot}}<div class="header-context">{{t "report.html.compare.currentInputMode"}}: {{snapshotInputMode .Result.CurrentSnapshot}}</div>{{end}}
          {{if snapshotInputMode .Result.BaselineSnapshot}}<div class="header-context">{{t "report.html.compare.baselineInputMode"}}: {{snapshotInputMode .Result.BaselineSnapshot}}</div>{{end}}
          {{if snapshotSource .Result.CurrentSnapshot}}<div class="header-context">{{t "report.html.compare.currentSource"}}: {{snapshotSource .Result.CurrentSnapshot}}</div>{{end}}
          {{if snapshotSource .Result.BaselineSnapshot}}<div class="header-context">{{t "report.html.compare.baselineSource"}}: {{snapshotSource .Result.BaselineSnapshot}}</div>{{end}}
          {{if snapshotFilters .Result.CurrentSnapshot}}<div class="header-context">{{t "report.html.compare.currentFilters"}}: {{snapshotFilters .Result.CurrentSnapshot}}</div>{{end}}
          {{if snapshotFilters .Result.BaselineSnapshot}}<div class="header-context">{{t "report.html.compare.baselineFilters"}}: {{snapshotFilters .Result.BaselineSnapshot}}</div>{{end}}
        </div>
        <div style="display:flex;align-items:center;gap:16px;">
          <div class="header-meta">
            <strong>{{t "report.html.common.generatedAt"}}</strong>
            <span>{{.GeneratedAt}}</span>
          </div>
          <div class="theme-switcher">
            <button class="theme-btn" data-t="nebula" title="Nebula"></button>
            <button class="theme-btn" data-t="forest" title="Forest"></button>
            <button class="theme-btn" data-t="navy"   title="Navy"></button>
            <button class="theme-btn" data-t="ember"  title="Ember"></button>
            <button class="theme-btn" data-t="light"  title="Light"></button>
          </div>
        </div>
      </header>
    </div>

    <section class="cards" id="compare-summary-cards">
      <article class="card">
        <div class="card-label">{{t "report.html.compare.rowsDelta"}}</div>
        <div class="card-value">{{formatDelta .Result.Summary.TotalRowsDelta}}</div>
        <div class="card-sub">{{compareLabel .Result.BaselineLabel}} {{.Result.Summary.BaselineTotalRows}} -> {{compareLabel .Result.CurrentLabel}} {{.Result.Summary.CurrentTotalRows}}</div>
      </article>
      <article class="card accent">
        <div class="card-label">{{t "report.html.compare.transactionsDelta"}}</div>
        <div class="card-value">{{formatDelta .Result.Summary.TotalTransactionsDelta}}</div>
        <div class="card-sub">{{compareLabel .Result.BaselineLabel}} {{.Result.Summary.BaselineTotalTransactions}} -> {{compareLabel .Result.CurrentLabel}} {{.Result.Summary.CurrentTotalTransactions}}</div>
      </article>
      <article class="card success">
        <div class="card-label">{{t "report.html.compare.addedAlerts"}}</div>
        <div class="card-value">{{len .Result.AlertChanges.Added}}</div>
        <div class="card-sub">{{t "report.html.compare.newAlertsIn"}} {{compareLabel .Result.CurrentLabel}}</div>
      </article>
      <article class="card warn">
        <div class="card-label">{{t "report.html.compare.removedAlerts"}}</div>
        <div class="card-value">{{len .Result.AlertChanges.Removed}}</div>
        <div class="card-sub">{{t "report.html.compare.alertsOnlyIn"}} {{compareLabel .Result.BaselineLabel}}</div>
      </article>
    </section>

    {{if .Result.KeyFindings}}
    <section class="section" id="compare-key-findings">
      <div class="section-header"><span class="dot"></span>{{t "report.html.compare.keyFindings"}}</div>
      <div class="section-body">
        <div id="compare-findings-list"></div>
      </div>
    </section>
    {{end}}

    {{if .Result.Recommendations}}
    <section class="section" id="compare-recommendations">
      <div class="section-header"><span class="dot"></span>{{t "report.html.compare.recommendedNextChecks"}}</div>
      <div class="section-body">
        <div id="compare-recommendations-list"></div>
      </div>
    </section>
    {{end}}

    <section class="section">
      <div class="section-header"><span class="dot"></span>{{t "report.html.compare.summary"}}</div>
      <div class="section-body">
        <div id="compare-summary-chart" class="chart-box"></div>
      </div>
    </section>

    <section class="section" id="section-table-changes">
      <div class="section-header"><span class="dot"></span>{{t "report.html.compare.topTableChanges"}}</div>
      <div class="section-body two-col">
        <div id="compare-top-tables" class="chart-box"></div>
        <div>
          <table>
            <thead>
              <tr>
                <th>{{t "report.html.common.table"}}</th>
                <th class="num">{{.Result.BaselineLabel}}</th>
                <th class="num">{{.Result.CurrentLabel}}</th>
                <th class="num">{{t "report.html.compare.delta"}}</th>
                <th class="num">{{t "report.html.compare.deltaPercent"}}</th>
              </tr>
            </thead>
            <tbody>
              {{range .Result.TableChanges}}
              <tr>
                <td style="font-family:inherit;font-weight:600">{{.Schema}}.{{.Table}}</td>
                <td class="num">{{.BaselineRows}}</td>
                <td class="num">{{.CurrentRows}}</td>
                <td class="num">{{formatDelta .DeltaRows}}</td>
                <td class="num">{{formatPercent .CurrentRows .BaselineRows .DeltaPercent}}</td>
              </tr>
              {{end}}
            </tbody>
          </table>
        </div>
      </div>
    </section>

    <section class="section" id="section-pattern-changes">
      <div class="section-header"><span class="dot"></span>{{t "report.html.compare.patternChanges"}}</div>
      <div class="section-body pattern-stack">
        <div id="compare-pattern-changes" class="chart-box chart-box-tall"></div>
        <div>
          <table>
            <thead>
              <tr>
                <th>{{t "report.html.compare.pattern"}}</th>
                <th class="num">{{.Result.BaselineLabel}}</th>
                <th class="num">{{.Result.CurrentLabel}}</th>
                <th class="num">{{t "report.html.compare.deltaRows"}}</th>
                <th class="num">{{t "report.html.compare.deltaPercent"}}</th>
                <th class="num">{{t "report.html.compare.baselineTxns"}}</th>
                <th class="num">{{t "report.html.compare.currentTxns"}}</th>
                <th class="num">{{t "report.html.compare.deltaTxns"}}</th>
              </tr>
            </thead>
            <tbody>
              {{if .Result.PatternChanges}}
                {{range .Result.PatternChanges}}
                <tr>
                  <td>
                    <div><strong style="color:var(--text-heading)">{{if .Label}}{{.Label}}{{else}}{{.PatternKey}}{{end}}</strong></div>
                    {{if .SampleQuerySummary}}<div style="font-size:11.5px;color:var(--muted)">{{.SampleQuerySummary}}</div>{{end}}
                  </td>
                  <td class="num">{{.BaselineRows}}</td>
                  <td class="num">{{.CurrentRows}}</td>
                  <td class="num">{{formatDelta .DeltaRows}}</td>
                  <td class="num">{{formatPercent .CurrentRows .BaselineRows .DeltaPercent}}</td>
                  <td class="num">{{.BaselineTxnCount}}</td>
                  <td class="num">{{.CurrentTxnCount}}</td>
                  <td class="num">{{formatDelta .DeltaTxnCount}}</td>
                </tr>
                {{end}}
              {{else}}
                <tr>
                  <td colspan="8">{{t "report.html.compare.noPatternChanges"}}</td>
                </tr>
              {{end}}
            </tbody>
          </table>
        </div>
      </div>
      <div id="compare-pattern-drilldowns"></div>
    </section>

    <section class="section" id="section-operation-mix">
      <div class="section-header"><span class="dot"></span>{{t "report.html.common.operationMix"}}</div>
      <div class="section-body two-col">
        <div id="compare-ops-mix" class="chart-box"></div>
        <div>
          <table>
            <thead>
              <tr>
                <th>{{t "report.html.compare.operation"}}</th>
                <th class="num">{{.Result.BaselineLabel}}</th>
                <th class="num">{{.Result.CurrentLabel}}</th>
                <th class="num">{{t "report.html.compare.delta"}}</th>
              </tr>
            </thead>
            <tbody>
              {{range .Result.OperationMix}}
              <tr>
                <td style="font-family:inherit;font-weight:600">{{.Operation}}</td>
                <td class="num">{{.Baseline}}</td>
                <td class="num">{{.Current}}</td>
                <td class="num">{{formatDelta .Delta}}</td>
              </tr>
              {{end}}
            </tbody>
          </table>
        </div>
      </div>
    </section>

    <section class="section">
      <div class="section-header"><span class="dot"></span>{{t "report.html.compare.alertChanges"}}</div>
      <div class="section-body alerts-layout">
        <div id="compare-alerts" class="chart-box"></div>
        <div class="alert-columns">
          <div class="alert-column">
            <h3>{{t "report.html.compare.addedAlerts"}} ({{len .Result.AlertChanges.Added}})</h3>
            {{if .Result.AlertChanges.Added}}
              {{range .Result.AlertChanges.Added}}
              <div class="alert-item">
                <strong>{{.Type}}</strong>
                <p>{{.Message}}</p>
              </div>
              {{end}}
            {{else}}
              <div class="empty-state">{{t "report.html.compare.noAddedAlerts"}}</div>
            {{end}}
          </div>
          <div class="alert-column">
            <h3>{{t "report.html.compare.removedAlerts"}} ({{len .Result.AlertChanges.Removed}})</h3>
            {{if .Result.AlertChanges.Removed}}
              {{range .Result.AlertChanges.Removed}}
              <div class="alert-item">
                <strong>{{.Type}}</strong>
                <p>{{.Message}}</p>
              </div>
              {{end}}
            {{else}}
              <div class="empty-state">{{t "report.html.compare.noRemovedAlerts"}}</div>
            {{end}}
          </div>
        </div>
      </div>
    </section>

    <section class="section" id="compare-ddl-changes">
      <div class="section-header"><span class="dot"></span>{{t "report.html.compare.ddlChanges"}}</div>
      <div class="section-body">
        <div id="compare-ddl-chart" class="chart-box"></div>
        <div class="alert-columns" style="margin-top:18px">
          <div class="alert-column">
            <h3>{{t "report.html.compare.ddlAdded"}} ({{len .Result.DiagnosticsDelta.DDLChanges.Added}})</h3>
            {{if .Result.DiagnosticsDelta.DDLChanges.Added}}
            {{range .Result.DiagnosticsDelta.DDLChanges.Added}}
            <div class="alert-item">
              <strong>{{.Operation}}</strong>
              <p>{{.Schema}}.{{.Table}}: {{.Statement}}</p>
            </div>
            {{end}}
            {{else}}
            <div class="empty-state">{{t "report.html.compare.noAddedDDLEvents"}}</div>
            {{end}}
          </div>
          <div class="alert-column">
            <h3>{{t "report.html.compare.ddlRemoved"}} ({{len .Result.DiagnosticsDelta.DDLChanges.Removed}})</h3>
            {{if .Result.DiagnosticsDelta.DDLChanges.Removed}}
            {{range .Result.DiagnosticsDelta.DDLChanges.Removed}}
            <div class="alert-item">
              <strong>{{.Operation}}</strong>
              <p>{{.Schema}}.{{.Table}}: {{.Statement}}</p>
            </div>
            {{end}}
            {{else}}
            <div class="empty-state">{{t "report.html.compare.noRemovedDDLEvents"}}</div>
            {{end}}
          </div>
        </div>
      </div>
    </section>

    <section class="section" id="compare-txn-diagnostics">
      <div class="section-header"><span class="dot"></span>{{t "report.html.compare.txnDiagnostics"}}</div>
      <div class="section-body">
        <div id="compare-txn-chart" class="chart-box"></div>
        <table style="margin-top:18px">
          <thead>
            <tr>
              <th>{{t "report.html.compare.metric"}}</th>
              <th class="num">{{.Result.BaselineLabel}}</th>
              <th class="num">{{.Result.CurrentLabel}}</th>
              <th class="num">{{t "report.html.compare.delta"}}</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td>{{t "report.html.compare.largestTxnRows"}}</td>
              <td class="num">{{.Result.DiagnosticsDelta.TxnDiagnostics.LargestTxnDelta.BaselineRows}}</td>
              <td class="num">{{.Result.DiagnosticsDelta.TxnDiagnostics.LargestTxnDelta.CurrentRows}}</td>
              <td class="num">{{formatDelta .Result.DiagnosticsDelta.TxnDiagnostics.LargestTxnDelta.DeltaRows}}</td>
            </tr>
            <tr>
              <td>{{t "report.html.compare.longestTxnDuration"}}</td>
              <td class="num">{{.Result.DiagnosticsDelta.TxnDiagnostics.LongestTxnDelta.BaselineDuration}}</td>
              <td class="num">{{.Result.DiagnosticsDelta.TxnDiagnostics.LongestTxnDelta.CurrentDuration}}</td>
              <td class="num">—</td>
            </tr>
          </tbody>
        </table>
      </div>
    </section>

    <section class="section" id="compare-hot-intervals">
      <div class="section-header"><span class="dot"></span>{{t "report.html.compare.hotIntervalChanges"}}</div>
      <div class="section-body">
        <div id="compare-hot-chart" class="chart-box"></div>
        <table style="margin-top:18px">
          <thead>
            <tr>
              <th>{{t "report.html.compare.minute"}}</th>
              <th>{{t "report.html.compare.source"}}</th>
              <th class="num">{{t "report.html.common.totalRows"}}</th>
              <th class="num">{{t "report.html.compare.txnCount"}}</th>
            </tr>
          </thead>
          <tbody>
            {{range .Result.DiagnosticsDelta.HotIntervalDelta.TopItems}}
            <tr>
              <td>{{.Minute}}</td>
              <td>{{compareSource .Source}}</td>
              <td class="num">{{.TotalRows}}</td>
              <td class="num">{{.TxnCount}}</td>
            </tr>
            {{end}}
          </tbody>
        </table>
      </div>
    </section>

    <section class="section" id="compare-event-mix-delta">
      <div class="section-header"><span class="dot"></span>{{t "report.html.compare.eventMixDelta"}}</div>
      <div class="section-body">
        <div id="compare-event-mix-chart" class="chart-box"></div>
      </div>
    </section>
  </div>

  <button id="btn-back-to-top" class="back-to-top" title="{{t "report.html.common.backToTop"}}" aria-label="{{t "report.html.common.backToTop"}}">
    <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round">
      <polyline points="18 15 12 9 6 15"></polyline>
    </svg>
  </button>

  <script>{{.EChartsJS}}</script>
  <script>
    window.compareSummaryPairs = {{.SummaryPairsJSON}};
    window.compareTopTables = {{.TopTablesJSON}};
    window.comparePatternChanges = {{.PatternChangesJSON}};
    window.comparePatternDrilldowns = {{.PatternDrilldownsJSON}};
    window.compareOpsMix = {{.OpsMixJSON}};
    window.compareAlertCounts = {{.AlertCountsJSON}};
    window.compareKeyFindings = {{.KeyFindingsJSON}};
    window.compareRecommendations = {{.RecommendationsJSON}};
    window.compareDDLChanges = {{.DDLChangesJSON}};
    window.compareTxnDiagnostics = {{.TxnDiagnosticsJSON}};
    window.compareHotIntervals = {{.HotIntervalsJSON}};
    window.compareEventMixDelta = {{.EventMixDeltaJSON}};

    var _lBaseline = {{t "report.html.compare.baseline"}};
    var _lCurrent = {{t "report.html.compare.current"}};
    var _lLargestTxnRows = {{t "report.html.compare.largestTxnRows"}};
    var _lLongestTxnSec = {{t "report.html.compare.longestTxnSec"}};
    var _lInsert = {{t "report.html.common.inserts"}};
    var _lUpdate = {{t "report.html.common.updates"}};
    var _lDelete = {{t "report.html.common.deletes"}};
    var _lDDL = {{t "report.html.common.ddl"}};
    var _lDrilldown = {{t "report.html.common.drilldown"}};

    const findingsEl = document.getElementById('compare-findings-list');
    if (findingsEl && window.compareKeyFindings && window.compareKeyFindings.length > 0) {
      const ol = document.createElement('ol');
      window.compareKeyFindings.forEach(f => {
        const li = document.createElement('li');
        const strong = document.createElement('strong');
        strong.textContent = f.kind;
        li.appendChild(strong);
        li.appendChild(document.createTextNode(': ' + f.summary));
        if (f.evidence_refs && f.evidence_refs.length > 0) {
          const span = document.createElement('span');
          span.className = 'evidence-refs';
          span.appendChild(document.createTextNode('['));
          f.evidence_refs.forEach((r, i) => {
            if (i > 0) { span.appendChild(document.createTextNode(', ')); }
            const a = document.createElement('a');
            a.setAttribute('href', '#' + r.anchor);
            a.textContent = r.label;
            span.appendChild(a);
          });
          span.appendChild(document.createTextNode(']'));
          li.appendChild(document.createTextNode(' '));
          span.classList.add('evidence-refs');
          li.appendChild(span);
        }
        ol.appendChild(li);
      });
      findingsEl.appendChild(ol);
    }

    const recommendationsEl = document.getElementById('compare-recommendations-list');
    if (recommendationsEl && window.compareRecommendations && window.compareRecommendations.length > 0) {
      const recOl = document.createElement('ol');
      window.compareRecommendations.forEach(rec => {
        const li = document.createElement('li');
        li.className = 'rec-item';
        const badge = document.createElement('span');
        badge.className = 'rec-badge ' + rec.priority;
        badge.textContent = rec.priority;
        li.appendChild(badge);
        li.appendChild(document.createTextNode(' '));
        const strong = document.createElement('strong');
        strong.textContent = rec.title;
        li.appendChild(strong);
        const summary = document.createElement('div');
        summary.className = 'rec-summary';
        summary.textContent = rec.summary;
        li.appendChild(summary);
        if (rec.evidence_refs && rec.evidence_refs.length > 0) {
          const span = document.createElement('span');
          span.className = 'evidence-refs';
          span.appendChild(document.createTextNode('['));
          rec.evidence_refs.forEach((r, i) => {
            if (i > 0) { span.appendChild(document.createTextNode(', ')); }
            const a = document.createElement('a');
            a.setAttribute('href', '#' + r.anchor);
            a.textContent = r.label;
            span.appendChild(a);
          });
          span.appendChild(document.createTextNode(']'));
          li.appendChild(document.createTextNode(' '));
          li.appendChild(span);
        }
        recOl.appendChild(li);
      });
      recommendationsEl.appendChild(recOl);
    }

    function cssVar(name) {
      return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
    }

    var summaryChart = echarts.init(document.getElementById('compare-summary-chart'));
    var topTablesChart = echarts.init(document.getElementById('compare-top-tables'));
    var patternChangesChart = echarts.init(document.getElementById('compare-pattern-changes'));
    var opsMixChart = echarts.init(document.getElementById('compare-ops-mix'));
    var alertChart = echarts.init(document.getElementById('compare-alerts'));
    var ddlChart = echarts.init(document.getElementById('compare-ddl-chart'));
    var txnChart = echarts.init(document.getElementById('compare-txn-chart'));
    var hotChart = echarts.init(document.getElementById('compare-hot-chart'));
    var eventMixChart = echarts.init(document.getElementById('compare-event-mix-chart'));

    function renderAllCharts() {
      var primary = cssVar('--primary') || '#818cf8';
      var accent  = cssVar('--accent') || '#22d3ee';
      var border  = cssVar('--border') || '#1c1c2e';
      var muted   = cssVar('--muted') || '#64748b';
      var text    = cssVar('--text') || '#f1f5f9';

      summaryChart.setOption({
        animation: false,
        backgroundColor: 'transparent',
        tooltip: { show: false },
        dataZoom: [{ type: 'inside', zoomOnMouseWheel: true, moveOnMouseMove: true }],
        legend: { data: ['{{t "report.html.compare.baseline"}}', '{{t "report.html.compare.current"}}'], bottom: 0, textStyle: { color: text } },
        grid: { left: 16, right: 24, top: 16, bottom: 48, containLabel: true },
        xAxis: { type: 'value', axisLabel: { color: muted }, splitLine: { lineStyle: { color: border } } },
        yAxis: { type: 'category', data: window.compareSummaryPairs.map((item) => item.name), axisLabel: { color: text } },
        series: [
          { name: '{{t "report.html.compare.baseline"}}', type: 'bar', data: window.compareSummaryPairs.map((item) => item.baseline), itemStyle: { color: primary, borderRadius: [0, 4, 4, 0] } },
          { name: '{{t "report.html.compare.current"}}', type: 'bar', data: window.compareSummaryPairs.map((item) => item.current), itemStyle: { color: accent, borderRadius: [0, 4, 4, 0] } },
        ],
      });

      topTablesChart.setOption({
        animation: false,
        backgroundColor: 'transparent',
        tooltip: { show: false },
        dataZoom: [{ type: 'inside', zoomOnMouseWheel: true, moveOnMouseMove: true }],
        legend: { data: ['{{t "report.html.compare.baseline"}}', '{{t "report.html.compare.current"}}'], bottom: 0, textStyle: { color: text } },
        grid: { left: 16, right: 24, top: 16, bottom: 48, containLabel: true },
        xAxis: { type: 'value', axisLabel: { color: muted }, splitLine: { lineStyle: { color: border } } },
        yAxis: { type: 'category', data: window.compareTopTables.map((item) => item.name), axisLabel: { color: text } },
        series: [
          { name: '{{t "report.html.compare.baseline"}}', type: 'bar', data: window.compareTopTables.map((item) => item.baseline), itemStyle: { color: primary, borderRadius: [0, 4, 4, 0] } },
          { name: '{{t "report.html.compare.current"}}', type: 'bar', data: window.compareTopTables.map((item) => item.current), itemStyle: { color: accent, borderRadius: [0, 4, 4, 0] } },
        ],
      });

      patternChangesChart.setOption({
        animation: false,
        backgroundColor: 'transparent',
        tooltip: { show: false },
        dataZoom: [{ type: 'inside', zoomOnMouseWheel: true, moveOnMouseMove: true }],
        legend: { data: ['{{t "report.html.compare.baseline"}}', '{{t "report.html.compare.current"}}'], bottom: 0, textStyle: { color: text } },
        grid: { left: 16, right: 24, top: 16, bottom: 56, containLabel: true },
        xAxis: { type: 'value', axisLabel: { color: muted }, splitLine: { lineStyle: { color: border } } },
        yAxis: { type: 'category', data: window.comparePatternChanges.map((item) => item.name), axisLabel: { color: text } },
        series: [
          { name: '{{t "report.html.compare.baseline"}}', type: 'bar', data: window.comparePatternChanges.map((item) => item.baseline), itemStyle: { color: primary, borderRadius: [0, 4, 4, 0] } },
          { name: '{{t "report.html.compare.current"}}', type: 'bar', data: window.comparePatternChanges.map((item) => item.current), itemStyle: { color: accent, borderRadius: [0, 4, 4, 0] } },
        ],
      });

      opsMixChart.setOption({
        animation: false,
        backgroundColor: 'transparent',
        tooltip: { show: false },
        dataZoom: [{ type: 'inside', zoomOnMouseWheel: true, moveOnMouseMove: true }],
        legend: { data: ['{{t "report.html.compare.baseline"}}', '{{t "report.html.compare.current"}}'], bottom: 0, textStyle: { color: text } },
        grid: { left: 16, right: 24, top: 16, bottom: 48, containLabel: true },
        xAxis: { type: 'category', data: window.compareOpsMix.map((item) => item.name), axisLabel: { color: text } },
        yAxis: { type: 'value', axisLabel: { color: muted }, splitLine: { lineStyle: { color: border } } },
        series: [
          { name: '{{t "report.html.compare.baseline"}}', type: 'bar', data: window.compareOpsMix.map((item) => item.baseline), itemStyle: { color: primary, borderRadius: [4, 4, 0, 0] } },
          { name: '{{t "report.html.compare.current"}}', type: 'bar', data: window.compareOpsMix.map((item) => item.current), itemStyle: { color: accent, borderRadius: [4, 4, 0, 0] } },
        ],
      });

      alertChart.setOption({
        animation: false,
        backgroundColor: 'transparent',
        tooltip: { show: false },
        dataZoom: [{ type: 'inside', zoomOnMouseWheel: true, moveOnMouseMove: true }],
        grid: { left: 16, right: 24, top: 20, bottom: 24, containLabel: true },
        xAxis: { type: 'category', data: window.compareAlertCounts.map((item) => item.name), axisLabel: { color: text } },
        yAxis: { type: 'value', axisLabel: { color: muted }, splitLine: { lineStyle: { color: border } } },
        series: [{
          type: 'bar',
          data: window.compareAlertCounts.map((item, idx) => ({ value: item.value, itemStyle: { color: idx === 0 ? '#34d399' : '#f87171', borderRadius: [4, 4, 0, 0] } })),
          barWidth: 48,
        }],
      });

      ddlChart.setOption({
        animation: false,
        backgroundColor: 'transparent',
        tooltip: { show: false },
        dataZoom: [{ type: 'inside', zoomOnMouseWheel: true, moveOnMouseMove: true }],
        grid: { left: 16, right: 24, top: 20, bottom: 24, containLabel: true },
        xAxis: { type: 'category', data: [_lBaseline, _lCurrent], axisLabel: { color: text } },
        yAxis: { type: 'value', axisLabel: { color: muted }, splitLine: { lineStyle: { color: border } } },
        series: [{
          type: 'bar',
          data: [
            { value: window.compareDDLChanges.baseline_count, itemStyle: { color: primary, borderRadius: [4, 4, 0, 0] } },
            { value: window.compareDDLChanges.current_count, itemStyle: { color: accent, borderRadius: [4, 4, 0, 0] } }
          ],
          barWidth: 48,
        }],
      });

      txnChart.setOption({
        animation: false,
        backgroundColor: 'transparent',
        tooltip: { show: false },
        dataZoom: [{ type: 'inside', zoomOnMouseWheel: true, moveOnMouseMove: true }],
        legend: { data: [_lBaseline, _lCurrent], bottom: 0, textStyle: { color: text } },
        grid: { left: 16, right: 24, top: 16, bottom: 48, containLabel: true },
        xAxis: { type: 'value', axisLabel: { color: muted }, splitLine: { lineStyle: { color: border } } },
        yAxis: { type: 'category', data: [_lLargestTxnRows, _lLongestTxnSec], axisLabel: { color: text } },
        series: [
          { name: _lBaseline, type: 'bar', data: [window.compareTxnDiagnostics.largest_txn.baseline_rows, parseDur(window.compareTxnDiagnostics.longest_txn.baseline_duration)], itemStyle: { color: primary, borderRadius: [0, 4, 4, 0] } },
          { name: _lCurrent, type: 'bar', data: [window.compareTxnDiagnostics.largest_txn.current_rows, parseDur(window.compareTxnDiagnostics.longest_txn.current_duration)], itemStyle: { color: accent, borderRadius: [0, 4, 4, 0] } },
        ],
      });

      hotChart.setOption({
        animation: false,
        backgroundColor: 'transparent',
        tooltip: { show: false },
        dataZoom: [{ type: 'inside', zoomOnMouseWheel: true, moveOnMouseMove: true }],
        grid: { left: 16, right: 24, top: 20, bottom: 24, containLabel: true },
        xAxis: { type: 'category', data: [_lBaseline, _lCurrent], axisLabel: { color: text } },
        yAxis: { type: 'value', axisLabel: { color: muted }, splitLine: { lineStyle: { color: border } } },
        series: [{
          type: 'bar',
          data: [
            { value: window.compareHotIntervals.baseline_top_rows, itemStyle: { color: primary, borderRadius: [4, 4, 0, 0] } },
            { value: window.compareHotIntervals.current_top_rows, itemStyle: { color: accent, borderRadius: [4, 4, 0, 0] } }
          ],
          barWidth: 48,
        }],
      });

      eventMixChart.setOption({
        animation: false,
        backgroundColor: 'transparent',
        tooltip: { show: false },
        dataZoom: [{ type: 'inside', zoomOnMouseWheel: true, moveOnMouseMove: true }],
        grid: { left: 16, right: 24, top: 20, bottom: 24, containLabel: true },
        xAxis: { type: 'category', data: [_lInsert, _lUpdate, _lDelete, _lDDL], axisLabel: { color: text } },
        yAxis: { type: 'value', axisLabel: { color: muted }, splitLine: { lineStyle: { color: border } } },
        series: [{
          type: 'bar',
          data: [
            { value: window.compareEventMixDelta.insert_delta, itemStyle: { color: window.compareEventMixDelta.insert_delta >= 0 ? '#34d399' : '#f87171', borderRadius: [4, 4, 0, 0] } },
            { value: window.compareEventMixDelta.update_delta, itemStyle: { color: window.compareEventMixDelta.update_delta >= 0 ? '#34d399' : '#f87171', borderRadius: [4, 4, 0, 0] } },
            { value: window.compareEventMixDelta.delete_delta, itemStyle: { color: window.compareEventMixDelta.delete_delta >= 0 ? '#34d399' : '#f87171', borderRadius: [4, 4, 0, 0] } },
            { value: window.compareEventMixDelta.ddl_delta, itemStyle: { color: window.compareEventMixDelta.ddl_delta >= 0 ? '#34d399' : '#f87171', borderRadius: [4, 4, 0, 0] } },
          ],
          barWidth: 48,
        }],
      });
    }

    const drilldownsEl = document.getElementById('compare-pattern-drilldowns');
    if (drilldownsEl && window.comparePatternDrilldowns && window.comparePatternDrilldowns.length > 0) {
      window.comparePatternDrilldowns.forEach(dd => {
        const detail = document.createElement('div');
        detail.className = 'drilldown-details';
        detail.style.cssText = 'margin:12px 18px;padding:12px 16px;border:1px solid var(--border);border-radius:8px;background:var(--surface2)';
        const title = document.createElement('div');
        title.className = 'drilldown-label';
        title.style.cssText = 'font-size:13px;font-weight:600;color:var(--text);margin-bottom:8px';
        title.textContent = dd.label || dd.pattern_key;
        detail.appendChild(title);
        const why = document.createElement('div');
        why.style.cssText = 'font-size:12px;color:var(--muted);margin-bottom:8px';
        why.appendChild(document.createTextNode(_lDrilldown));
        const whyStrong = document.createElement('strong');
        whyStrong.textContent = dd.why_selected;
        why.appendChild(whyStrong);
        detail.appendChild(why);
        if (dd.key_points && dd.key_points.length > 0) {
          dd.key_points.forEach(kp => {
            const kpRow = document.createElement('div');
            kpRow.style.cssText = 'font-size:12px;margin-top:4px';
            const kpLabel = document.createElement('span');
            kpLabel.className = 'kp-label';
            kpLabel.style.cssText = 'color:var(--accent);margin-right:6px';
            kpLabel.textContent = kp.label + ':';
            kpRow.appendChild(kpLabel);
            kpRow.appendChild(document.createTextNode(kp.summary));
            detail.appendChild(kpRow);
          });
        }
        drilldownsEl.appendChild(detail);
      });
    }

    function parseDur(s) {
      if (!s) return 0;
      var total = 0;
      var m = s.match(/(\d+)h/); if (m) total += parseInt(m[1]) * 3600;
      m = s.match(/(\d+)m(?!s)/); if (m) total += parseInt(m[1]) * 60;
      m = s.match(/(\d+(?:\.\d+)?)s/); if (m) total += parseFloat(m[1]);
      m = s.match(/(\d+)ms/); if (m) total += parseInt(m[1]) / 1000;
      m = s.match(/(\d+)us/); if (m) total += parseInt(m[1]) / 1000000;
      return total;
    }

    function setTheme(name) {
      document.documentElement.setAttribute('data-theme', name);
      localStorage.setItem('bvtheme', name);
      document.querySelectorAll('.theme-btn').forEach(function(b) {
        b.classList.toggle('active', b.getAttribute('data-t') === name);
      });
      renderAllCharts();
    }

    document.querySelectorAll('.theme-btn').forEach(function(btn) {
      btn.addEventListener('click', function() { setTheme(btn.getAttribute('data-t')); });
    });

    var savedTheme = localStorage.getItem('bvtheme') || 'nebula';
    setTheme(savedTheme);

    // Back to Top button listener
    var backToTopBtn = document.getElementById('btn-back-to-top');
    if (backToTopBtn) {
      window.addEventListener('scroll', function() {
        if (window.scrollY > 300) {
          backToTopBtn.classList.add('visible');
        } else {
          backToTopBtn.classList.remove('visible');
        }
      });
      backToTopBtn.addEventListener('click', function() {
        window.scrollTo({ top: 0, behavior: 'smooth' });
      });
    }

    window.addEventListener('resize', function () {
      summaryChart.resize();
      topTablesChart.resize();
      patternChangesChart.resize();
      opsMixChart.resize();
      alertChart.resize();
      ddlChart.resize();
      txnChart.resize();
      hotChart.resize();
      eventMixChart.resize();
    });
  </script>
</body>
</html>`
