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
  :root {
    --bg: #000000;
    --surface: #0a0a12;
    --surface2: #0f0f1a;
    --border: #1c1c2e;
    --primary: #818cf8;
    --accent: #22d3ee;
    --text: #f1f5f9;
    --muted: #64748b;
    --success: #34d399;
    --warn: #fbbf24;
    --danger: #f87171;
  }
  * { box-sizing: border-box; }
  body {
    margin: 0;
    background: var(--bg);
    color: var(--text);
    font-family: 'Fira Sans', 'Inter', system-ui, sans-serif;
    line-height: 1.5;
  }
  .page { max-width: 1280px; margin: 0 auto; padding: 24px 20px 56px; }
  .header {
    display: flex;
    justify-content: space-between;
    align-items: flex-end;
    gap: 16px;
    margin-bottom: 24px;
    padding: 20px 24px;
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 10px;
  }
  .header-logo {
    font-family: 'Fira Code', monospace;
    font-size: 22px;
    font-weight: 700;
    color: var(--primary);
  }
  .header-logo span { color: var(--accent); }
  .header-meta { color: var(--muted); font-size: 12px; text-align: right; }
  .header-meta strong { display: block; color: var(--text); font-size: 13px; margin-bottom: 4px; }
  .cards {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
    gap: 16px;
    margin-bottom: 24px;
  }
  .card {
    padding: 18px 20px;
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 10px;
    position: relative;
    overflow: hidden;
  }
  .card::before {
    content: "";
    position: absolute;
    left: 0;
    top: 0;
    width: 100%;
    height: 3px;
    background: var(--primary);
  }
  .card.accent::before { background: var(--accent); }
  .card.success::before { background: var(--success); }
  .card.warn::before { background: var(--warn); }
  .card-label {
    font-size: 11px;
    font-weight: 600;
    letter-spacing: 0.7px;
    text-transform: uppercase;
    color: var(--muted);
    margin-bottom: 10px;
  }
  .card-value {
    font-family: 'Fira Code', monospace;
    font-size: 28px;
    font-weight: 700;
  }
  .card-sub { margin-top: 8px; color: var(--muted); font-size: 12px; }
  .section {
    margin-bottom: 20px;
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 10px;
    overflow: hidden;
  }
  .section-header {
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 14px 18px;
    border-bottom: 1px solid var(--border);
    font-size: 13px;
    font-weight: 600;
  }
  .section-header .dot {
    width: 8px;
    height: 8px;
    border-radius: 50%;
    background: var(--primary);
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
    padding: 10px 12px;
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
    padding: 10px 12px;
    border-bottom: 1px solid var(--border);
  }
  tbody tr:last-child td { border-bottom: none; }
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
    color: var(--text);
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
  .evidence-refs { font-size: 12px; color: var(--muted); margin-left: 4px; }
  .evidence-refs a { color: var(--accent); text-decoration: none; }
  .evidence-refs a:hover { text-decoration: underline; }
  .rec-badge {
    display: inline-block;
    padding: 2px 8px;
    border-radius: 4px;
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.5px;
  }
  .rec-badge.high { background: rgba(248,113,113,0.15); color: #f87171; }
  .rec-badge.medium { background: rgba(251,191,36,0.15); color: #fbbf24; }
  .rec-badge.low { background: rgba(52,211,153,0.15); color: #34d399; }
  .rec-item { margin-bottom: 14px; }
  .rec-item:last-child { margin-bottom: 0; }
  .rec-summary { font-size: 13px; margin-top: 4px; }
  @media (max-width: 900px) {
    .two-col, .alerts-layout, .alert-columns { grid-template-columns: 1fr; }
  }
</style>
</head>
<body>
  <div class="page">
    <header class="header">
      <div>
        <div class="header-logo">Binlog<span>Viz</span> {{t "report.html.compare.logoSuffix"}}</div>
        <div>{{.Result.CurrentLabel}} vs {{.Result.BaselineLabel}}</div>
        {{if snapshotWindow .Result.CurrentSnapshot}}<div class="header-context">{{t "report.html.compare.currentWindow"}}: {{snapshotWindow .Result.CurrentSnapshot}}</div>{{end}}
        {{if snapshotWindow .Result.BaselineSnapshot}}<div class="header-context">{{t "report.html.compare.baselineWindow"}}: {{snapshotWindow .Result.BaselineSnapshot}}</div>{{end}}
        {{if snapshotInputMode .Result.CurrentSnapshot}}<div class="header-context">{{t "report.html.compare.currentInputMode"}}: {{snapshotInputMode .Result.CurrentSnapshot}}</div>{{end}}
        {{if snapshotInputMode .Result.BaselineSnapshot}}<div class="header-context">{{t "report.html.compare.baselineInputMode"}}: {{snapshotInputMode .Result.BaselineSnapshot}}</div>{{end}}
        {{if snapshotSource .Result.CurrentSnapshot}}<div class="header-context">{{t "report.html.compare.currentSource"}}: {{snapshotSource .Result.CurrentSnapshot}}</div>{{end}}
        {{if snapshotSource .Result.BaselineSnapshot}}<div class="header-context">{{t "report.html.compare.baselineSource"}}: {{snapshotSource .Result.BaselineSnapshot}}</div>{{end}}
        {{if snapshotFilters .Result.CurrentSnapshot}}<div class="header-context">{{t "report.html.compare.currentFilters"}}: {{snapshotFilters .Result.CurrentSnapshot}}</div>{{end}}
        {{if snapshotFilters .Result.BaselineSnapshot}}<div class="header-context">{{t "report.html.compare.baselineFilters"}}: {{snapshotFilters .Result.BaselineSnapshot}}</div>{{end}}
      </div>
      <div class="header-meta">
        <strong>{{t "report.html.common.generatedAt"}}</strong>
        <span>{{.GeneratedAt}}</span>
      </div>
    </header>

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
                <td>{{.Schema}}.{{.Table}}</td>
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
                    <div><strong>{{if .Label}}{{.Label}}{{else}}{{.PatternKey}}{{end}}</strong></div>
                    {{if .SampleQuerySummary}}<div>{{.SampleQuerySummary}}</div>{{end}}
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
                <td>{{.Operation}}</td>
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

    const summaryChart = echarts.init(document.getElementById('compare-summary-chart'));
    summaryChart.setOption({
      animation: false,
      backgroundColor: 'transparent',
      tooltip: { show: false },
      legend: { data: ['{{t "report.html.compare.baseline"}}', '{{t "report.html.compare.current"}}'], bottom: 0, textStyle: { color: '#f1f5f9' } },
      grid: { left: 70, right: 20, top: 16, bottom: 48 },
      xAxis: { type: 'value', axisLabel: { color: '#64748b' }, splitLine: { lineStyle: { color: '#1c1c2e' } } },
      yAxis: { type: 'category', data: window.compareSummaryPairs.map((item) => item.name), axisLabel: { color: '#f1f5f9' } },
      series: [
        { name: '{{t "report.html.compare.baseline"}}', type: 'bar', data: window.compareSummaryPairs.map((item) => item.baseline), itemStyle: { color: '#818cf8' } },
        { name: '{{t "report.html.compare.current"}}', type: 'bar', data: window.compareSummaryPairs.map((item) => item.current), itemStyle: { color: '#22d3ee' } },
      ],
    });

    const topTablesChart = echarts.init(document.getElementById('compare-top-tables'));
    topTablesChart.setOption({
      animation: false,
      backgroundColor: 'transparent',
      tooltip: { show: false },
      legend: { data: ['{{t "report.html.compare.baseline"}}', '{{t "report.html.compare.current"}}'], bottom: 0, textStyle: { color: '#f1f5f9' } },
      grid: { left: 110, right: 20, top: 16, bottom: 48 },
      xAxis: { type: 'value', axisLabel: { color: '#64748b' }, splitLine: { lineStyle: { color: '#1c1c2e' } } },
      yAxis: { type: 'category', data: window.compareTopTables.map((item) => item.name), axisLabel: { color: '#f1f5f9' } },
      series: [
        { name: '{{t "report.html.compare.baseline"}}', type: 'bar', data: window.compareTopTables.map((item) => item.baseline), itemStyle: { color: '#818cf8' } },
        { name: '{{t "report.html.compare.current"}}', type: 'bar', data: window.compareTopTables.map((item) => item.current), itemStyle: { color: '#22d3ee' } },
      ],
    });

    const patternChangesChart = echarts.init(document.getElementById('compare-pattern-changes'));
    patternChangesChart.setOption({
      animation: false,
      backgroundColor: 'transparent',
      tooltip: { show: false },
      legend: { data: ['{{t "report.html.compare.baseline"}}', '{{t "report.html.compare.current"}}'], bottom: 0, textStyle: { color: '#f1f5f9' } },
      grid: { left: 110, right: 20, top: 16, bottom: 56 },
      xAxis: { type: 'value', axisLabel: { color: '#64748b' }, splitLine: { lineStyle: { color: '#1c1c2e' } } },
      yAxis: { type: 'category', data: window.comparePatternChanges.map((item) => item.name), axisLabel: { color: '#f1f5f9' } },
      series: [
        { name: '{{t "report.html.compare.baseline"}}', type: 'bar', data: window.comparePatternChanges.map((item) => item.baseline), itemStyle: { color: '#818cf8' } },
        { name: '{{t "report.html.compare.current"}}', type: 'bar', data: window.comparePatternChanges.map((item) => item.current), itemStyle: { color: '#22d3ee' } },
      ],
    });

    const opsMixChart = echarts.init(document.getElementById('compare-ops-mix'));
    opsMixChart.setOption({
      animation: false,
      backgroundColor: 'transparent',
      tooltip: { show: false },
      legend: { data: ['{{t "report.html.compare.baseline"}}', '{{t "report.html.compare.current"}}'], bottom: 0, textStyle: { color: '#f1f5f9' } },
      grid: { left: 70, right: 20, top: 16, bottom: 48 },
      xAxis: { type: 'category', data: window.compareOpsMix.map((item) => item.name), axisLabel: { color: '#f1f5f9' } },
      yAxis: { type: 'value', axisLabel: { color: '#64748b' }, splitLine: { lineStyle: { color: '#1c1c2e' } } },
      series: [
        { name: '{{t "report.html.compare.baseline"}}', type: 'bar', data: window.compareOpsMix.map((item) => item.baseline), itemStyle: { color: '#818cf8' } },
        { name: '{{t "report.html.compare.current"}}', type: 'bar', data: window.compareOpsMix.map((item) => item.current), itemStyle: { color: '#22d3ee' } },
      ],
    });

    const alertChart = echarts.init(document.getElementById('compare-alerts'));
    alertChart.setOption({
      animation: false,
      backgroundColor: 'transparent',
      tooltip: { show: false },
      grid: { left: 50, right: 20, top: 20, bottom: 24 },
      xAxis: { type: 'category', data: window.compareAlertCounts.map((item) => item.name), axisLabel: { color: '#f1f5f9' } },
      yAxis: { type: 'value', axisLabel: { color: '#64748b' }, splitLine: { lineStyle: { color: '#1c1c2e' } } },
      series: [{
        type: 'bar',
        data: window.compareAlertCounts.map((item, idx) => ({ value: item.value, itemStyle: { color: idx === 0 ? '#34d399' : '#f87171' } })),
        barWidth: 48,
      }],
    });

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

    const ddlChart = echarts.init(document.getElementById('compare-ddl-chart'));
    ddlChart.setOption({
      animation: false,
      backgroundColor: 'transparent',
      tooltip: { show: false },
      grid: { left: 50, right: 20, top: 20, bottom: 24 },
      xAxis: { type: 'category', data: [_lBaseline, _lCurrent], axisLabel: { color: '#f1f5f9' } },
      yAxis: { type: 'value', axisLabel: { color: '#64748b' }, splitLine: { lineStyle: { color: '#1c1c2e' } } },
      series: [{
        type: 'bar',
        data: [
          { value: window.compareDDLChanges.baseline_count, itemStyle: { color: '#818cf8' } },
          { value: window.compareDDLChanges.current_count, itemStyle: { color: '#22d3ee' } }
        ],
        barWidth: 48,
      }],
    });

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

    const txnChart = echarts.init(document.getElementById('compare-txn-chart'));
    txnChart.setOption({
      animation: false,
      backgroundColor: 'transparent',
      tooltip: { show: false },
      legend: { data: [_lBaseline, _lCurrent], bottom: 0, textStyle: { color: '#f1f5f9' } },
      grid: { left: 80, right: 20, top: 16, bottom: 48 },
      xAxis: { type: 'value', axisLabel: { color: '#64748b' }, splitLine: { lineStyle: { color: '#1c1c2e' } } },
      yAxis: { type: 'category', data: [_lLargestTxnRows, _lLongestTxnSec], axisLabel: { color: '#f1f5f9' } },
      series: [
        { name: _lBaseline, type: 'bar', data: [window.compareTxnDiagnostics.largest_txn.baseline_rows, parseDur(window.compareTxnDiagnostics.longest_txn.baseline_duration)], itemStyle: { color: '#818cf8' } },
        { name: _lCurrent, type: 'bar', data: [window.compareTxnDiagnostics.largest_txn.current_rows, parseDur(window.compareTxnDiagnostics.longest_txn.current_duration)], itemStyle: { color: '#22d3ee' } },
      ],
    });

    const hotChart = echarts.init(document.getElementById('compare-hot-chart'));
    hotChart.setOption({
      animation: false,
      backgroundColor: 'transparent',
      tooltip: { show: false },
      grid: { left: 50, right: 20, top: 20, bottom: 24 },
      xAxis: { type: 'category', data: [_lBaseline, _lCurrent], axisLabel: { color: '#f1f5f9' } },
      yAxis: { type: 'value', axisLabel: { color: '#64748b' }, splitLine: { lineStyle: { color: '#1c1c2e' } } },
      series: [{
        type: 'bar',
        data: [
          { value: window.compareHotIntervals.baseline_top_rows, itemStyle: { color: '#818cf8' } },
          { value: window.compareHotIntervals.current_top_rows, itemStyle: { color: '#22d3ee' } }
        ],
        barWidth: 48,
      }],
    });

    const eventMixChart = echarts.init(document.getElementById('compare-event-mix-chart'));
    eventMixChart.setOption({
      animation: false,
      backgroundColor: 'transparent',
      tooltip: { show: false },
      grid: { left: 70, right: 20, top: 20, bottom: 24 },
      xAxis: { type: 'category', data: [_lInsert, _lUpdate, _lDelete, _lDDL], axisLabel: { color: '#f1f5f9' } },
      yAxis: { type: 'value', axisLabel: { color: '#64748b' }, splitLine: { lineStyle: { color: '#1c1c2e' } } },
      series: [{
        type: 'bar',
        data: [
          { value: window.compareEventMixDelta.insert_delta, itemStyle: { color: window.compareEventMixDelta.insert_delta >= 0 ? '#34d399' : '#f87171' } },
          { value: window.compareEventMixDelta.update_delta, itemStyle: { color: window.compareEventMixDelta.update_delta >= 0 ? '#34d399' : '#f87171' } },
          { value: window.compareEventMixDelta.delete_delta, itemStyle: { color: window.compareEventMixDelta.delete_delta >= 0 ? '#34d399' : '#f87171' } },
          { value: window.compareEventMixDelta.ddl_delta, itemStyle: { color: window.compareEventMixDelta.ddl_delta >= 0 ? '#34d399' : '#f87171' } },
        ],
        barWidth: 48,
      }],
    });

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
