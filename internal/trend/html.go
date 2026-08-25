// Package trend renders self-contained HTML trend reports.
// input: deterministic trend Result values plus localized labels and embedded chart assets.
// output: trend HTML pages with summary cards, charts, and drilldown sections.
// pos: HTML renderer used by the trend command output path.
// note: if this file changes, keep internal/trend/README.md synchronized.
package trend

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

type htmlData struct {
	Result                Result
	GeneratedAt           string
	EChartsJS             template.JS
	LabelsJSON            template.JS
	RowsJSON              template.JS
	TxnsJSON              template.JS
	EventsJSON            template.JS
	InsertJSON            template.JS
	UpdateJSON            template.JS
	DeleteJSON            template.JS
	TableSeriesJSON       template.JS
	PatternSeriesJSON     template.JS
	PatternDrilldownsJSON template.JS
	TrendSummaryJSON      template.JS
	RecommendationsJSON   template.JS
	TPSSeriesJSON         template.JS
	DDLSeriesJSON         template.JS
	TxnSeriesJSON         template.JS
	EventMixSeriesJSON    template.JS
	HotIntervalJSON       template.JS
}

type htmlTableSeries struct {
	Name   string `json:"name"`
	Type   string `json:"type"`
	Smooth bool   `json:"smooth"`
	Data   []int  `json:"data"`
}

type htmlPatternTrendSeries struct {
	Key         string    `json:"key"`
	Label       string    `json:"label"`
	Rows        []int     `json:"rows"`
	ShareOfRows []float64 `json:"share_of_rows"`
	SampleQuery string    `json:"sample_query,omitempty"`
}

func RenderHTML(result Result) (string, error) {
	echartsJS, err := report.ReadEmbeddedECharts()
	if err != nil {
		return "", err
	}
	tmpl, err := report.NewHTMLTemplate("trend", trendHTMLTemplate, nil)
	if err != nil {
		return "", err
	}

	data := htmlData{
		Result:                result,
		GeneratedAt:           time.Now().UTC().Format("2006-01-02 15:04:05 UTC"),
		EChartsJS:             template.JS(echartsJS), //nolint:gosec
		LabelsJSON:            mustHTMLJSON(buildLabels(result.Points)),
		RowsJSON:              mustHTMLJSON(buildMetricSeries(result.Points, func(point Point) int { return point.Summary.TotalRows })),
		TxnsJSON:              mustHTMLJSON(buildMetricSeries(result.Points, func(point Point) int { return point.Summary.TotalTransactions })),
		EventsJSON:            mustHTMLJSON(buildMetricSeries(result.Points, func(point Point) int { return point.Summary.TotalEvents })),
		InsertJSON:            mustHTMLJSON(buildMetricSeries(result.Points, func(point Point) int { return point.Operations.Inserts })),
		UpdateJSON:            mustHTMLJSON(buildMetricSeries(result.Points, func(point Point) int { return point.Operations.Updates })),
		DeleteJSON:            mustHTMLJSON(buildMetricSeries(result.Points, func(point Point) int { return point.Operations.Deletes })),
		TableSeriesJSON:       mustHTMLJSON(buildTableSeries(result.TableTrends)),
		PatternSeriesJSON:     mustHTMLJSON(buildPatternSeries(result.PatternTrends)),
		PatternDrilldownsJSON: mustHTMLJSON(result.PatternDrilldowns),
		TrendSummaryJSON:      mustHTMLJSON(result.TrendSummary),
		RecommendationsJSON:   mustHTMLJSON(result.Recommendations),
		TPSSeriesJSON:         mustHTMLJSON(result.DiagnosticsTrends.TPSTrends),
		DDLSeriesJSON:         mustHTMLJSON(result.DiagnosticsTrends.DDLTrends),
		TxnSeriesJSON:         mustHTMLJSON(buildTxnTrendData(result.DiagnosticsTrends)),
		EventMixSeriesJSON:    mustHTMLJSON(result.DiagnosticsTrends.EventMixTrends.Snapshots),
		HotIntervalJSON:       mustHTMLJSON(result.DiagnosticsTrends.HotIntervalSummary),
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("render trend html template: %w", err)
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

func buildLabels(points []Point) []string {
	labels := make([]string, 0, len(points))
	for _, point := range points {
		labels = append(labels, point.Snapshot.Name)
	}
	return labels
}

func buildMetricSeries(points []Point, getter func(Point) int) []int {
	values := make([]int, 0, len(points))
	for _, point := range points {
		values = append(values, getter(point))
	}
	return values
}

func buildTableSeries(trends []TableTrend) []htmlTableSeries {
	limit := len(trends)
	if limit > 5 {
		limit = 5
	}
	result := make([]htmlTableSeries, 0, limit)
	for _, trend := range trends[:limit] {
		data := make([]int, 0, len(trend.Series))
		for _, point := range trend.Series {
			data = append(data, point.Rows)
		}
		result = append(result, htmlTableSeries{
			Name:   trend.Schema + "." + trend.Table,
			Type:   "line",
			Smooth: true,
			Data:   data,
		})
	}
	return result
}

func buildPatternSeries(trends []PatternTrend) []htmlPatternTrendSeries {
	limit := len(trends)
	if limit > 5 {
		limit = 5
	}
	result := make([]htmlPatternTrendSeries, 0, limit)
	for _, trend := range trends[:limit] {
		rows := make([]int, 0, len(trend.RowsSeries))
		for _, point := range trend.RowsSeries {
			rows = append(rows, point.Rows)
		}
		shares := make([]float64, 0, len(trend.ShareOfRowsSeries))
		for _, point := range trend.ShareOfRowsSeries {
			shares = append(shares, point.ShareOfRows)
		}
		result = append(result, htmlPatternTrendSeries{
			Key:         trend.PatternKey,
			Label:       patternTrendDisplayLabel(trend),
			Rows:        rows,
			ShareOfRows: shares,
			SampleQuery: trend.SampleQuerySummary,
		})
	}
	return result
}

func patternTrendDisplayLabel(trend PatternTrend) string {
	key := strings.TrimSpace(trend.PatternKey)
	label := strings.TrimSpace(trend.Label)
	switch {
	case key != "" && label != "" && key != label:
		return label + " (" + key + ")"
	case label != "":
		return label
	case key != "":
		return key
	default:
		return i18n.T("report.html.trend.patternFallback")
	}
}

type htmlTxnTrendPoint struct {
	SnapshotName string  `json:"snapshot_name"`
	SizeValue    float64 `json:"size_value"`
	DurValue     float64 `json:"dur_value"`
}

func buildTxnTrendData(dt DiagnosticsTrends) []htmlTxnTrendPoint {
	result := make([]htmlTxnTrendPoint, 0, len(dt.TxnSizeTrends))
	for i, point := range dt.TxnSizeTrends {
		durValue := 0.0
		if i < len(dt.TxnDurationTrends) {
			durValue = dt.TxnDurationTrends[i].Value
		}
		result = append(result, htmlTxnTrendPoint{
			SnapshotName: point.SnapshotName,
			SizeValue:    point.Value,
			DurValue:     durValue,
		})
	}
	return result
}

const trendHTMLTemplate = `<!DOCTYPE html>
<html lang="{{lang}}">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{{t "report.html.trend.title"}}</title>
<style>
  :root, [data-theme="nebula"] {
    --bg: #07090e;
    --bg-mesh: radial-gradient(ellipse 80% 50% at 50% -20%, rgba(34, 211, 238, 0.15), transparent 70%), radial-gradient(circle 40% at 90% 20%, rgba(129, 140, 248, 0.08), transparent 60%);
    --surface: #0e121e;
    --surface2: #141a2b;
    --surface-hover: #1b233a;
    --border: rgba(255, 255, 255, 0.08);
    --primary: #22d3ee;
    --primary-rgb: 34, 211, 238;
    --accent: #fbbf24;
    --accent-rgb: 251, 191, 36;
    --text: #f1f5f9;
    --text-heading: #ffffff;
    --muted: #8899b0;
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
    --bg-mesh: radial-gradient(ellipse 80% 50% at 50% -20%, rgba(8, 145, 178, 0.08), transparent 70%);
    --surface: #ffffff;
    --surface2: #f1f5f9;
    --surface-hover: #e2e8f0;
    --border: #e2e8f0;
    --primary: #0891b2;
    --primary-rgb: 8, 145, 178;
    --accent: #d97706;
    --accent-rgb: 217, 119, 6;
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

  .hero {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 14px;
    padding: 20px 24px;
    margin-bottom: 20px;
    box-shadow: var(--card-shadow);
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 16px;
    flex-wrap: wrap;
  }
  .hero-brand { display: flex; align-items: center; gap: 12px; }
  .hero h1 {
    margin: 0 0 4px;
    font-size: 22px;
    font-weight: 800;
    color: var(--text-heading);
    font-family: "JetBrains Mono", "Fira Code", monospace;
  }
  .hero h1 span {
    background: linear-gradient(135deg, var(--primary), var(--accent));
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
  }
  .hero p { margin: 0; color: var(--muted); font-size: 12px; }
  
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
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 12px;
    padding: 16px 18px;
    box-shadow: var(--card-shadow);
    position: relative;
    overflow: hidden;
    transition: transform 0.2s;
  }
  .card:hover { transform: translateY(-2px); border-color: rgba(var(--primary-rgb), 0.35); }
  .card::before {
    content: "";
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 3px;
    background: linear-gradient(90deg, var(--primary), var(--accent));
  }
  .label {
    color: var(--muted);
    font-size: 11px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.8px;
  }
  .value {
    font-size: 24px;
    font-weight: 800;
    margin-top: 8px;
    color: var(--text-heading);
    font-family: "JetBrains Mono", "Fira Code", monospace;
    font-variant-numeric: tabular-nums;
  }

  .section {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 14px;
    margin-bottom: 22px;
    overflow: hidden;
    box-shadow: var(--card-shadow);
  }
  .section-header {
    padding: 14px 18px;
    border-bottom: 1px solid var(--border);
    font-weight: 700;
    font-size: 13.5px;
    color: var(--text-heading);
    display: flex;
    align-items: center;
    gap: 8px;
  }
  .section-body { padding: 18px; }
  .section-tools {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
    margin-bottom: 14px;
    flex-wrap: wrap;
  }
  .segment-control {
    display: inline-flex;
    padding: 3px;
    background: var(--surface2);
    border: 1px solid var(--border);
    border-radius: 999px;
    gap: 4px;
  }
  .segment-control button {
    appearance: none;
    border: 0;
    border-radius: 999px;
    background: transparent;
    color: var(--muted);
    padding: 6px 14px;
    font-size: 12px;
    font-weight: 700;
    letter-spacing: 0.5px;
    cursor: pointer;
    transition: background 0.15s, color 0.15s;
  }
  .segment-control button[aria-pressed="true"] {
    background: var(--primary);
    color: #04111c;
  }
  .pattern-chart { width: 100%; height: 420px; min-height: 420px; }
  .empty-state {
    padding: 18px;
    border: 1px dashed var(--border);
    border-radius: 10px;
    color: var(--muted);
    background: var(--surface2);
  }
  .evidence-refs { font-size: 12px; color: var(--muted); margin-left: 4px; }
  .evidence-refs a { color: var(--accent); text-decoration: none; }
  .evidence-refs a:hover { text-decoration: underline; }
  .pattern-hint { color: var(--muted); font-size: 12px; }
  .chart-box { width: 100%; height: 320px; }
  
  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  th, td {
    padding: 10px 14px;
    border-bottom: 1px solid var(--border);
    text-align: left;
  }
  th {
    color: var(--muted);
    text-transform: uppercase;
    font-size: 11px;
    letter-spacing: 0.6px;
    background: var(--surface2);
    font-weight: 700;
  }
  td {
    font-family: "JetBrains Mono", "Fira Code", monospace;
    font-size: 12.5px;
  }
  tbody tr:hover { background: var(--surface-hover); }
  tbody tr:last-child td { border-bottom: none; }

  .rec-badge {
    display: inline-block;
    padding: 2px 8px;
    border-radius: 4px;
    font-size: 11px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.5px;
  }
  .rec-badge.high { background: rgba(248,113,113,0.18); color: #f87171; border: 1px solid rgba(248,113,113,0.3); }
  .rec-badge.medium { background: rgba(251,191,36,0.18); color: #fbbf24; border: 1px solid rgba(251,191,36,0.3); }
  .rec-badge.low { background: rgba(52,211,153,0.18); color: #34d399; border: 1px solid rgba(52,211,153,0.3); }
  .rec-item { margin-bottom: 14px; }
  .rec-item:last-child { margin-bottom: 0; }
  .rec-summary { font-size: 13px; margin-top: 4px; }

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
  <section class="hero">
    <div class="hero-brand">
      <div>
        <h1><span>Binlog</span>Viz {{t "report.html.trend.hero"}}</h1>
        <p>{{t "report.html.common.generatedAt"}} {{.GeneratedAt}}</p>
        {{if .Result.BaselineSnapshot}}<p style="margin-top:2px">{{t "report.html.trend.baseline"}}: {{.Result.BaselineSnapshot.Label}} ({{.Result.BaselineSnapshot.Name}})</p>{{end}}
      </div>
    </div>
    <div class="theme-switcher">
      <button class="theme-btn" data-t="nebula" title="Nebula"></button>
      <button class="theme-btn" data-t="forest" title="Forest"></button>
      <button class="theme-btn" data-t="navy"   title="Navy"></button>
      <button class="theme-btn" data-t="ember"  title="Ember"></button>
      <button class="theme-btn" data-t="light"  title="Light"></button>
    </div>
  </section>

  <div class="cards">
    <div class="card">
      <div class="label">{{t "report.html.trend.firstSnapshot"}}</div>
      <div class="value">{{.Result.Insights.FirstSnapshot}}</div>
    </div>
    <div class="card">
      <div class="label">{{t "report.html.trend.lastSnapshot"}}</div>
      <div class="value">{{.Result.Insights.LastSnapshot}}</div>
    </div>
    <div class="card">
      <div class="label">{{t "report.html.trend.rowsDelta"}}</div>
      <div class="value">{{.Result.Insights.RowsDelta}}</div>
    </div>
    <div class="card">
      <div class="label">{{t "report.html.trend.txnDelta"}}</div>
      <div class="value">{{.Result.Insights.TxnsDelta}}</div>
    </div>
  </div>

  {{if .Result.TrendSummary}}
  <section class="section" id="trend-key-findings">
    <div class="section-header">{{t "report.html.compare.keyFindings"}}</div>
    <div class="section-body">
      <div id="trend-findings-list"></div>
    </div>
  </section>
  {{end}}

  {{if .Result.Recommendations}}
  <section class="section" id="trend-recommendations">
    <div class="section-header">{{t "report.html.compare.recommendedNextChecks"}}</div>
    <div class="section-body">
      <div id="trend-recommendations-list"></div>
    </div>
  </section>
  {{end}}

  <section class="section">
    <div class="section-header">{{t "report.html.trend.overallTrend"}}</div>
    <div class="section-body">
      <div id="trend-overall-chart" class="chart-box"></div>
    </div>
  </section>

  <section class="section">
    <div class="section-header">{{t "report.html.common.operationMix"}}</div>
    <div class="section-body">
      <div id="trend-ops-chart" class="chart-box"></div>
    </div>
  </section>

  <section class="section" id="section-table-trends">
    <div class="section-header">{{t "report.html.trend.topTableMovement"}}</div>
    <div class="section-body">
      {{range $idx, $trend := .Result.TableTrends}}<span id="table-{{$idx}}"></span>{{end}}
      <div id="trend-tables-chart" class="chart-box"></div>
    </div>
  </section>

  <section class="section" id="section-pattern-trends">
    <div class="section-header">{{t "report.html.trend.patternTrends"}}</div>
    <div class="section-body">
      {{range $idx, $trend := .Result.PatternTrends}}<span id="pattern-{{$idx}}"></span>{{end}}
      {{if .Result.PatternTrends}}
      <div class="section-tools">
        <div class="segment-control" role="tablist" aria-label="{{t "report.html.trend.patternViewMode"}}">
          <button type="button" id="pattern-view-share" data-pattern-view="share" aria-pressed="true">{{t "report.html.trend.shareOfRows"}}</button>
          <button type="button" id="pattern-view-rows" data-pattern-view="rows" aria-pressed="false">{{t "report.html.common.rows"}}</button>
        </div>
        <div class="pattern-hint">{{t "report.html.trend.patternHint"}}</div>
      </div>
      <div id="trend-pattern-chart" class="pattern-chart"></div>
      {{else}}
      <div class="empty-state">{{t "report.html.trend.noPatternTrends"}}</div>
      {{end}}
      <div id="trend-pattern-drilldowns"></div>
    </div>
  </section>

  <section class="section" id="section-ordered-points">
    <div class="section-header">{{t "report.html.trend.orderedPoints"}}</div>
    <div class="section-body">
      <table>
        <thead>
          <tr>
            <th>{{t "report.html.trend.snapshot"}}</th>
            <th>{{t "report.html.trend.start"}}</th>
            <th>{{t "report.html.common.rows"}}</th>
            <th>{{t "report.html.common.txns"}}</th>
            <th>{{t "report.html.common.events"}}</th>
            <th>{{t "report.html.common.alerts"}}</th>
          </tr>
        </thead>
        <tbody>
          {{range $idx, $point := .Result.Points}}
          <tr id="point-{{$idx}}">
            <td>{{$point.Snapshot.Name}}</td>
            <td>{{$point.Window.StartTime}}</td>
            <td>{{$point.Summary.TotalRows}}</td>
            <td>{{$point.Summary.TotalTransactions}}</td>
            <td>{{$point.Summary.TotalEvents}}</td>
            <td>{{$point.AlertCount}}</td>
          </tr>
          {{end}}
        </tbody>
      </table>
    </div>
  </section>

  <section class="section">
    <div class="section-header">{{t "report.html.trend.tpsTrends"}}</div>
    <div class="section-body">
      <div id="trend-tps-chart" class="chart-box"></div>
    </div>
  </section>

  <section class="section">
    <div class="section-header">{{t "report.html.trend.ddlTrends"}}</div>
    <div class="section-body">
      <div id="trend-ddl-chart" class="chart-box"></div>
    </div>
  </section>

  <section class="section">
    <div class="section-header">{{t "report.html.trend.txnTrends"}}</div>
    <div class="section-body">
      <div id="trend-txn-chart" class="chart-box"></div>
    </div>
  </section>

  <section class="section">
    <div class="section-header">{{t "report.html.trend.eventMixTrends"}}</div>
    <div class="section-body">
      <div id="trend-event-mix-chart" class="chart-box"></div>
    </div>
  </section>

  <section class="section">
    <div class="section-header">{{t "report.html.trend.hotIntervalTrends"}}</div>
    <div class="section-body">
      <div id="trend-hot-interval-chart" class="chart-box"></div>
    </div>
  </section>
</div>

<button id="btn-back-to-top" class="back-to-top" title="{{t "report.html.common.backToTop"}}" aria-label="{{t "report.html.common.backToTop"}}">
  <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round">
    <polyline points="18 15 12 9 6 15"></polyline>
  </svg>
</button>

<script>
  const labels = {{.LabelsJSON}};
  const rows = {{.RowsJSON}};
  const txns = {{.TxnsJSON}};
  const events = {{.EventsJSON}};
  const inserts = {{.InsertJSON}};
  const updates = {{.UpdateJSON}};
  const deletes = {{.DeleteJSON}};
  const tableSeries = {{.TableSeriesJSON}};
  const patternSeries = {{.PatternSeriesJSON}};
  const patternDrilldowns = {{.PatternDrilldownsJSON}};
  const trendSummary = {{.TrendSummaryJSON}};
  window.trendRecommendations = {{.RecommendationsJSON}};
  window.trendTPSSeries = {{.TPSSeriesJSON}};
  window.trendDDLSeries = {{.DDLSeriesJSON}};
  window.trendTxnSeries = {{.TxnSeriesJSON}};
  window.trendEventMixSeries = {{.EventMixSeriesJSON}};
  window.trendHotIntervalSeries = {{.HotIntervalJSON}};

  var _lMaxTPS = {{t "report.html.trend.chartMaxTPS"}};
  var _lDDLEvents = {{t "report.html.trend.chartDDLEvents"}};
  var _lLargestTxnRows = {{t "report.html.trend.chartLargestTxnRows"}};
  var _lLongestTxnSec = {{t "report.html.trend.chartLongestTxnSec"}};
  var _lInsert = {{t "report.html.common.inserts"}};
  var _lUpdate = {{t "report.html.common.updates"}};
  var _lDelete = {{t "report.html.common.deletes"}};
  var _lDDL = {{t "report.html.common.ddl"}};
  var _lMaxHotRows = {{t "report.html.trend.chartMaxHotRows"}};
  var _lHotCount = {{t "report.html.trend.chartHotCount"}};
  var _lDrilldown = {{t "report.html.common.drilldown"}};

  const trendFindingsEl = document.getElementById('trend-findings-list');
  if (trendFindingsEl && trendSummary && trendSummary.length > 0) {
    const ol = document.createElement('ol');
    trendSummary.forEach(f => {
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
    trendFindingsEl.appendChild(ol);
  }

  const trendRecEl = document.getElementById('trend-recommendations-list');
  if (trendRecEl && window.trendRecommendations && window.trendRecommendations.length > 0) {
    const recOl = document.createElement('ol');
    window.trendRecommendations.forEach(rec => {
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
    trendRecEl.appendChild(recOl);
  }

  function cssVar(name) {
    return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  }

  const overallChart = echarts.init(document.getElementById('trend-overall-chart'));
  const opsChart = echarts.init(document.getElementById('trend-ops-chart'));
  const tablesChart = echarts.init(document.getElementById('trend-tables-chart'));
  const patternChartEl = document.getElementById('trend-pattern-chart');
  let patternChart = patternChartEl && patternSeries.length > 0 ? echarts.init(patternChartEl) : null;
  const tpsChart = echarts.init(document.getElementById('trend-tps-chart'));
  const ddlChart = echarts.init(document.getElementById('trend-ddl-chart'));
  const txnChart = echarts.init(document.getElementById('trend-txn-chart'));
  const eventMixChart = echarts.init(document.getElementById('trend-event-mix-chart'));
  const hotIntervalChart = echarts.init(document.getElementById('trend-hot-interval-chart'));

  function renderTrendCharts() {
    var primary = cssVar('--primary') || '#22d3ee';
    var accent  = cssVar('--accent') || '#fbbf24';
    var border  = cssVar('--border') || '#223053';
    var muted   = cssVar('--muted') || '#8899b0';
    var text    = cssVar('--text') || '#f1f5f9';

    overallChart.setOption({
      backgroundColor: 'transparent',
      tooltip: { trigger: 'axis' },
      dataZoom: [{ type: 'inside', zoomOnMouseWheel: true, moveOnMouseMove: true }],
      legend: { textStyle: { color: text } },
      xAxis: { type: 'category', data: labels, axisLabel: { color: muted } },
      yAxis: { type: 'value', axisLabel: { color: muted }, splitLine: { lineStyle: { color: border } } },
      series: [
        { name: '{{t "report.html.common.rows"}}', type: 'line', smooth: true, data: rows, itemStyle: { color: primary } },
        { name: '{{t "report.html.common.transactions"}}', type: 'line', smooth: true, data: txns, itemStyle: { color: accent } },
        { name: '{{t "report.html.common.events"}}', type: 'line', smooth: true, data: events, itemStyle: { color: '#818cf8' } }
      ]
    });

    opsChart.setOption({
      backgroundColor: 'transparent',
      tooltip: { trigger: 'axis' },
      dataZoom: [{ type: 'inside', zoomOnMouseWheel: true, moveOnMouseMove: true }],
      legend: { textStyle: { color: text } },
      xAxis: { type: 'category', data: labels, axisLabel: { color: muted } },
      yAxis: { type: 'value', axisLabel: { color: muted }, splitLine: { lineStyle: { color: border } } },
      series: [
        { name: '{{t "report.html.common.inserts"}}', type: 'bar', stack: 'ops', data: inserts, itemStyle: { color: '#34d399' } },
        { name: '{{t "report.html.common.updates"}}', type: 'bar', stack: 'ops', data: updates, itemStyle: { color: '#38bdf8' } },
        { name: '{{t "report.html.common.deletes"}}', type: 'bar', stack: 'ops', data: deletes, itemStyle: { color: '#fb7185' } }
      ]
    });

    tablesChart.setOption({
      backgroundColor: 'transparent',
      tooltip: { trigger: 'axis' },
      dataZoom: [{ type: 'inside', zoomOnMouseWheel: true, moveOnMouseMove: true }],
      legend: { textStyle: { color: text } },
      xAxis: { type: 'category', data: labels, axisLabel: { color: muted } },
      yAxis: { type: 'value', axisLabel: { color: muted }, splitLine: { lineStyle: { color: border } } },
      series: tableSeries
    });

    tpsChart.setOption({
      backgroundColor: 'transparent',
      tooltip: { trigger: 'axis' },
      dataZoom: [{ type: 'inside', zoomOnMouseWheel: true, moveOnMouseMove: true }],
      legend: { textStyle: { color: text } },
      xAxis: { type: 'category', data: window.trendTPSSeries.map(p => p.snapshot_name), axisLabel: { color: muted } },
      yAxis: { type: 'value', axisLabel: { color: muted }, splitLine: { lineStyle: { color: border } } },
      series: [{ name: _lMaxTPS, type: 'line', smooth: true, data: window.trendTPSSeries.map(p => p.value), itemStyle: { color: primary } }]
    });

    ddlChart.setOption({
      backgroundColor: 'transparent',
      tooltip: { trigger: 'axis' },
      dataZoom: [{ type: 'inside', zoomOnMouseWheel: true, moveOnMouseMove: true }],
      legend: { textStyle: { color: text } },
      xAxis: { type: 'category', data: window.trendDDLSeries.map(p => p.snapshot_name), axisLabel: { color: muted } },
      yAxis: { type: 'value', axisLabel: { color: muted }, splitLine: { lineStyle: { color: border } } },
      series: [{ name: _lDDLEvents, type: 'bar', data: window.trendDDLSeries.map(p => p.value), itemStyle: { color: accent, borderRadius: [4, 4, 0, 0] } }]
    });

    txnChart.setOption({
      backgroundColor: 'transparent',
      tooltip: { trigger: 'axis' },
      dataZoom: [{ type: 'inside', zoomOnMouseWheel: true, moveOnMouseMove: true }],
      legend: { textStyle: { color: text } },
      xAxis: { type: 'category', data: window.trendTxnSeries.map(p => p.snapshot_name), axisLabel: { color: muted } },
      yAxis: { type: 'value', axisLabel: { color: muted }, splitLine: { lineStyle: { color: border } } },
      series: [
        { name: _lLargestTxnRows, type: 'line', smooth: true, data: window.trendTxnSeries.map(p => p.size_value), itemStyle: { color: primary } },
        { name: _lLongestTxnSec, type: 'line', smooth: true, data: window.trendTxnSeries.map(p => p.dur_value), itemStyle: { color: accent } }
      ]
    });

    eventMixChart.setOption({
      backgroundColor: 'transparent',
      tooltip: { trigger: 'axis' },
      dataZoom: [{ type: 'inside', zoomOnMouseWheel: true, moveOnMouseMove: true }],
      legend: { textStyle: { color: text } },
      xAxis: { type: 'category', data: window.trendEventMixSeries.map(p => p.snapshot_name), axisLabel: { color: muted } },
      yAxis: { type: 'value', axisLabel: { color: muted }, splitLine: { lineStyle: { color: border } } },
      series: [
        { name: _lInsert, type: 'bar', stack: 'events', data: window.trendEventMixSeries.map(p => p.inserts), itemStyle: { color: '#34d399' } },
        { name: _lUpdate, type: 'bar', stack: 'events', data: window.trendEventMixSeries.map(p => p.updates), itemStyle: { color: '#22d3ee' } },
        { name: _lDelete, type: 'bar', stack: 'events', data: window.trendEventMixSeries.map(p => p.deletes), itemStyle: { color: '#f87171' } },
        { name: _lDDL, type: 'bar', stack: 'events', data: window.trendEventMixSeries.map(p => p.ddl), itemStyle: { color: '#fbbf24' } }
      ]
    });

    hotIntervalChart.setOption({
      backgroundColor: 'transparent',
      tooltip: { trigger: 'axis' },
      dataZoom: [{ type: 'inside', zoomOnMouseWheel: true, moveOnMouseMove: true }],
      legend: { textStyle: { color: text } },
      xAxis: { type: 'category', data: (window.trendHotIntervalSeries.max_hot_rows || []).map(p => p.snapshot_name), axisLabel: { color: muted } },
      yAxis: { type: 'value', axisLabel: { color: muted }, splitLine: { lineStyle: { color: border } } },
      series: [
        { name: _lMaxHotRows, type: 'line', smooth: true, data: (window.trendHotIntervalSeries.max_hot_rows || []).map(p => p.value), itemStyle: { color: '#f87171' } },
        { name: _lHotCount, type: 'bar', data: (window.trendHotIntervalSeries.hot_count_series || []).map(p => p.value), itemStyle: { color: accent, borderRadius: [4, 4, 0, 0] } }
      ]
    });
  }

  if (patternChart && patternSeries.length > 0) {
    const patternViewButtons = Array.from(document.querySelectorAll('[data-pattern-view]'));
    const applyPatternView = (view) => {
      const metricKey = view === 'rows' ? 'rows' : 'share_of_rows';
      const muted = cssVar('--muted') || '#94a3b8';
      const border = cssVar('--border') || '#1d2844';
      const text = cssVar('--text') || '#e5eefc';
      const axisLabel = view === 'rows'
        ? { color: muted }
        : { color: muted, formatter: (value) => (Number(value) * 100).toFixed(0) + '%' };
      const series = patternSeries.map((pattern) => {
        const data = metricKey === 'rows' ? pattern.rows : pattern.share_of_rows;
        return {
          name: pattern.label,
          type: 'line',
          smooth: true,
          symbolSize: 8,
          data
        };
      });

      patternChart.setOption({
        backgroundColor: 'transparent',
        tooltip: {
          trigger: 'axis',
          valueFormatter: metricKey === 'rows'
            ? (value) => String(value) + ' {{t "report.html.common.rows"}}'
            : (value) => (Number(value) * 100).toFixed(1) + '%'
        },
        dataZoom: [{ type: 'inside', zoomOnMouseWheel: true, moveOnMouseMove: true }],
        legend: { type: 'scroll', bottom: 0, textStyle: { color: '#e5eefc' } },
        grid: { left: 50, right: 24, top: 24, bottom: 72 },
        xAxis: { type: 'category', data: labels, axisLabel: { color: muted } },
        yAxis: {
          type: 'value',
          axisLabel,
          splitLine: { lineStyle: { color: border } }
        },
        series
      });

      patternViewButtons.forEach((button) => {
        button.setAttribute('aria-pressed', String(button.dataset.patternView === view));
      });
    };

    patternViewButtons.forEach((button) => {
      button.addEventListener('click', () => applyPatternView(button.dataset.patternView || 'share'));
    });

    applyPatternView('share');
  }

  const trendDrilldownsEl = document.getElementById('trend-pattern-drilldowns');
  if (trendDrilldownsEl && patternDrilldowns && patternDrilldowns.length > 0) {
    patternDrilldowns.forEach(dd => {
      const detail = document.createElement('div');
      detail.className = 'drilldown-details';
      detail.style.cssText = 'margin:12px 0;padding:12px 16px;border:1px solid var(--border);border-radius:8px;background:var(--surface2)';
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
      trendDrilldownsEl.appendChild(detail);
    });
  }

  function setTheme(name) {
    document.documentElement.setAttribute('data-theme', name);
    localStorage.setItem('bvtheme', name);
    document.querySelectorAll('.theme-btn').forEach(function(b) {
      b.classList.toggle('active', b.getAttribute('data-t') === name);
    });
    renderTrendCharts();
    if (patternChart) {
      var activeViewBtn = document.querySelector('[data-pattern-view][aria-pressed="true"]');
      var view = activeViewBtn ? activeViewBtn.dataset.patternView : 'share';
      const metricKey = view === 'rows' ? 'rows' : 'share_of_rows';
      const muted = cssVar('--muted') || '#94a3b8';
      const border = cssVar('--border') || '#1d2844';
      const text = cssVar('--text') || '#e5eefc';
      patternChart.setOption({
        legend: { textStyle: { color: text } },
        xAxis: { axisLabel: { color: muted } },
        yAxis: { splitLine: { lineStyle: { color: border } } }
      });
    }
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
    overallChart.resize();
    opsChart.resize();
    tablesChart.resize();
    if (patternChart) patternChart.resize();
    tpsChart.resize();
    ddlChart.resize();
    txnChart.resize();
    eventMixChart.resize();
    hotIntervalChart.resize();
  });
</script>
</body>
</html>`
