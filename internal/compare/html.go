// Package compare renders self-contained HTML compare reports.
// input: deterministic CompareResult values produced by the compare diff engine.
// output: compare-specific HTML pages with summary cards, charts, and detail tables.
// pos: compare renderer used by the compare command HTML output path.
package compare

import (
	"bytes"
	"encoding/json"
	"fmt"
	"html/template"
	"time"

	"binlogviz/internal/report"
)

type htmlCompareData struct {
	Result              CompareResult
	GeneratedAt         string
	EChartsJS           template.JS
	SummaryPairsJSON    template.JS
	TopTablesJSON       template.JS
	PatternChangesJSON    template.JS
	PatternDrilldownsJSON template.JS
	OpsMixJSON            template.JS
	AlertCountsJSON     template.JS
	KeyFindingsJSON     template.JS
	RecommendationsJSON template.JS
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

	tmpl, err := template.New("compare").Funcs(template.FuncMap{
		"formatDelta":       formatHTMLDelta,
		"formatPercent":     formatHTMLPercent,
		"snapshotWindow":    formatSnapshotWindow,
		"snapshotInputMode": formatSnapshotInputMode,
		"snapshotSource":    formatSnapshotSource,
		"snapshotFilters":   formatSnapshotFilters,
	}).Parse(compareHTMLTemplate)
	if err != nil {
		return "", fmt.Errorf("parse compare html template: %w", err)
	}

	data := htmlCompareData{
		Result:             result,
		GeneratedAt:        time.Now().UTC().Format("2006-01-02 15:04:05 UTC"),
		EChartsJS:          template.JS(echartsJS), //nolint:gosec
		SummaryPairsJSON:   mustHTMLJSON(buildSummaryPairs(result)),
		TopTablesJSON:      mustHTMLJSON(buildTopTableSeries(result.TableChanges)),
		PatternChangesJSON:    mustHTMLJSON(buildPatternSeries(result.PatternChanges)),
		PatternDrilldownsJSON: mustHTMLJSON(result.PatternDrilldowns),
		OpsMixJSON:            mustHTMLJSON(buildOperationSeries(result.OperationMix)),
		AlertCountsJSON:    mustHTMLJSON(buildAlertCounts(result.AlertChanges)),
		KeyFindingsJSON:     mustHTMLJSON(result.KeyFindings),
		RecommendationsJSON: mustHTMLJSON(result.Recommendations),
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
			Name:     "Rows",
			Baseline: result.Summary.BaselineTotalRows,
			Current:  result.Summary.CurrentTotalRows,
			Delta:    result.Summary.TotalRowsDelta,
		},
		{
			Name:     "Transactions",
			Baseline: result.Summary.BaselineTotalTransactions,
			Current:  result.Summary.CurrentTotalTransactions,
			Delta:    result.Summary.TotalTransactionsDelta,
		},
		{
			Name:     "Warnings",
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
		{Name: "Added Alerts", Value: len(changes.Added)},
		{Name: "Removed Alerts", Value: len(changes.Removed)},
	}
}

func formatHTMLDelta(v int) string {
	return fmt.Sprintf("%+d", v)
}

func formatHTMLPercent(v float64) string {
	return fmt.Sprintf("%.1f%%", v)
}

const compareHTMLTemplate = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>BinlogViz Compare</title>
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
  .two-col {
    display: grid;
    grid-template-columns: 1.1fr 0.9fr;
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
        <div class="header-logo">Binlog<span>Viz</span> Compare</div>
        <div>{{.Result.CurrentLabel}} vs {{.Result.BaselineLabel}}</div>
        {{if snapshotWindow .Result.CurrentSnapshot}}<div class="header-context">Current Window: {{snapshotWindow .Result.CurrentSnapshot}}</div>{{end}}
        {{if snapshotWindow .Result.BaselineSnapshot}}<div class="header-context">Baseline Window: {{snapshotWindow .Result.BaselineSnapshot}}</div>{{end}}
        {{if snapshotInputMode .Result.CurrentSnapshot}}<div class="header-context">Current Input Mode: {{snapshotInputMode .Result.CurrentSnapshot}}</div>{{end}}
        {{if snapshotInputMode .Result.BaselineSnapshot}}<div class="header-context">Baseline Input Mode: {{snapshotInputMode .Result.BaselineSnapshot}}</div>{{end}}
        {{if snapshotSource .Result.CurrentSnapshot}}<div class="header-context">Current Source: {{snapshotSource .Result.CurrentSnapshot}}</div>{{end}}
        {{if snapshotSource .Result.BaselineSnapshot}}<div class="header-context">Baseline Source: {{snapshotSource .Result.BaselineSnapshot}}</div>{{end}}
        {{if snapshotFilters .Result.CurrentSnapshot}}<div class="header-context">Current Filters: {{snapshotFilters .Result.CurrentSnapshot}}</div>{{end}}
        {{if snapshotFilters .Result.BaselineSnapshot}}<div class="header-context">Baseline Filters: {{snapshotFilters .Result.BaselineSnapshot}}</div>{{end}}
      </div>
      <div class="header-meta">
        <strong>Generated</strong>
        <span>{{.GeneratedAt}}</span>
      </div>
    </header>

    <section class="cards" id="compare-summary-cards">
      <article class="card">
        <div class="card-label">Rows Delta</div>
        <div class="card-value">{{formatDelta .Result.Summary.TotalRowsDelta}}</div>
        <div class="card-sub">{{.Result.BaselineLabel}} {{.Result.Summary.BaselineTotalRows}} -> {{.Result.CurrentLabel}} {{.Result.Summary.CurrentTotalRows}}</div>
      </article>
      <article class="card accent">
        <div class="card-label">Transactions Delta</div>
        <div class="card-value">{{formatDelta .Result.Summary.TotalTransactionsDelta}}</div>
        <div class="card-sub">{{.Result.BaselineLabel}} {{.Result.Summary.BaselineTotalTransactions}} -> {{.Result.CurrentLabel}} {{.Result.Summary.CurrentTotalTransactions}}</div>
      </article>
      <article class="card success">
        <div class="card-label">Added Alerts</div>
        <div class="card-value">{{len .Result.AlertChanges.Added}}</div>
        <div class="card-sub">new alerts in {{.Result.CurrentLabel}}</div>
      </article>
      <article class="card warn">
        <div class="card-label">Removed Alerts</div>
        <div class="card-value">{{len .Result.AlertChanges.Removed}}</div>
        <div class="card-sub">alerts only in {{.Result.BaselineLabel}}</div>
      </article>
    </section>

    {{if .Result.KeyFindings}}
    <section class="section" id="compare-key-findings">
      <div class="section-header"><span class="dot"></span>Key Findings</div>
      <div class="section-body">
        <div id="compare-findings-list"></div>
      </div>
    </section>
    {{end}}

    {{if .Result.Recommendations}}
    <section class="section" id="compare-recommendations">
      <div class="section-header"><span class="dot"></span>Recommended Next Checks</div>
      <div class="section-body">
        <div id="compare-recommendations-list"></div>
      </div>
    </section>
    {{end}}

    <section class="section">
      <div class="section-header"><span class="dot"></span>Compare Summary</div>
      <div class="section-body">
        <div id="compare-summary-chart" class="chart-box"></div>
      </div>
    </section>

    <section class="section" id="section-table-changes">
      <div class="section-header"><span class="dot"></span>Top Table Changes</div>
      <div class="section-body two-col">
        <div id="compare-top-tables" class="chart-box"></div>
        <div>
          <table>
            <thead>
              <tr>
                <th>Table</th>
                <th class="num">{{.Result.BaselineLabel}}</th>
                <th class="num">{{.Result.CurrentLabel}}</th>
                <th class="num">Delta</th>
                <th class="num">Delta %</th>
              </tr>
            </thead>
            <tbody>
              {{range .Result.TableChanges}}
              <tr>
                <td>{{.Schema}}.{{.Table}}</td>
                <td class="num">{{.BaselineRows}}</td>
                <td class="num">{{.CurrentRows}}</td>
                <td class="num">{{formatDelta .DeltaRows}}</td>
                <td class="num">{{formatPercent .DeltaPercent}}</td>
              </tr>
              {{end}}
            </tbody>
          </table>
        </div>
      </div>
    </section>

    <section class="section" id="section-pattern-changes">
      <div class="section-header"><span class="dot"></span>Pattern Changes</div>
      <div class="section-body two-col">
        <div id="compare-pattern-changes" class="chart-box"></div>
        <div>
          <table>
            <thead>
              <tr>
                <th>Pattern</th>
                <th class="num">{{.Result.BaselineLabel}}</th>
                <th class="num">{{.Result.CurrentLabel}}</th>
                <th class="num">Delta Rows</th>
                <th class="num">Delta %</th>
                <th class="num">Baseline Txns</th>
                <th class="num">Current Txns</th>
                <th class="num">Delta Txns</th>
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
                  <td class="num">{{formatPercent .DeltaPercent}}</td>
                  <td class="num">{{.BaselineTxnCount}}</td>
                  <td class="num">{{.CurrentTxnCount}}</td>
                  <td class="num">{{formatDelta .DeltaTxnCount}}</td>
                </tr>
                {{end}}
              {{else}}
                <tr>
                  <td colspan="8">No pattern changes.</td>
                </tr>
              {{end}}
            </tbody>
          </table>
        </div>
      </div>
      <div id="compare-pattern-drilldowns"></div>
    </section>

    <section class="section" id="section-operation-mix">
      <div class="section-header"><span class="dot"></span>Operation Mix</div>
      <div class="section-body two-col">
        <div id="compare-ops-mix" class="chart-box"></div>
        <div>
          <table>
            <thead>
              <tr>
                <th>Operation</th>
                <th class="num">{{.Result.BaselineLabel}}</th>
                <th class="num">{{.Result.CurrentLabel}}</th>
                <th class="num">Delta</th>
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
      <div class="section-header"><span class="dot"></span>Alert Changes</div>
      <div class="section-body alerts-layout">
        <div id="compare-alerts" class="chart-box"></div>
        <div class="alert-columns">
          <div class="alert-column">
            <h3>Added Alerts ({{len .Result.AlertChanges.Added}})</h3>
            {{if .Result.AlertChanges.Added}}
              {{range .Result.AlertChanges.Added}}
              <div class="alert-item">
                <strong>{{.Type}}</strong>
                <p>{{.Message}}</p>
              </div>
              {{end}}
            {{else}}
              <div class="empty-state">No added alerts.</div>
            {{end}}
          </div>
          <div class="alert-column">
            <h3>Removed Alerts ({{len .Result.AlertChanges.Removed}})</h3>
            {{if .Result.AlertChanges.Removed}}
              {{range .Result.AlertChanges.Removed}}
              <div class="alert-item">
                <strong>{{.Type}}</strong>
                <p>{{.Message}}</p>
              </div>
              {{end}}
            {{else}}
              <div class="empty-state">No removed alerts.</div>
            {{end}}
          </div>
        </div>
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
      legend: { data: ['Baseline', 'Current'], textStyle: { color: '#f1f5f9' } },
      grid: { left: 70, right: 20, top: 36, bottom: 24 },
      xAxis: { type: 'value', axisLabel: { color: '#64748b' }, splitLine: { lineStyle: { color: '#1c1c2e' } } },
      yAxis: { type: 'category', data: window.compareSummaryPairs.map((item) => item.name), axisLabel: { color: '#f1f5f9' } },
      series: [
        { name: 'Baseline', type: 'bar', data: window.compareSummaryPairs.map((item) => item.baseline), itemStyle: { color: '#818cf8' } },
        { name: 'Current', type: 'bar', data: window.compareSummaryPairs.map((item) => item.current), itemStyle: { color: '#22d3ee' } },
      ],
    });

    const topTablesChart = echarts.init(document.getElementById('compare-top-tables'));
    topTablesChart.setOption({
      animation: false,
      backgroundColor: 'transparent',
      tooltip: { show: false },
      legend: { data: ['Baseline', 'Current'], textStyle: { color: '#f1f5f9' } },
      grid: { left: 110, right: 20, top: 36, bottom: 24 },
      xAxis: { type: 'value', axisLabel: { color: '#64748b' }, splitLine: { lineStyle: { color: '#1c1c2e' } } },
      yAxis: { type: 'category', data: window.compareTopTables.map((item) => item.name), axisLabel: { color: '#f1f5f9' } },
      series: [
        { name: 'Baseline', type: 'bar', data: window.compareTopTables.map((item) => item.baseline), itemStyle: { color: '#818cf8' } },
        { name: 'Current', type: 'bar', data: window.compareTopTables.map((item) => item.current), itemStyle: { color: '#22d3ee' } },
      ],
    });

    const patternChangesChart = echarts.init(document.getElementById('compare-pattern-changes'));
    patternChangesChart.setOption({
      animation: false,
      backgroundColor: 'transparent',
      tooltip: { show: false },
      legend: { data: ['Baseline', 'Current'], textStyle: { color: '#f1f5f9' } },
      grid: { left: 110, right: 20, top: 36, bottom: 24 },
      xAxis: { type: 'value', axisLabel: { color: '#64748b' }, splitLine: { lineStyle: { color: '#1c1c2e' } } },
      yAxis: { type: 'category', data: window.comparePatternChanges.map((item) => item.name), axisLabel: { color: '#f1f5f9' } },
      series: [
        { name: 'Baseline', type: 'bar', data: window.comparePatternChanges.map((item) => item.baseline), itemStyle: { color: '#818cf8' } },
        { name: 'Current', type: 'bar', data: window.comparePatternChanges.map((item) => item.current), itemStyle: { color: '#22d3ee' } },
      ],
    });

    const opsMixChart = echarts.init(document.getElementById('compare-ops-mix'));
    opsMixChart.setOption({
      animation: false,
      backgroundColor: 'transparent',
      tooltip: { show: false },
      legend: { data: ['Baseline', 'Current'], textStyle: { color: '#f1f5f9' } },
      grid: { left: 70, right: 20, top: 36, bottom: 24 },
      xAxis: { type: 'category', data: window.compareOpsMix.map((item) => item.name), axisLabel: { color: '#f1f5f9' } },
      yAxis: { type: 'value', axisLabel: { color: '#64748b' }, splitLine: { lineStyle: { color: '#1c1c2e' } } },
      series: [
        { name: 'Baseline', type: 'bar', data: window.compareOpsMix.map((item) => item.baseline), itemStyle: { color: '#818cf8' } },
        { name: 'Current', type: 'bar', data: window.compareOpsMix.map((item) => item.current), itemStyle: { color: '#22d3ee' } },
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
        why.appendChild(document.createTextNode('drilldown: '));
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

    window.addEventListener('resize', function () {
      summaryChart.resize();
      topTablesChart.resize();
      patternChangesChart.resize();
      opsMixChart.resize();
      alertChart.resize();
    });
  </script>
</body>
</html>`
