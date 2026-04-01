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
)

type htmlCompareData struct {
	Result           CompareResult
	GeneratedAt      string
	SummaryPairsJSON template.JS
	TopTablesJSON    template.JS
	OpsMixJSON       template.JS
	AlertCountsJSON  template.JS
}

type htmlMetricDatum struct {
	Label    string `json:"label"`
	Baseline int    `json:"baseline"`
	Current  int    `json:"current"`
	Delta    int    `json:"delta"`
}

type htmlAlertDatum struct {
	Label string `json:"label"`
	Value int    `json:"value"`
	Tone  string `json:"tone"`
}

func RenderHTML(result CompareResult) (string, error) {
	tmpl, err := template.New("compare").Funcs(template.FuncMap{
		"formatDelta":   formatHTMLDelta,
		"formatPercent": formatHTMLPercent,
	}).Parse(compareHTMLTemplate)
	if err != nil {
		return "", fmt.Errorf("parse compare html template: %w", err)
	}

	data := htmlCompareData{
		Result:           result,
		GeneratedAt:      time.Now().UTC().Format("2006-01-02 15:04:05 UTC"),
		SummaryPairsJSON: mustHTMLJSON(buildSummaryPairs(result)),
		TopTablesJSON:    mustHTMLJSON(buildTopTableSeries(result.TableChanges)),
		OpsMixJSON:       mustHTMLJSON(buildOperationSeries(result.OperationMix)),
		AlertCountsJSON:  mustHTMLJSON(buildAlertCounts(result.AlertChanges)),
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
			Label:    "Rows",
			Baseline: result.Summary.BaselineTotalRows,
			Current:  result.Summary.CurrentTotalRows,
			Delta:    result.Summary.TotalRowsDelta,
		},
		{
			Label:    "Transactions",
			Baseline: result.Summary.BaselineTotalTransactions,
			Current:  result.Summary.CurrentTotalTransactions,
			Delta:    result.Summary.TotalTransactionsDelta,
		},
		{
			Label:    "Warnings",
			Baseline: result.Summary.BaselineWarnings,
			Current:  result.Summary.CurrentWarnings,
			Delta:    result.Summary.CurrentWarnings - result.Summary.BaselineWarnings,
		},
		{
			Label:    "Alerts",
			Baseline: len(result.AlertChanges.Removed),
			Current:  len(result.AlertChanges.Added),
			Delta:    len(result.AlertChanges.Added) - len(result.AlertChanges.Removed),
		},
	}
}

func buildTopTableSeries(changes []TableChange) []htmlMetricDatum {
	series := make([]htmlMetricDatum, 0, len(changes))
	for _, change := range changes {
		series = append(series, htmlMetricDatum{
			Label:    change.Schema + "." + change.Table,
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
			Label:    change.Operation,
			Baseline: change.Baseline,
			Current:  change.Current,
			Delta:    change.Delta,
		})
	}
	return series
}

func buildAlertCounts(changes AlertDelta) []htmlAlertDatum {
	return []htmlAlertDatum{
		{Label: "Added", Value: len(changes.Added), Tone: "positive"},
		{Label: "Removed", Value: len(changes.Removed), Tone: "negative"},
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
  .chart-box {
    display: flex;
    flex-direction: column;
    gap: 14px;
    min-height: 120px;
  }
  .metric-row { display: grid; gap: 8px; }
  .metric-head {
    display: flex;
    justify-content: space-between;
    gap: 12px;
    font-size: 12px;
    color: var(--muted);
  }
  .metric-head strong {
    color: var(--text);
    font-size: 13px;
  }
  .bar-group { display: flex; flex-direction: column; gap: 8px; }
  .bar-line { display: grid; grid-template-columns: 72px 1fr 64px; gap: 10px; align-items: center; }
  .bar-label { color: var(--muted); font-size: 12px; }
  .bar-track {
    height: 10px;
    background: var(--surface2);
    border-radius: 999px;
    overflow: hidden;
    border: 1px solid var(--border);
  }
  .bar-fill {
    height: 100%;
    border-radius: inherit;
  }
  .bar-fill.baseline { background: rgba(129, 140, 248, 0.45); }
  .bar-fill.current { background: rgba(34, 211, 238, 0.8); }
  .bar-value {
    font-family: 'Fira Code', monospace;
    font-size: 12px;
    text-align: right;
    color: var(--text);
  }
  .delta-positive { color: var(--success); }
  .delta-negative { color: var(--danger); }
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

    <section class="section">
      <div class="section-header"><span class="dot"></span>Compare Summary</div>
      <div class="section-body">
        <div id="compare-summary-chart" class="chart-box"></div>
      </div>
    </section>

    <section class="section">
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

    <section class="section">
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

  <script>
    window.compareSummaryPairs = {{.SummaryPairsJSON}};
    window.compareTopTables = {{.TopTablesJSON}};
    window.compareOpsMix = {{.OpsMixJSON}};
    window.compareAlertCounts = {{.AlertCountsJSON}};

    function renderCompareBars(containerId, items) {
      const root = document.getElementById(containerId);
      if (!root) return;
      const max = Math.max(1, ...items.flatMap((item) => [item.baseline || 0, item.current || 0, item.value || 0]));
      root.innerHTML = items.map((item) => {
        const baseline = item.baseline || 0;
        const current = item.current || 0;
        const value = item.value || 0;
        if (Object.prototype.hasOwnProperty.call(item, "value")) {
          const width = (value / max) * 100;
          const toneClass = item.tone === "negative" ? "delta-negative" : "delta-positive";
          return '<div class="metric-row">' +
            '<div class="metric-head"><strong>' + item.label + '</strong><span class="' + toneClass + '">' + value + '</span></div>' +
            '<div class="bar-group">' +
              '<div class="bar-line"><span class="bar-label">count</span><div class="bar-track"><div class="bar-fill current" style="width:' + width + '%"></div></div><span class="bar-value">' + value + '</span></div>' +
            '</div>' +
          '</div>';
        }
        const baselineWidth = (baseline / max) * 100;
        const currentWidth = (current / max) * 100;
        const deltaClass = item.delta < 0 ? "delta-negative" : "delta-positive";
        const deltaText = (item.delta >= 0 ? "+" : "") + item.delta;
        return '<div class="metric-row">' +
          '<div class="metric-head"><strong>' + item.label + '</strong><span class="' + deltaClass + '">' + deltaText + '</span></div>' +
          '<div class="bar-group">' +
            '<div class="bar-line"><span class="bar-label">baseline</span><div class="bar-track"><div class="bar-fill baseline" style="width:' + baselineWidth + '%"></div></div><span class="bar-value">' + baseline + '</span></div>' +
            '<div class="bar-line"><span class="bar-label">current</span><div class="bar-track"><div class="bar-fill current" style="width:' + currentWidth + '%"></div></div><span class="bar-value">' + current + '</span></div>' +
          '</div>' +
        '</div>';
      }).join('');
    }

    renderCompareBars('compare-summary-chart', window.compareSummaryPairs);
    renderCompareBars('compare-top-tables', window.compareTopTables);
    renderCompareBars('compare-ops-mix', window.compareOpsMix);
    renderCompareBars('compare-alerts', window.compareAlertCounts);
  </script>
</body>
</html>`
