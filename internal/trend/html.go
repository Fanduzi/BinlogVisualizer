package trend

import (
	"bytes"
	"encoding/json"
	"fmt"
	"html/template"
	"time"

	"binlogviz/internal/report"
)

type htmlData struct {
	Result          Result
	GeneratedAt     string
	EChartsJS       template.JS
	LabelsJSON      template.JS
	RowsJSON        template.JS
	TxnsJSON        template.JS
	EventsJSON      template.JS
	InsertJSON      template.JS
	UpdateJSON      template.JS
	DeleteJSON      template.JS
	TableSeriesJSON template.JS
}

type htmlTableSeries struct {
	Name   string `json:"name"`
	Type   string `json:"type"`
	Smooth bool   `json:"smooth"`
	Data   []int  `json:"data"`
}

func RenderHTML(result Result) (string, error) {
	echartsJS, err := report.ReadEmbeddedECharts()
	if err != nil {
		return "", err
	}
	tmpl, err := template.New("trend").Parse(trendHTMLTemplate)
	if err != nil {
		return "", fmt.Errorf("parse trend html template: %w", err)
	}

	data := htmlData{
		Result:          result,
		GeneratedAt:     time.Now().UTC().Format("2006-01-02 15:04:05 UTC"),
		EChartsJS:       template.JS(echartsJS), //nolint:gosec
		LabelsJSON:      mustHTMLJSON(buildLabels(result.Points)),
		RowsJSON:        mustHTMLJSON(buildMetricSeries(result.Points, func(point Point) int { return point.Summary.TotalRows })),
		TxnsJSON:        mustHTMLJSON(buildMetricSeries(result.Points, func(point Point) int { return point.Summary.TotalTransactions })),
		EventsJSON:      mustHTMLJSON(buildMetricSeries(result.Points, func(point Point) int { return point.Summary.TotalEvents })),
		InsertJSON:      mustHTMLJSON(buildMetricSeries(result.Points, func(point Point) int { return point.Operations.Inserts })),
		UpdateJSON:      mustHTMLJSON(buildMetricSeries(result.Points, func(point Point) int { return point.Operations.Updates })),
		DeleteJSON:      mustHTMLJSON(buildMetricSeries(result.Points, func(point Point) int { return point.Operations.Deletes })),
		TableSeriesJSON: mustHTMLJSON(buildTableSeries(result.TableTrends)),
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

const trendHTMLTemplate = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>BinlogViz Trend</title>
<style>
  :root {
    --bg: #050816;
    --surface: #0d1326;
    --surface2: #111a34;
    --border: #223053;
    --primary: #22d3ee;
    --accent: #f59e0b;
    --text: #e5eefc;
    --muted: #94a3b8;
  }
  * { box-sizing: border-box; }
  body { margin: 0; background: var(--bg); color: var(--text); font-family: 'Fira Sans', system-ui, sans-serif; }
  .page { max-width: 1280px; margin: 0 auto; padding: 24px 20px 56px; }
  .hero, .section { background: var(--surface); border: 1px solid var(--border); border-radius: 12px; }
  .hero { padding: 22px 24px; margin-bottom: 20px; }
  .hero h1 { margin: 0 0 6px; font-size: 28px; }
  .hero p { margin: 0; color: var(--muted); }
  .cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 16px; margin: 20px 0; }
  .card { background: var(--surface2); border: 1px solid var(--border); border-radius: 12px; padding: 16px 18px; }
  .label { color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: 0.8px; }
  .value { font-size: 28px; font-weight: 700; margin-top: 8px; }
  .section { margin-bottom: 18px; overflow: hidden; }
  .section-header { padding: 14px 18px; border-bottom: 1px solid var(--border); font-weight: 700; }
  .section-body { padding: 18px; }
  .chart-box { width: 100%; height: 320px; }
  table { width: 100%; border-collapse: collapse; }
  th, td { padding: 10px 12px; border-bottom: 1px solid var(--border); text-align: left; font-size: 13px; }
  th { color: var(--muted); text-transform: uppercase; font-size: 11px; letter-spacing: 0.6px; }
</style>
</head>
<body>
<div class="page">
  <section class="hero">
    <h1>BinlogViz Trend</h1>
    <p>Generated at {{.GeneratedAt}}</p>
    {{if .Result.BaselineSnapshot}}<p>Baseline: {{.Result.BaselineSnapshot.Label}} ({{.Result.BaselineSnapshot.Name}})</p>{{end}}
  </section>

  <div class="cards">
    <div class="card">
      <div class="label">First Snapshot</div>
      <div class="value">{{.Result.Insights.FirstSnapshot}}</div>
    </div>
    <div class="card">
      <div class="label">Last Snapshot</div>
      <div class="value">{{.Result.Insights.LastSnapshot}}</div>
    </div>
    <div class="card">
      <div class="label">Rows Delta</div>
      <div class="value">{{.Result.Insights.RowsDelta}}</div>
    </div>
    <div class="card">
      <div class="label">Txn Delta</div>
      <div class="value">{{.Result.Insights.TxnsDelta}}</div>
    </div>
  </div>

  <section class="section">
    <div class="section-header">Overall Trend</div>
    <div class="section-body">
      <div id="trend-overall-chart" class="chart-box"></div>
    </div>
  </section>

  <section class="section">
    <div class="section-header">Operation Mix</div>
    <div class="section-body">
      <div id="trend-ops-chart" class="chart-box"></div>
    </div>
  </section>

  <section class="section">
    <div class="section-header">Top Table Movement</div>
    <div class="section-body">
      <div id="trend-tables-chart" class="chart-box"></div>
    </div>
  </section>

  <section class="section">
    <div class="section-header">Ordered Points</div>
    <div class="section-body">
      <table>
        <thead>
          <tr>
            <th>Snapshot</th>
            <th>Start</th>
            <th>Rows</th>
            <th>Txns</th>
            <th>Events</th>
            <th>Alerts</th>
          </tr>
        </thead>
        <tbody>
          {{range .Result.Points}}
          <tr>
            <td>{{.Snapshot.Name}}</td>
            <td>{{.Window.StartTime}}</td>
            <td>{{.Summary.TotalRows}}</td>
            <td>{{.Summary.TotalTransactions}}</td>
            <td>{{.Summary.TotalEvents}}</td>
            <td>{{.AlertCount}}</td>
          </tr>
          {{end}}
        </tbody>
      </table>
    </div>
  </section>
</div>

<script>{{.EChartsJS}}</script>
<script>
  const labels = {{.LabelsJSON}};
  const rows = {{.RowsJSON}};
  const txns = {{.TxnsJSON}};
  const events = {{.EventsJSON}};
  const inserts = {{.InsertJSON}};
  const updates = {{.UpdateJSON}};
  const deletes = {{.DeleteJSON}};
  const tableSeries = {{.TableSeriesJSON}};

  const overallChart = echarts.init(document.getElementById('trend-overall-chart'));
  overallChart.setOption({
    backgroundColor: 'transparent',
    tooltip: { trigger: 'axis' },
    legend: { textStyle: { color: '#e5eefc' } },
    xAxis: { type: 'category', data: labels, axisLabel: { color: '#94a3b8' } },
    yAxis: { type: 'value', axisLabel: { color: '#94a3b8' } },
    series: [
      { name: 'Rows', type: 'line', smooth: true, data: rows },
      { name: 'Transactions', type: 'line', smooth: true, data: txns },
      { name: 'Events', type: 'line', smooth: true, data: events }
    ]
  });

  const opsChart = echarts.init(document.getElementById('trend-ops-chart'));
  opsChart.setOption({
    backgroundColor: 'transparent',
    tooltip: { trigger: 'axis' },
    legend: { textStyle: { color: '#e5eefc' } },
    xAxis: { type: 'category', data: labels, axisLabel: { color: '#94a3b8' } },
    yAxis: { type: 'value', axisLabel: { color: '#94a3b8' } },
    series: [
      { name: 'Insert', type: 'bar', stack: 'ops', data: inserts },
      { name: 'Update', type: 'bar', stack: 'ops', data: updates },
      { name: 'Delete', type: 'bar', stack: 'ops', data: deletes }
    ]
  });

  const tablesChart = echarts.init(document.getElementById('trend-tables-chart'));
  tablesChart.setOption({
    backgroundColor: 'transparent',
    tooltip: { trigger: 'axis' },
    legend: { textStyle: { color: '#e5eefc' } },
    xAxis: { type: 'category', data: labels, axisLabel: { color: '#94a3b8' } },
    yAxis: { type: 'value', axisLabel: { color: '#94a3b8' } },
    series: tableSeries
  });
</script>
</body>
</html>`
