package report

const htmlReportTemplate = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>BinlogViz Report</title>
<style>
  /* ── Nebula (default) ── */
  :root, [data-theme="nebula"] {
    --bg:        #000000;
    --surface:   #0a0a12;
    --surface2:  #0f0f1a;
    --border:    #1c1c2e;
    --primary:   #818cf8;
    --accent:    #22d3ee;
    --text:      #f1f5f9;
    --muted:     #64748b;
    --success:   #34d399;
    --warn:      #fbbf24;
    --danger:    #f87171;
    --insert:    #34d399;
    --update:    #38bdf8;
    --delete:    #fb7185;
    --bar:       #818cf8;
  }
  /* ── Forest ── */
  [data-theme="forest"] {
    --bg:        #060a07;
    --surface:   #0b1009;
    --surface2:  #111a0e;
    --border:    #1a2e19;
    --primary:   #4ade80;
    --accent:    #fbbf24;
    --text:      #f0fdf4;
    --muted:     #6b7f6a;
    --success:   #4ade80;
    --warn:      #fbbf24;
    --danger:    #f87171;
    --insert:    #4ade80;
    --update:    #38bdf8;
    --delete:    #f87171;
    --bar:       #4ade80;
  }
  /* ── Navy ── */
  [data-theme="navy"] {
    --bg:        #03060f;
    --surface:   #080e1e;
    --surface2:  #0e1630;
    --border:    #1a2545;
    --primary:   #60a5fa;
    --accent:    #fcd34d;
    --text:      #e8f0ff;
    --muted:     #5a6a8a;
    --success:   #34d399;
    --warn:      #fcd34d;
    --danger:    #f87171;
    --insert:    #34d399;
    --update:    #60a5fa;
    --delete:    #fb7185;
    --bar:       #60a5fa;
  }
  /* ── Ember ── */
  [data-theme="ember"] {
    --bg:        #0c0a09;
    --surface:   #161210;
    --surface2:  #1e1916;
    --border:    #2e2420;
    --primary:   #fb923c;
    --accent:    #f43f5e;
    --text:      #fef3ee;
    --muted:     #78635a;
    --success:   #4ade80;
    --warn:      #fbbf24;
    --danger:    #f43f5e;
    --insert:    #4ade80;
    --update:    #fb923c;
    --delete:    #f43f5e;
    --bar:       #fb923c;
  }
  /* ── Light ── */
  [data-theme="light"] {
    --bg:        #f8fafc;
    --surface:   #ffffff;
    --surface2:  #f1f5f9;
    --border:    #e2e8f0;
    --primary:   #4f46e5;
    --accent:    #0891b2;
    --text:      #0f172a;
    --muted:     #64748b;
    --success:   #16a34a;
    --warn:      #d97706;
    --danger:    #dc2626;
    --insert:    #16a34a;
    --update:    #4f46e5;
    --delete:    #dc2626;
    --bar:       #4f46e5;
  }

  /* ── Theme switcher ── */
  .theme-switcher {
    display: flex;
    gap: 6px;
    align-items: center;
  }
  .theme-btn {
    width: 22px;
    height: 22px;
    border-radius: 50%;
    border: 2px solid transparent;
    cursor: pointer;
    transition: border-color 150ms, transform 150ms;
    padding: 0;
    outline: none;
  }
  .theme-btn:hover { transform: scale(1.15); }
  .theme-btn.active { border-color: var(--text); }
  .theme-btn[data-t="nebula"] { background: linear-gradient(135deg, #818cf8 50%, #22d3ee 50%); }
  .theme-btn[data-t="forest"] { background: linear-gradient(135deg, #4ade80 50%, #fbbf24 50%); }
  .theme-btn[data-t="navy"]   { background: linear-gradient(135deg, #60a5fa 50%, #fcd34d 50%); }
  .theme-btn[data-t="ember"]  { background: linear-gradient(135deg, #fb923c 50%, #f43f5e 50%); }
  .theme-btn[data-t="light"]  { background: linear-gradient(135deg, #f8fafc 50%, #4f46e5 50%); border-color: #e2e8f0; }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    background: var(--bg);
    color: var(--text);
    font-family: 'Fira Sans', 'Inter', system-ui, sans-serif;
    font-size: 14px;
    line-height: 1.6;
    min-height: 100vh;
  }
  a { color: var(--primary); }

  /* Layout */
  .page { max-width: 1280px; margin: 0 auto; padding: 24px 20px 60px; }

  /* Header */
  .header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 20px 24px;
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 10px;
    margin-bottom: 24px;
  }
  .header-logo {
    font-family: 'Fira Code', monospace;
    font-size: 20px;
    font-weight: 700;
    color: var(--primary);
    letter-spacing: -0.5px;
  }
  .header-logo span { color: var(--accent); }
  .header-meta { text-align: right; color: var(--muted); font-size: 12px; }
  .header-meta strong { color: var(--text); font-size: 13px; display: block; margin-bottom: 2px; }

  /* Summary cards */
  .cards {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
    gap: 16px;
    margin-bottom: 24px;
  }
  .card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 20px 22px;
    position: relative;
    overflow: hidden;
  }
  .card::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 2px;
    background: var(--primary);
  }
  .card.accent::before { background: var(--accent); }
  .card.success::before { background: var(--success); }
  .card.warn::before   { background: var(--warn); }
  .card-label {
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.8px;
    color: var(--muted);
    margin-bottom: 8px;
  }
  .card-value {
    font-family: 'Fira Code', monospace;
    font-size: 28px;
    font-weight: 700;
    color: var(--text);
    line-height: 1;
  }
  .card-sub {
    font-size: 11px;
    color: var(--muted);
    margin-top: 6px;
  }

  /* Section */
  .section {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 10px;
    margin-bottom: 20px;
    overflow: hidden;
  }
  .section-header {
    padding: 14px 20px;
    border-bottom: 1px solid var(--border);
    font-size: 13px;
    font-weight: 600;
    color: var(--text);
    display: flex;
    align-items: center;
    gap: 8px;
  }
  .section-header .dot {
    width: 8px; height: 8px;
    border-radius: 50%;
    background: var(--primary);
    display: inline-block;
  }
  .section-body { padding: 0; }

  /* Charts grid */
  .charts-grid {
    display: grid;
    grid-template-columns: 1fr 1fr 320px;
    gap: 0;
  }
  .chart-panel {
    padding: 16px;
    border-right: 1px solid var(--border);
  }
  .chart-panel:last-child { border-right: none; }
  .chart-title {
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.7px;
    color: var(--muted);
    margin-bottom: 12px;
  }
  .chart-box { width: 100%; height: 220px; }

  /* Table */
  table {
    width: 100%;
    border-collapse: collapse;
    font-size: 13px;
  }
  thead th {
    padding: 10px 16px;
    text-align: left;
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.6px;
    color: var(--muted);
    background: var(--surface2);
    border-bottom: 1px solid var(--border);
  }
  thead th.num { text-align: right; }
  tbody tr {
    border-bottom: 1px solid var(--border);
    transition: background 150ms;
  }
  tbody tr:last-child { border-bottom: none; }
  tbody tr:hover { background: var(--surface2); }
  tbody td {
    padding: 10px 16px;
    color: var(--text);
    font-family: 'Fira Code', monospace;
    font-size: 12px;
  }
  tbody td.name { font-family: inherit; font-size: 13px; }
  tbody td.num { text-align: right; }
  .op-ins { color: var(--insert); }
  .op-upd { color: var(--update); }
  .op-del { color: var(--delete); }

  /* Alerts */
  .alert-list { padding: 12px 16px; display: flex; flex-direction: column; gap: 10px; }
  .alert-item {
    display: flex;
    align-items: flex-start;
    gap: 12px;
    padding: 12px 16px;
    border-radius: 8px;
    border: 1px solid var(--border);
    background: var(--surface2);
  }
  .alert-item.warning { border-left: 3px solid var(--warn); }
  .alert-item.critical { border-left: 3px solid var(--danger); }
  .alert-item.info     { border-left: 3px solid var(--primary); }
  .alert-badge {
    font-family: 'Fira Code', monospace;
    font-size: 10px;
    font-weight: 700;
    padding: 2px 7px;
    border-radius: 4px;
    white-space: nowrap;
    margin-top: 1px;
  }
  .badge-WARN { background: rgba(210,153,34,0.15); color: var(--warn); }
  .badge-CRIT { background: rgba(248,81,73,0.15);  color: var(--danger); }
  .badge-INFO { background: rgba(37,99,235,0.15);  color: var(--primary); }
  .alert-msg  { color: var(--text); font-size: 13px; line-height: 1.5; }

  .no-alerts {
    padding: 24px 20px;
    color: var(--muted);
    font-size: 13px;
    display: flex;
    align-items: center;
    gap: 8px;
  }
  .no-alerts-icon { color: var(--success); font-size: 16px; }

  /* Drilldown */
  .drilldown-card {
    border-radius: 8px;
    border: 1px solid var(--border);
    background: var(--surface2);
    overflow: hidden;
  }
  .drilldown-summary {
    padding: 12px 16px;
    cursor: pointer;
    display: flex;
    align-items: center;
    justify-content: space-between;
    font-size: 13px;
    color: var(--text);
    user-select: none;
  }
  .drilldown-summary:hover { background: var(--surface); }
  .drilldown-flags { display: flex; gap: 6px; }
  .drilldown-flag {
    font-family: 'Fira Code', monospace;
    font-size: 10px;
    font-weight: 600;
    padding: 2px 7px;
    border-radius: 4px;
    text-transform: uppercase;
  }
  .drilldown-flag-dominance { background: rgba(129,140,248,0.15); color: var(--primary); }
  .drilldown-flag-anomaly { background: rgba(251,191,36,0.15); color: var(--warn); }
  .drilldown-body { padding: 0 16px 16px; }
  .drilldown-why { color: var(--muted); font-size: 12px; margin: 0 0 10px; }
  .drilldown-metrics { display: flex; gap: 16px; margin-bottom: 10px; }
  .drilldown-metric {
    font-family: 'Fira Code', monospace;
    font-size: 11px;
    color: var(--text);
    cursor: help;
    border-bottom: 1px dotted var(--muted);
  }
  .drilldown-subsection { margin-bottom: 8px; }
  .drilldown-sublabel {
    font-size: 10px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.6px;
    color: var(--muted);
    display: block;
    margin-bottom: 4px;
  }
  .drilldown-minute, .drilldown-txn {
    font-family: 'Fira Code', monospace;
    font-size: 11px;
    color: var(--text);
    padding: 2px 0;
  }

  /* Footer */
  .footer {
    margin-top: 32px;
    text-align: center;
    color: var(--muted);
    font-size: 11px;
  }

  @media (max-width: 900px) {
    .charts-grid { grid-template-columns: 1fr; }
    .chart-panel { border-right: none; border-bottom: 1px solid var(--border); }
    .chart-panel:last-child { border-bottom: none; }
  }
  @media (max-width: 600px) {
    .cards { grid-template-columns: 1fr 1fr; }
    .header { flex-direction: column; gap: 12px; text-align: center; }
    .header-meta { text-align: center; }
  }
</style>
</head>
<body>
<div class="page">

  <!-- Header -->
  <div class="header">
    <div class="header-logo">Binlog<span>Viz</span></div>
    <div style="display:flex;align-items:center;gap:20px;">
      <div class="header-meta">
        <strong>Analysis Report</strong>
        Generated: {{.GeneratedAt}}
      </div>
      <div class="theme-switcher">
        <button class="theme-btn" data-t="nebula" title="Nebula"></button>
        <button class="theme-btn" data-t="forest" title="Forest"></button>
        <button class="theme-btn" data-t="navy"   title="Navy"></button>
        <button class="theme-btn" data-t="ember"  title="Ember"></button>
        <button class="theme-btn" data-t="light"  title="Light"></button>
      </div>
    </div>
  </div>

  <!-- Summary Cards -->
  <div class="cards">
    <div class="card">
      <div class="card-label">Transactions</div>
      <div class="card-value">{{.TotalTxns}}</div>
    </div>
    <div class="card accent">
      <div class="card-label">Affected Rows</div>
      <div class="card-value">{{.TotalRows}}</div>
    </div>
    <div class="card success">
      <div class="card-label">Events</div>
      <div class="card-value">{{.TotalEvents}}</div>
    </div>
    <div class="card warn">
      <div class="card-label">Time Range</div>
      {{if .StartTime}}
      <div class="card-value" style="font-size:13px;margin-top:4px">{{.StartTime}}</div>
      <div class="card-sub">→ {{.EndTime}}</div>
      <div class="card-sub">Duration: {{.Duration}}</div>
      {{else}}
      <div class="card-value" style="font-size:16px;color:var(--muted)">N/A</div>
      {{end}}
    </div>
  </div>

  <!-- Charts -->
  <div class="section">
    <div class="section-header"><span class="dot"></span>Activity Charts</div>
    <div class="section-body">
      <div class="charts-grid">
        <div class="chart-panel">
          <div class="chart-title">Rows per Minute</div>
          <div class="chart-box" id="chart-timeline"></div>
        </div>
        <div class="chart-panel">
          <div class="chart-title">Top Tables by Rows</div>
          <div class="chart-box" id="chart-tables"></div>
        </div>
        <div class="chart-panel">
          <div class="chart-title">Operation Mix</div>
          <div class="chart-box" id="chart-ops"></div>
        </div>
      </div>
    </div>
  </div>

  <!-- Top Tables -->
  <div class="section">
    <div class="section-header"><span class="dot" style="background:var(--accent)"></span>Top Tables</div>
    <div class="section-body">
      {{if .Tables}}
      <table>
        <thead>
          <tr>
            <th>Schema</th>
            <th>Table</th>
            <th class="num">Total Rows</th>
            <th class="num">Inserts</th>
            <th class="num">Updates</th>
            <th class="num">Deletes</th>
            <th class="num">Txns</th>
          </tr>
        </thead>
        <tbody>
          {{range .Tables}}
          <tr>
            <td class="name">{{.Schema}}</td>
            <td class="name">{{.Table}}</td>
            <td class="num">{{.Total}}</td>
            <td class="num op-ins">{{.Inserts}}</td>
            <td class="num op-upd">{{.Updates}}</td>
            <td class="num op-del">{{.Deletes}}</td>
            <td class="num">{{.Txns}}</td>
          </tr>
          {{end}}
        </tbody>
      </table>
      {{else}}
      <div class="no-alerts"><span>No table data available.</span></div>
      {{end}}
    </div>
  </div>

  <!-- Alerts -->
  <div class="section">
    <div class="section-header"><span class="dot" style="background:var(--danger)"></span>Alerts</div>
    <div class="section-body">
      {{if .HasAlerts}}
      <div class="alert-list">
        {{range .Alerts}}
        <div class="alert-item {{.Severity}}">
          <span class="alert-badge badge-{{.Badge}}">{{.Badge}}</span>
          <span class="alert-msg">{{.Message}}</span>
        </div>
        {{end}}
      </div>
      {{else}}
      <div class="no-alerts">
        <span class="no-alerts-icon">✓</span>
        <span>No alerts detected.</span>
      </div>
      {{end}}
    </div>
  </div>

  <!-- Pattern Drilldowns -->
  {{if .HasDrilldowns}}
  <div class="section">
    <div class="section-header"><span class="dot" style="background:var(--accent)"></span>Pattern Drilldowns</div>
    <div class="section-body">
      <div class="alert-list">
        {{range .Drilldowns}}
        <details class="drilldown-card">
          <summary class="drilldown-summary">
            <strong>{{.Label}}</strong>
            <span class="drilldown-flags">
              {{if .SignalFlags.Dominance}}<span class="drilldown-flag drilldown-flag-dominance">dominance</span>{{end}}
              {{if .SignalFlags.Anomaly}}<span class="drilldown-flag drilldown-flag-anomaly">anomaly</span>{{end}}
            </span>
          </summary>
          <div class="drilldown-body">
            <p class="drilldown-why">{{.WhySelected}}</p>
            <div class="drilldown-metrics">
              <span class="drilldown-metric" title="Fraction of all binlog rows attributed to this pattern">share of rows: {{printf "%.0f" (mulFloat .ShareOfRows 100)}}%</span>
              <span class="drilldown-metric" title="Fraction of all transactions attributed to this pattern">share of txns: {{printf "%.0f" (mulFloat .ShareOfTxns 100)}}%</span>
              <span class="drilldown-metric">avg rows/txn: {{printf "%.0f" .AvgRowsPerTxn}}</span>
            </div>
            {{if .BusiestMinutes}}
            <div class="drilldown-subsection">
              <span class="drilldown-sublabel">Peak Minutes</span>
              {{range .BusiestMinutes}}
              <div class="drilldown-minute">{{.Minute}} &mdash; {{fmtIntHTML .TotalRows}} rows, {{fmtIntHTML .TxnCount}} txns</div>
              {{end}}
            </div>
            {{end}}
            {{if .RepTxns}}
            <div class="drilldown-subsection">
              <span class="drilldown-sublabel">Representative Transactions</span>
              {{range .RepTxns}}
              <div class="drilldown-txn">{{.TxnKey}} &mdash; {{fmtIntHTML .TotalRows}} rows, {{.Duration}}</div>
              {{end}}
            </div>
            {{end}}
          </div>
        </details>
        {{end}}
      </div>
    </div>
  </div>
  {{end}}

  <div class="footer">Generated by BinlogViz &middot; {{.GeneratedAt}}</div>
</div>

<script>{{.EChartsJS}}</script>
<script>
(function() {
  var minuteLabels = {{.MinuteLabels}};
  var minuteRows   = {{.MinuteRows}};
  var minuteTxns   = {{.MinuteTxns}};
  var tableNames   = {{.TableBarNames}};
  var tableRows    = {{.TableBarRows}};
  var opsPie       = {{.OpsPie}};

  var c1, c2, c3;

  function cssVar(name) {
    return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  }

  function makeTheme() {
    return {
      backgroundColor: 'transparent',
      textStyle: { color: cssVar('--muted') },
      title: { textStyle: { color: cssVar('--text') } },
      legend: { textStyle: { color: cssVar('--muted') } },
      tooltip: {
        backgroundColor: cssVar('--surface2'),
        borderColor: cssVar('--border'),
        textStyle: { color: cssVar('--text') }
      },
      axisPointer: { lineStyle: { color: cssVar('--border') } }
    };
  }

  function renderCharts() {
    var t = makeTheme();
    var primary = cssVar('--primary');
    var accent  = cssVar('--accent');
    var insert  = cssVar('--insert');
    var update  = cssVar('--update');
    var del     = cssVar('--delete');
    var border  = cssVar('--border');
    var muted   = cssVar('--muted');
    var text    = cssVar('--text');
    var bg      = cssVar('--bg');

    if (c1) c1.dispose();
    if (c2) c2.dispose();
    if (c3) c3.dispose();

    c1 = echarts.init(document.getElementById('chart-timeline'), null, {renderer: 'svg'});
    c1.setOption({
      ...t,
      grid: { top: 10, bottom: 30, left: 50, right: 16 },
      xAxis: {
        type: 'category',
        data: minuteLabels,
        axisLine: { lineStyle: { color: border } },
        axisLabel: { color: muted, fontSize: 10 },
        splitLine: { show: false }
      },
      yAxis: {
        type: 'value',
        axisLine: { show: false },
        axisLabel: { color: muted, fontSize: 10 },
        splitLine: { lineStyle: { color: border, type: 'dashed' } }
      },
      tooltip: { ...t.tooltip, trigger: 'axis' },
      series: [{
        name: 'Rows',
        type: 'line',
        data: minuteRows,
        smooth: 0.3,
        symbol: 'none',
        lineStyle: { color: primary, width: 2 },
        areaStyle: { color: { type: 'linear', x: 0, y: 0, x2: 0, y2: 1,
          colorStops: [{offset: 0, color: primary + '4d'}, {offset: 1, color: primary + '00'}] } }
      }]
    });

    c2 = echarts.init(document.getElementById('chart-tables'), null, {renderer: 'svg'});
    c2.setOption({
      ...t,
      grid: { top: 10, bottom: 30, left: 16, right: 16, containLabel: true },
      xAxis: {
        type: 'value',
        axisLabel: { color: muted, fontSize: 10 },
        axisLine: { show: false },
        splitLine: { lineStyle: { color: border, type: 'dashed' } }
      },
      yAxis: {
        type: 'category',
        data: tableNames,
        inverse: true,
        axisLine: { show: false },
        axisLabel: { color: muted, fontSize: 10, width: 120, overflow: 'truncate' },
        splitLine: { show: false }
      },
      tooltip: { ...t.tooltip, trigger: 'axis', axisPointer: { type: 'shadow' } },
      series: [{
        name: 'Rows',
        type: 'bar',
        data: tableRows,
        barMaxWidth: 20,
        itemStyle: { color: accent, borderRadius: [0, 4, 4, 0] }
      }]
    });

    c3 = echarts.init(document.getElementById('chart-ops'), null, {renderer: 'svg'});
    c3.setOption({
      ...t,
      tooltip: { ...t.tooltip, trigger: 'item', formatter: '{b}: {c} ({d}%)' },
      legend: { bottom: 0, textStyle: { color: muted, fontSize: 10 } },
      series: [{
        name: 'Operations',
        type: 'pie',
        radius: ['40%', '70%'],
        center: ['50%', '44%'],
        data: opsPie,
        label: { show: false },
        emphasis: { label: { show: true, fontSize: 12, color: text } },
        itemStyle: { borderColor: bg, borderWidth: 2 },
        color: [insert, update, del]
      }]
    });
  }

  // Theme switching
  function setTheme(name) {
    document.documentElement.setAttribute('data-theme', name);
    localStorage.setItem('bvtheme', name);
    document.querySelectorAll('.theme-btn').forEach(function(b) {
      b.classList.toggle('active', b.getAttribute('data-t') === name);
    });
    renderCharts();
  }

  document.querySelectorAll('.theme-btn').forEach(function(btn) {
    btn.addEventListener('click', function() { setTheme(btn.getAttribute('data-t')); });
  });

  // Init
  var saved = localStorage.getItem('bvtheme') || 'nebula';
  setTheme(saved);

  window.addEventListener('resize', function() {
    c1 && c1.resize();
    c2 && c2.resize();
    c3 && c3.resize();
  });
})();
</script>
</body>
</html>
`
