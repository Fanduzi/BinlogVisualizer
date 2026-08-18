// Package report defines the embedded HTML template for analyze reports.
// input: localized labels plus chart-ready analyze report view data.
// output: template source used by the analyze HTML renderer.
// pos: static template layer behind internal/report HTML rendering.
// note: if this file changes, keep internal/report/README.md synchronized.
package report

const htmlReportTemplate = `<!DOCTYPE html>
<html lang="{{lang}}">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{{t "report.html.analyze.title"}}</title>
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
    grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
    gap: 14px;
  }
  .card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 18px 20px;
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
  .card.warn::before { background: var(--warn); }
  .card.danger::before { background: var(--danger); }
  .card-label {
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.8px;
    color: var(--muted);
    margin-bottom: 6px;
  }
  .card-value {
    font-family: 'Fira Code', monospace;
    font-size: 26px;
    font-weight: 700;
    color: var(--text);
    line-height: 1;
  }
  .card-sub {
    font-size: 11px;
    color: var(--muted);
    margin-top: 4px;
  }

  /* Key findings strip inside summary */
  .key-findings {
    margin-top: 14px;
    padding: 10px 16px;
    border-radius: 8px;
    border: 1px solid var(--border);
    background: var(--surface2);
  }
  .key-findings-title {
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.7px;
    color: var(--muted);
    margin-bottom: 6px;
  }
  .key-finding-item {
    display: flex;
    align-items: baseline;
    gap: 8px;
    padding: 3px 0;
    font-size: 12px;
    color: var(--text);
  }
  .key-finding-badge {
    font-family: 'Fira Code', monospace;
    font-size: 9px;
    font-weight: 700;
    padding: 1px 6px;
    border-radius: 3px;
    white-space: nowrap;
    flex-shrink: 0;
  }
  .key-finding-badge.critical { background: rgba(248,81,73,0.15); color: var(--danger); }
  .key-finding-badge.warning  { background: rgba(251,191,36,0.15); color: var(--warn); }
  .key-finding-badge.info     { background: rgba(37,99,235,0.15); color: var(--primary); }

  .verdict {
    display: flex;
    align-items: flex-start;
    gap: 12px;
    padding: 14px 16px;
    border-radius: 8px;
    border: 1px solid var(--border);
    background: var(--surface2);
    margin-bottom: 14px;
  }
  .verdict.critical { border-left: 4px solid var(--danger); }
  .verdict.warning  { border-left: 4px solid var(--warn); }
  .verdict.info     { border-left: 4px solid var(--primary); }
  .verdict-label {
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.7px;
    color: var(--muted);
    margin-bottom: 4px;
  }
  .verdict-text { font-size: 15px; font-weight: 600; color: var(--text); line-height: 1.45; }
  .incident-meta {
    display: flex;
    flex-wrap: wrap;
    gap: 14px 20px;
    padding: 0 2px 14px;
    color: var(--muted);
    font-size: 12px;
  }
  .incident-meta strong { color: var(--text); font-family: 'Fira Code', monospace; font-weight: 600; }
  .incident-grid {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 12px;
  }
  .incident-card {
    padding: 14px 16px;
    border: 1px solid var(--border);
    border-radius: 8px;
    background: var(--surface2);
  }
  .incident-card.peak { border-color: var(--danger); }
  .incident-card-wide { grid-column: 1 / -1; }
  .peak-tables { display: flex; flex-wrap: wrap; gap: 6px; margin-top: 8px; }
  .peak-table-btn {
    font-family: 'Fira Code', monospace;
    font-size: 11px;
    padding: 4px 8px;
    border-radius: 999px;
    border: 1px solid var(--border);
    background: var(--surface);
    color: var(--primary);
    cursor: pointer;
  }
  .peak-table-btn:hover { border-color: var(--primary); }
  tbody tr.table-summary-row.is-filtered { outline: 1px solid var(--accent); background: var(--surface2); }
  .mono { font-family: 'Fira Code', monospace; }
  .footer-row {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 16px;
    margin-top: 32px;
    color: var(--muted);
    font-size: 11px;
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
  .section-desc {
    padding: 12px 20px 0;
    color: var(--muted);
    font-size: 12px;
  }

  /* Charts grid */
  .charts-grid {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 14px;
    padding: 16px;
  }
  .chart-panel {
    padding: 16px;
    border: 1px solid var(--border);
    border-radius: 10px;
    background: var(--surface2);
  }
  .chart-panel-wide { grid-column: 1 / -1; }
  .chart-panel-large { grid-column: 1 / -1; }
  .chart-title {
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.7px;
    color: var(--muted);
    margin-bottom: 10px;
  }
  .chart-box { width: 100%; height: 320px; }
  .chart-large { min-height: 420px; }
  .chart-controls,
  .chart-legend-note { position: static; }

  /* Evidence sub-section styling */
  .evidence-sub {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 10px;
    margin-bottom: 16px;
    overflow: hidden;
  }
  .evidence-sub-header {
    padding: 12px 20px;
    border-bottom: 1px solid var(--border);
    font-size: 12px;
    font-weight: 600;
    color: var(--text);
    display: flex;
    align-items: center;
    gap: 8px;
  }
  .evidence-sub-header .dot {
    width: 6px; height: 6px;
    border-radius: 50%;
    display: inline-block;
  }

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
  tbody tr.table-summary-row.has-activity { cursor: pointer; }
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
  .table-detail-row[hidden] { display: none; }
  .table-detail-panel {
    padding: 16px;
    background: var(--surface2);
    border-top: 1px solid var(--border);
  }
  .table-detail-grid {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 16px;
  }
  .table-detail-panel .chart-box { height: 260px; }

  /* Alerts / Findings */
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
  .diagnostic-list {
    padding: 16px;
    display: flex;
    flex-direction: column;
    gap: 12px;
  }
  .diagnostic-item {
    padding: 14px 16px;
    border: 1px solid var(--border);
    border-radius: 10px;
    background: var(--surface2);
  }
  .diagnostic-head {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
    margin-bottom: 8px;
  }
  .diagnostic-title {
    font-size: 13px;
    font-weight: 700;
    color: var(--text);
  }
  .diagnostic-meta {
    font-family: 'Fira Code', monospace;
    font-size: 11px;
    color: var(--muted);
  }
  .diagnostic-body {
    font-size: 12px;
    color: var(--text);
    display: flex;
    flex-direction: column;
    gap: 6px;
  }
  .diagnostic-tables {
    display: flex;
    flex-wrap: wrap;
    gap: 6px;
  }
  .diagnostic-chip {
    font-family: 'Fira Code', monospace;
    font-size: 11px;
    padding: 3px 7px;
    border-radius: 999px;
    background: rgba(129,140,248,0.15);
    color: var(--primary);
  }

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
    .chart-panel-wide { grid-column: auto; }
    .chart-box { height: 280px; }
    .table-detail-grid { grid-template-columns: 1fr; }
  }
  @media (max-width: 600px) {
    .cards { grid-template-columns: 1fr 1fr; }
    .incident-grid { grid-template-columns: 1fr; }
    .header { flex-direction: column; gap: 12px; text-align: center; }
    .header-meta { text-align: center; }
    .footer-row { flex-direction: column; }
  }
</style>
</head>
<body>
<div class="page">

  <!-- Header -->
  <div class="header">
    <div class="header-logo">Binlog<span>Viz</span></div>
    <div class="header-meta">
      <strong>{{t "report.html.analyze.header"}}</strong>
      {{t "report.html.common.generatedAt"}}: {{.GeneratedAt}}
    </div>
  </div>

  <!-- ═══ Section 1: Incident ═══ -->
  <section class="section" id="executive-summary">
    <div class="section-header"><span class="dot"></span>{{t "report.html.analyze.executiveSummary"}}</div>
    <div class="section-body" style="padding:16px">
      <div class="verdict {{.Verdict.Severity}}" id="incident-verdict">
        <span class="key-finding-badge {{.Verdict.Severity}}">{{.Verdict.Badge}}</span>
        <div>
          <div class="verdict-label">{{t "report.html.analyze.verdict"}}</div>
          <div class="verdict-text">{{.Verdict.Message}}</div>
        </div>
      </div>
      <div class="incident-meta">
        <span>{{t "report.html.common.affectedRows"}}: <strong>{{fmtIntHTML .TotalRows}}</strong></span>
        <span>{{t "report.html.common.transactions"}}: <strong>{{fmtIntHTML .TotalTxns}}</strong></span>
        <span>{{t "report.html.common.events"}}: <strong>{{fmtIntHTML .TotalEvents}}</strong></span>
        {{if .StartTime}}
        <span>{{t "report.html.common.timeRange"}}: <strong>{{.StartTime}}</strong> → {{.EndTime}} ({{.Duration}})</span>
        {{end}}
        {{if .HasDDLEvents}}<span>{{t "report.html.analyze.ddlCount"}}: <strong>{{.DDLCount}}</strong></span>{{end}}
        {{if .HasRollbackHint}}<span>{{t "report.text.firstSuspiciousPosition"}}: <strong class="mono">{{.RollbackHint}}</strong></span>{{end}}
      </div>
      <div class="incident-grid">
        {{if .HasPeakMinute}}
        <div class="incident-card peak incident-card-wide" id="peak-minute">
          <div class="diagnostic-title">{{t "report.html.analyze.peakMinute"}}</div>
          <div class="diagnostic-head" style="margin-top:8px">
            <div class="diagnostic-title">{{.PeakMinute.Timestamp}}</div>
            <div class="diagnostic-meta">{{t "report.html.common.rows"}}={{fmtIntHTML .PeakMinute.Rows}} · {{t "report.html.common.txns"}}={{fmtIntHTML .PeakMinute.Txns}}</div>
          </div>
          {{if .PeakMinute.HasBaseline}}
          <div class="diagnostic-body">
            {{if eq .PeakMinute.BaselineLabel "alert"}}{{t "report.html.analyze.vsSpikeBaseline"}}{{else}}{{t "report.html.analyze.vsWindowMedian"}}{{end}}:
            <span class="mono">{{fmtIntHTML .PeakMinute.BaselineRows}}</span>
            {{if .PeakMinute.BaselineFactor}} · {{t "report.html.analyze.factor"}} {{printf "%.1f" .PeakMinute.BaselineFactor}}×{{end}}
          </div>
          {{end}}
          {{if .PeakMinute.Tables}}
          <div class="key-findings-title" style="margin-top:10px">{{t "report.html.analyze.tablesInPeakMinute"}}</div>
          <div class="peak-tables">
            {{range .PeakMinute.Tables}}
            <button type="button" class="peak-table-btn" data-filter-table="{{.Key}}">{{.Key}} · {{fmtIntHTML .Rows}}</button>
            {{end}}
          </div>
          {{end}}
        </div>
        {{end}}
        {{if .HasHottestTable}}
        <div class="incident-card" id="hottest-table">
          <div class="diagnostic-title">{{t "report.html.analyze.hottestTable"}}</div>
          <div class="diagnostic-head" style="margin-top:8px">
            <button type="button" class="peak-table-btn" data-filter-table="{{.HottestTable.Key}}">{{.HottestTable.Key}}</button>
            <div class="diagnostic-meta">{{t "report.html.common.rows"}}={{fmtIntHTML .HottestTable.Total}} · {{t "report.html.common.txns"}}={{fmtIntHTML .HottestTable.Txns}}</div>
          </div>
          <div class="diagnostic-body">
            <div><span class="op-ins">I {{.HottestTable.InsertPct}}</span> · <span class="op-upd">U {{.HottestTable.UpdatePct}}</span> · <span class="op-del">D {{.HottestTable.DeletePct}}</span></div>
            {{if .HottestTable.QuerySummary}}<div>{{t "report.html.analyze.querySummary"}}: <span class="mono">{{.HottestTable.QuerySummary}}</span></div>{{end}}
          </div>
        </div>
        {{end}}
        {{if .HasIncidentTxn}}
        <div class="incident-card" id="largest-txn">
          <div class="diagnostic-title">{{t "report.html.analyze.largestTransactionsByRows"}}</div>
          <div class="diagnostic-head" style="margin-top:8px">
            <div class="diagnostic-title">{{.IncidentTxn.TxnKey}}</div>
            <div class="diagnostic-meta">{{t "report.html.common.rows"}}={{fmtIntHTML .IncidentTxn.Rows}}</div>
          </div>
          <div class="diagnostic-body">
            {{if .IncidentTxn.Location}}<div>{{t "report.html.analyze.binlogSpan"}}: <span class="mono">{{.IncidentTxn.Location}}</span></div>{{end}}
            {{if .IncidentTxn.LocationNote}}<div class="diagnostic-meta">{{.IncidentTxn.LocationNote}}</div>{{end}}
            {{if .IncidentTxn.HasOps}}<div><span class="op-ins">I {{fmtIntHTML .IncidentTxn.Inserts}}</span> · <span class="op-upd">U {{fmtIntHTML .IncidentTxn.Updates}}</span> · <span class="op-del">D {{fmtIntHTML .IncidentTxn.Deletes}}</span></div>{{end}}
            {{if .IncidentTxn.QuerySummary}}<div>{{t "report.html.analyze.querySummary"}}: <span class="mono">{{.IncidentTxn.QuerySummary}}</span></div>{{end}}
          </div>
        </div>
        {{end}}
      </div>
    </div>
  </section>

  <!-- ═══ Section 2: Risks & Findings ═══ -->
  <section class="section" id="section-findings">
    <div class="section-header"><span class="dot" style="background:var(--danger)"></span>{{t "report.html.analyze.sectionFindings"}}</div>
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
        <span>{{t "report.text.noFindings"}}</span>
      </div>
      {{end}}
    </div>
  </section>

  <!-- ═══ Section 3: Hot Objects ═══ -->
  <section class="section" id="section-objects">
    <div class="section-header"><span class="dot" style="background:var(--accent)"></span>{{t "report.html.analyze.sectionObjects"}}</div>
    <div class="section-body">
      <div class="charts-grid" style="padding-bottom:0">
        <div class="chart-panel chart-panel-wide">
          <div class="chart-title">{{t "report.html.analyze.topTablesByRows"}}</div>
          <div class="chart-box" id="chart-tables"></div>
        </div>
      </div>
      <div class="diagnostic-title" style="padding:16px 16px 0">{{t "report.html.analyze.topTables"}}</div>
      {{if .Tables}}
      <table>
        <thead>
          <tr>
            <th>{{t "report.html.common.schema"}}</th>
            <th>{{t "report.html.common.table"}}</th>
            <th class="num">{{t "report.html.common.totalRows"}}</th>
            <th class="num">{{t "report.html.analyze.insertPct"}}</th>
            <th class="num">{{t "report.html.analyze.updatePct"}}</th>
            <th class="num">{{t "report.html.analyze.deletePct"}}</th>
            <th class="num">{{t "report.html.analyze.ddlPct"}}</th>
            <th class="num">{{t "report.html.common.txns"}}</th>
          </tr>
        </thead>
        <tbody>
          {{range .Tables}}
          <tr class="table-summary-row{{if .HasActivity}} has-activity{{end}}" data-table-row="{{.Key}}" data-table-key="{{.Key}}" {{if .HasActivity}}data-table-dom-id="{{.DOMID}}"{{end}}>
            <td class="name">{{.Schema}}</td>
            <td class="name">{{.Table}}</td>
            <td class="num">{{.Total}}</td>
            <td class="num op-ins">{{.InsertPct}}</td>
            <td class="num op-upd">{{.UpdatePct}}</td>
            <td class="num op-del">{{.DeletePct}}</td>
            <td class="num">{{.DDLPct}}</td>
            <td class="num">{{.Txns}}</td>
          </tr>
          {{if .HasActivity}}
          <tr class="table-detail-row" data-table-key="{{.Key}}" hidden>
            <td colspan="8">
              <div class="table-detail-panel">
                <div class="table-detail-grid">
                  <div class="chart-panel">
                    <div class="chart-title">{{t "report.html.analyze.tableActivityChart"}}</div>
                    <div class="chart-box" id="chart-table-activity-{{.DOMID}}"></div>
                  </div>
                  <div class="chart-panel">
                    <div class="chart-title">{{t "report.html.analyze.tableOpsChart"}}</div>
                    <div class="chart-box" id="chart-table-ops-{{.DOMID}}"></div>
                  </div>
                </div>
              </div>
            </td>
          </tr>
          {{end}}
          {{end}}
        </tbody>
      </table>
      <div class="section-desc" style="padding:8px 0 0">{{t "report.html.analyze.opsNote"}}</div>
      {{else}}
      <div class="no-alerts"><span>{{t "report.html.analyze.noTableData"}}</span></div>
      {{end}}
    </div>
  </section>

  <!-- ═══ Section 5: Diagnostic Evidence ═══ -->
  <section class="section" id="section-evidence">
    <div class="section-header"><span class="dot" style="background:var(--primary)"></span>{{t "report.html.analyze.sectionEvidence"}}</div>
    <div class="section-body" style="padding:16px 16px 0">

    {{if or .HasLargestTxns .HasLongestTxns .HasWidestTxns}}
    <section class="evidence-sub" id="transaction-evidence">
      <div class="evidence-sub-header"><span class="dot" style="background:var(--primary)"></span>{{t "report.html.analyze.transactionEvidence"}}</div>
      <div class="section-body">
        <div class="diagnostic-list">
          {{if .HasLargestTxns}}
          <div class="diagnostic-title">{{t "report.html.analyze.largestTransactionsByRows"}}</div>
          {{range .LargestTransactions}}
          <div class="diagnostic-item">
            <div class="diagnostic-head">
              <div class="diagnostic-title">{{.TxnKey}} <span class="drilldown-flag drilldown-flag-dominance" style="font-size:10px">{{t "report.html.analyze.largestTag"}}</span></div>
              <div class="diagnostic-meta">{{t "report.html.common.rows"}}={{fmtIntHTML .Rows}} · {{t "report.html.common.events"}}={{fmtIntHTML .Events}} · {{t "report.html.common.duration"}}={{.Duration}} · {{len .Tables}} {{t "report.html.analyze.touchedTables"}}</div>
            </div>
            <div class="diagnostic-body">
              {{if .Location}}<div>{{t "report.html.analyze.binlogSpan"}}: {{.Location}}</div>{{end}}
              {{if .LocationNote}}<div class="diagnostic-meta">{{.LocationNote}}</div>{{end}}
              <div>{{t "report.html.analyze.binlogBytes"}}: {{fmtIntHTML .BinlogBytes}}</div>
              {{if .QuerySummary}}<div>{{.QuerySummary}}</div>{{end}}
              {{if .Tables}}
              <div class="diagnostic-tables">
                {{range .Tables}}<span class="diagnostic-chip">{{.Name}} · {{fmtIntHTML .Rows}}</span>{{end}}
              </div>
              {{end}}
            </div>
          </div>
          {{end}}
          {{end}}
          {{if .HasLongestTxns}}
          <div class="diagnostic-title">{{t "report.html.analyze.longestTransactionsByDuration"}}</div>
          {{range .LongestTransactions}}
          <div class="diagnostic-item">
            <div class="diagnostic-head">
              <div class="diagnostic-title">{{.TxnKey}} <span class="drilldown-flag drilldown-flag-anomaly" style="font-size:10px">{{t "report.html.analyze.longestTag"}}</span></div>
              <div class="diagnostic-meta">{{t "report.html.common.duration"}}={{.Duration}} · {{t "report.html.common.rows"}}={{fmtIntHTML .Rows}} · {{t "report.html.common.events"}}={{fmtIntHTML .Events}} · {{len .Tables}} {{t "report.html.analyze.touchedTables"}}</div>
            </div>
            <div class="diagnostic-body">
              {{if .Location}}<div>{{t "report.html.analyze.binlogSpan"}}: {{.Location}}</div>{{end}}
              {{if .LocationNote}}<div class="diagnostic-meta">{{.LocationNote}}</div>{{end}}
              <div>{{t "report.html.analyze.binlogBytes"}}: {{fmtIntHTML .BinlogBytes}}</div>
              {{if .QuerySummary}}<div>{{.QuerySummary}}</div>{{end}}
              {{if .Tables}}
              <div class="diagnostic-tables">
                {{range .Tables}}<span class="diagnostic-chip">{{.Name}} · {{fmtIntHTML .Rows}}</span>{{end}}
              </div>
              {{end}}
            </div>
          </div>
          {{end}}
          {{end}}
          {{if .HasWidestTxns}}
          <div class="diagnostic-title">{{t "report.html.analyze.widestTransactionsByTouchedTables"}}</div>
          {{range .WidestTransactions}}
          <div class="diagnostic-item">
            <div class="diagnostic-head">
              <div class="diagnostic-title">{{.TxnKey}} <span class="drilldown-flag drilldown-flag-dominance" style="font-size:10px">{{t "report.html.analyze.widestTag"}}</span></div>
              <div class="diagnostic-meta">{{len .Tables}} {{t "report.html.analyze.touchedTables"}} · {{t "report.html.common.rows"}}={{fmtIntHTML .Rows}} · {{t "report.html.common.duration"}}={{.Duration}}</div>
            </div>
            <div class="diagnostic-body">
              {{if .Location}}<div>{{t "report.html.analyze.binlogSpan"}}: {{.Location}}</div>{{end}}
              {{if .LocationNote}}<div class="diagnostic-meta">{{.LocationNote}}</div>{{end}}
              <div>{{t "report.html.analyze.binlogBytes"}}: {{fmtIntHTML .BinlogBytes}}</div>
              {{if .QuerySummary}}<div>{{.QuerySummary}}</div>{{end}}
              {{if .Tables}}
              <div class="diagnostic-tables">
                {{range .Tables}}<span class="diagnostic-chip">{{.Name}} · {{fmtIntHTML .Rows}}</span>{{end}}
              </div>
              {{end}}
            </div>
          </div>
          {{end}}
          {{end}}
        </div>
      </div>
    </section>
    {{end}}

    {{if .HasDDLEvents}}
    <section class="evidence-sub" id="ddl-timeline">
      <div class="evidence-sub-header"><span class="dot" style="background:var(--warn)"></span>{{t "report.html.analyze.ddlTimeline"}}</div>
      <div class="section-body">
        <div class="diagnostic-list">
          {{range .DDLEvents}}
          <div class="diagnostic-item">
            <div class="diagnostic-head">
              <div class="diagnostic-title">{{.Operation}} {{.Object}}</div>
              <div class="diagnostic-meta">{{.Timestamp}}</div>
            </div>
            <div class="diagnostic-body">
              <div>{{.Statement}}</div>
              {{if .Location}}<div class="diagnostic-meta">{{.Location}}</div>{{end}}
            </div>
          </div>
          {{end}}
        </div>
      </div>
    </section>
    {{end}}

    {{if .HasDrilldowns}}
    <section class="evidence-sub" id="write-shape-patterns">
      <div class="evidence-sub-header"><span class="dot" style="background:var(--accent)"></span>{{t "report.html.analyze.writeShapePatterns"}}</div>
      <div class="section-body">
        <div class="alert-list">
          {{range .Drilldowns}}
          <details class="drilldown-card">
            <summary class="drilldown-summary">
              <strong>{{.Label}}</strong>
              <span class="drilldown-flags">
                {{if .SignalFlags.Dominance}}<span class="drilldown-flag drilldown-flag-dominance">{{t "report.html.common.dominance"}}</span>{{end}}
                {{if .SignalFlags.Anomaly}}<span class="drilldown-flag drilldown-flag-anomaly">{{t "report.html.common.anomaly"}}</span>{{end}}
              </span>
            </summary>
            <div class="drilldown-body">
              <p class="drilldown-why">{{.WhySelected}}</p>
              <div class="drilldown-metrics">
                <span class="drilldown-metric" title="{{t "report.html.analyze.shareOfRowsTitle"}}" aria-label="{{t "report.html.analyze.shareOfRowsTitle"}}">{{t "report.html.analyze.shareOfRowsLabel"}}: {{printf "%.0f" (mulFloat .ShareOfRows 100)}}%</span>
                <span class="drilldown-metric" title="{{t "report.html.analyze.shareOfTxnsTitle"}}" aria-label="{{t "report.html.analyze.shareOfTxnsTitle"}}">{{t "report.html.analyze.shareOfTxnsLabel"}}: {{printf "%.0f" (mulFloat .ShareOfTxns 100)}}%</span>
                <span class="drilldown-metric" title="{{t "report.html.analyze.avgRowsPerTxnTitle"}}" aria-label="{{t "report.html.analyze.avgRowsPerTxnTitle"}}">{{t "report.html.analyze.avgRowsPerTxn"}}: {{printf "%.0f" .AvgRowsPerTxn}}</span>
              </div>
              {{if .BusiestMinutes}}
              <div class="drilldown-subsection">
                <span class="drilldown-sublabel" title="{{t "report.html.analyze.workloadPeakMinutesTitle"}}">{{t "report.html.analyze.workloadPeakMinutes"}}</span>
                {{range .BusiestMinutes}}
                <div class="drilldown-minute">{{.Minute}} &mdash; {{fmtIntHTML .TotalRows}} {{t "report.html.common.rows"}}, {{fmtIntHTML .TxnCount}} {{t "report.html.common.txns"}}</div>
                {{end}}
              </div>
              {{end}}
              {{if .RepTxns}}
              <div class="drilldown-subsection">
                <span class="drilldown-sublabel" title="{{t "report.html.analyze.workloadTransactionsTitle"}}">{{t "report.html.analyze.workloadTransactions"}}</span>
                {{range .RepTxns}}
                <div class="drilldown-txn">{{.TxnKey}} &mdash; {{fmtIntHTML .TotalRows}} {{t "report.html.common.rows"}}, {{.Duration}}</div>
                {{end}}
              </div>
              {{end}}
            </div>
          </details>
          {{end}}
        </div>
      </div>
    </section>
    {{end}}

    {{if .HasFileCoverage}}
    <section class="evidence-sub" id="analyzed-files">
      <div class="evidence-sub-header"><span class="dot" style="background:var(--success)"></span>{{t "report.html.analyze.analyzedFiles"}}</div>
      <div class="section-body">
        <div class="section-desc">{{t "report.html.analyze.analyzedFilesNote"}}</div>
        <div class="diagnostic-list" id="file-coverage">
          {{if .FileCoverage.Selected}}
          <div class="diagnostic-item">
            <div class="diagnostic-head">
              <div class="diagnostic-title">{{t "report.html.analyze.selectedFiles"}}</div>
              <div class="diagnostic-meta">{{len .FileCoverage.Selected}}</div>
            </div>
            <div class="diagnostic-body">
              <table>
                <thead>
                  <tr>
                    <th>{{t "report.html.analyze.binlogFile"}}</th>
                    <th class="num">{{t "report.html.analyze.fileSize"}}</th>
                    <th>{{t "report.html.analyze.selectionReason"}}</th>
                    <th>{{t "report.html.analyze.timeRange"}}</th>
                  </tr>
                </thead>
                <tbody>
                  {{range .FileCoverage.Selected}}
                  <tr>
                    <td class="name">{{.BinlogPath}}</td>
                    <td class="num">{{.Size}}</td>
                    <td class="name">{{.Reason}}</td>
                    <td class="name">{{if .FirstEventAt}}{{.FirstEventAt}} → {{.LastEventAt}}{{end}}</td>
                  </tr>
                  {{end}}
                </tbody>
              </table>
            </div>
          </div>
          {{end}}
          {{if .FileCoverage.Skipped}}
          <div class="diagnostic-item">
            <div class="diagnostic-head">
              <div class="diagnostic-title">{{t "report.html.analyze.skippedFiles"}}</div>
              <div class="diagnostic-meta">{{len .FileCoverage.Skipped}}</div>
            </div>
            <div class="diagnostic-body">
              <table>
                <thead>
                  <tr>
                    <th>{{t "report.html.analyze.binlogFile"}}</th>
                    <th class="num">{{t "report.html.analyze.fileSize"}}</th>
                    <th>{{t "report.html.analyze.skipReason"}}</th>
                  </tr>
                </thead>
                <tbody>
                  {{range .FileCoverage.Skipped}}
                  <tr>
                    <td class="name">{{.BinlogPath}}</td>
                    <td class="num">{{.Size}}</td>
                    <td class="name">{{.Reason}}</td>
                  </tr>
                  {{end}}
                </tbody>
              </table>
            </div>
          </div>
          {{end}}
          {{if not .FileCoverage.Selected}}{{if not .FileCoverage.Skipped}}
          <div class="no-alerts"><span>{{t "report.html.analyze.noFileCoverage"}}</span></div>
          {{end}}{{end}}
        </div>
      </div>
    </section>
    {{end}}

    {{if .HasFileSegments}}
    <section class="evidence-sub" id="binlog-throughput">
      <div class="evidence-sub-header"><span class="dot" style="background:var(--accent)"></span>{{t "report.html.analyze.binlogThroughput"}}</div>
      <div class="section-body">
        <div class="charts-grid">
          <div class="chart-panel chart-panel-wide">
            <div class="chart-title">{{t "report.html.analyze.throughputChart"}}</div>
            <div class="chart-box" id="chart-throughput" style="height:360px"></div>
          </div>
        </div>
      </div>
    </section>
    {{end}}

    </div>
  </section><!-- /section-evidence -->

  <!-- ═══ Demoted charts ═══ -->
  <section class="section" id="section-activity">
    <div class="section-header"><span class="dot" style="background:var(--accent)"></span>{{t "report.html.analyze.sectionActivity"}}</div>
    <div class="section-body">
      <div class="section-desc">{{t "report.html.analyze.activityCharts"}}</div>
      <div class="charts-grid">
        <div class="chart-panel chart-panel-wide">
          <div class="chart-title">{{t "report.html.analyze.rowsPerMinute"}}</div>
          <div class="chart-box" id="chart-timeline"></div>
        </div>
        <div class="chart-panel">
          <div class="chart-title">{{t "report.html.analyze.avgTPSPerMinute"}}</div>
          <div class="chart-box" id="chart-tps"></div>
        </div>
        <div class="chart-panel">
          <div class="chart-title">{{t "report.html.common.operationMix"}}</div>
          <div class="chart-box" id="chart-ops"></div>
        </div>
        {{if .HasHotIntervals}}
        <div class="chart-panel">
          <div class="chart-title">{{t "report.html.analyze.hotIntervals"}}</div>
          <div class="diagnostic-list" style="padding:0">
            {{range .HotIntervals}}
            <div class="diagnostic-item">
              <div class="diagnostic-head">
                <div class="diagnostic-title">{{.Timestamp}}</div>
                <div class="diagnostic-meta">{{t "report.html.common.rows"}}={{fmtIntHTML .Rows}} · {{t "report.html.common.txns"}}={{fmtIntHTML .Txns}} · {{t "report.html.common.events"}}={{fmtIntHTML .Events}}</div>
              </div>
              <div class="diagnostic-body">
                <div>{{t "report.html.analyze.binlogBytes"}}: {{fmtIntHTML .BinlogBytes}}</div>
                {{if .DDLCount}}<div>{{t "report.html.analyze.ddlEvents"}}: {{fmtIntHTML .DDLCount}}</div>{{end}}
              </div>
            </div>
            {{end}}
          </div>
        </div>
        {{end}}
      </div>
    </div>
  </section>

  <div class="footer footer-row">
    <div>{{t "report.html.common.generatedBy"}} &middot; {{.GeneratedAt}}</div>
    <div class="theme-switcher" title="{{t "report.html.analyze.theme"}}">
      <button class="theme-btn" data-t="nebula" title="Nebula"></button>
      <button class="theme-btn" data-t="forest" title="Forest"></button>
      <button class="theme-btn" data-t="navy"   title="Navy"></button>
      <button class="theme-btn" data-t="ember"  title="Ember"></button>
      <button class="theme-btn" data-t="light"  title="Light"></button>
    </div>
  </div>
</div>

<script>{{.EChartsJS}}</script>
<script>
(function() {
  var tpsLabels   = {{.TPSLabels}};
  var tpsValues   = {{.TPSValues}};
  var minuteLabels = {{.MinuteLabels}};
  var minuteRows   = {{.MinuteRows}};
  var minuteTxns   = {{.MinuteTxns}};
  var tableNames   = {{.TableBarNames}};
  var tableRows    = {{.TableBarRows}};
  var opsPie       = {{.OpsPie}};
  window.tableActivitySeries = {{.TableActivitySeries}};

  var c0, c1, c2, c3;
  var tableCharts = {};

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

    if (c0) c0.dispose();
    if (c1) c1.dispose();
    if (c2) c2.dispose();
    if (c3) c3.dispose();

    var tpsEl = document.getElementById('chart-tps');
    if (!tpsEl) { return; }
    c0 = echarts.init(tpsEl, null, {renderer: 'svg'});
    c0.setOption({
      ...t,
      legend: { top: 0, textStyle: { color: muted, fontSize: 10 } },
      grid: { top: 30, bottom: 30, left: 50, right: 16 },
      xAxis: {
        type: 'category',
        data: tpsLabels,
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
        name: '{{t "report.html.analyze.avgTPSPerMinute"}}',
        type: 'line',
        data: tpsValues,
        smooth: 0.25,
        symbol: 'none',
        lineStyle: { color: accent, width: 2 }
      }]
    });

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
        name: '{{t "report.html.common.rows"}}',
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
      legend: { show: false },
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
        name: '{{t "report.html.common.operations"}}',
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

  function renderTableCharts(tableKey, domID) {
    var data = window.tableActivitySeries[tableKey];
    if (!data) {
      return;
    }
    if (tableCharts[tableKey]) {
      tableCharts[tableKey].activity.resize();
      tableCharts[tableKey].ops.resize();
      return;
    }

    var t = makeTheme();
    var primary = cssVar('--primary');
    var insert  = cssVar('--insert');
    var update  = cssVar('--update');
    var del     = cssVar('--delete');
    var border  = cssVar('--border');
    var muted   = cssVar('--muted');

    var activity = echarts.init(document.getElementById('chart-table-activity-' + domID), null, {renderer: 'svg'});
    activity.setOption({
      ...t,
      legend: { textStyle: { color: muted, fontSize: 10 } },
      grid: { top: 28, bottom: 30, left: 58, right: 16 },
      xAxis: {
        type: 'category',
        data: data.labels,
        axisLine: { lineStyle: { color: border } },
        axisLabel: { color: muted, fontSize: 10 },
      },
      yAxis: {
        type: 'value',
        name: '{{t "report.text.rowsPerMinuteShort"}}',
        nameTextStyle: { color: muted, fontSize: 10 },
        axisLabel: { color: muted, fontSize: 10 },
        splitLine: { lineStyle: { color: border, type: 'dashed' } }
      },
      tooltip: { ...t.tooltip, trigger: 'axis' },
      series: [{
        name: '{{t "report.html.common.rows"}}',
        type: 'line',
        smooth: 0.25,
        symbol: 'circle',
        symbolSize: 6,
        data: data.rows,
        lineStyle: { color: primary, width: 2 }
      }]
    });

    var ops = echarts.init(document.getElementById('chart-table-ops-' + domID), null, {renderer: 'svg'});
    ops.setOption({
      ...t,
      legend: { textStyle: { color: muted, fontSize: 10 } },
      grid: { top: 28, bottom: 30, left: 58, right: 16 },
      xAxis: {
        type: 'category',
        data: data.labels,
        axisLine: { lineStyle: { color: border } },
        axisLabel: { color: muted, fontSize: 10 },
      },
      yAxis: {
        type: 'value',
        name: '{{t "report.text.rowsPerMinuteShort"}}',
        nameTextStyle: { color: muted, fontSize: 10 },
        axisLabel: { color: muted, fontSize: 10 },
        splitLine: { lineStyle: { color: border, type: 'dashed' } }
      },
      tooltip: { ...t.tooltip, trigger: 'axis', axisPointer: { type: 'shadow' } },
      series: [
        { name: '{{t "report.html.common.inserts"}}', type: 'bar', stack: 'ops', data: data.insert_rows, itemStyle: { color: insert } },
        { name: '{{t "report.html.common.updates"}}', type: 'bar', stack: 'ops', data: data.update_rows, itemStyle: { color: update } },
        { name: '{{t "report.html.common.deletes"}}', type: 'bar', stack: 'ops', data: data.delete_rows, itemStyle: { color: del } }
      ]
    });

    tableCharts[tableKey] = { activity: activity, ops: ops };
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

  function openTable(tableKey) {
    var row = document.querySelector('tr.table-summary-row[data-table-key="' + tableKey + '"]');
    if (!row) {
      return;
    }
    document.querySelectorAll('tr.table-summary-row').forEach(function(other) {
      other.classList.toggle('is-filtered', other.getAttribute('data-table-key') === tableKey);
    });
    row.scrollIntoView({block: 'center'});
    var domID = row.getAttribute('data-table-dom-id');
    var detail = document.querySelector('tr.table-detail-row[data-table-key="' + tableKey + '"]');
    if (!detail) {
      return;
    }
    document.querySelectorAll('tr.table-detail-row').forEach(function(other) {
      other.setAttribute('hidden', '');
    });
    detail.removeAttribute('hidden');
    if (domID) {
      renderTableCharts(tableKey, domID);
    }
  }

  document.querySelectorAll('tr.table-summary-row.has-activity').forEach(function(row) {
    row.addEventListener('click', function() {
      var tableKey = row.getAttribute('data-table-key');
      var detail = document.querySelector('tr.table-detail-row[data-table-key="' + tableKey + '"]');
      if (detail && !detail.hasAttribute('hidden') && row.classList.contains('is-filtered')) {
        detail.setAttribute('hidden', '');
        row.classList.remove('is-filtered');
        return;
      }
      openTable(tableKey);
    });
  });

  document.querySelectorAll('[data-filter-table]').forEach(function(el) {
    el.addEventListener('click', function() {
      openTable(el.getAttribute('data-filter-table'));
    });
  });

  // Throughput chart
  var throughputLabels = {{.ThroughputLabels}};
  var throughputBytes = {{.ThroughputBytes}};
  var throughputRows = {{.ThroughputRows}};

  function renderThroughputChart() {
    var el = document.getElementById('chart-throughput');
    if (!el || throughputLabels.length === 0) return;
    var t = makeTheme();
    var primary = cssVar('--primary');
    var accent = cssVar('--accent');
    var border = cssVar('--border');
    var muted = cssVar('--muted');

    var c = echarts.init(el, null, {renderer: 'svg'});
    c.setOption({
      ...t,
      legend: { textStyle: { color: muted, fontSize: 10 } },
      grid: { top: 30, bottom: 30, left: 60, right: 60 },
      xAxis: {
        type: 'category',
        data: throughputLabels,
        axisLine: { lineStyle: { color: border } },
        axisLabel: { color: muted, fontSize: 10 }
      },
      yAxis: [
        {
          type: 'value',
          name: '{{t "report.html.analyze.binlogBytesShort"}}',
          nameTextStyle: { color: muted, fontSize: 10 },
          axisLabel: { color: muted, fontSize: 10 },
          splitLine: { lineStyle: { color: border, type: 'dashed' } }
        },
        {
          type: 'value',
          name: '{{t "report.html.common.rows"}}',
          nameTextStyle: { color: muted, fontSize: 10 },
          axisLabel: { color: muted, fontSize: 10 },
          splitLine: { show: false }
        }
      ],
      tooltip: { ...t.tooltip, trigger: 'axis' },
      series: [
        {
          name: '{{t "report.html.analyze.binlogBytesShort"}}',
          type: 'bar',
          data: throughputBytes,
          itemStyle: { color: primary, borderRadius: [3, 3, 0, 0] },
          barMaxWidth: 40
        },
        {
          name: '{{t "report.html.common.rows"}}',
          type: 'line',
          yAxisIndex: 1,
          data: throughputRows,
          smooth: 0.3,
          symbol: 'circle',
          symbolSize: 6,
          lineStyle: { color: accent, width: 2 }
        }
      ]
    });

    window.addEventListener('resize', function() { c.resize(); });
  }
  renderThroughputChart();

  // Init
          var saved = localStorage.getItem('bvtheme') || 'nebula';
          setTheme(saved);

          window.addEventListener('resize', function() {
            c0 && c0.resize();
            c1 && c1.resize();
            c2 && c2.resize();
            c3 && c3.resize();
    Object.keys(tableCharts).forEach(function(key) {
      tableCharts[key].activity.resize();
      tableCharts[key].ops.resize();
    });
  });
})();
</script>
</body>
</html>
`
