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
  /* ── Nebula (default dark OLED) ── */
  :root, [data-theme="nebula"] {
    --bg:          #07090e;
    --bg-mesh:     radial-gradient(ellipse 80% 50% at 50% -20%, rgba(129, 140, 248, 0.15), transparent 70%), radial-gradient(circle 40% at 90% 20%, rgba(34, 211, 238, 0.08), transparent 60%);
    --surface:     #0e121e;
    --surface2:    #141a2b;
    --surface-hover: #1b233a;
    --border:      rgba(255, 255, 255, 0.08);
    --border-subtle: rgba(255, 255, 255, 0.04);
    --border-focus: rgba(129, 140, 248, 0.4);
    --primary:     #818cf8;
    --primary-rgb: 129, 140, 248;
    --accent:      #22d3ee;
    --accent-rgb:  34, 211, 238;
    --text:        #f1f5f9;
    --text-heading:#ffffff;
    --muted:       #8899b0;
    --success:     #34d399;
    --warn:        #fbbf24;
    --danger:      #f87171;
    --insert:      #34d399;
    --update:      #38bdf8;
    --delete:      #fb7185;
    --bar:         #818cf8;
    --card-shadow: 0 10px 30px -10px rgba(0, 0, 0, 0.5), 0 0 1px 1px rgba(255, 255, 255, 0.05);
    --header-bg:   rgba(14, 18, 30, 0.85);
  }
  /* ── Forest (Emerald Dark) ── */
  [data-theme="forest"] {
    --bg:          #050c07;
    --bg-mesh:     radial-gradient(ellipse 80% 50% at 50% -20%, rgba(74, 222, 128, 0.15), transparent 70%), radial-gradient(circle 40% at 90% 20%, rgba(251, 191, 36, 0.08), transparent 60%);
    --surface:     #0a160e;
    --surface2:    #102016;
    --surface-hover: #162c1e;
    --border:      rgba(74, 222, 128, 0.12);
    --border-subtle: rgba(74, 222, 128, 0.05);
    --border-focus: rgba(74, 222, 128, 0.4);
    --primary:     #4ade80;
    --primary-rgb: 74, 222, 128;
    --accent:      #fbbf24;
    --accent-rgb:  251, 191, 36;
    --text:        #f0fdf4;
    --text-heading:#ffffff;
    --muted:       #7d9884;
    --success:     #4ade80;
    --warn:        #fbbf24;
    --danger:      #f87171;
    --insert:      #4ade80;
    --update:      #38bdf8;
    --delete:      #f87171;
    --bar:         #4ade80;
    --card-shadow: 0 10px 30px -10px rgba(0, 0, 0, 0.5), 0 0 1px 1px rgba(74, 222, 128, 0.08);
    --header-bg:   rgba(10, 22, 14, 0.85);
  }
  /* ── Navy (Midnight Sapphire) ── */
  [data-theme="navy"] {
    --bg:          #040814;
    --bg-mesh:     radial-gradient(ellipse 80% 50% at 50% -20%, rgba(96, 165, 250, 0.15), transparent 70%), radial-gradient(circle 40% at 90% 20%, rgba(252, 211, 77, 0.08), transparent 60%);
    --surface:     #081024;
    --surface2:    #0e1a38;
    --surface-hover: #14244d;
    --border:      rgba(96, 165, 250, 0.12);
    --border-subtle: rgba(96, 165, 250, 0.05);
    --border-focus: rgba(96, 165, 250, 0.4);
    --primary:     #60a5fa;
    --primary-rgb: 96, 165, 250;
    --accent:      #fcd34d;
    --accent-rgb:  252, 211, 77;
    --text:        #e8f0ff;
    --text-heading:#ffffff;
    --muted:       #7286a8;
    --success:     #34d399;
    --warn:        #fcd34d;
    --danger:      #f87171;
    --insert:      #34d399;
    --update:      #60a5fa;
    --delete:      #fb7185;
    --bar:         #60a5fa;
    --card-shadow: 0 10px 30px -10px rgba(0, 0, 0, 0.5), 0 0 1px 1px rgba(96, 165, 250, 0.08);
    --header-bg:   rgba(8, 16, 36, 0.85);
  }
  /* ── Ember (Sunset Volcanic) ── */
  [data-theme="ember"] {
    --bg:          #0a0604;
    --bg-mesh:     radial-gradient(ellipse 80% 50% at 50% -20%, rgba(251, 146, 60, 0.15), transparent 70%), radial-gradient(circle 40% at 90% 20%, rgba(244, 63, 94, 0.08), transparent 60%);
    --surface:     #150d09;
    --surface2:    #20140f;
    --surface-hover: #2d1d16;
    --border:      rgba(251, 146, 60, 0.12);
    --border-subtle: rgba(251, 146, 60, 0.05);
    --border-focus: rgba(251, 146, 60, 0.4);
    --primary:     #fb923c;
    --primary-rgb: 251, 146, 60;
    --accent:      #f43f5e;
    --accent-rgb:  244, 63, 94;
    --text:        #fef3ee;
    --text-heading:#ffffff;
    --muted:       #9a7f72;
    --success:     #4ade80;
    --warn:        #fbbf24;
    --danger:      #f43f5e;
    --insert:      #4ade80;
    --update:      #fb923c;
    --delete:      #f43f5e;
    --bar:         #fb923c;
    --card-shadow: 0 10px 30px -10px rgba(0, 0, 0, 0.5), 0 0 1px 1px rgba(251, 146, 60, 0.08);
    --header-bg:   rgba(21, 13, 9, 0.85);
  }
  /* ── Light (Pristine Pro Slate) ── */
  [data-theme="light"] {
    --bg:          #f8fafc;
    --bg-mesh:     radial-gradient(ellipse 80% 50% at 50% -20%, rgba(79, 70, 229, 0.08), transparent 70%), radial-gradient(circle 40% at 90% 20%, rgba(8, 145, 178, 0.05), transparent 60%);
    --surface:     #ffffff;
    --surface2:    #f1f5f9;
    --surface-hover: #e2e8f0;
    --border:      #e2e8f0;
    --border-subtle: #f1f5f9;
    --border-focus: rgba(79, 70, 229, 0.35);
    --primary:     #4f46e5;
    --primary-rgb: 79, 70, 229;
    --accent:      #0891b2;
    --accent-rgb:  8, 145, 178;
    --text:        #1e293b;
    --text-heading:#0f172a;
    --muted:       #64748b;
    --success:     #16a34a;
    --warn:        #d97706;
    --danger:      #dc2626;
    --insert:      #16a34a;
    --update:      #4f46e5;
    --delete:      #dc2626;
    --bar:         #4f46e5;
    --card-shadow: 0 4px 20px -4px rgba(0, 0, 0, 0.06), 0 1px 2px rgba(0, 0, 0, 0.04);
    --header-bg:   rgba(255, 255, 255, 0.9);
  }

  * { box-sizing: border-box; margin: 0; padding: 0; }
  html { scroll-behavior: smooth; }
  body {
    background: var(--bg);
    background-image: var(--bg-mesh);
    background-attachment: fixed;
    color: var(--text);
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
    font-size: 13.5px;
    line-height: 1.6;
    min-height: 100vh;
    -webkit-font-smoothing: antialiased;
    -moz-osx-font-smoothing: grayscale;
  }
  a { color: var(--primary); text-decoration: none; transition: color 0.15s; }
  a:hover { color: var(--accent); }

  /* ── Layout Container ── */
  .page { max-width: 1320px; margin: 0 auto; padding: 16px 20px 60px; }

  /* ── Sticky Top Bar & Header ── */
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
    transition: background 0.3s, border-color 0.3s;
  }
  .header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 14px 20px;
    gap: 16px;
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
    letter-spacing: -0.5px;
    color: var(--text-heading);
    display: flex;
    align-items: center;
    gap: 6px;
  }
  .header-logo .logo-badge {
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
  .nav-pills {
    display: flex;
    align-items: center;
    gap: 4px;
    background: var(--surface2);
    padding: 3px;
    border-radius: 999px;
    border: 1px solid var(--border);
  }
  .nav-pill {
    padding: 4px 10px;
    border-radius: 999px;
    font-size: 11px;
    font-weight: 600;
    color: var(--muted);
    transition: all 0.15s;
    white-space: nowrap;
  }
  .nav-pill:hover {
    color: var(--text-heading);
    background: var(--surface);
  }
  .header-actions {
    display: flex;
    align-items: center;
    gap: 14px;
  }
  .header-meta {
    text-align: right;
    color: var(--muted);
    font-size: 11.5px;
    line-height: 1.3;
  }
  .header-meta strong {
    color: var(--text-heading);
    font-size: 12.5px;
    display: block;
    font-weight: 600;
  }

  /* ── Action Buttons ── */
  .action-btn {
    display: inline-flex;
    align-items: center;
    gap: 5px;
    font-size: 11.5px;
    font-weight: 600;
    padding: 5px 10px;
    border-radius: 6px;
    border: 1px solid var(--border);
    background: var(--surface2);
    color: var(--text);
    cursor: pointer;
    transition: all 0.15s;
    outline: none;
  }
  .action-btn:hover {
    background: var(--surface-hover);
    border-color: var(--primary);
    color: var(--primary);
  }

  /* ── Theme Switcher ── */
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
    transition: transform 0.15s, border-color 0.15s;
    padding: 0;
    outline: none;
    position: relative;
  }
  .theme-btn:hover { transform: scale(1.2); }
  .theme-btn.active { border-color: var(--text-heading); box-shadow: 0 0 8px rgba(var(--primary-rgb), 0.6); }
  .theme-btn[data-t="nebula"] { background: linear-gradient(135deg, #818cf8 50%, #22d3ee 50%); }
  .theme-btn[data-t="forest"] { background: linear-gradient(135deg, #4ade80 50%, #fbbf24 50%); }
  .theme-btn[data-t="navy"]   { background: linear-gradient(135deg, #60a5fa 50%, #fcd34d 50%); }
  .theme-btn[data-t="ember"]  { background: linear-gradient(135deg, #fb923c 50%, #f43f5e 50%); }
  .theme-btn[data-t="light"]  { background: linear-gradient(135deg, #f8fafc 50%, #4f46e5 50%); border-color: #cbd5e1; }

  /* ── Cards Grid (Bento Style) ── */
  .cards {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(210px, 1fr));
    gap: 12px;
  }
  .card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 12px;
    padding: 16px 18px;
    position: relative;
    overflow: hidden;
    box-shadow: var(--card-shadow);
    transition: transform 0.2s cubic-bezier(0.16, 1, 0.3, 1), border-color 0.2s;
  }
  .card:hover {
    transform: translateY(-2px);
    border-color: rgba(var(--primary-rgb), 0.35);
  }
  .card::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 3px;
    background: linear-gradient(90deg, var(--primary), var(--accent));
  }
  .card.accent::before  { background: linear-gradient(90deg, var(--accent), #38bdf8); }
  .card.success::before { background: linear-gradient(90deg, var(--success), #a7f3d0); }
  .card.warn::before    { background: linear-gradient(90deg, var(--warn), #fde68a); }
  .card.danger::before  { background: linear-gradient(90deg, var(--danger), #fecdd3); }

  .card-head {
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 8px;
  }
  .card-label {
    font-size: 11px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.8px;
    color: var(--muted);
  }
  .card-icon {
    width: 24px;
    height: 24px;
    border-radius: 6px;
    display: flex;
    align-items: center;
    justify-content: center;
    background: var(--surface2);
    color: var(--primary);
    font-size: 12px;
  }
  .card.accent .card-icon { color: var(--accent); }
  .card.success .card-icon { color: var(--success); }
  .card.warn .card-icon { color: var(--warn); }
  .card.danger .card-icon { color: var(--danger); }

  .card-value {
    font-family: "JetBrains Mono", "Fira Code", monospace;
    font-size: 26px;
    font-weight: 800;
    color: var(--text-heading);
    line-height: 1.1;
    font-variant-numeric: tabular-nums;
  }
  .card-sub {
    font-size: 11px;
    color: var(--muted);
    margin-top: 6px;
    display: flex;
    align-items: center;
    gap: 4px;
  }

  /* ── Key Findings Strip ── */
  .key-findings {
    margin-top: 14px;
    padding: 12px 16px;
    border-radius: 10px;
    border: 1px solid var(--border);
    background: var(--surface2);
    border-left: 3px solid var(--warn);
  }
  .key-findings-title {
    font-size: 11px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.8px;
    color: var(--warn);
    margin-bottom: 8px;
    display: flex;
    align-items: center;
    gap: 6px;
  }
  .key-finding-item {
    display: flex;
    align-items: flex-start;
    gap: 10px;
    padding: 4px 0;
    font-size: 12.5px;
    color: var(--text);
  }
  .key-finding-badge {
    font-family: "JetBrains Mono", "Fira Code", monospace;
    font-size: 9.5px;
    font-weight: 700;
    padding: 2px 7px;
    border-radius: 4px;
    white-space: nowrap;
    flex-shrink: 0;
    text-transform: uppercase;
    letter-spacing: 0.4px;
  }
  .key-finding-badge.critical { background: rgba(248, 113, 113, 0.18); color: var(--danger); border: 1px solid rgba(248, 113, 113, 0.3); }
  .key-finding-badge.warning  { background: rgba(251, 191, 36, 0.18);  color: var(--warn);   border: 1px solid rgba(251, 191, 36, 0.3); }
  .key-finding-badge.info     { background: rgba(129, 140, 248, 0.18); color: var(--primary);border: 1px solid rgba(129, 140, 248, 0.3); }

  /* ── Section Shell ── */
  .section {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 14px;
    margin-bottom: 22px;
    overflow: hidden;
    box-shadow: var(--card-shadow);
    transition: border-color 0.2s;
  }
  .section:hover {
    border-color: rgba(var(--primary-rgb), 0.2);
  }
  .section-header {
    padding: 14px 20px;
    border-bottom: 1px solid var(--border);
    font-size: 13.5px;
    font-weight: 700;
    color: var(--text-heading);
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 10px;
    background: var(--surface);
  }
  .section-header-title {
    display: flex;
    align-items: center;
    gap: 10px;
  }
  .section-header .dot {
    width: 9px;
    height: 9px;
    border-radius: 50%;
    background: var(--primary);
    display: inline-block;
    box-shadow: 0 0 8px rgba(var(--primary-rgb), 0.6);
  }
  .section-body { padding: 0; }
  .section-desc {
    padding: 12px 20px 0;
    color: var(--muted);
    font-size: 12px;
  }

  /* ── Charts Grid ── */
  .charts-grid {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 14px;
    padding: 16px;
  }
  .chart-panel {
    padding: 16px;
    border: 1px solid var(--border);
    border-radius: 12px;
    background: var(--surface2);
    position: relative;
    transition: border-color 0.2s;
  }
  .chart-panel:hover {
    border-color: rgba(var(--primary-rgb), 0.25);
  }
  .chart-panel-wide { grid-column: 1 / -1; }
  .chart-panel-large { grid-column: 1 / -1; }
  .chart-title {
    font-size: 11.5px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.8px;
    color: var(--muted);
    margin-bottom: 12px;
    display: flex;
    align-items: center;
    justify-content: space-between;
  }
  .chart-box { width: 100%; height: 320px; }
  .chart-large { min-height: 420px; }
  .chart-controls,
  .chart-legend-note { position: static; }

  /* ── Evidence Sub-section Styling ── */
  .evidence-sub {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 12px;
    margin-bottom: 16px;
    overflow: hidden;
  }
  .evidence-sub-header {
    padding: 12px 20px;
    border-bottom: 1px solid var(--border);
    font-size: 12.5px;
    font-weight: 700;
    color: var(--text-heading);
    display: flex;
    align-items: center;
    gap: 8px;
    background: var(--surface);
  }
  .evidence-sub-header .dot {
    width: 7px;
    height: 7px;
    border-radius: 50%;
    display: inline-block;
  }

  /* ── Interactive Tables ── */
  .table-toolbar {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 12px 16px;
    border-bottom: 1px solid var(--border);
    background: var(--surface2);
    gap: 12px;
    flex-wrap: wrap;
  }
  .table-search-box {
    display: flex;
    align-items: center;
    gap: 8px;
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 6px 12px;
    min-width: 240px;
  }
  .table-search-box input {
    background: transparent;
    border: none;
    color: var(--text);
    font-size: 12px;
    outline: none;
    width: 100%;
    font-family: inherit;
  }
  .table-search-box input::placeholder { color: var(--muted); }
  .table-meta-note {
    font-size: 11.5px;
    color: var(--muted);
  }

  .table-container {
    overflow-x: auto;
    width: 100%;
  }
  table {
    width: 100%;
    border-collapse: collapse;
    font-size: 13px;
    text-align: left;
  }
  thead th {
    padding: 11px 16px;
    font-size: 11px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.7px;
    color: var(--muted);
    background: var(--surface2);
    border-bottom: 1px solid var(--border);
    user-select: none;
    white-space: nowrap;
  }
  thead th.sortable { cursor: pointer; transition: color 0.15s; }
  thead th.sortable:hover { color: var(--primary); }
  thead th.num { text-align: right; }
  tbody tr {
    border-bottom: 1px solid var(--border-subtle);
    transition: background 0.15s;
  }
  tbody tr:last-child { border-bottom: none; }
  tbody tr:hover { background: var(--surface-hover); }
  tbody tr.table-summary-row.has-activity { cursor: pointer; }
  tbody td {
    padding: 10px 16px;
    color: var(--text);
    font-family: "JetBrains Mono", "Fira Code", monospace;
    font-size: 12.5px;
    font-variant-numeric: tabular-nums;
  }
  tbody td.name {
    font-family: inherit;
    font-size: 13px;
    font-weight: 500;
  }
  tbody td.num { text-align: right; }
  .op-ins { color: var(--insert); font-weight: 600; }
  .op-upd { color: var(--update); font-weight: 600; }
  .op-del { color: var(--delete); font-weight: 600; }
  .table-chevron {
    display: inline-block;
    margin-right: 6px;
    font-size: 9px;
    color: var(--muted);
    transition: transform 0.2s;
  }
  tr.open .table-chevron { transform: rotate(90deg); color: var(--primary); }

  /* ── Table Detail Expandable Panel ── */
  .table-detail-row[hidden] { display: none; }
  .table-detail-panel {
    padding: 18px;
    background: var(--surface2);
    border-top: 1px solid var(--border);
    border-bottom: 1px solid var(--border);
  }
  .table-detail-grid {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 16px;
  }
  .table-detail-panel .chart-box { height: 260px; }

  /* ── Alerts & Findings ── */
  .alert-list { padding: 14px 16px; display: flex; flex-direction: column; gap: 10px; }
  .alert-item {
    display: flex;
    align-items: flex-start;
    gap: 12px;
    padding: 12px 16px;
    border-radius: 10px;
    border: 1px solid var(--border);
    background: var(--surface2);
    transition: transform 0.15s, border-color 0.15s;
  }
  .alert-item:hover { transform: translateX(2px); }
  .alert-item.warning  { border-left: 4px solid var(--warn); }
  .alert-item.critical { border-left: 4px solid var(--danger); }
  .alert-item.info     { border-left: 4px solid var(--primary); }
  .alert-badge {
    font-family: "JetBrains Mono", "Fira Code", monospace;
    font-size: 10px;
    font-weight: 800;
    padding: 2px 7px;
    border-radius: 4px;
    white-space: nowrap;
    margin-top: 1px;
    letter-spacing: 0.5px;
  }
  .badge-WARN { background: rgba(251, 191, 36, 0.18); color: var(--warn); }
  .badge-CRIT { background: rgba(248, 113, 113, 0.18); color: var(--danger); }
  .badge-INFO { background: rgba(129, 140, 248, 0.18); color: var(--primary); }
  .alert-msg  { color: var(--text); font-size: 13px; line-height: 1.5; font-weight: 450; }

  .no-alerts {
    padding: 28px 20px;
    color: var(--muted);
    font-size: 13px;
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 10px;
    background: var(--surface2);
    border-radius: 10px;
    margin: 14px;
  }
  .no-alerts-icon {
    width: 22px;
    height: 22px;
    border-radius: 50%;
    background: rgba(52, 211, 153, 0.15);
    color: var(--success);
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 12px;
    font-weight: 800;
  }

  /* ── Diagnostic Champion Cards ── */
  .diagnostic-list {
    padding: 16px;
    display: flex;
    flex-direction: column;
    gap: 12px;
  }
  .diagnostic-item {
    padding: 16px;
    border: 1px solid var(--border);
    border-radius: 12px;
    background: var(--surface2);
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
    transition: border-color 0.2s, transform 0.15s;
  }
  .diagnostic-item:hover {
    border-color: rgba(var(--primary-rgb), 0.3);
    transform: translateY(-1px);
  }
  .diagnostic-head {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
    margin-bottom: 10px;
    flex-wrap: wrap;
  }
  .diagnostic-title {
    font-size: 13.5px;
    font-weight: 700;
    color: var(--text-heading);
    display: flex;
    align-items: center;
    gap: 8px;
  }
  .diagnostic-meta {
    font-family: "JetBrains Mono", "Fira Code", monospace;
    font-size: 11.5px;
    color: var(--muted);
    background: var(--surface);
    padding: 2px 8px;
    border-radius: 6px;
    border: 1px solid var(--border-subtle);
  }
  .diagnostic-body {
    font-size: 12.5px;
    color: var(--text);
    display: flex;
    flex-direction: column;
    gap: 8px;
  }
  .diagnostic-tables {
    display: flex;
    flex-wrap: wrap;
    gap: 6px;
    margin-top: 4px;
  }
  .diagnostic-chip {
    font-family: "JetBrains Mono", "Fira Code", monospace;
    font-size: 11px;
    padding: 3px 9px;
    border-radius: 999px;
    background: rgba(var(--primary-rgb), 0.12);
    color: var(--primary);
    border: 1px solid rgba(var(--primary-rgb), 0.2);
  }

  /* ── Terminal Shell for Replay ── */
  .mysqlbinlog-row {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 10px;
    margin-top: 6px;
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 8px 12px;
  }
  .mysqlbinlog-cmd {
    font-family: "JetBrains Mono", "Fira Code", monospace;
    font-size: 11.5px;
    color: var(--accent);
    word-break: break-all;
    user-select: all;
  }
  .copy-btn {
    font-size: 11px;
    font-weight: 600;
    padding: 4px 10px;
    border-radius: 6px;
    border: 1px solid var(--border);
    background: var(--surface2);
    color: var(--primary);
    cursor: pointer;
    flex-shrink: 0;
    transition: all 0.15s;
  }
  .copy-btn:hover {
    background: var(--primary);
    color: #ffffff;
    border-color: var(--primary);
  }

  /* ── Drilldown Accordion Cards ── */
  .drilldown-card {
    border-radius: 10px;
    border: 1px solid var(--border);
    background: var(--surface2);
    overflow: hidden;
    transition: border-color 0.2s;
  }
  .drilldown-card:hover { border-color: rgba(var(--primary-rgb), 0.3); }
  .drilldown-summary {
    padding: 14px 18px;
    cursor: pointer;
    display: flex;
    align-items: center;
    justify-content: space-between;
    font-size: 13px;
    color: var(--text-heading);
    user-select: none;
    font-weight: 600;
  }
  .drilldown-summary:hover { background: var(--surface-hover); }
  .drilldown-flags { display: flex; gap: 6px; }
  .drilldown-flag {
    font-family: "JetBrains Mono", "Fira Code", monospace;
    font-size: 10px;
    font-weight: 700;
    padding: 2px 8px;
    border-radius: 999px;
    text-transform: uppercase;
    letter-spacing: 0.5px;
  }
  .drilldown-flag-dominance { background: rgba(var(--primary-rgb), 0.15); color: var(--primary); border: 1px solid rgba(var(--primary-rgb), 0.3); }
  .drilldown-flag-anomaly   { background: rgba(251, 191, 36, 0.15);  color: var(--warn);    border: 1px solid rgba(251, 191, 36, 0.3); }
  .drilldown-body { padding: 0 18px 18px; border-top: 1px solid var(--border-subtle); margin-top: 6px; padding-top: 14px; }
  .drilldown-why { color: var(--muted); font-size: 12.5px; margin: 0 0 12px; font-style: italic; }
  .drilldown-metrics { display: flex; flex-wrap: wrap; gap: 10px; margin-bottom: 14px; }
  .drilldown-metric {
    font-family: "JetBrains Mono", "Fira Code", monospace;
    font-size: 11px;
    color: var(--text);
    cursor: help;
    background: var(--surface);
    padding: 4px 10px;
    border-radius: 6px;
    border: 1px solid var(--border);
  }
  .drilldown-subsection { margin-bottom: 10px; }
  .drilldown-sublabel {
    font-size: 10.5px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.7px;
    color: var(--muted);
    display: block;
    margin-bottom: 6px;
  }
  .drilldown-minute, .drilldown-txn {
    font-family: "JetBrains Mono", "Fira Code", monospace;
    font-size: 11.5px;
    color: var(--text);
    padding: 3px 0;
  }

  /* ── Toast Notification ── */
  .toast {
    position: fixed;
    bottom: 24px;
    right: 24px;
    background: var(--surface);
    color: var(--text-heading);
    padding: 10px 18px;
    border-radius: 8px;
    border: 1px solid var(--primary);
    box-shadow: 0 10px 25px rgba(0, 0, 0, 0.4);
    font-size: 12.5px;
    font-weight: 600;
    z-index: 1000;
    opacity: 0;
    transform: translateY(10px);
    transition: opacity 0.2s, transform 0.2s;
    pointer-events: none;
  }
  .toast.show {
    opacity: 1;
    transform: translateY(0);
  }

  /* ── Footer ── */
  .footer {
    margin-top: 36px;
    text-align: center;
    color: var(--muted);
    font-size: 11.5px;
    padding-top: 20px;
    border-top: 1px solid var(--border-subtle);
  }

  /* ── Responsive Queries ── */
  @media (max-width: 960px) {
    .charts-grid { grid-template-columns: 1fr; }
    .chart-panel-wide { grid-column: auto; }
    .chart-box { height: 280px; }
    .table-detail-grid { grid-template-columns: 1fr; }
    .nav-pills { display: none; }
  }
  @media (max-width: 640px) {
    .cards { grid-template-columns: 1fr; }
    .header { flex-direction: column; align-items: flex-start; gap: 12px; }
    .header-actions { width: 100%; justify-content: space-between; }
    .header-meta { text-align: left; }
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

  /* ── Print Optimization ── */
  @media print {
    body { background: #ffffff !important; color: #000000 !important; }
    .topbar, .theme-switcher, .nav-pills, .action-btn, .copy-btn, .back-to-top { display: none !important; }
    .section, .card, .chart-panel { box-shadow: none !important; border: 1px solid #cccccc !important; break-inside: avoid; }
    .page { max-width: 100% !important; padding: 0 !important; }
  }
</style>
</head>
<body>
<div class="page">

  <!-- Sticky Header / Topbar -->
  <div class="topbar">
    <div class="header">
      <div class="header-brand">
        <div class="header-logo"><span class="logo-badge">Binlog</span>Viz</div>
        <div class="header-type-tag">{{t "report.html.analyze.header"}}</div>
      </div>

      <nav class="nav-pills" aria-label="Sections">
        <a class="nav-pill" href="#executive-summary">⚡ {{t "report.html.analyze.executiveSummary"}}</a>
        <a class="nav-pill" href="#section-findings">🚨 {{t "report.html.analyze.sectionFindings"}}</a>
        <a class="nav-pill" href="#section-activity">📈 {{t "report.html.analyze.sectionActivity"}}</a>
        <a class="nav-pill" href="#section-objects">🗄️ {{t "report.html.analyze.sectionObjects"}}</a>
        <a class="nav-pill" href="#section-evidence">🔬 {{t "report.html.analyze.sectionEvidence"}}</a>
      </nav>

      <div class="header-actions">
        <button type="button" class="action-btn" id="btn-copy-summary" title="Copy report summary to clipboard">
          <span>📋</span> {{t "report.html.analyze.copy"}}
        </button>
        <button type="button" class="action-btn" onclick="window.print()" title="Print or save as PDF">
          <span>🖨️</span> PDF
        </button>
        <div class="header-meta">
          <strong>{{t "report.html.common.generatedAt"}}</strong>
          {{.GeneratedAt}}
        </div>
        <div class="theme-switcher" aria-label="Theme selection">
          <button class="theme-btn" data-t="nebula" title="Nebula (Dark OLED)"></button>
          <button class="theme-btn" data-t="forest" title="Forest (Emerald)"></button>
          <button class="theme-btn" data-t="navy"   title="Navy (Sapphire)"></button>
          <button class="theme-btn" data-t="ember"  title="Ember (Sunset)"></button>
          <button class="theme-btn" data-t="light"  title="Light (Pro Slate)"></button>
        </div>
      </div>
    </div>
  </div>

  <!-- ═══ Section 1: Executive Summary ═══ -->
  <section class="section" id="executive-summary">
    <div class="section-header">
      <div class="section-header-title">
        <span class="dot"></span>
        <span>{{t "report.html.analyze.executiveSummary"}}</span>
      </div>
    </div>
    <div class="section-body" style="padding:16px">
      <div class="cards" style="margin-bottom:0">
        <div class="card">
          <div class="card-head">
            <div class="card-label">{{t "report.html.common.transactions"}}</div>
            <div class="card-icon">⚡</div>
          </div>
          <div class="card-value">{{fmtIntHTML .TotalTxns}}</div>
          <div class="card-sub">{{t "report.html.common.txns"}}</div>
        </div>
        <div class="card accent">
          <div class="card-head">
            <div class="card-label">{{t "report.html.common.affectedRows"}}</div>
            <div class="card-icon">📝</div>
          </div>
          <div class="card-value">{{fmtIntHTML .TotalRows}}</div>
          <div class="card-sub">{{t "report.html.common.rows"}}</div>
        </div>
        <div class="card success">
          <div class="card-head">
            <div class="card-label">{{t "report.html.common.events"}}</div>
            <div class="card-icon">📊</div>
          </div>
          <div class="card-value">{{fmtIntHTML .TotalEvents}}</div>
          <div class="card-sub">{{t "report.html.common.events"}}</div>
        </div>
        {{if .StartTime}}
        <div class="card warn">
          <div class="card-head">
            <div class="card-label">{{t "report.html.common.timeRange"}}</div>
            <div class="card-icon">⏱️</div>
          </div>
          <div class="card-value" style="font-size:13.5px;margin-top:4px;line-height:1.4">{{.StartTime}}</div>
          <div class="card-sub">→ {{.EndTime}}</div>
          <div class="card-sub" style="font-weight:600;color:var(--text)">{{t "report.html.common.duration"}}: {{.Duration}}</div>
        </div>
        {{else}}
        <div class="card">
          <div class="card-head">
            <div class="card-label">{{t "report.html.common.timeRange"}}</div>
            <div class="card-icon">⏱️</div>
          </div>
          <div class="card-value" style="font-size:16px;color:var(--muted)">{{t "report.html.common.notAvailable"}}</div>
        </div>
        {{end}}
        {{if .HasDDLEvents}}
        <div class="card danger">
          <div class="card-head">
            <div class="card-label">{{t "report.html.analyze.ddlCount"}}</div>
            <div class="card-icon">🚨</div>
          </div>
          <div class="card-value">{{fmtIntHTML .DDLCount}}</div>
          <div class="card-sub">{{t "report.html.analyze.ddlEvents"}}</div>
        </div>
        {{end}}
      </div>
      {{if .HasAlerts}}
      <div class="key-findings" id="key-findings">
        <div class="key-findings-title">
          <span>⚠️</span> {{t "report.html.analyze.keyFindings"}}
        </div>
        {{range .TopAlerts}}
        <div class="key-finding-item">
          <span class="key-finding-badge {{.Severity}}">{{.Badge}}</span>
          <span>{{.Message}}</span>
        </div>
        {{end}}
      </div>
      {{end}}
    </div>
  </section>

  <!-- ═══ Section 2: Risks & Findings ═══ -->
  <section class="section" id="section-findings">
    <div class="section-header">
      <div class="section-header-title">
        <span class="dot" style="background:var(--danger);box-shadow:0 0 8px rgba(248,113,113,0.6)"></span>
        <span>{{t "report.html.analyze.sectionFindings"}}</span>
      </div>
    </div>
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
      {{else if .HasDDLEvents}}
      <div class="alert-list">
        <div class="alert-item info">
          <span class="alert-badge badge-INFO">INFO</span>
          <span class="alert-msg">{{t "report.html.analyze.ddlActivityNotice"}}</span>
        </div>
      </div>
      {{else}}
      <div class="no-alerts">
        <span class="no-alerts-icon">✓</span>
        <span>{{t "report.html.analyze.noAlerts"}}</span>
      </div>
      {{end}}
    </div>
  </section>

  <!-- ═══ Section 3: Activity Overview ═══ -->
  <section class="section" id="section-activity">
    <div class="section-header">
      <div class="section-header-title">
        <span class="dot" style="background:var(--accent);box-shadow:0 0 8px rgba(34,211,238,0.6)"></span>
        <span>{{t "report.html.analyze.sectionActivity"}}</span>
      </div>
    </div>
    <div class="section-body">
      <div class="section-desc">{{t "report.html.analyze.activityCharts"}}</div>
      <div class="charts-grid">
        <div class="chart-panel chart-panel-large">
          <div class="chart-title">
            <span>{{t "report.html.analyze.avgTPSPerMinute"}}</span>
          </div>
          <div class="chart-box chart-large" id="chart-tps"></div>
        </div>
        <div class="chart-panel chart-panel-wide">
          <div class="chart-title">
            <span>{{t "report.html.analyze.rowsPerMinute"}}</span>
          </div>
          <div class="chart-box" id="chart-timeline"></div>
        </div>
        <div class="chart-panel">
          <div class="chart-title">
            <span>{{t "report.html.common.operationMix"}}</span>
          </div>
          <div class="chart-box" id="chart-ops"></div>
        </div>
        {{if .HasHotIntervals}}
        <div class="chart-panel">
          <div class="chart-title">
            <span>{{t "report.html.analyze.hotIntervals"}}</span>
          </div>
          <div class="diagnostic-list" style="padding:0">
            {{range .HotIntervals}}
            <div class="diagnostic-item">
              <div class="diagnostic-head">
                <div class="diagnostic-title">⏱️ {{.Timestamp}}</div>
                <div class="diagnostic-meta">{{t "report.html.common.rows"}}={{fmtIntHTML .Rows}} · {{t "report.html.common.txns"}}={{fmtIntHTML .Txns}} · {{t "report.html.common.events"}}={{fmtIntHTML .Events}}</div>
              </div>
              <div class="diagnostic-body">
                <div>{{t "report.html.analyze.binlogBytes"}}: {{fmtIntHTML .BinlogBytes}} <span style="color:var(--primary);font-weight:600;font-family:'JetBrains Mono',monospace;margin-left:4px">({{.BinlogBytesFormatted}})</span></div>
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

  <!-- ═══ Section 4: Hot Objects ═══ -->
  <section class="section" id="section-objects">
    <div class="section-header">
      <div class="section-header-title">
        <span class="dot" style="background:var(--accent);box-shadow:0 0 8px rgba(34,211,238,0.6)"></span>
        <span>{{t "report.html.analyze.sectionObjects"}}</span>
      </div>
    </div>
    <div class="section-body">
      <div class="charts-grid" style="padding-bottom:0">
        <div class="chart-panel chart-panel-wide">
          <div class="chart-title">
            <span>{{t "report.html.analyze.topTablesByRows"}}</span>
          </div>
          <div class="chart-box" id="chart-tables"></div>
        </div>
      </div>
      
      <div class="table-toolbar">
        <div class="diagnostic-title">
          <span>🗄️ {{t "report.html.analyze.topTables"}}</span>
        </div>
        <div class="table-search-box">
          <span>🔍</span>
          <input type="text" id="table-filter-input" placeholder="Search tables..." aria-label="Search tables">
        </div>
      </div>

      {{if .Tables}}
      <div class="table-container">
        <table id="top-tables-table">
          <thead>
            <tr>
              <th class="sortable" data-sort="schema">{{t "report.html.common.schema"}}</th>
              <th class="sortable" data-sort="table">{{t "report.html.common.table"}}</th>
              <th class="num sortable" data-sort="total">{{t "report.html.common.totalRows"}}</th>
              <th class="num sortable" data-sort="bytes">{{t "report.html.analyze.binlogBytes"}}</th>
              <th class="num sortable" data-sort="ins">{{t "report.html.analyze.insertPct"}}</th>
              <th class="num sortable" data-sort="upd">{{t "report.html.analyze.updatePct"}}</th>
              <th class="num sortable" data-sort="del">{{t "report.html.analyze.deletePct"}}</th>
              <th class="num sortable" data-sort="ddl">{{t "report.html.analyze.ddlPct"}}</th>
              <th class="num sortable" data-sort="txns">{{t "report.html.common.txns"}}</th>
            </tr>
          </thead>
          <tbody>
            {{range .Tables}}
            <tr class="table-summary-row{{if .HasActivity}} has-activity{{end}}" data-table-row="{{.Key}}" data-table-key="{{.Key}}" {{if .HasActivity}}data-table-dom-id="{{.DOMID}}"{{end}} title="{{if .HasActivity}}Click to view activity timeline & ops chart{{end}}">
              <td class="name">{{if .HasActivity}}<span class="table-chevron">▶</span>{{end}}{{.Schema}}</td>
              <td class="name" style="font-weight:600">{{.Table}}</td>
              <td class="num">{{fmtIntHTML .Total}}</td>
              <td class="num" data-raw="{{.BinlogBytes}}" style="font-family:'JetBrains Mono',monospace;color:var(--primary);font-weight:600">{{.BinlogBytesFormatted}}</td>
              <td class="num op-ins">{{.InsertPct}}</td>
              <td class="num op-upd">{{.UpdatePct}}</td>
              <td class="num op-del">{{.DeletePct}}</td>
              <td class="num">{{.DDLPct}}</td>
              <td class="num">{{fmtIntHTML .Txns}}</td>
            </tr>
            {{if .HasActivity}}
            <tr class="table-detail-row" data-table-key="{{.Key}}" hidden>
              <td colspan="9">
                <div class="table-detail-panel">
                  <div class="table-detail-grid">
                    <div class="chart-panel">
                      <div class="chart-title">{{t "report.html.analyze.tableActivityChart"}} &mdash; {{.Key}}</div>
                      <div class="chart-box" id="chart-table-activity-{{.DOMID}}"></div>
                    </div>
                    <div class="chart-panel">
                      <div class="chart-title">{{t "report.html.analyze.tableOpsChart"}} &mdash; {{.Key}}</div>
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
      </div>
      <div class="section-desc" style="padding:10px 16px 14px">{{t "report.html.analyze.opsNote"}}</div>
      {{else}}
      <div class="no-alerts"><span>{{t "report.html.analyze.noTableData"}}</span></div>
      {{end}}
    </div>
  </section>

  <!-- ═══ Section 5: Diagnostic Evidence ═══ -->
  <section class="section" id="section-evidence">
    <div class="section-header">
      <div class="section-header-title">
        <span class="dot" style="background:var(--primary);box-shadow:0 0 8px rgba(var(--primary-rgb),0.6)"></span>
        <span>{{t "report.html.analyze.sectionEvidence"}}</span>
      </div>
    </div>
    <div class="section-body" style="padding:16px 16px 0">

    {{if or .HasLargestTxns .HasLongestTxns .HasWidestTxns}}
    <section class="evidence-sub" id="transaction-evidence">
      <div class="evidence-sub-header">
        <span class="dot" style="background:var(--primary)"></span>
        <span>{{t "report.html.analyze.transactionEvidence"}}</span>
      </div>
      <div class="section-body">
        <div class="diagnostic-list">
          {{if .HasLargestTxns}}
          <div class="diagnostic-title">🏆 {{t "report.html.analyze.largestTransactionsByRows"}}</div>
          {{range .LargestTransactions}}
          <div class="diagnostic-item">
            <div class="diagnostic-head">
              <div class="diagnostic-title">
                <span>{{.TxnKey}}</span>
                <span class="drilldown-flag drilldown-flag-dominance">{{t "report.html.analyze.largestTag"}}</span>
              </div>
              <div class="diagnostic-meta">{{t "report.html.common.rows"}}={{fmtIntHTML .Rows}} · {{t "report.html.common.events"}}={{fmtIntHTML .Events}} · {{t "report.html.common.duration"}}={{.Duration}} · {{len .Tables}} {{t "report.html.analyze.touchedTables"}}</div>
            </div>
            <div class="diagnostic-body">
              {{if .Location}}<div>📍 {{t "report.html.analyze.binlogSpan"}}: <code style="font-family:'JetBrains Mono',monospace;color:var(--accent)">{{.Location}}</code></div>{{end}}
              <div>📦 {{t "report.html.analyze.binlogBytes"}}: {{fmtIntHTML .BinlogBytes}} <span style="color:var(--primary);font-weight:600;font-family:'JetBrains Mono',monospace;margin-left:4px">({{.BinlogBytesFormatted}})</span></div>
              {{if .MysqlbinlogCmd}}
              <div class="mysqlbinlog-row">
                <code class="mysqlbinlog-cmd">{{.MysqlbinlogCmd}}</code>
                <button type="button" class="copy-btn" data-copy="{{.MysqlbinlogCmd}}">📋 {{t "report.html.analyze.copy"}}</button>
              </div>
              {{end}}
              {{if .QuerySummary}}<div style="padding:6px 10px;background:var(--surface);border-radius:6px;border:1px solid var(--border-subtle);font-family:'JetBrains Mono',monospace;font-size:11.5px">{{.QuerySummary}}</div>{{end}}
              {{if .Tables}}
              <div class="diagnostic-tables">
                {{range .Tables}}<span class="diagnostic-chip">{{.Name}} · {{fmtIntHTML .Rows}} rows</span>{{end}}
              </div>
              {{end}}
            </div>
          </div>
          {{end}}
          {{end}}

          {{if .HasLongestTxns}}
          <div class="diagnostic-title" style="margin-top:8px">⏱️ {{t "report.html.analyze.longestTransactionsByDuration"}}</div>
          {{range .LongestTransactions}}
          <div class="diagnostic-item">
            <div class="diagnostic-head">
              <div class="diagnostic-title">
                <span>{{.TxnKey}}</span>
                <span class="drilldown-flag drilldown-flag-anomaly">{{t "report.html.analyze.longestTag"}}</span>
              </div>
              <div class="diagnostic-meta">{{t "report.html.common.duration"}}={{.Duration}} · {{t "report.html.common.rows"}}={{fmtIntHTML .Rows}} · {{t "report.html.common.events"}}={{fmtIntHTML .Events}} · {{len .Tables}} {{t "report.html.analyze.touchedTables"}}</div>
            </div>
            <div class="diagnostic-body">
              {{if .Location}}<div>📍 {{t "report.html.analyze.binlogSpan"}}: <code style="font-family:'JetBrains Mono',monospace;color:var(--accent)">{{.Location}}</code></div>{{end}}
              <div>📦 {{t "report.html.analyze.binlogBytes"}}: {{fmtIntHTML .BinlogBytes}} <span style="color:var(--primary);font-weight:600;font-family:'JetBrains Mono',monospace;margin-left:4px">({{.BinlogBytesFormatted}})</span></div>
              {{if .MysqlbinlogCmd}}
              <div class="mysqlbinlog-row">
                <code class="mysqlbinlog-cmd">{{.MysqlbinlogCmd}}</code>
                <button type="button" class="copy-btn" data-copy="{{.MysqlbinlogCmd}}">📋 {{t "report.html.analyze.copy"}}</button>
              </div>
              {{end}}
              {{if .QuerySummary}}<div style="padding:6px 10px;background:var(--surface);border-radius:6px;border:1px solid var(--border-subtle);font-family:'JetBrains Mono',monospace;font-size:11.5px">{{.QuerySummary}}</div>{{end}}
              {{if .Tables}}
              <div class="diagnostic-tables">
                {{range .Tables}}<span class="diagnostic-chip">{{.Name}} · {{fmtIntHTML .Rows}} rows</span>{{end}}
              </div>
              {{end}}
            </div>
          </div>
          {{end}}
          {{end}}

          {{if .HasWidestTxns}}
          <div class="diagnostic-title" style="margin-top:8px">🌐 {{t "report.html.analyze.widestTransactionsByTouchedTables"}}</div>
          {{range .WidestTransactions}}
          <div class="diagnostic-item">
            <div class="diagnostic-head">
              <div class="diagnostic-title">
                <span>{{.TxnKey}}</span>
                <span class="drilldown-flag drilldown-flag-dominance">{{t "report.html.analyze.widestTag"}}</span>
              </div>
              <div class="diagnostic-meta">{{len .Tables}} {{t "report.html.analyze.touchedTables"}} · {{t "report.html.common.rows"}}={{fmtIntHTML .Rows}} · {{t "report.html.common.duration"}}={{.Duration}}</div>
            </div>
            <div class="diagnostic-body">
              {{if .Location}}<div>📍 {{t "report.html.analyze.binlogSpan"}}: <code style="font-family:'JetBrains Mono',monospace;color:var(--accent)">{{.Location}}</code></div>{{end}}
              <div>📦 {{t "report.html.analyze.binlogBytes"}}: {{fmtIntHTML .BinlogBytes}} <span style="color:var(--primary);font-weight:600;font-family:'JetBrains Mono',monospace;margin-left:4px">({{.BinlogBytesFormatted}})</span></div>
              {{if .MysqlbinlogCmd}}
              <div class="mysqlbinlog-row">
                <code class="mysqlbinlog-cmd">{{.MysqlbinlogCmd}}</code>
                <button type="button" class="copy-btn" data-copy="{{.MysqlbinlogCmd}}">📋 {{t "report.html.analyze.copy"}}</button>
              </div>
              {{end}}
              {{if .QuerySummary}}<div style="padding:6px 10px;background:var(--surface);border-radius:6px;border:1px solid var(--border-subtle);font-family:'JetBrains Mono',monospace;font-size:11.5px">{{.QuerySummary}}</div>{{end}}
              {{if .Tables}}
              <div class="diagnostic-tables">
                {{range .Tables}}<span class="diagnostic-chip">{{.Name}} · {{fmtIntHTML .Rows}} rows</span>{{end}}
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
      <div class="evidence-sub-header">
        <span class="dot" style="background:var(--warn)"></span>
        <span>{{t "report.html.analyze.ddlTimeline"}}</span>
      </div>
      <div class="section-body">
        <div class="diagnostic-list">
          {{range .DDLEvents}}
          <div class="diagnostic-item">
            <div class="diagnostic-head">
              <div class="diagnostic-title">
                <span class="key-finding-badge warning">{{.Operation}}</span>
                <span>{{.Object}}</span>
              </div>
              <div class="diagnostic-meta">{{.Timestamp}}</div>
            </div>
            <div class="diagnostic-body">
              <div style="padding:8px 12px;background:var(--surface);border-radius:6px;border:1px solid var(--border-subtle);font-family:'JetBrains Mono',monospace;font-size:12px;color:var(--warn)">{{.Statement}}</div>
              {{if .Location}}<div class="diagnostic-meta" style="align-self:flex-start">📍 {{.Location}}</div>{{end}}
            </div>
          </div>
          {{end}}
        </div>
      </div>
    </section>
    {{end}}

    {{if .HasDrilldowns}}
    <section class="evidence-sub" id="write-shape-patterns">
      <div class="evidence-sub-header">
        <span class="dot" style="background:var(--accent)"></span>
        <span>{{t "report.html.analyze.writeShapePatterns"}}</span>
      </div>
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
      <div class="evidence-sub-header">
        <span class="dot" style="background:var(--success)"></span>
        <span>{{t "report.html.analyze.analyzedFiles"}}</span>
      </div>
      <div class="section-body">
        <div class="section-desc">{{t "report.html.analyze.analyzedFilesNote"}}</div>
        <div class="diagnostic-list" id="file-coverage">
          {{if .FileCoverage.Selected}}
          <div class="diagnostic-item">
            <div class="diagnostic-head">
              <div class="diagnostic-title">✅ {{t "report.html.analyze.selectedFiles"}}</div>
              <div class="diagnostic-meta">{{len .FileCoverage.Selected}}</div>
            </div>
            <div class="diagnostic-body">
              <div class="table-container">
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
                      <td class="name" style="font-weight:600">{{.BinlogPath}}</td>
                      <td class="num">{{.Size}}</td>
                      <td class="name">{{.Reason}}</td>
                      <td class="name">{{if .FirstEventAt}}{{.FirstEventAt}} → {{.LastEventAt}}{{end}}</td>
                    </tr>
                    {{end}}
                  </tbody>
                </table>
              </div>
            </div>
          </div>
          {{end}}
          {{if .FileCoverage.Skipped}}
          <div class="diagnostic-item">
            <div class="diagnostic-head">
              <div class="diagnostic-title">⏭️ {{t "report.html.analyze.skippedFiles"}}</div>
              <div class="diagnostic-meta">{{len .FileCoverage.Skipped}}</div>
            </div>
            <div class="diagnostic-body">
              <div class="table-container">
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
                      <td class="name" style="font-weight:600">{{.BinlogPath}}</td>
                      <td class="num">{{.Size}}</td>
                      <td class="name">{{.Reason}}</td>
                    </tr>
                    {{end}}
                  </tbody>
                </table>
              </div>
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
      <div class="evidence-sub-header">
        <span class="dot" style="background:var(--accent)"></span>
        <span>{{t "report.html.analyze.binlogThroughput"}}</span>
      </div>
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

  <div class="footer">
    {{t "report.html.common.generatedBy"}} &middot; {{.GeneratedAt}}
  </div>
</div>

<button id="btn-back-to-top" class="back-to-top" title="{{t "report.html.common.backToTop"}}" aria-label="{{t "report.html.common.backToTop"}}">
  <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round">
    <polyline points="18 15 12 9 6 15"></polyline>
  </svg>
</button>

<div class="toast" id="toast-notify"></div>

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

  function showToast(msg) {
    var toast = document.getElementById('toast-notify');
    if (!toast) return;
    toast.textContent = msg;
    toast.classList.add('show');
    setTimeout(function() { toast.classList.remove('show'); }, 2000);
  }

  function makeTheme() {
    return {
      backgroundColor: 'transparent',
      textStyle: { color: cssVar('--muted'), fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif' },
      title: { textStyle: { color: cssVar('--text-heading') } },
      tooltip: {
        backgroundColor: cssVar('--surface2'),
        borderColor: cssVar('--border'),
        borderWidth: 1,
        padding: [10, 14],
        textStyle: { color: cssVar('--text'), fontSize: 12 },
        extraCssText: 'box-shadow: 0 8px 24px rgba(0,0,0,0.35); border-radius: 8px; backdrop-filter: blur(8px);'
      },
      axisPointer: { lineStyle: { color: cssVar('--primary'), width: 1.5, type: 'dashed' } }
    };
  }

  function makeToolbox() {
    var primary = cssVar('--primary');
    var muted = cssVar('--muted');
    return {
      right: 14,
      top: 0,
      itemSize: 13,
      iconStyle: { borderColor: muted },
      emphasis: { iconStyle: { borderColor: primary } },
      feature: {
        dataZoom: { yAxisIndex: 'none', title: { zoom: 'Select Zoom Range', back: 'Reset Zoom' } },
        restore: { title: 'Restore' }
      }
    };
  }

  function makeDataZoom() {
    var primary = cssVar('--primary');
    var muted = cssVar('--muted');
    return [
      { type: 'inside', zoomOnMouseWheel: true, moveOnMouseMove: true },
      {
        type: 'slider',
        height: 14,
        bottom: 2,
        borderColor: 'transparent',
        backgroundColor: 'rgba(255, 255, 255, 0.03)',
        fillerColor: 'rgba(56, 189, 248, 0.16)',
        handleStyle: { color: primary, borderColor: primary },
        textStyle: { color: muted, fontSize: 10 }
      }
    ];
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
    if (tpsEl) {
      c0 = echarts.init(tpsEl, null, {renderer: 'svg'});
      c0.group = 'analyze_activity';
      c0.setOption({
        ...t,
        toolbox: makeToolbox(),
        legend: { show: false },
        dataZoom: makeDataZoom(),
        grid: { top: 28, bottom: 35, left: 55, right: 20 },
        xAxis: {
          type: 'category',
          data: tpsLabels,
          axisLine: { lineStyle: { color: border } },
          axisLabel: { color: muted, fontSize: 11, margin: 12 },
          splitLine: { show: false }
        },
        yAxis: {
          type: 'value',
          axisLine: { show: false },
          axisLabel: { color: muted, fontSize: 11 },
          splitLine: { lineStyle: { color: border, type: 'dashed' } }
        },
        tooltip: {
          ...t.tooltip,
          trigger: 'axis',
          formatter: function(params) {
            if (!params || !params.length) return '';
            var p = params[0];
            return '<div style="font-weight:700;margin-bottom:4px;color:' + text + '">' + p.name + '</div>' +
                   '<div style="display:flex;align-items:center;gap:6px">' +
                   '<span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:' + accent + '"></span>' +
                   '<span>{{t "report.html.analyze.avgTPSPerMinute"}}:</span> <strong>' + Number(p.value).toFixed(2) + '</strong></div>';
          }
        },
        series: [{
          name: '{{t "report.html.analyze.avgTPSPerMinute"}}',
          type: 'line',
          data: tpsValues,
          smooth: 0.3,
          symbol: 'none',
          lineStyle: { color: accent, width: 2.5 },
          areaStyle: {
            color: {
              type: 'linear', x: 0, y: 0, x2: 0, y2: 1,
              colorStops: [{offset: 0, color: accent + '33'}, {offset: 1, color: accent + '00'}]
            }
          }
        }]
      });
    }

    var timelineEl = document.getElementById('chart-timeline');
    if (timelineEl) {
      c1 = echarts.init(timelineEl, null, {renderer: 'svg'});
      c1.group = 'analyze_activity';
      c1.setOption({
        ...t,
        toolbox: makeToolbox(),
        legend: { show: false },
        dataZoom: makeDataZoom(),
        grid: { top: 28, bottom: 35, left: 55, right: 20 },
        xAxis: {
          type: 'category',
          data: minuteLabels,
          axisLine: { lineStyle: { color: border } },
          axisLabel: { color: muted, fontSize: 11, margin: 12 },
          splitLine: { show: false }
        },
        yAxis: {
          type: 'value',
          axisLine: { show: false },
          axisLabel: { color: muted, fontSize: 11 },
          splitLine: { lineStyle: { color: border, type: 'dashed' } }
        },
        tooltip: {
          ...t.tooltip,
          trigger: 'axis',
          formatter: function(params) {
            if (!params || !params.length) return '';
            var p = params[0];
            return '<div style="font-weight:700;margin-bottom:4px;color:' + text + '">' + p.name + '</div>' +
                   '<div style="display:flex;align-items:center;gap:6px">' +
                   '<span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:' + primary + '"></span>' +
                   '<span>{{t "report.html.common.rows"}}:</span> <strong>' + Number(p.value).toLocaleString() + '</strong></div>';
          }
        },
        series: [{
          name: '{{t "report.html.common.rows"}}',
          type: 'line',
          data: minuteRows,
          smooth: 0.3,
          symbol: 'none',
          lineStyle: { color: primary, width: 2.5 },
          areaStyle: {
            color: {
              type: 'linear', x: 0, y: 0, x2: 0, y2: 1,
              colorStops: [{offset: 0, color: primary + '4d'}, {offset: 1, color: primary + '00'}]
            }
          }
        }]
      });
    }

    if (c0 && c1) {
      echarts.connect('analyze_activity');
    }

    var tablesEl = document.getElementById('chart-tables');
    if (tablesEl) {
      c2 = echarts.init(tablesEl, null, {renderer: 'svg'});
      c2.setOption({
        ...t,
        legend: { show: false },
        dataZoom: [{ type: 'inside', zoomOnMouseWheel: true, moveOnMouseMove: true, yAxisIndex: [0] }],
        grid: { top: 10, bottom: 25, left: 16, right: 24, containLabel: true },
        xAxis: {
          type: 'value',
          axisLabel: { color: muted, fontSize: 11 },
          axisLine: { show: false },
          splitLine: { lineStyle: { color: border, type: 'dashed' } }
        },
        yAxis: {
          type: 'category',
          data: tableNames,
          inverse: true,
          axisLine: { show: false },
          axisLabel: { color: muted, fontSize: 11, width: 140, overflow: 'truncate' },
          splitLine: { show: false }
        },
        tooltip: {
          ...t.tooltip,
          trigger: 'axis',
          axisPointer: { type: 'shadow' },
          formatter: function(params) {
            if (!params || !params.length) return '';
            var p = params[0];
            return '<div style="font-weight:700;margin-bottom:4px">' + p.name + '</div>' +
                   '<div>{{t "report.html.common.totalRows"}}: <strong>' + Number(p.value).toLocaleString() + '</strong></div>';
          }
        },
        series: [{
          type: 'bar',
          data: tableRows,
          barMaxWidth: 18,
          itemStyle: {
            color: {
              type: 'linear', x: 0, y: 0, x2: 1, y2: 0,
              colorStops: [{offset: 0, color: accent + 'bb'}, {offset: 1, color: accent}]
            },
            borderRadius: [0, 6, 6, 0]
          }
        }]
      });
    }

    var opsEl = document.getElementById('chart-ops');
    if (opsEl) {
      c3 = echarts.init(opsEl, null, {renderer: 'svg'});
      c3.setOption({
        ...t,
        tooltip: {
          ...t.tooltip,
          trigger: 'item',
          formatter: function(p) {
            return '<div style="font-weight:700;margin-bottom:4px">' + p.name + '</div>' +
                   '<div>' + p.marker + ' ' + Number(p.value).toLocaleString() + ' (' + p.percent + '%)</div>';
          }
        },
        legend: { bottom: 0, textStyle: { color: muted, fontSize: 11 }, itemGap: 14 },
        series: [{
          name: '{{t "report.html.common.operations"}}',
          type: 'pie',
          radius: ['45%', '72%'],
          center: ['50%', '44%'],
          data: opsPie,
          label: { show: false },
          emphasis: {
            label: { show: true, fontSize: 13, fontWeight: 700, color: text },
            itemStyle: { shadowBlur: 10, shadowOffsetX: 0, shadowColor: 'rgba(0, 0, 0, 0.5)' }
          },
          itemStyle: { borderColor: bg, borderWidth: 3 },
          color: [insert, update, del]
        }]
      });
    }
  }

  function renderTableCharts(tableKey, domID) {
    var data = window.tableActivitySeries[tableKey];
    if (!data) return;
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

    var groupKey = 'table_' + domID;

    var actEl = document.getElementById('chart-table-activity-' + domID);
    if (!actEl) return;
    var activity = echarts.init(actEl, null, {renderer: 'svg'});
    activity.group = groupKey;
    activity.setOption({
      ...t,
      toolbox: makeToolbox(),
      legend: { show: false },
      dataZoom: makeDataZoom(),
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

    var opsEl = document.getElementById('chart-table-ops-' + domID);
    if (!opsEl) return;
    var ops = echarts.init(opsEl, null, {renderer: 'svg'});
    ops.group = groupKey;
    ops.setOption({
      ...t,
      toolbox: makeToolbox(),
      legend: { top: 0, right: 48, textStyle: { color: muted, fontSize: 10 } },
      dataZoom: makeDataZoom(),
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

    echarts.connect(groupKey);
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
    Object.keys(tableCharts).forEach(function(key) {
      tableCharts[key].activity.dispose();
      tableCharts[key].ops.dispose();
    });
    tableCharts = {};
    document.querySelectorAll('tr.table-detail-row:not([hidden])').forEach(function(row) {
      var key = row.getAttribute('data-table-key');
      var summaryRow = document.querySelector('tr.table-summary-row[data-table-key="' + key + '"]');
      if (summaryRow) {
        var domID = summaryRow.getAttribute('data-table-dom-id');
        renderTableCharts(key, domID);
      }
    });
    renderThroughputChart();
  }

  document.querySelectorAll('.theme-btn').forEach(function(btn) {
    btn.addEventListener('click', function() { setTheme(btn.getAttribute('data-t')); });
  });

  // Table Row Expand/Collapse
  document.querySelectorAll('tr.table-summary-row.has-activity').forEach(function(row) {
    row.addEventListener('click', function() {
      var tableKey = row.getAttribute('data-table-key');
      var domID = row.getAttribute('data-table-dom-id');
      var detail = document.querySelector('tr.table-detail-row[data-table-key="' + tableKey + '"]');
      if (!detail) return;
      var isHidden = detail.hasAttribute('hidden');
      document.querySelectorAll('tr.table-detail-row').forEach(function(other) {
        other.setAttribute('hidden', '');
      });
      document.querySelectorAll('tr.table-summary-row').forEach(function(r) {
        r.classList.remove('open');
      });
      if (!isHidden) {
        detail.setAttribute('hidden', '');
        return;
      }
      detail.removeAttribute('hidden');
      row.classList.add('open');
      renderTableCharts(tableKey, domID);
    });
  });

  // Live Table Filtering
  var tableFilterInput = document.getElementById('table-filter-input');
  if (tableFilterInput) {
    tableFilterInput.addEventListener('input', function(e) {
      var query = (e.target.value || '').toLowerCase().trim();
      var summaryRows = document.querySelectorAll('tr.table-summary-row');
      summaryRows.forEach(function(row) {
        var key = (row.getAttribute('data-table-key') || '').toLowerCase();
        var match = key.indexOf(query) !== -1;
        row.style.display = match ? '' : 'none';
        var detail = document.querySelector('tr.table-detail-row[data-table-key="' + row.getAttribute('data-table-key') + '"]');
        if (detail && !match) {
          detail.setAttribute('hidden', '');
          row.classList.remove('open');
        }
      });
    });
  }

  // Table Column Sorting
  var currentSortCol = null;
  var currentSortAsc = true;
  document.querySelectorAll('#top-tables-table thead th.sortable').forEach(function(th) {
    th.addEventListener('click', function() {
      var sortKey = th.getAttribute('data-sort');
      if (currentSortCol === sortKey) {
        currentSortAsc = !currentSortAsc;
      } else {
        currentSortCol = sortKey;
        currentSortAsc = false; // default desc for metrics
      }
      var tbody = document.querySelector('#top-tables-table tbody');
      if (!tbody) return;
      var rows = Array.from(tbody.querySelectorAll('tr.table-summary-row'));
      rows.sort(function(a, b) {
        var aVal, bVal;
        if (sortKey === 'schema') {
          aVal = a.cells[0].textContent.trim();
          bVal = b.cells[0].textContent.trim();
          return currentSortAsc ? aVal.localeCompare(bVal) : bVal.localeCompare(aVal);
        } else if (sortKey === 'table') {
          aVal = a.cells[1].textContent.trim();
          bVal = b.cells[1].textContent.trim();
          return currentSortAsc ? aVal.localeCompare(bVal) : bVal.localeCompare(aVal);
        } else if (sortKey === 'bytes') {
          aVal = parseInt(a.cells[3].getAttribute('data-raw'), 10) || 0;
          bVal = parseInt(b.cells[3].getAttribute('data-raw'), 10) || 0;
          return currentSortAsc ? aVal - bVal : bVal - aVal;
        } else {
          var colIdx = { total: 2, bytes: 3, ins: 4, upd: 5, del: 6, ddl: 7, txns: 8 }[sortKey] || 2;
          aVal = parseInt(a.cells[colIdx].textContent.replace(/[^0-9]/g, ''), 10) || 0;
          bVal = parseInt(b.cells[colIdx].textContent.replace(/[^0-9]/g, ''), 10) || 0;
          return currentSortAsc ? aVal - bVal : bVal - aVal;
        }
      });
      rows.forEach(function(row) {
        tbody.appendChild(row);
        var key = row.getAttribute('data-table-key');
        var detail = tbody.querySelector('tr.table-detail-row[data-table-key="' + key + '"]');
        if (detail) tbody.appendChild(detail);
      });
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
      toolbox: makeToolbox(),
      legend: { textStyle: { color: muted, fontSize: 11 }, top: 2, left: 'center', itemGap: 24 },
      dataZoom: makeDataZoom(),
      grid: { top: 38, bottom: 35, left: 65, right: 65 },
      xAxis: {
        type: 'category',
        data: throughputLabels,
        axisLine: { lineStyle: { color: border } },
        axisLabel: { color: muted, fontSize: 11 }
      },
      yAxis: [
        {
          type: 'value',
          name: '{{t "report.html.analyze.binlogBytesShort"}}',
          nameTextStyle: { color: muted, fontSize: 11 },
          axisLabel: {
            color: muted,
            fontSize: 11,
            formatter: function(v) {
              if (v >= 1e9) return (v/1e9).toFixed(1) + ' GB';
              if (v >= 1e6) return (v/1e6).toFixed(1) + ' MB';
              if (v >= 1e3) return (v/1e3).toFixed(1) + ' KB';
              return v + ' B';
            }
          },
          splitLine: { lineStyle: { color: border, type: 'dashed' } }
        },
        {
          type: 'value',
          name: '{{t "report.html.common.rows"}}',
          nameTextStyle: { color: muted, fontSize: 11 },
          axisLabel: { color: muted, fontSize: 11 },
          splitLine: { show: false }
        }
      ],
      tooltip: { ...t.tooltip, trigger: 'axis' },
      series: [
        {
          name: '{{t "report.html.analyze.binlogBytesShort"}}',
          type: 'bar',
          data: throughputBytes,
          itemStyle: { color: primary, borderRadius: [4, 4, 0, 0] },
          barMaxWidth: 36
        },
        {
          name: '{{t "report.html.common.rows"}}',
          type: 'line',
          yAxisIndex: 1,
          data: throughputRows,
          smooth: 0.3,
          symbol: 'circle',
          symbolSize: 6,
          lineStyle: { color: accent, width: 2.5 }
        }
      ]
    });

    window.addEventListener('resize', function() { c.resize(); });
  }
  renderThroughputChart();

  // One-click Copy Helper
  document.querySelectorAll('button.copy-btn[data-copy]').forEach(function(btn) {
    btn.addEventListener('click', function(e) {
      e.stopPropagation();
      var text = btn.getAttribute('data-copy') || '';
      if (!text || !navigator.clipboard) return;
      navigator.clipboard.writeText(text).then(function() {
        var prev = btn.textContent;
        btn.textContent = '{{t "report.html.analyze.copied"}}';
        showToast('{{t "report.html.analyze.copied"}}');
        setTimeout(function() { btn.textContent = prev; }, 1500);
      });
    });
  });

  // Copy Executive Summary
  var copySummaryBtn = document.getElementById('btn-copy-summary');
  if (copySummaryBtn) {
    copySummaryBtn.addEventListener('click', function() {
      var summaryText = "# BinlogViz Analyze Report\n" +
        "- Transactions: {{.TotalTxns}}\n" +
        "- Affected Rows: {{.TotalRows}}\n" +
        "- Events: {{.TotalEvents}}\n" +
        "{{if .StartTime}}- Time Window: {{.StartTime}} -> {{.EndTime}} ({{.Duration}})\n{{end}}" +
        "- Generated: {{.GeneratedAt}}\n";
      if (navigator.clipboard) {
        navigator.clipboard.writeText(summaryText).then(function() {
          showToast("Summary copied to clipboard!");
        });
      }
    });
  }

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

  // Initialize Theme
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
