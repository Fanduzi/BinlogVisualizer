# Report Module

Analyze report renderers for text, JSON, Markdown, and HTML output.

## Members

| File | Responsibility |
|------|----------------|
| `options.go` | Defines renderer presentation controls, including `summary/off/full` SQL context modes and table display limits. |
| `product.go` | Owns shared report presentation defaults, metric labels, and byte-coverage helpers used by all renderers. |
| `text.go` | Renders the concise diagnostic text report with separate input-file and counted-event byte metrics plus opt-in minute and write-shape detail sections. |
| `json.go` | Serializes the stable analyze JSON report shape, including counted event-byte diagnostics, optional transaction `xa_xid` and top-level snapshot metadata, and applies SQL context field visibility rules. |
| `markdown.go` | Renders GitHub-flavored Markdown output. |
| `html.go` | Renders the self-contained HTML report, including physical-file/count-event byte cards, responsive activity-chart layout, and ECharts data preparation. |
| `mysqlbinlog.go` | Formats transaction spans and builds copy-paste `mysqlbinlog` / `mariadb-binlog` commands with absolute file paths from usable spans; omits `--start-position` when only an XID interval is known. |
| `*_test.go` | Verifies diagnostic text defaults, opt-in detail sections, JSON field stability, snapshot behavior, SQL context mode behavior, and mysqlbinlog command emission. |

## Interfaces

| API | Contract |
|-----|----------|
| `DefaultOptions() Options` | Returns renderer defaults with `SQLContextMode=summary` and shared product limits. |
| `ParseSQLContextMode(raw string) (SQLContextMode, error)` | Validates CLI-provided SQL context modes: `summary`, `off`, `full`. |
| `RenderText(result model.AnalysisResult) (string, error)` | Renders the default concise diagnostic text report. |
| `RenderTextWithOptions(result model.AnalysisResult, opts Options) (string, error)` | Renders text output with explicit detail and SQL context presentation controls. |
| `RenderJSON(result model.AnalysisResult) (string, error)` | Renders the default backward-compatible JSON report, including `snapshot` when present. |
| `RenderJSONWithOptions(result model.AnalysisResult, opts Options) (string, error)` | Renders the JSON report with explicit presentation controls. |
| `FormatBinlogSpan(txn model.Transaction)` | Formats a transaction file span using the same display rules as analyze HTML. |
| `FormatReplayCommand(txn model.Transaction, serverVersion string)` | Builds the same safe replay command used by analyze JSON/HTML. |

## Dependencies

- Upstream:
  - `internal/model` provides complete `AnalysisResult`, `Transaction`, and bounded `QueryContext` data.
- Downstream:
  - `cmd/binlogviz` passes CLI-selected SQL context mode into this module.

## Notes

- `summary` preserves the current default behavior.
- `off` omits query lines in text and omits all query-related JSON fields.
- `full` only exposes bounded SQL from `QueryContext.SQL`; it never reconstructs or emits unbounded original SQL.
- `product.go` owns presentation defaults such as `DefaultTopN` so text, HTML, and command flags share one report contract.
- `AnalysisResult.Tables` is complete; JSON preserves every table, while human renderers apply `TopTables` and report how many tables were omitted.
- An explicit `TopTables=0` with `TopTablesSet=true` keeps human table sections unbounded; an omitted table limit inherits `TopN` for compatibility.
- The default text report is an incident brief: summary, hot tables, and largest transactions first; findings and activity come after. Minute activity and write-shape patterns require explicit detail options.
- Text Top Findings are the same `diagnostics.findings` / `alerts` as JSON. Hot intervals and longest transactions are evidence only; they are not synthesized into critical/warning findings.
- Text Next Actions shows a suspicious position only when a finding or alert references a transaction with recorded location evidence; clean workload diagnostics do not promote ranked evidence to an alert.
- The text Top Tables report sizes its table-name column to the widest displayed name; `Affected Rows` covers INSERT/UPDATE/DELETE rows and `Row Share` is that table's portion of all affected rows.
- Text rendering is intentionally kept on a fast path: it must not build HTML chart data, read embedded ECharts assets, or render pattern drilldowns unless detail options request them.
- When `summary.duration` is shorter than one second and there is at least one transaction, the text activity TPS peak is `N/A (sub-second)` (i18n) instead of `TxnCount/60`. Rows/min and JSON `timeseries.tps_series` stay numeric.
- The JSON renderer omits `snapshot` entirely when `AnalysisResult.Snapshot` is nil, preserving legacy analyze JSON shape.
- The HTML renderer keeps activity charts readable on large reports by using a larger responsive grid and suppressing non-essential legends that can overlap chart content.
- Analyze HTML shows a neutral DDL activity notice in Risks & Findings when DDL events exist without alerts; the healthy empty state is reserved for reports without alerts or DDL.
- The HTML renderer now follows a DBA reading path: executive summary (with key findings strip), risks & findings, activity overview, hot objects, then diagnostic evidence (transaction evidence, DDL timeline, pattern drilldowns, file coverage, binlog throughput).
- Analyze text and HTML label selected physical input-file bytes separately from counted filtered event bytes; missing selected-file size metadata renders as unavailable.
- Transaction evidence in HTML highlights the single current champion for largest, longest, and widest transactions instead of rendering a crowded top-N list.
