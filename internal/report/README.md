# Report Module

Analyze report renderers for text, JSON, Markdown, and HTML output.

## Members

| File | Responsibility |
|------|----------------|
| `options.go` | Defines renderer presentation controls, including `summary/off/full` SQL context modes and table display limits. |
| `product.go` | Owns shared report presentation defaults, metric labels, and byte-coverage helpers used by all renderers. |
| `text.go` | Owns shared UTC human timestamp formatting and renders completeness-aware incident briefs with separate input-file and counted-event byte metrics plus opt-in minute and write-shape detail sections. |
| `json.go` | Serializes report v3 with explicit workload identity/scope, RFC3339 `Z` timestamps, requested/effective position and GTID evidence, transaction list/completeness counts, counted bytes, producer provenance, SQL metadata, bounded query fields, XA identity, and explicit safe replay metadata. |
| `markdown.go` | Renders UTC-labelled GitHub-flavored Markdown incident records with transaction completeness/span/replay, DDL, input-format, and finding evidence. |
| `html.go` | Renders the self-contained HTML report with UTC-labelled ranges/evidence/charts, completeness and byte cards, deduplicated transaction evidence, transaction-key lookup, human-only table limits, responsive charts, and trusted replay commands. |
| `html_chrome.go` | Shared five-theme CSS tokens spliced into analyze, compare, and trend HTML. |
| `mysqlbinlog.go` | Formats retained evidence spans and builds `mysqlbinlog` / `mariadb-binlog` commands only from trusted single-file full replay spans, preferring per-transaction producer versions. |
| `*_test.go` | Verifies UTC timestamp presentation, RFC3339 JSON stability, complete JSON tables and selector round trips, Markdown/HTML evidence, deadlock-free stdout wrappers, SQL modes, counted bytes, completeness, and explicit full-span replay behavior. |

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
- `off` omits query summaries, full SQL, and truncation metadata from transaction, pattern, and drilldown JSON projections while leaving provenance unchanged.
- `summary` emits one whitespace-normalized line bounded to 160 characters.
- `full` only exposes UTF-8-safe SQL bounded to 4096 bytes from `QueryContext.SQL`; it never reconstructs row values or unbounded original SQL.
- JSON always records the selected SQL-context mode and report-wide source-SQL availability; `full` with `available=false` is valid.
- `product.go` owns presentation defaults such as `DefaultTopN` so text, HTML, and command flags share one report contract.
- Human analyze timestamps are normalized to UTC and display a `UTC` suffix; each human summary also states `Timestamps: UTC (binlog)`. Report-v3 JSON keeps machine-readable RFC3339 timestamps normalized to `Z`.
- `AnalysisResult.Tables` is complete; JSON preserves every table, while human renderers apply `TopTables` and report how many tables were omitted.
- An explicit `TopTables=0` with `TopTablesSet=true` keeps human table sections unbounded; an omitted table limit inherits `TopN` for compatibility.
- The default text report is an incident brief: summary, hot tables, and largest transactions first; findings and activity come after. Minute activity and write-shape patterns require explicit detail options.
- Text Top Findings are the same `diagnostics.findings` / `alerts` as JSON. Hot intervals and longest transactions are evidence only; they are not synthesized into critical/warning findings.
- Text Next Actions shows a suspicious position only when a finding or alert references a transaction with recorded location evidence; clean workload diagnostics do not promote ranked evidence to an alert.
- The text Top Tables report sizes its table-name column to the widest displayed name; `Affected Rows` covers INSERT/UPDATE/DELETE rows and `Row Share` is that table's portion of all affected rows.
- Text rendering is intentionally kept on a fast path: it must not build HTML chart data, read embedded ECharts assets, or render pattern drilldowns unless detail options request them.
- When `summary.duration` is shorter than one second and there is at least one transaction, the text activity TPS peak is `N/A (sub-second)` (i18n) instead of `TxnCount/60`. Rows/min and JSON `timeseries.tps_series` stay numeric.
- Report v3 always emits transaction completeness and replay availability plus summary partial/unknown counts. Compare and snapshot loaders continue accepting legacy report versions and treat missing completeness as unknown.
- Report v3 persists optional `workload_id` and exact configured `scope`; only the explicit non-empty workload token can positively identify the same workload across reports.
- The JSON renderer omits `snapshot` entirely when `AnalysisResult.Snapshot` is nil, preserving optional snapshot behavior.
- The HTML renderer keeps activity charts readable on large reports by using a larger responsive grid and suppressing non-essential legends that can overlap chart content.
- Analyze HTML shows a neutral DDL activity notice in Risks & Findings when DDL events exist without alerts; the healthy empty state is reserved for reports without alerts or DDL.
- The HTML renderer now follows a DBA reading path: executive summary (with key findings strip), risks & findings, activity overview, hot objects, then diagnostic evidence (transaction evidence, DDL timeline, pattern drilldowns, file coverage, binlog throughput).
- Analyze text and HTML label selected physical input-file bytes separately from counted filtered event bytes; missing selected-file size metadata renders as unavailable.
- Transaction evidence in HTML renders each category champion once, annotates every category it wins, and keeps a separate lookup for the bounded transaction payload.
- JSON always reports `transactions_listed` and `transactions_omitted` alongside the bounded `transactions` array.
- Rendered transaction evidence and pattern representatives expose transaction keys to the HTML search control.
- Report v3 emits optional top-level `selection` evidence: requested/effective positions, canonical include/exclude GTIDs, resolved flavor, and matched identities. Existing provenance, SQL, completeness, and replay fields are unchanged.
