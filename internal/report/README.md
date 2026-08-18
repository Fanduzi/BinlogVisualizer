# Report Module

Analyze report renderers for text, JSON, Markdown, and HTML output.

## Members

| File | Responsibility |
|------|----------------|
| `options.go` | Defines renderer presentation controls, including `summary/off/full` SQL context modes. |
| `product.go` | Owns shared report presentation defaults and metric labels used by all renderers. |
| `findings.go` | Builds the shared Top Findings list used by text and HTML so both stay in parity. |
| `text.go` | Renders the concise diagnostic text report plus opt-in minute and write-shape detail sections. |
| `json.go` | Serializes the stable analyze JSON report shape, including optional top-level snapshot metadata, and applies SQL context field visibility rules. |
| `markdown.go` | Renders GitHub-flavored Markdown output. |
| `html.go` | Renders the self-contained HTML incident page, including peak-minute evidence and demoted ECharts. |
| `*_test.go` | Verifies diagnostic text defaults, findings parity, opt-in detail sections, JSON field stability, snapshot behavior, and SQL context mode behavior. |

## Interfaces

| API | Contract |
|-----|----------|
| `DefaultOptions() Options` | Returns renderer defaults with `SQLContextMode=summary` and shared product limits. |
| `ParseSQLContextMode(raw string) (SQLContextMode, error)` | Validates CLI-provided SQL context modes: `summary`, `off`, `full`. |
| `RenderText(result model.AnalysisResult) (string, error)` | Renders the default concise diagnostic text report. |
| `RenderTextWithOptions(result model.AnalysisResult, opts Options) (string, error)` | Renders text output with explicit detail and SQL context presentation controls. |
| `RenderJSON(result model.AnalysisResult) (string, error)` | Renders the default backward-compatible JSON report, including `snapshot` when present. |
| `RenderJSONWithOptions(result model.AnalysisResult, opts Options) (string, error)` | Renders the JSON report with explicit presentation controls. |

## Dependencies

- Upstream:
  - `internal/model` provides `AnalysisResult`, `Transaction`, and bounded `QueryContext` data.
- Downstream:
  - `cmd/binlogviz` passes CLI-selected SQL context mode into this module.

## Notes

- `summary` preserves the current default behavior.
- `off` omits query lines in text and omits all query-related JSON fields.
- `full` only exposes bounded SQL from `QueryContext.SQL`; it never reconstructs or emits unbounded original SQL.
- `product.go` owns presentation defaults such as `DefaultTopN` so text, HTML, and command flags share one report contract.
- The default text report is diagnostic-first: summary, top findings, top tables, and next actions. Minute activity and write-shape patterns require explicit detail options.
- The text Top Tables report sizes its table-name column to the widest displayed name; `Affected Rows` covers INSERT/UPDATE/DELETE rows and `Row Share` is that table's portion of all affected rows.
- Text rendering is intentionally kept on a fast path: it must not build HTML chart data, read embedded ECharts assets, or render pattern drilldowns unless detail options request them.
- The JSON renderer omits `snapshot` entirely when `AnalysisResult.Snapshot` is nil, preserving legacy analyze JSON shape.
- The HTML renderer keeps activity charts readable on large reports by using a larger responsive grid and suppressing non-essential legends that can overlap chart content.
- TPS/min and rows-per-minute keep a visible circle (size 10) when a series has fewer than 2 points so a Duration-0s / single-minute report does not render an empty polyline.
- The HTML renderer follows an incident reading path: one-line verdict (same findings as text), peak minute with that minute's tables, hottest table and largest transaction, remaining findings, hot objects, diagnostic evidence, then demoted activity charts. Theme switchers sit in the footer.
- HTML never treats an empty `alerts` slice as a healthy workload. It renders `collectDisplayFindings`, which matches the text Top Findings list (diagnostics findings, then a synthesized write spike / longest txn / DDL).
- Rollback hints use `firstUsableRollbackLocation`. A recorded 31-byte XID interval (dogfood #18) is shown as an XID-only span and is not offered as `--start-position`. The renderer does not invent a real start pos.
- Pattern drilldown representative transactions stay a static list. The incident page does not add a click path onto those keys (dogfood #20).
- Transaction evidence in HTML highlights the single current champion for largest, longest, and widest transactions instead of rendering a crowded top-N list.
