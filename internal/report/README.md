# Report Module

Analyze report renderers for text, JSON, Markdown, and HTML output.

## Members

| File | Responsibility |
|------|----------------|
| `options.go` | Defines renderer presentation controls, including `summary/off/full` SQL context modes. |
| `product.go` | Owns shared report presentation defaults and metric labels used by all renderers. |
| `text.go` | Renders the concise diagnostic text report plus opt-in minute and write-shape detail sections. |
| `json.go` | Serializes the stable analyze JSON report shape, including optional top-level snapshot metadata, and applies SQL context field visibility rules. |
| `markdown.go` | Renders GitHub-flavored Markdown output. |
| `html.go` | Renders the self-contained HTML report, including responsive activity-chart layout and ECharts data preparation. |
| `*_test.go` | Verifies diagnostic text defaults, opt-in detail sections, JSON field stability, snapshot behavior, and SQL context mode behavior. |

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
- Text rendering is intentionally kept on a fast path: it must not build HTML chart data, read embedded ECharts assets, or render pattern drilldowns unless detail options request them.
- The JSON renderer omits `snapshot` entirely when `AnalysisResult.Snapshot` is nil, preserving legacy analyze JSON shape.
- The HTML renderer keeps activity charts readable on large reports by using a larger responsive grid and suppressing non-essential legends that can overlap chart content.
- The HTML renderer now follows a DBA reading path: executive summary (with key findings strip), risks & findings, activity overview, hot objects, then diagnostic evidence (transaction evidence, DDL timeline, pattern drilldowns, file coverage, binlog throughput).
- Transaction evidence in HTML highlights the single current champion for largest, longest, and widest transactions instead of rendering a crowded top-N list.
