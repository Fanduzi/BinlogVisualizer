# Trend Module

Trend-result construction and renderer output for multi-snapshot historical review.

## Files

| File | Responsibility |
|------|----------------|
| `model.go` | Defines trend build inputs, ordered point models, baseline deltas, table and pattern movement series, and renderer-facing result contracts. |
| `build.go` | Validates snapshot metadata, orders trend points by `snapshot.window.start_time`, computes per-point operation totals, baseline deltas, table and pattern movement series, and aggregate insights. |
| `text.go` | Renders human-readable trend output for terminal and file workflows, including pattern trend summaries. |
| `json.go` | Serializes stable JSON trend output for automation, including `pattern_trends`. |
| `html.go` | Renders the self-contained HTML trend report with embedded ECharts charts and a Pattern Trends section. |
| `*_test.go` | Covers ordering, baseline delta behavior, pattern trend output, and renderer anchors/sections. |

## Exports

- `BuildResult(opts BuildOptions) (Result, error)` — Builds the ordered multi-snapshot trend result.
- `RenderText(result Result) (string, error)` — Renders terminal-friendly trend output.
- `RenderJSON(result Result) (string, error)` — Serializes machine-readable trend output.
- `RenderHTML(result Result) (string, error)` — Renders the chart-based HTML trend report.

## Dependencies

- Upstream:
  - `internal/compare` provides the analyze JSON input contract reused by trend snapshots.
  - `internal/report` provides the embedded ECharts runtime used by the HTML renderer.
- Downstream:
  - `cmd/binlogviz/trend.go` loads snapshot reports and calls this module for build/render work.

## Notes

- Trend input is snapshot-oriented; it does not accept raw binlog files directly.
- Ordering is always derived from `snapshot.window.start_time`, not CLI order or snapshot creation time.
- Baseline handling is additive: a baseline can be loaded for deltas without becoming a plotted trend point.
- Pattern trends are first-class trend data and are available in text, JSON, and HTML output.
