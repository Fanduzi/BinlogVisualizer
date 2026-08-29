# Trend Module

Trend-result construction and renderer output for multi-snapshot historical review.

## Files

| File | Responsibility |
|------|----------------|
| `model.go` | Defines trend build inputs, ordered point models, baseline deltas, table and pattern movement series, replay evidence, and renderer-facing result contracts. |
| `build.go` | Validates snapshot metadata, keeps explicit/workflow input order by default (`--order cli`), optionally sorts by `snapshot.window.start_time` (`--order time`), computes per-point operation totals, baseline deltas, table and pattern movement series, transaction replay evidence, and aggregate insights. |
| `patterns.go` | Builds per-pattern movement series across ordered trend points. |
| `summary.go` | Selects capped trend findings from pattern, table, and concentration changes. |
| `evidence.go` | Maps trend findings back to stable section anchors and evidence references. |
| `recommendations.go` | Derives prioritized follow-up recommendations from trend findings. |
| `drilldown.go` | Selects bounded pattern drilldowns for high-signal trend shifts. |
| `text.go` | Renders human-readable trend output for terminal and file workflows, including pattern trend summaries. |
| `json.go` | Serializes stable JSON trend output for automation, including `pattern_trends`. |
| `html.go` | Renders the self-contained HTML trend report with embedded ECharts charts, a Pattern Trends section, and transaction replay evidence. |
| `*_test.go` | Covers ordering, baseline delta behavior, findings and drilldowns, diagnostics/i18n sections, pattern trend output, and renderer anchors or sections. |

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
- Default ordering is the operator's input order (`cli`): explicit `trend A B` stays A→B, and workflow snapshot lists stay as written. `--order time` sorts by `snapshot.window.start_time` and records `reordered` when that changes the story.
- Baseline handling is additive: a baseline can be loaded for deltas without becoming a plotted trend point.
- Pattern trends are first-class trend data and are available in text, JSON, and HTML output.
- Trend summary findings, bounded drilldowns, evidence refs, and recommendations are derived inside this module from the same deterministic trend result.
- Transaction trend points preserve the analyze-compatible span and replay command for each representative largest/longest transaction; missing or unusable spans remain command-free.
