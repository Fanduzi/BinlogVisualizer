# Trend Module

Trend-result construction and renderer output for multi-snapshot historical review.

## Files

| File | Responsibility |
|------|----------------|
| `model.go` | Defines trend build inputs, structured comparability, completeness-aware ordered points and baseline deltas, table/pattern movement series, replay evidence, and renderer-facing results. |
| `build.go` | Validates snapshot metadata, orders inputs, assesses the optional baseline and every point, preserves raw metrics, and gates causal narratives on series comparability. |
| `patterns.go` | Builds per-pattern movement series across ordered trend points. |
| `summary.go` | Selects capped trend findings from pattern, table, and concentration changes. |
| `evidence.go` | Maps trend findings back to stable section anchors and evidence references. |
| `recommendations.go` | Derives prioritized follow-up recommendations from trend findings. |
| `drilldown.go` | Selects bounded pattern drilldowns for high-signal trend shifts. |
| `text.go` | Renders human-readable trend output for terminal and file workflows, including pattern trend summaries. |
| `json.go` | Serializes stable JSON trend output for automation, including `pattern_trends`. |
| `html.go` | Renders the self-contained HTML trend report with embedded ECharts charts, a Pattern Trends section, and transaction replay evidence. |
| `*_test.go` | Covers ordering, completeness and baseline preservation, selector-aware comparability, findings, drilldowns, trusted replay diagnostics, i18n, and renderer anchors. |

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
- One `unknown` or `not_comparable` baseline/point gates causal findings, recommendations, and drilldowns for the whole series and emits one first guard finding; raw points, movements, insights, and diagnostics remain available, while renderers derive the localized guard safely from the verdict.
- Persisted report-v3 position/GTID selection participates in the shared compare scope key, so one incompatible point gates causal narratives across the entire trend while retaining selector evidence; legacy selector absence stays unknown without hiding known v3 conflicts.
- Transaction trend points preserve the analyze-compatible span and replay command for each representative largest/longest transaction; missing or unusable spans remain command-free.
