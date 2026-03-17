# Report Module

## Members

| File | Responsibility |
|------|----------------|
| `options.go` | Defines renderer presentation controls, including `summary/off/full` SQL context modes. |
| `text.go` | Renders the fixed five-section text report and applies SQL context visibility rules for transactions. |
| `json.go` | Serializes the stable JSON report shape and applies SQL context field visibility rules. |
| `*_test.go` | Verifies section ordering, JSON field stability, and SQL context mode behavior. |

## Interfaces

| API | Contract |
|-----|----------|
| `DefaultOptions() Options` | Returns the backward-compatible renderer defaults with `SQLContextMode=summary`. |
| `ParseSQLContextMode(raw string) (SQLContextMode, error)` | Validates CLI-provided SQL context modes: `summary`, `off`, `full`. |
| `RenderText(result model.AnalysisResult) (string, error)` | Renders the default backward-compatible text report. |
| `RenderTextWithOptions(result model.AnalysisResult, opts Options) (string, error)` | Renders the text report with explicit SQL context presentation controls. |
| `RenderJSON(result model.AnalysisResult) (string, error)` | Renders the default backward-compatible JSON report. |
| `RenderJSONWithOptions(result model.AnalysisResult, opts Options) (string, error)` | Renders the JSON report with explicit SQL context presentation controls. |

## Dependencies

- Upstream:
  - `internal/model` provides `AnalysisResult`, `Transaction`, and bounded `QueryContext` data.
- Downstream:
  - `cmd/binlogviz` passes CLI-selected SQL context mode into this module.

## Notes

- `summary` preserves the current default behavior.
- `off` omits query lines in text and omits all query-related JSON fields.
- `full` only exposes bounded SQL from `QueryContext.SQL`; it never reconstructs or emits unbounded original SQL.
