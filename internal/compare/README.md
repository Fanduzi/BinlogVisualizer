# Compare Module

Compare-input validation, diff construction, and renderer output for text/JSON/HTML compare workflows.

## Files

| File | Responsibility |
|------|----------------|
| `load.go` | Loads and validates analyze JSON reports from files or in-memory bytes. |
| `model.go` | Defines compare input contracts, optional snapshot metadata, and compare result structures. |
| `diff.go` | Computes deterministic summary, table, operation, and alert deltas. |
| `text.go` | Renders human-readable compare output. |
| `json.go` | Serializes compare results for downstream tools. |
| `html.go` | Renders the self-contained HTML compare report. |
| `*_test.go` | Covers loading, diff behavior, renderer output, snapshot-aware labels, and contract stability. |

## Exports

- `LoadReport(path string) (InputReport, error)` — Loads and validates a compare-compatible analyze JSON file.
- `DecodeReportJSON(data []byte) (InputReport, error)` — Validates compare-compatible analyze JSON from bytes for reuse by snapshot commands.
- `BuildCompareResult(current, baseline InputReport) CompareResult` — Produces a deterministic compare result from two validated inputs.
- `RenderText(result CompareResult) (string, error)` — Renders terminal-friendly compare output.
- `RenderJSON(result CompareResult) (string, error)` — Serializes compare results with stable JSON fields.
- `RenderHTML(result CompareResult) (string, error)` — Renders the chart-based compare HTML report.

## Dependencies

- Upstream:
  - `cmd/binlogviz/compare.go` and `cmd/binlogviz/snapshot.go` call into this module.
- Downstream:
  - Compare outputs feed terminal, automation, and HTML review workflows.

## Notes

- Snapshot metadata in analyze JSON is optional; compare must continue to work when it is absent.
- Compare labels fall back to `current` / `baseline` when snapshot metadata is not present.
- Legacy file-based compare and snapshot-based compare share the same validated `InputReport` contract.
