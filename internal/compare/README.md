# Compare Module

Compare-input validation, diff construction, and renderer output for text/JSON/HTML compare workflows.

## Files

| File | Responsibility |
|------|----------------|
| `load.go` | Loads and validates analyze report versions 0 through 3 from files or bytes, normalizing legacy missing completeness to unknown. |
| `model.go` | Defines compare input contracts that preserve report-v3 identity/scope, selection, provenance/SQL metadata, completeness, counted-event diagnostics, snapshot metadata, pattern shapes, and structured comparability evidence without inventing legacy identity. |
| `comparability.go` | Assesses explicit workload identity, provenance/flavor, configured scope, report version, and transaction completeness across inputs. |
| `diff.go` | Computes deterministic raw summary, completeness, table, pattern, operation, alert, and byte-coverage deltas, and gates causal findings, recommendations, and drilldowns on comparability. |
| `findings.go` | Selects capped key findings from compare deltas for downstream rendering. |
| `evidence.go` | Maps compare findings back to stable section anchors and evidence references. |
| `replay.go` | Preserves analyze transaction spans and replay commands for compare and trend evidence. |
| `recommendations.go` | Derives prioritized follow-up recommendations from compare findings. |
| `drilldown.go` | Selects bounded pattern drilldowns for high-signal compare changes. |
| `text.go` | Renders human-readable compare output, including named baseline/current input-file and counted-event byte metrics, table drift, pattern drift, and snapshot-aware context such as window, input mode, source summary, and filters. |
| `json.go` | Serializes compare results for downstream tools. |
| `html.go` | Renders the self-contained HTML compare report with snapshot-aware context, byte coverage, pattern drift, and current transaction replay evidence. |
| `*_test.go` | Covers loading, diff behavior, findings and drilldowns, renderer output, diagnostics/i18n sections, snapshot-aware context, and contract stability. |

## Exports

- `LoadReport(path string) (InputReport, error)` — Loads and validates a compare-compatible analyze JSON file.
- `DecodeReportJSON(data []byte) (InputReport, error)` — Validates compare-compatible analyze JSON from bytes for reuse by snapshot commands.
- `BuildCompareResult(current, baseline InputReport) CompareResult` — Produces a deterministic compare result from two validated inputs.
- `AssessComparability(inputs []ComparabilityInput) Comparability` — Returns the shared structured verdict and visible evidence used by compare and trend.
- `RenderText(result CompareResult) (string, error)` — Renders terminal-friendly compare output.
- `RenderJSON(result CompareResult) (string, error)` — Serializes compare results with stable JSON fields.
- `RenderHTML(result CompareResult) (string, error)` — Renders the chart-based compare HTML report.
- `TransactionEvidenceFor(txn InputTransaction) *TransactionEvidence` — Converts analyze transaction fields into replay-ready evidence while omitting unusable commands.

## Dependencies

- Upstream:
  - `cmd/binlogviz/compare.go` and `cmd/binlogviz/snapshot.go` call into this module.
  - `internal/report` provides shared transaction span and replay-command formatting.
- Downstream:
  - Compare outputs feed terminal, automation, and HTML review workflows.

## Notes

- Snapshot metadata in analyze JSON is optional; compare must continue to work when it is absent.
- Compare labels fall back to `current` / `baseline` when snapshot metadata is not present.
- Legacy file-based compare and snapshot-based compare share the same validated `InputReport` contract; v0-v2 absence never becomes fabricated identity.
- `large_transaction` alerts and largest-txn compare identity use content (dominant table, op, rows, file+span), not the local `txn-N` sequence number.
- Text compare prints a `Largest txn` line and marks `NEW` when the two sides do not share that content identity.
- A new write shape (`baseline=0`, `current>0`) uses `delta_percent: null` and text/HTML `new` instead of `0.0%`.
- `pattern_changes` is a first-class compare result area, separate from `table_changes`.
- Legacy reports with no top-level `patterns` are treated as empty pattern sets rather than rejected.
- Report v3 requires completeness and replay availability for every top-level transaction. Legacy v0-v2 reports remain loadable and missing completeness is exposed as `unknown`, never inferred as complete.
- Text and HTML outputs now expose snapshot context beyond the time window so incident reviews can see input mode, source shape, and active filters at a glance.
- Key findings, bounded drilldowns, evidence refs, and follow-up recommendations are derived inside this module from the same deterministic compare result.
- Current largest and longest transaction evidence preserves completeness and replay fields; a supplied command is trusted only when report v3 explicitly marks full-transaction replay available, so retained legacy or incomplete spans never synthesize a command.
- Compare HTML shows selected physical input-file bytes and counted event bytes in consistently labeled baseline/current columns; byte coverage is omitted from the stable compare JSON wire shape.
- Optional report-v3 selector evidence survives `DecodeReportJSON` unchanged so snapshots and compare callers can round-trip requested/effective position and GTID provenance.
- Raw deltas are always available. Ordinary causal findings, recommendations, and drilldowns require `comparable`; guarded results contain exactly one first `comparability_guard` finding, and renderers derive the localized guard directly from the verdict so public rendering remains safe even without a prebuilt finding slice.
- Only matching non-empty workload IDs prove shared identity. Server IDs/versions, observed schemas, filenames, and flavors remain visible evidence; legacy v0-v2 or insufficient completeness is `unknown`, while explicit identity/flavor/scope conflicts are `not_comparable`.
