# Model Module

Shared analysis-result contracts reused across parsing, aggregation, rendering, compare, and trend workflows.

## Members

| File | Responsibility |
|------|----------------|
| `event.go` | Defines normalized binlog event contracts emitted by `internal/binlog` and consumed by `internal/analyzer`. |
| `transaction.go` | Defines reconstructed transaction and bounded SQL context contracts. |
| `stats.go` | Defines workload, minute-bucket, and table-statistics contracts for analysis outputs. |
| `pattern.go` | Defines top-pattern summary contracts for repeated write shapes. |
| `pattern_drilldown.go` | Defines bounded pattern drilldown contracts for high-signal explanations. |
| `diagnostics.go` | Defines diagnostics, file coverage, DDL, and finding contracts for operator review. |
| `report.go` | Defines the top-level `AnalysisResult` envelope plus snapshot metadata contracts. |
| `timeseries.go` | Defines chart-ready timeseries and transaction-size histogram contracts. |
| `query_context.go` | Provides bounded SQL context constructors and truncation helpers. |
| `*_test.go` | Verifies shared model invariants and helper behavior. |

## Interfaces

| API | Contract |
|-----|----------|
| `NormalizedEvent` | Stable parser-to-analyzer event contract with bounded SQL context fields. |
| `Transaction`, `QueryContext` | Stable reconstructed transaction contract plus bounded SQL payload metadata. |
| `WorkloadSummary`, `MinuteBucket`, `TableStats`, `Timeseries` | Stable aggregate workload contracts for analyze renderers. |
| `PatternStats`, `PatternDrilldown`, `Diagnostics` | Stable higher-signal analysis contracts for findings, drilldowns, and operator review. |
| `AnalysisResult`, `Snapshot` | Stable top-level analyze report envelope reused by renderers and snapshot-based follow-on commands. |
| `NewQueryContext(sql string) *QueryContext` | Creates bounded SQL context from raw SQL text with truncation. |
| `NewQueryContextFromNormalized(sql string, truncated bool, originalBytes int) *QueryContext` | Rehydrates bounded SQL context from already-normalized values. |

## Dependencies

- Upstream:
  - `internal/binlog` populates `NormalizedEvent` and bounded query metadata.
  - `internal/analyzer` produces aggregate structs and final `AnalysisResult` values.
- Downstream:
  - `internal/report` renders analyze output from these shared contracts.
  - `internal/compare` and `internal/trend` consume snapshot-backed analyze report shapes derived from these contracts.
  - `cmd/binlogviz` passes these contracts across CLI, snapshot, and output workflows.

## Notes

- This module is contract-only: it should describe stable shared data shapes, not command or renderer policy.
- `Timeseries.TPSSeries` is a semantic contract for transactions-per-second rates computed from minute buckets as `TxnCount / 60`; older snapshots that stored raw transactions-per-minute counts must not be compared directly to newer avg-TPS values.
- Keep this README synchronized when shared model files, exported contracts, or cross-module boundaries change.
