# Model Module

Shared analysis-result contracts reused across parsing, aggregation, rendering, compare, and trend workflows.

## Members

| File | Responsibility |
|------|----------------|
| `event.go` | Defines normalized binlog events with optional producer/transaction provenance, XA identity, and LOAD_DATA intent. |
| `transaction.go` | Defines reconstructed transaction evidence with provenance, explicit completeness, optional trusted full replay spans, XA identity, and bounded SQL context. |
| `stats.go` | Defines workload, minute-bucket, and table-statistics contracts for analysis outputs. |
| `pattern.go` | Defines top-pattern summary contracts for repeated write shapes. |
| `pattern_drilldown.go` | Defines bounded pattern drilldown contracts for high-signal explanations. |
| `diagnostics.go` | Defines diagnostics, selected-file coverage, counted filtered event bytes, DDL, finding, and Format Description server-version contracts for operator review. |
| `report.go` | Defines `AnalysisResult`, explicit workload identity and analyzed scope, requested/effective selector evidence, deterministic report-level producer provenance, report-wide SQL availability, and snapshot metadata. |
| `timeseries.go` | Defines chart-ready timeseries and transaction-size histogram contracts. |
| `query_context.go` | Provides bounded SQL context constructors and truncation helpers. |
| `*_test.go` | Verifies shared model invariants and helper behavior. |

## Interfaces

| API | Contract |
|-----|----------|
| `NormalizedEvent` | Stable parser-to-analyzer event contract with optional provenance and bounded SQL context fields. |
| `Transaction`, `TransactionCompleteness`, `TransactionReplaySpan`, `QueryContext` | Stable retained-evidence contract with provenance, complete/partial/unknown status, optional trusted full replay span, XA identity, and bounded SQL metadata. |
| `WorkloadSummary`, `MinuteBucket`, `TableStats`, `Timeseries` | Stable aggregate workload contracts for analyze renderers, including partial and unknown transaction counts. |
| `PatternStats`, `PatternDrilldown`, `Diagnostics` | Stable higher-signal analysis contracts for findings, drilldowns, and operator review. |
| `AnalysisResult`, `AnalysisSelection`, `ReportProvenance`, `Snapshot`, `SnapshotFilters` | Stable report/snapshot contracts; workload identity and exact scope remain explicit, selection evidence and producer sets stay canonical, and SQL availability covers the whole report. |
| `NewQueryContext(sql string) *QueryContext` | Creates bounded SQL context from raw SQL text with truncation. |
| `NewQueryContextFromNormalized(sql string, truncated bool, originalBytes int) *QueryContext` | Rehydrates bounded SQL context from already-normalized values. |
| `(Transaction).FullReplayAvailable() bool` | Reports whether trusted full evidence is safe to expose as one bounded replay command; cross-file endpoints without an intermediate-file list are unavailable. |

## Dependencies

- Upstream:
  - `internal/binlog` populates `NormalizedEvent` and bounded query metadata.
  - `internal/analyzer` produces aggregate structs and final `AnalysisResult` values.
- Downstream:
  - `internal/report` renders analyze output from these shared contracts.
  - `internal/compare` and `internal/trend` consume snapshot-backed analyze report shapes derived from these contracts.
  - `cmd/binlogviz` passes these contracts across CLI, snapshot, and output workflows.

## Notes

- This module is contract-only: it should describe stable shared data shapes, not command or renderer policy. `Alert.Type` includes `large_transaction`, `spike`, `input_format`, `partial_transaction`, and `unknown_transaction`.
- `Timeseries.TPSSeries` is a semantic contract for transactions-per-second rates computed from minute buckets as `TxnCount / 60`; older snapshots that stored raw transactions-per-minute counts must not be compared directly to newer avg-TPS values.
- `Diagnostics.CountedEventBytes` is the filtered row/DDL event-byte coverage; `FileCoverageItem.Size` is physical input-file size and is unavailable when metadata is missing.
- Keep this README synchronized when shared model files, exported contracts, or cross-module boundaries change.
