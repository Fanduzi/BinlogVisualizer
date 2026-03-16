# Analyzer Module

## Members

| File | Responsibility |
|------|----------------|
| `analyzer.go` | Public analyzer entrypoint, streaming lifecycle, final result assembly. |
| `transactions.go` | Reconstructs completed transactions from normalized event boundaries. |
| `tables.go` | Aggregates per-table row and operation totals. |
| `buckets.go` | Aggregates per-minute workload buckets and per-table minute rows. |
| `alerts.go` | Builds large transaction alerts from completed transactions. |
| `spikes.go` | Detects overall and table-level spike alerts from minute buckets. |
| `*_test.go` | Verifies analyzer behavior, boundary handling, window filtering, and benchmark coverage. |

## Interfaces

| API | Contract |
|-----|----------|
| `New(opts Options) *Analyzer` | Creates a fresh analyzer with bounded in-memory live state. |
| `(*Analyzer).Consume(ev model.NormalizedEvent) error` | Incrementally consumes one normalized event, applying time-window filtering and failing atomically on transaction-boundary errors. |
| `(*Analyzer).Finalize() (*model.AnalysisResult, error)` | Flushes in-flight state and assembles the final analysis result. Successful calls are idempotent. |
| `(*Analyzer).Analyze(events []model.NormalizedEvent) (*model.AnalysisResult, error)` | Compatibility wrapper that resets state, streams the slice through `Consume`, then calls `Finalize`. |
| `NewTransactionBuilder() *TransactionBuilder` | Reconstructs transaction boundaries and completed transaction snapshots. |
| `NewTableAggregator() *TableAggregator` | Tracks table-level aggregates for reporting. |
| `NewMinuteAggregator() *MinuteAggregator` | Tracks minute buckets for activity and spike detection. |

## Dependencies

- Upstream:
  - `internal/model` provides normalized event input plus result/report structures.
  - `cmd/binlogviz/analyze.go` constructs `Analyzer` and still calls the compatibility `Analyze` wrapper in Stage 1.
- Downstream:
  - `internal/report` renders `model.AnalysisResult` produced by this module.
  - Analyzer tests and benchmarks validate ordering, aggregation, and failure semantics.

## Notes

- Stage 1 Phase 2 keeps the analyzer's live state bounded to the in-flight transaction, table aggregates, minute buckets, and summary counters.
- DuckDB integration, command-layer streaming, CLI flag changes, and renderer changes are intentionally out of scope for this module revision.
