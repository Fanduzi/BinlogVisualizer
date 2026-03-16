# Analyzer Module

## Members

| File | Responsibility |
|------|----------------|
| `analyzer.go` | Public analyzer entrypoint, streaming lifecycle, final result assembly. |
| `store.go` | DuckDB-backed internal result store with batch flush and Finalize-time query assembly. |
| `transactions.go` | Reconstructs completed transactions from normalized event boundaries. |
| `tables.go` | Aggregates per-table row and operation totals. |
| `buckets.go` | Aggregates per-minute workload buckets and per-table minute rows. |
| `alerts.go` | Builds large transaction alerts from completed transactions. |
| `spikes.go` | Detects overall and table-level spike alerts from minute buckets. |
| `*_test.go` | Verifies analyzer behavior, boundary handling, window filtering, and benchmark coverage. |

## Interfaces

| API | Contract |
|-----|----------|
| `New(opts Options) *Analyzer` | Creates a fresh analyzer with bounded in-memory live state and an internal in-memory result store. It does not create DuckDB temp resources. |
| `NewWithStore(opts Options, store *DuckDBStore) *Analyzer` | Creates an analyzer that uses a caller-managed DuckDB temp store. |
| `NewDuckDBStore(path string, batchRows int) (*DuckDBStore, error)` | Opens and initializes the internal DuckDB result store schema. |
| `(*Analyzer).Consume(ev model.NormalizedEvent) error` | Incrementally consumes one normalized event, applying time-window filtering and failing atomically on transaction-boundary errors. |
| `(*Analyzer).Finalize() (*model.AnalysisResult, error)` | Flushes in-flight state to DuckDB, queries persisted transactions/minutes/alerts, and assembles the final analysis result. Successful calls are idempotent. |
| `(*Analyzer).Analyze(events []model.NormalizedEvent) (*model.AnalysisResult, error)` | Compatibility wrapper that resets state, streams the slice through `Consume`, then calls `Finalize`. |
| `NewTransactionBuilder() *TransactionBuilder` | Reconstructs transaction boundaries and completed transaction snapshots. |
| `NewTableAggregator() *TableAggregator` | Tracks table-level aggregates for reporting. |
| `NewMinuteAggregator() *MinuteAggregator` | Tracks minute buckets for activity and spike detection. |

## Dependencies

- Upstream:
  - `internal/model` provides normalized event input plus result/report structures.
  - `cmd/binlogviz/analyze.go` creates a command-owned DuckDB temp store and injects it into `NewWithStore`.
- Downstream:
  - `internal/report` renders `model.AnalysisResult` produced by this module.
  - Analyzer tests and benchmarks validate ordering, aggregation, and failure semantics.

## Notes

- Stage 2 persists completed transactions, minute buckets, minute-level table rows, and alerts into DuckDB with a default `1000`-row batch flush threshold and a secondary approximate `4MB` byte threshold.
- Live state remains bounded to the in-flight transaction builder, live table aggregates, current minute buckets pending flush, and summary counters.
- `New(opts)` is now the explicit no-external-resource path; only `NewWithStore` participates in command-managed DuckDB lifecycle.
- Command-layer streaming, CLI flag changes, renderer changes, benchmarks, and release tasks remain out of scope for this module revision.
