# Analyzer Module

## Members

| File | Responsibility |
|------|----------------|
| `analyzer.go` | Public analyzer entrypoint, streaming lifecycle, final result assembly. |
| `store.go` | DuckDB-backed internal result store with batch flush, reusable batch slices across flushes, pre-sized hot-path buffers, COUNT(*)-preallocated transaction row scans with lazy map hydration, and Finalize-time query assembly. |
| `transactions.go` | Reconstructs completed transactions from normalized event boundaries. |
| `tables.go` | Aggregates per-table row and operation totals. |
| `buckets.go` | Aggregates per-minute workload buckets and per-table minute rows, using a fast minute-truncation helper on the hot path. |
| `ddl.go` | Extracts DDL timeline metadata and fast-filters non-DDL ROWS_QUERY SQL before full tokenization. |
| `alerts.go` | Builds large transaction alerts from completed transactions. |
| `spikes.go` | Detects overall and table-level spike alerts from minute buckets. |
| `diagnostics.go` | Builds DBA-oriented findings with alert-referenced-only transaction indexing, bounded top-N transaction/minute rankings, hot intervals, and file throughput segments. Internal helpers are indexed lookups only; legacy linear scans have been removed. |
| `pattern_drilldowns.go` | Selects high-signal pattern drilldown candidates with bounded top-N representative transaction selection using bounded insertion sort instead of full-slice copy. |
| `report_aggregator.go` | Maintains bounded streaming state for report assembly so default analyze output does not require full transaction rehydration. |
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

- Stage 2 persists completed transactions, minute buckets, minute-level table rows, and alerts into DuckDB with a default `10000`-row batch flush threshold and a secondary approximate `4MB` byte threshold.
- DuckDB keeps the fixed Stage 2 schema; bounded `query_sql` for `--sql-context=full` is stored in the `transaction_sql_contexts` subtable and only resolved for final top transactions on demand, so `QueryAllTransactions()` stays metadata-only.
- Completed transactions and minute buckets are not mirrored indefinitely in Go heap; Finalize reads persisted rows back from DuckDB so large multi-file workloads stay bounded.
- Top-transaction reads query the bounded top-N transaction rows and hydrate only those requested keys from DuckDB side tables.
- Minute bucket reads query DuckDB at Finalize time instead of retaining all drained bucket snapshots in memory.
- Finalize computes alerts after transaction/minute queries and feeds them directly into findings/drilldowns, avoiding an extra DuckDB alert round-trip on the hot path.
- Live state remains bounded to the in-flight transaction builder, live table aggregates, current minute buckets pending flush, and summary counters.
- `MinuteAggregator.Snapshot()` returns defensive table-row copies, while `DrainBefore()` and `DrainAll()` transfer ownership of removed bucket maps to avoid copy churn in the streaming persistence path.
- Hot-path minute/table/transaction aggregation keeps table keys as structured internal identities and only materializes `schema.table` strings during final result projection.
- `New(opts)` is now the explicit no-external-resource path; only `NewWithStore` participates in command-managed DuckDB lifecycle.
- Command-layer streaming, CLI flag changes, renderer changes, benchmarks, and release tasks remain out of scope for this module revision.
