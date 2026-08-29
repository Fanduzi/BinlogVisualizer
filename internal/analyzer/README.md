# Analyzer Module

## Members

| File | Responsibility |
|------|----------------|
| `analyzer.go` | Public analyzer entrypoint, streaming lifecycle, final result assembly. |
| `store.go` | DuckDB-backed internal result store with batch flush, reusable batch slices across flushes, pre-sized hot-path buffers, COUNT(*)-preallocated transaction row scans with lazy map hydration, and Finalize-time query assembly. |
| `transactions.go` | Reconstructs completed transactions from MySQL and MariaDB XA boundaries, preserves XA XIDs and LOAD_DATA intent, clears query context and operation hints for filtered objects, and computes same-file `binlog_bytes` as `pos_end - pos_start`. |
| `tables.go` | Aggregates per-table row and operation totals. |
| `buckets.go` | Aggregates per-minute workload buckets and per-table minute rows, using a fast minute-truncation helper on the hot path. |
| `ddl.go` | Extracts DDL timeline metadata from Query and ROWS_QUERY SQL, including CREATE/ALTER/DROP DATABASE, RENAME, and TRUNCATE. |
| `alerts.go` | Builds large transaction alerts from completed transactions. |
| `spikes.go` | Detects overall and table-level spike alerts from minute buckets. |
| `diagnostics.go` | Builds DBA-oriented findings with alert-referenced-only transaction indexing, bounded top-N transaction/minute rankings, hot intervals, and file throughput segments. Internal helpers are indexed lookups only; legacy linear scans have been removed. |
| `pattern_drilldowns.go` | Selects high-signal pattern drilldown candidates. Representative transactions must share the pattern identity (table set + ops + shape); sub-1% shares stay visible. |
| `report_aggregator.go` | Maintains bounded streaming state for report assembly so default analyze output does not require full transaction rehydration. Tracks operation counts for timeseries, alert-referenced transactions for evidence, and txn-size histograms. |
| `detail_store.go` | Defines optional detail persistence backends. The default mode is `none`; DuckDB remains available for explicit detail storage. |
| `*_test.go` | Verifies analyzer behavior, boundary handling, window/object filtering, and benchmark coverage. |

## Interfaces

| API | Contract |
|-----|----------|
| `New(opts Options) *Analyzer` | Creates a fresh analyzer with bounded in-memory live state. When `DetailStoreMode` is `none` (default), uses a no-op detail store and generates reports from streaming aggregates without DuckDB. When `duckdb`, uses an in-memory store for detail persistence. |
| `NewWithStore(opts Options, store *DuckDBStore) *Analyzer` | Creates an analyzer that uses a caller-managed DuckDB temp store. Forces `DetailStoreMode` to `duckdb`. |
| `NewDuckDBStore(path string, batchRows int) (*DuckDBStore, error)` | Opens and initializes the internal DuckDB result store schema. |
| `(Options).HasObjectFilters() bool` | Reports whether any schema or table include/exclude filter is configured. |
| `(*Analyzer).Consume(ev model.NormalizedEvent) error` | Incrementally consumes one normalized event, applying time-window and object filtering before workload aggregation while retaining boundaries needed for transaction reconstruction. |
| `(*Analyzer).Finalize() (*model.AnalysisResult, error)` | Flushes in-flight state to DuckDB, queries persisted transactions/minutes/alerts, and assembles the final analysis result. Successful calls are idempotent. |
| `(*Analyzer).Analyze(events []model.NormalizedEvent) (*model.AnalysisResult, error)` | Compatibility wrapper that resets state, streams the slice through `Consume`, then calls `Finalize`. |
| `NewTransactionBuilder() *TransactionBuilder` | Reconstructs MySQL/MariaDB XA transaction boundaries and completed transaction snapshots. |
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
- `assembleResult()` reads from `ReportAggregator.Snapshot()` instead of `QueryAllTransactions()`, eliminating the full-transaction rehydration path for default report output. `QueryAllTransactions` is no longer called during Finalize.
- ReportAggregator receives events, transactions, and minute buckets during streaming, and DDL events at Finalize time. It maintains bounded top-N transaction lists, operation counts for timeseries, alert-referenced transaction evidence, and txn-size histograms.
- Active schema/table filters remove excluded row and DDL events before workload aggregation; control events remain available for transaction boundaries, and empty filtered transactions are omitted from reports.
- Alert-referenced transactions are tracked in a bounded map so `BuildFindingsFromAlerts` and `BuildPatternDrilldowns` can resolve evidence even when the referenced transaction is not in the top-5 largest.
- Pattern maps in snapshot use non-nil empty maps (`make(map[string]int)`) to match `BuildPatterns` semantics for `reflect.DeepEqual` parity.
- Top-transaction reads hydrate SQL on demand via `attachTopTransactionSQL` using the store's `ResolveTransactionQuerySQL`.
- Minute bucket reads query DuckDB at Finalize time instead of retaining all drained bucket snapshots in memory.
- Finalize computes alerts after transaction/minute queries and feeds them directly into findings/drilldowns, avoiding an extra DuckDB alert round-trip on the hot path.
- Live state remains bounded to the in-flight transaction builder, live table aggregates, current minute buckets pending flush, and summary counters.
- `MinuteAggregator.Snapshot()` returns defensive table-row copies, while `DrainBefore()` and `DrainAll()` transfer ownership of removed bucket maps to avoid copy churn in the streaming persistence path.
- Hot-path minute/table/transaction aggregation keeps table keys as structured internal identities and only materializes `schema.table` strings during final result projection.
- `New(opts)` is now the explicit no-external-resource path; only `NewWithStore` participates in command-managed DuckDB lifecycle.
- Command-layer streaming, CLI flag changes, renderer changes, benchmarks, and release tasks remain out of scope for this module revision.
