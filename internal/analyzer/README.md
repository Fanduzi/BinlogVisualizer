# Analyzer Module

## Members

| File | Responsibility |
|------|----------------|
| `analyzer.go` | Public analyzer entrypoint, intersected time/position windows, deferred complete-group GTID filtering, filter-excluded DDL boundary forwarding, streaming lifecycle, selector evidence, and final result assembly retaining row transactions plus physically located zero-row XA evidence. |
| `gtid_selector.go` | Parses canonical MySQL UUID sequence/range sets and exact MariaDB identities, resolves one selector flavor, applies exclude-wins matching, and rejects anonymous groups while a selector is active. |
| `store.go` | Persists transaction provenance, bounded SQL, completeness, and replay spans through the DuckDB detail path, with batch flush, reusable hot-path buffers, and on-demand SQL hydration. |
| `transactions.go` | Reconstructs MySQL/MariaDB transaction evidence, closes GTID-started DDL groups at their implicit boundary, records complete/partial/unknown status, preserves canonical GTID/server/thread/XID/actor/XA evidence (including zero-row XA COMMIT groups) and LOAD_DATA intent, rejects true in-transaction GTID conflicts, and separates retained evidence from trusted full replay spans. |
| `tables.go` | Aggregates per-table row and operation totals. |
| `buckets.go` | Aggregates per-minute workload buckets and per-table minute rows, using a fast minute-truncation helper on the hot path. |
| `ddl.go` | Extracts DDL timeline metadata from Query and ROWS_QUERY SQL, including CREATE/ALTER/DROP DATABASE, RENAME, and TRUNCATE. |
| `alerts.go` | Builds large transaction alerts from completed transactions. |
| `spikes.go` | Detects overall and table-level spike alerts from minute buckets. |
| `diagnostics.go` | Builds DBA-oriented findings with alert-referenced-only transaction indexing, bounded top-N transaction/minute rankings, hot intervals, and file throughput segments. Internal helpers are indexed lookups only; legacy linear scans have been removed. |
| `pattern_drilldowns.go` | Selects high-signal pattern drilldown candidates. Representative transactions must share the pattern identity (table set + ops + shape); sub-1% shares stay visible. |
| `report_aggregator.go` | Maintains bounded report state, filtered event-byte coverage, SQL availability, producer sets, and numeric-key-ordered transaction evidence while excluding incomplete transactions from whole-transaction rankings, patterns, histograms, and ordinary large alerts. |
| `detail_store.go` | Defines optional detail persistence backends. The default mode is `none`; DuckDB remains available for explicit detail storage. |
| `*_test.go` | Verifies intersected selection, complete/partial replay evidence, GTID flavor/precedence/rotation/DDL/XA/LOAD_DATA behavior, object filtering, detail-store parity, and benchmarks. |

## Interfaces

| API | Contract |
|-----|----------|
| `New(opts Options) *Analyzer` | Creates a fresh analyzer with bounded in-memory live state. When `DetailStoreMode` is `none` (default), uses a no-op detail store and generates reports from streaming aggregates without DuckDB. When `duckdb`, uses an in-memory store for detail persistence. |
| `NewWithStore(opts Options, store *DuckDBStore) *Analyzer` | Creates an analyzer that uses a caller-managed DuckDB temp store. Forces `DetailStoreMode` to `duckdb`. |
| `NewDuckDBStore(path string, batchRows int) (*DuckDBStore, error)` | Opens and initializes the internal DuckDB result store schema. |
| `(Options).HasObjectFilters() bool` | Reports whether any schema or table include/exclude filter is configured. |
| `(Options).HasPositionSelectors() bool`, `(Options).HasGTIDSelectors() bool` | Report active exact-position or complete-group GTID selectors. |
| `ParseGTIDSelector(include, exclude []string) (*GTIDSelector, error)` | Parses and canonicalizes one explicit MySQL or MariaDB selector flavor. |
| `Options.WorkloadID` | Carries the optional operator-provided workload token into report v3 without inference. |
| `(*Analyzer).Consume(ev model.NormalizedEvent) error` | Incrementally consumes one normalized event. Workload totals remain inclusive, filtered, and event-window scoped while adjacent physical events may supply transaction-boundary evidence only. |
| `(*Analyzer).Finalize() (*model.AnalysisResult, error)` | Flushes in-flight state to DuckDB, queries persisted transactions/minutes/alerts, and assembles the complete final analysis result. Successful calls are idempotent. |
| `(*Analyzer).Analyze(events []model.NormalizedEvent) (*model.AnalysisResult, error)` | Compatibility wrapper that resets state, streams the slice through `Consume`, then calls `Finalize`. |
| `NewTransactionBuilder() *TransactionBuilder` | Reconstructs GTID-aware MySQL/MariaDB XA transaction boundaries and fails on conflicting canonical GTIDs. |
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
- DuckDB stores optional provenance in `transactions`; bounded `query_sql` remains in `transaction_sql_contexts` and is resolved only for selected transactions, so `QueryAllTransactions()` stays SQL-metadata-only.
- `assembleResult()` reads from `ReportAggregator.Snapshot()` instead of `QueryAllTransactions()`, eliminating the full-transaction rehydration path for default report output. `QueryAllTransactions` is no longer called during Finalize.
- Final table aggregates remain complete and deterministically ordered regardless of `Options.TopTables`; human report renderers apply that presentation limit after totals and shares are known.
- ReportAggregator receives events, transactions, and minute buckets during streaming, and DDL events at Finalize time. It maintains bounded top-N transaction lists, operation counts for timeseries, alert-referenced transaction evidence, and txn-size histograms.
- ReportAggregator keeps the configured transaction list bounded by default; complete rankings come first, remaining slots contain incomplete evidence in natural numeric `txn-N` order, and `TopTransactions=0` is the explicit unlimited mode.
- Detail-store parity covers provenance, XA identity, retained/full spans, completeness, maps, and bounded SQL metadata in both in-memory and DuckDB paths.
- Time windows keep inclusive per-event workload totals. Adjacent parsed events may establish transaction completeness and a trusted full replay span, but never add out-of-window rows, events, table totals, operations, or minute buckets.
- Active schema/table filters remove excluded row and DDL events before workload aggregation; control events remain available for transaction boundaries, and empty filtered transactions are omitted from reports.
- Final results preserve `Options.WorkloadID` and the canonical configured include/exclude filters as comparability evidence.
- ReportAggregator derives counted event bytes from filtered row/DDL minute buckets; physical selected-file bytes remain in command-supplied file coverage.
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
- Position and time predicates intersect per event while the transaction builder continues observing adjacent evidence. Known physical boundaries may retain a trusted `FullReplaySpan`; unresolved cuts remain `unknown` with no replay span.
- Active GTID selectors delay aggregate fan-out until each transaction group closes, reject unresolved group boundaries, preserve ordered cross-file groups without replaying cross-file spans, and produce identical in-memory/DuckDB results.
- Active GTID selectors never fan out unattributable anonymous events and never retain anonymous groups, including when only exclude selectors are configured.
