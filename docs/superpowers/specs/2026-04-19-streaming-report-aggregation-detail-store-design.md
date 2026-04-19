# Streaming Report Aggregation And Optional Detail Store Design

## Summary

This design changes the analyze pipeline from a DuckDB-centered finalize model into a streaming report model with an optional detail store.

The current implementation still depends on `QueryAllTransactions()` during `Analyzer.assembleResult()`. That means the analyzer writes completed transactions into DuckDB, flushes them, then reads all transactions back to build summary, patterns, timeseries, diagnostics, findings, and pattern drilldowns. Recent optimization reduced some allocation pressure, but the architecture still scales with total transaction count during finalize.

The target architecture is:

```text
binlog parser
  -> analyzer streaming fan-out
     -> report aggregators required for JSON/HTML/Text
     -> optional detail store for later transaction-level lookup
  -> final report assembled from bounded streaming state
```

The first two implementation phases are mandatory. Parser replacement remains a third, gated phase and should not start until the first two phases have real-binlog measurements.

## Goals

- Make default `binlogviz analyze` report generation independent of full transaction rehydration.
- Keep JSON, HTML, text, compare, and trend report contracts compatible.
- Keep memory bounded for multi-file and 100 GB-class analysis by avoiding full in-memory transaction slices.
- Preserve DBA-facing diagnostics: TPS, operation event series, DDL timeline, hot intervals, top tables, largest/longest/widest transaction evidence, file coverage, file segments, findings, and pattern drilldowns.
- Keep DuckDB available as an optional detail backend rather than a mandatory report-generation dependency.
- Use TDD and real-binlog benchmarks to decide whether a third parser-layer optimization phase is necessary.

## Non-Goals

- No `binlog2sql`, rollback SQL, or row-value reconstruction.
- No full SQL reconstruction beyond existing bounded query context.
- No change to MySQL target scope: MySQL 5.7/8.0 ROW-format binlog and relay-log-equivalent streams only.
- No parser fork in phases 1 or 2.
- No removal of DuckDB support. DuckDB remains valuable for optional detail inspection and future ad-hoc querying.
- No JSON field removals in the default report output.

## Current Coupling

`Analyzer.assembleResult()` currently executes this finalize flow:

```text
QueryAllTransactions()
QueryTopTransactions()
QueryMinuteBuckets()
DetectLargeTransactionAlerts(allTransactions)
buildSummary(allTransactions)
BuildPatterns(allTransactions)
timeseriesAgg.Snapshot(minutes, allTransactions)
SelectDiagnosticTransactions(allTransactions)
SelectWidestTransactions(allTransactions)
BuildFindingsFromAlerts(alerts, minutes, allTransactions, ddlTimeline)
BuildPatternDrilldowns(patterns, minutes, allTransactions, alerts)
```

This creates two architectural problems:

- Report output depends on a full transaction slice even though most report fields only need aggregates or top-N champions.
- DuckDB is part of the required report path, not a detail backend. If DuckDB is slow, alloc-heavy, or unsuitable for a huge input, default analyze cannot bypass it.

## Target Architecture

### Phase 1: Single-Pass Report Aggregation

Introduce analyzer-owned streaming report aggregation that receives the same canonical events already accepted by the current analyzer:

- normalized events after time-window and schema/table filtering
- completed transactions from `TransactionBuilder`
- drained minute buckets from `MinuteAggregator`
- DDL events from `DDLAggregator`
- file coverage injected by the CLI planner

The aggregator owns the report data that currently requires full finalize scans:

- workload summary totals
- warning count
- transaction-size histogram
- largest transactions top-N
- longest transactions top-N
- widest transactions top-N
- large-transaction alerts and findings
- pattern aggregates
- pattern drilldown candidate data
- minute buckets needed for charts and hot intervals
- file segments

The analyzer still may write to DuckDB during this phase, but the final report must be assembled from streaming report state. The implementation is complete only when `Analyzer.assembleResult()` no longer calls `QueryAllTransactions()` for default report fields.

### Phase 2: Optional Detail Store

Split the storage responsibilities:

```go
type DetailStore interface {
    Reset() error
    RecordTransactions([]persistedTransaction) error
    RecordMinuteBuckets([]model.MinuteBucket) error
    RecordAlerts([]model.Alert) error
    Flush() error
    ResolveTransactionQuerySQL(txnKeys []string) (map[string]string, error)
    Close() error
}
```

The default report path should be able to use a no-op detail store:

```text
--detail-store=none
```

DuckDB remains available:

```text
--detail-store=duckdb
```

The analyzer must produce equivalent default JSON/HTML/Text report content in both modes for all fields that belong to the report contract. DuckDB-specific behavior is limited to optional detail lookup and future ad-hoc transaction inspection.

Once equivalence is proven, `none` should become the default for `analyze`; `duckdb` remains opt-in.

### Phase 3: Parser Optimization Gate

Do not start parser replacement immediately.

After phases 1 and 2, rerun the real 988 MB benchmark and profile:

- If analyze is at or below the target envelope, keep the upstream `go-mysql` parser.
- If parser allocation and decode time dominate after DuckDB removal, design a separate parser project.

Parser work must be a separate spec because it changes a lower-level dependency boundary and may require either:

- an upstream `go-mysql` contribution,
- a maintained fork,
- or a BinlogViz-specific lightweight ROW parser that extracts only timestamps, event types, schema/table identity, row counts, positions, DDL query text, and transaction boundaries.

## Report Aggregator Contract

Add a new analyzer component with this role:

```go
type ReportAggregator struct {
    // Owns bounded and minute-level report state.
}
```

Required methods:

```go
func NewReportAggregator(opts Options) *ReportAggregator
func (a *ReportAggregator) ConsumeEvent(ev model.NormalizedEvent)
func (a *ReportAggregator) ConsumeTransaction(txn model.Transaction)
func (a *ReportAggregator) ConsumeMinuteBucket(bucket model.MinuteBucket)
func (a *ReportAggregator) SetFileCoverage(coverage model.FileCoverage)
func (a *ReportAggregator) Snapshot() ReportSnapshot
```

`ReportSnapshot` is an internal analyzer type, not a public JSON model. It should contain enough materialized state to build `model.AnalysisResult` without all transactions:

```go
type ReportSnapshot struct {
    Summary              model.WorkloadSummary
    Tables               []model.TableStats
    Transactions         []model.Transaction
    Patterns             []model.PatternStats
    Minutes              []model.MinuteBucket
    Timeseries           model.Timeseries
    Diagnostics          model.Diagnostics
    Alerts               []model.Alert
    Warnings             int
    PatternDrilldowns    []model.PatternDrilldown
}
```

The exact internal type may be adjusted during implementation, but the boundary is fixed: `model.AnalysisResult` is assembled from `ReportSnapshot`, not from `QueryAllTransactions()`.

## Data Ownership Rules

- `TransactionBuilder` remains the source of truth for transaction boundaries.
- `MinuteAggregator` remains the source of truth for minute bucket construction.
- `TableAggregator` may remain separate initially; `ReportAggregator` can consume its snapshot until table aggregation is folded in.
- `ReportAggregator` must not retain all transactions.
- Top-N transaction lists must be maintained with bounded insertion or heap logic.
- Pattern aggregation stores `model.PatternStats` by pattern key, not raw transactions.
- Pattern drilldowns store bounded representative transactions per pattern or globally, not full transaction slices.
- Findings should be created from alerts and bounded evidence already available at alert time.
- DDL events remain bounded by actual DDL count. DDL-heavy workloads should still be tested.

## Memory Model

Default `--detail-store=none` memory should scale with:

```text
O(active transactions + table count + minute count + pattern count + topN + DDL count)
```

It must not scale with:

```text
O(total completed transactions)
```

`--detail-store=duckdb` may still use disk-backed batches and DuckDB resources, but it should not be required for default reports.

## Compatibility Requirements

- `model.AnalysisResult` public fields remain present.
- `Transactions` still contains top transactions, not all transactions.
- `Patterns` still sorts by total rows, then transaction count, then key.
- `PatternDrilldowns` remains bounded by existing limits.
- `Warnings` remains a count of truncated SQL contexts.
- Compare and trend load old and new JSON without schema breaks.
- Existing `--format text|json|html` behavior remains.
- `--snapshot-name` and snapshot files continue to work.

## CLI Contract

Add a new analyze flag:

```text
--detail-store none|duckdb
```

Initial rollout:

- Phase 2 starts with default `duckdb` if needed for safety.
- Switch default to `none` only after equivalence tests pass.

Final intended state:

```text
default: --detail-store=none
opt-in:  --detail-store=duckdb
```

If a future command requires detail lookup, it must either require a snapshot/detail-store artifact or state that the operation needs `--detail-store=duckdb`.

## Testing Strategy

### Unit Tests

- `ReportAggregator` matches existing analyzer output for synthetic transactions.
- Pattern aggregation matches `BuildPatterns`.
- Timeseries histogram matches `buildTxnSizeSeriesSummary`.
- Largest/longest/widest top-N match existing selector functions.
- Findings and alerts match current behavior for large transaction and spike cases.
- Nil/empty inputs produce stable empty slices where existing reports expect them.

### Integration Tests

- Existing analyze corpus outputs remain equivalent for JSON core fields.
- `--detail-store=none` and `--detail-store=duckdb` produce equivalent default report JSON for corpus fixtures.
- HTML required sections still render.
- Text report remains concise.
- Compare and trend can consume snapshots produced by both modes.

### Performance Tests

- Existing external benchmark `BenchmarkAnalyzeExternalRealBinlog` must run in both modes when `BINLOGVIZ_REAL_BINLOG` is set.
- Track wall time, `B/op`, `allocs/op`, and max RSS for the 988 MB sample.
- Add a many-transaction benchmark proving default report memory does not grow with total completed transaction count.

## Rollout Plan

### Phase 1 Completion Criteria

- `Analyzer.assembleResult()` no longer calls `QueryAllTransactions()` for default report generation.
- JSON/HTML/Text outputs match existing fixtures except for explicitly approved ordering or performance metadata differences.
- Real-binlog benchmark shows no regression.
- Tests prove no unbounded transaction retention in report aggregation.

### Phase 2 Completion Criteria

- `--detail-store=none|duckdb` exists.
- `none` mode avoids DuckDB creation and transaction persistence.
- `duckdb` mode remains available and tested.
- Default analyze uses `none`.
- 100 GB-class multi-file analysis is bounded by active transactions, table cardinality, minute buckets, patterns, top-N, and optional DDL count, not total transaction count.

### Phase 3 Decision Criteria

Start parser design only if all are true after phases 1 and 2:

- Real 988 MB analyze still misses the accepted performance target.
- Profile shows parser decode/allocation as the dominant remaining controllable cost.
- DuckDB and finalize costs are no longer the main bottleneck.
- The team accepts the maintenance cost of parser specialization or a `go-mysql` fork/upstream patch.

## Risks

- Pattern drilldown semantics may subtly change if representatives are maintained streaming instead of selected after full transaction scans. Tests must lock ordering and evidence.
- Alerts currently built after all transactions may need to move to transaction-time and minute-time generation. This can affect ordering; sort deterministically before output.
- `--detail-store=none` may expose hidden report dependencies on DuckDB SQL context hydration. Tests must cover top transaction SQL context behavior explicitly.
- Large DDL-heavy workloads may still retain many DDL events. This is acceptable because DDL count is normally much smaller than row events, but stress tests should define expectations.

## Open Decisions

- Whether `--detail-store=duckdb` should write a user-visible reusable artifact or continue using command-owned temporary files.
- Whether future drilldown commands should load from JSON snapshots, DuckDB detail artifacts, or both.
- Whether default `none` mode should omit any optional evidence that requires full SQL text; current design says no, it must preserve existing bounded evidence.

## Spec Self-Review

- No parser replacement is included in phases 1 or 2.
- The design preserves existing public report models.
- The memory contract explicitly forbids full completed-transaction retention in default mode.
- DuckDB is not removed; it is made optional.
- Phase 3 has measurable gates and is not automatic.
