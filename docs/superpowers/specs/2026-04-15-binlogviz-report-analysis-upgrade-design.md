# BinlogViz Report And Analysis Upgrade Design

## Summary

This design upgrades BinlogViz from a lightweight binlog summarizer into a DBA-oriented analysis tool for MySQL 5.7/8.0 ROW-format binlogs.

The work combines four major changes:

1. Rebuild the analyze, compare, and trend HTML reports on top of a shared report shell and view-model layer.
2. Expand the analysis model with DBA diagnostics such as TPS, DDL timelines, transaction sizing, table-level binlog distribution, and evidence-backed findings.
3. Redesign multi-file time-window planning and execution so directory-based analysis avoids scanning unnecessary files and improves end-to-end throughput.
4. Add a comprehensive regression suite covering model logic, HTML behavior, i18n, interaction flows, and performance benchmarks.

## Goals

- Make HTML reports readable and useful for DBA investigation rather than decorative summaries.
- Treat charts as primary analytical surfaces with explicit sizing and anti-overlap constraints.
- Support `--lang en` and `--lang zh-CN` consistently across all HTML output.
- Support inline drilldown interactions, especially table-level expansion in analyze HTML.
- Add DBA-oriented metrics and findings for workload, transactions, tables, DDL, and file coverage.
- Improve throughput for multi-file time-window analysis by narrowing the input set early and processing the selected files in parallel.
- Preserve strong automated coverage across the refactor.

## Non-Goals

- No `binlog2sql` or replay/recovery workflows.
- No general support promise for `STATEMENT` or `MIXED` binlog formats.
- No cross-version compatibility effort beyond MySQL 5.7/8.0 ROW-format binlogs.
- No attempt to fully reconstruct SQL semantics when the binlog does not contain bounded query context.

## Input Scope

- Primary target: MySQL 5.7 and 8.0 ROW-format binlogs.
- Relay logs may be processed through the same parser pipeline if they expose equivalent event structure, but the product contract and tests remain binlog-centric.
- DDL is a first-class diagnostic category and must be captured, summarized, trended, and compared.

## Problem Statement

Current report and execution behavior is insufficient in four ways:

- HTML reports use separate hard-coded templates with inconsistent layout, poor chart sizing, and controls that can obscure data.
- HTML output ignores `--lang` and remains English-only.
- The analysis model lacks several DBA-critical metrics and evidence structures.
- Directory-based time-window analysis still risks selecting too many files, and the execution path does not fully optimize for multi-file throughput.

## Product Direction

BinlogViz should evolve into an investigation-first tool:

- A DBA should be able to identify suspicious time ranges quickly.
- The tool should surface the hottest tables, the heaviest transactions, relevant DDL, and the file or position range that contains the evidence.
- Compare and trend reports should expose changes in diagnostic behavior, not just summary deltas.

## Architecture

The upgraded architecture is divided into four layers.

### 1. Core Analysis

The analyzer produces a richer structured result instead of forcing reports to derive advanced metrics from minimal summary data.

Responsibilities:

- Aggregate workload totals, time-series, table metrics, transaction metrics, DDL events, and file coverage.
- Produce evidence-backed findings and diagnostic alerts.
- Expose structured outputs suitable for analyze, compare, and trend.

### 2. Report View-Model

A shared view-model layer prepares report-specific HTML payloads from structured analysis outputs.

Responsibilities:

- Generate localized UI text dictionaries.
- Normalize chart configuration data for analyze, compare, and trend pages.
- Encode drilldown payloads for inline interaction.
- Keep presentation logic out of raw analyzer structures.

### 3. Report Shells

Analyze, compare, and trend retain separate entrypoints but share the same HTML design system and interaction rules.

Responsibilities:

- Shared page shell, layout tokens, chart container patterns, and interaction affordances.
- Page-specific sections for analyze, compare, and trend.
- Shared i18n injection and chart readability constraints.

### 4. Execution Engine

Input planning and worker execution are split from analysis aggregation.

Responsibilities:

- Discover candidate files.
- Probe and narrow the file set for a requested time window.
- Schedule parse work across multiple files.
- Merge worker outputs into a single ordered analysis result.

## Data Model

The analyze result should be refactored into explicit domains instead of a shallow flat payload.

### Summary Domain

The summary domain keeps current totals and adds:

- `total_binlog_bytes`
- `covered_files`
- `covered_rotates`
- `tps_peak`
- `rows_peak_per_minute`
- `events_peak_per_minute`
- `ddl_count`

### Timeseries Domain

Unified timeseries structures are introduced for shared report consumption:

- `tps_series`
- `rows_series`
- `events_series`
- `insert_event_series`
- `update_event_series`
- `delete_event_series`
- `ddl_event_series`
- `binlog_bytes_series`
- `txn_size_series_summary`

These series become the base for analyze charts and for compare/trend rollups.

### Tables Domain

Table records should include:

- Existing row totals and operation totals
- `total_binlog_bytes`
- `event_count`
- `txn_count`
- `ddl_count`
- `last_changed_at`
- Time-series scoped to the table:
  - `rows_series`
  - `events_series`
  - `binlog_bytes_series`
- `top_transactions`
- Table evidence and anchors for drilldown

### Transactions Domain

Transactions should be categorized instead of exposed only as a generic top list:

- `largest_transactions`
- `longest_transactions`
- `widest_transactions`

Each transaction record should include:

- `txn_key`
- `start_time`
- `end_time`
- `duration`
- `row_count`
- `event_count`
- `binlog_bytes`
- `tables`
- `binlog_file_start`
- `binlog_file_end`
- `position_start`
- `position_end`
- `has_ddl`
- `sample_query_summary`

### Diagnostics Domain

This domain provides DBA-facing evidence:

- `ddl_timeline`
- `large_transaction_alerts`
- `long_transaction_alerts`
- `hot_intervals`
- `file_coverage`
- `file_segments`
- `table_distribution`
- `workload_findings`
- `evidence_refs`

## Additional DBA Metrics

Beyond the initial request, the analyzer should add:

- Transaction percentile distributions for row impact and duration.
- Histogram-style transaction size distribution rather than only maxima.
- Table rankings by rows, events, transactions, and binlog bytes.
- Table-level operation mix ratios.
- Cross-table transaction width to identify large multi-table transactions.
- DDL count and DDL object breakdown.
- Hot interval detection based on rows, events, TPS, and binlog-byte bursts.
- File coverage metadata showing which files were selected, skipped, and why.

## DDL As A First-Class Signal

DDL must not be treated as a generic query footnote.

The system should capture:

- DDL event time
- DDL type such as `ALTER`, `CREATE`, `DROP`, `TRUNCATE`, `RENAME`
- Object identity where possible
- Whether the DDL appears inside or adjacent to heavy write windows
- Evidence anchors so compare and trend outputs can reference the exact object or time bucket

DDL should appear in:

- Analyze: timeline, summary cards, drilldowns, alerts
- Compare: DDL delta, object-level change summaries
- Trend: DDL count trend, object/time correlation

## HTML Report Design

All HTML reports should share a common interaction and layout contract.

### Global Rules

- Main analytical charts must default to at least `360px` and preferably `420px` height for dense timelines.
- Controls must never overlap the plotting area.
- Legend use is minimized; when required, place it outside the chart area or below the plot.
- Responsive layout prefers vertical stacking over compressed multi-column charts.
- Chart readability is a product requirement, not optional styling.

### Analyze HTML

The analyze page is reorganized into three layers.

#### Workload Overview

- Summary cards for rows, transactions, events, DDL count, file coverage, and peak TPS.
- Clear time-window and selected-file context.

#### Timeline Workbench

- Primary chart surface for `TPS`, `rows`, and `events`.
- External control row to switch the primary metric set:
  - `INSERT`
  - `UPDATE`
  - `DELETE`
  - `DDL`
  - `binlog bytes per slice`

#### DBA Drilldowns

- `Top Tables`
- `Transactions`
- `DDL Timeline`
- `Hot Intervals`
- `File Coverage`

`Top Tables` supports inline row expansion. Each expanded panel shows:

- Table-scoped timeline
- Operation mix
- Recent DDL touching the table
- Related top transactions
- File and position evidence

### Compare HTML

Compare should move beyond summary deltas and include:

- Timeline delta surfaces
- DDL delta summaries
- Table distribution delta
- Large/long transaction delta
- Hot interval delta
- File coverage delta

`Pattern Changes` and the related chart should move to a larger single-column treatment instead of a cramped double-column block.

### Trend HTML

Trend should include:

- TPS trend
- DDL trend
- Largest transaction trend
- Top table movement by rows and binlog bytes
- Event mix trend

Pattern trend controls move outside the chart plotting area. The pattern chart and related narrative should not share a compressed layout that hides data.

## i18n Design

All HTML reports must respect the current language.

Approach:

- Generate `ui_text` dictionaries on the backend for each page.
- Templates render localized text via injected values, not hard-coded English strings.
- `<html lang="...">` follows the active language.
- Shared report shells consume the same key set across analyze, compare, and trend.
- Support only `en` and `zh-CN` in this phase.

## Performance Design

The primary performance target is higher throughput for multi-file time-window analysis.

### Input Planner

The planner is responsible for:

- Directory discovery
- Time-window narrowing
- File coverage probing
- Worker scheduling

It should build or refresh a file coverage index containing:

- `path`
- `mtime`
- `first_event_time`
- `last_event_time`
- `size`

### Time-Window Narrowing

The planner should:

- Avoid parsing every matching file in the directory.
- Probe candidate files cheaply to determine true time coverage.
- Produce an explicit selected-file set and skip rationale.

### Parallel Parse Workers

- Parallelism is applied at the file level.
- File-internal event order remains sequential.
- Worker outputs are intermediate aggregates rather than direct report objects.

### Merge Analyzer

The merge layer combines worker outputs into ordered global results, preserving:

- Correct time-series ordering
- Cross-file transaction continuity where required
- Global table and transaction rankings
- Unified diagnostics and findings

### Secondary Performance Goal

Single-file speed is still important, but it is secondary to multi-file time-window throughput.

## Testing Strategy

The refactor requires five test layers.

### Unit Tests

Focused logic tests for:

- TPS aggregation
- DDL timeline extraction
- Table distribution
- Transaction ranking
- Hot interval detection
- Binlog byte slice aggregation

### Integration Tests

End-to-end parser to analyzer to report coverage for:

- Multi-file time windows
- Cross-file transactions
- Mixed DDL and DML workloads
- MySQL 5.7/8.0 ROW fixtures
- English and Chinese HTML output

### Golden Tests

Golden coverage for analyze, compare, and trend JSON and HTML outputs to lock:

- i18n text
- Page structure
- Chart containers
- Drilldown anchors
- New diagnostic fields

### Interaction Tests

Browser-level tests for:

- Analyze table row inline expansion
- Trend pattern view switching
- Compare/trend control placement behavior

### Benchmark Tests

Benchmarks should cover:

- Single-file analysis
- Multi-file analysis
- Narrow time-window workloads
- Broad time-window workloads
- DDL-heavy workloads
- Large-transaction-heavy workloads

## Delivery Plan

Implementation should proceed in these phases:

1. New analysis model and JSON expansion with unit coverage.
2. Analyze HTML rebuild with shared shell, i18n, and interaction tests.
3. Compare and trend HTML rebuild with upgraded diagnostics.
4. Input planner and multi-file parallel execution refactor with benchmarks.
5. Golden refresh, documentation updates, and final verification.

## Risks

- Shared report shell work can create wide HTML diffs; golden and interaction tests are required to keep regressions contained.
- Transaction continuity across file boundaries becomes more complex once worker-level parallelism is introduced.
- DDL extraction may require careful normalization to preserve accurate object identity without overpromising SQL reconstruction.
- Performance gains must be measured; they cannot be assumed from language choice alone.

## Acceptance Criteria

This upgrade is complete when:

- Analyze, compare, and trend HTML all support `en` and `zh-CN`.
- Charts are large and readable, with no controls obscuring data.
- Analyze `Top Tables` rows expand inline with evidence-rich table drilldowns.
- DDL is visible and comparable across analyze, compare, and trend.
- The analysis model exposes DBA metrics for workload, tables, transactions, files, and timelines.
- Multi-file time-window analysis shows benchmarked throughput improvement over the current execution path.
- Unit, integration, golden, interaction, and benchmark coverage are all in place and passing.
