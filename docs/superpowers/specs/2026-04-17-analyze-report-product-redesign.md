# BinlogViz Analyze Report Product Redesign

## Context

The current `binlogviz analyze` output has useful raw material, but the terminal and HTML reports do not yet behave like a mature DBA diagnostic product. The main problems are not isolated bugs. They come from unclear product goals, inconsistent metric semantics, weak report information architecture, and performance goals that are not enforced by design.

Observed issues include:

- The analyze HTML report does not make TPS a first-class timeline.
- HTML applies top-N limits in some places while CLI text output prints too much detail.
- A 1 GB binlog taking roughly 30 seconds is too slow for incident triage; the target is 10 seconds.
- "largest", "longest", and "widest" transactions are not presented with clear metric definitions and ranking semantics.
- "File Coverage" is unclear to users.
- Transaction evidence shows multiple entries without saying these are top-N ranked lists.
- HTML layout feels like unrelated sections were appended over time.
- Pattern drilldown help icons imply explanations, but do not provide useful help.
- Default terminal output dumps minute-level and pattern detail that should belong in HTML or explicit detail modes.
- `testdata/sql-corpus` is too weak to validate realistic DBA-facing reports.

This redesign turns `analyze` into a guided incident-analysis report rather than a dump of every statistic the analyzer can compute.

## Product Goal

`binlogviz analyze` should help a DBA quickly answer five questions:

1. Did this time window contain abnormal write load?
2. When did the abnormal activity happen?
3. Which tables and operation types caused it?
4. Were there DDLs or abnormal transactions?
5. Which binlog file and position should be investigated next?

Any default report element that does not help answer those questions must be removed, folded into a drilldown, or exposed only through explicit detail flags.

## Non-Goals

- Do not add binlog2sql or rollback SQL generation.
- Do not add new diagnostics before the existing report structure is coherent.
- Do not make CLI text output a full replacement for HTML.
- Do not optimize for pretty charts at the expense of incident readability.
- Do not continue expanding pattern drilldowns unless their DBA value is clear and documented.

## Design Principles

- **Default output must be concise.** Text output is a diagnostic summary, not a raw data dump.
- **HTML must have a reading path.** Sections should answer questions in order, not mirror internal data structures.
- **Every metric needs one primary definition.** Auxiliary values are allowed, but the ranking metric must be explicit.
- **Top-N must be consistent.** CLI and HTML should use the same default limits unless there is a clear reason not to.
- **Charts are evidence, not decoration.** Important charts must be large enough to read and must not be covered by controls or legends.
- **Performance gates are product requirements.** A report that is correct but too slow for incident triage is not acceptable.

## Terminal Report Design

The default `--format text` report should fit in roughly one to two terminal screens. It should show only high-signal summary and next actions.

Default sections:

1. `Summary`
2. `Top Findings`
3. `Top Tables`
4. `Next Actions`

Example shape:

```text
Summary
  Window: 2026-04-05 09:00:00 ~ 10:00:00 UTC
  Files: 2 selected, 25 skipped
  Rows: 12.3M
  Transactions: 48.2K
  Events: 392K
  Peak avg TPS/min: 820 at 09:37
  Peak Rows/min: 1.8M at 09:37
  DDL: 3

Top Findings
  [critical] Write spike at 09:37: rows=1.8M, txns=2.1K, top_table=orders
  [warning] Longest transaction: 48s, rows=320K, tables=5, file=mysql-bin.000044:12345-98765
  [warning] DDL detected: ALTER TABLE users at 09:42

Top Tables
  #  Table              Rows       Txns     Events    Share
  1  app.orders         8.2M       12.1K    88K       66.7%
  2  app.order_items    2.4M       9.8K     73K       19.5%
  3  app.users          760K       2.3K     19K       6.1%

Next Actions
  Open HTML for minute charts and transaction evidence.
  First suspicious position: mysql-bin.000044:12345
```

Default text output must not print:

- Full minute-by-minute activity lists.
- Full write-pattern ranking lists.
- Full active-table dumps without alignment.
- Pattern drilldown detail.

Explicit detail flags may expose these:

- `--details`
- `--top N`
- `--show-minutes`
- `--show-patterns`

`--top N` controls default top-N output consistently across text and HTML where practical.

## HTML Report Design

The HTML report should be organized around the DBA's reading path.

### 1. Executive Summary

Purpose: make the first screen answer whether the window is normal or suspicious.

Content:

- Window start and end.
- Selected and skipped file counts.
- Total rows, transactions, events, and binlog bytes.
- Peak average TPS/min.
- Peak rows/min.
- DDL count.
- Rank-1 largest, longest, and widest transaction summaries.
- Highest-severity findings.

### 2. Timeline

Purpose: show when the workload changed.

Required charts:

- TPS and transactions per minute.
- Rows and events per minute.
- INSERT / UPDATE / DELETE / DDL event counts.
- Binlog bytes per minute.

Layout requirements:

- Main timeline charts are single-column by default.
- Primary chart height should be at least `420px`.
- Legends and toggles must be outside the plotting area.
- Responsive layout may stack charts vertically, but must not shrink them into unreadable cards.

### 3. Hotspots

Purpose: identify which minutes and tables caused the load.

Content:

- Hot intervals top-N, default sorted by rows descending.
- Top tables top-N, default sorted by rows descending.
- Operation mix summary.
- Table rows and transaction counts must be displayed in aligned tables.
- HTML and CLI must use the same default top-N limit.

### 4. DDL Timeline

Purpose: make DDL a first-class incident signal.

Content:

- DDL timestamp.
- Operation type: CREATE, ALTER, DROP, TRUNCATE, or OTHER.
- Object name if available.
- Binlog file and position.
- Bounded query summary.

DDL should appear in:

- Executive summary count.
- Timeline DDL series.
- Dedicated DDL Timeline section.
- Compare/trend diagnostics when available.

### 5. Transaction Evidence

Purpose: explain abnormal transactions with precise, ranked evidence.

The section must use explicit ranked titles:

- `Top N Largest Transactions by Rows`
- `Top N Longest Transactions by Duration`
- `Top N Widest Transactions by Touched Tables`

Summary cards may show only rank 1. Detail sections may show top-N.

Each transaction card must show:

- Rank.
- Primary metric value.
- Rows.
- Duration.
- Binlog bytes.
- Event count.
- Touched table count.
- Binlog file and position span.
- Touched tables with per-table rows.
- Bounded query summary.

If a transaction touches five tables, the UI must phrase it as one transaction with five touched tables. It must not imply five separate "largest transactions".

### 6. Analyzed Files

Rename the current "File Coverage" section to `Analyzed Files`.

Purpose: explain which binlog files were selected or skipped and why.

Content:

- Selected files.
- Skipped files.
- Reason per file.
- First and last event timestamps when available.
- Size.
- Note explaining that filesystem mtime can be used as a coarse pre-filter before event timestamp probing.

Reason labels:

- `selected: overlaps requested time window`
- `skipped: before requested time window`
- `skipped: after requested time window`
- `skipped: mtime coarse filter`
- `skipped: probe failed`

### 7. Pattern Drilldowns

Purpose: provide optional deeper shape analysis after the user understands the core incident.

Default placement: late in the HTML report.

Rules:

- Do not show pattern detail by default in CLI text output.
- Rename to `Write Shape Patterns` if kept.
- Default sort: `total_rows desc`, then `txn_count desc`.
- Display the sort rule near the section title.
- Help icons must either open real tooltip content or be removed.

Tooltip definitions:

- `Row share`: this pattern's rows divided by total rows in the analyzed window.
- `Transaction share`: this pattern's transactions divided by total transactions in the analyzed window.
- `Avg rows per transaction`: total rows for this pattern divided by transaction count for this pattern.

## Metric Definitions

Metric definitions must be shared across code comments, JSON documentation, HTML labels, and README text.

### TPS

Primary definition for this iteration:

```text
avg_tps_per_minute = transactions_in_minute / 60
```

UI labels must say `avg TPS/min` or equivalent wording. Do not imply second-level precision unless the analyzer later adds second-level buckets.

### Largest Transaction

Primary definition:

```text
largest transaction = transaction with highest total_rows
```

Display title:

```text
Largest by Rows
```

Auxiliary fields:

- Binlog bytes.
- Duration.
- Event count.
- Touched tables.

### Longest Transaction

Primary definition:

```text
longest transaction = transaction with highest duration
```

Display title:

```text
Longest by Duration
```

Rows must never be used as a proxy for duration.

### Widest Transaction

Primary definition:

```text
widest transaction = transaction touching the highest number of distinct tables
```

Display title:

```text
Widest by Touched Tables
```

### Hot Tables

Default ranking:

```text
total_rows desc
```

Tie-breakers:

```text
transaction_count desc, event_count desc, table_name asc
```

### Hot Intervals

Default ranking:

```text
total_rows desc
```

Tie-breakers:

```text
transaction_count desc, event_count desc, minute asc
```

### Write Shape Patterns

Default ranking:

```text
total_rows desc, transaction_count desc, pattern_key asc
```

Write shape patterns are optional deep-dive evidence, not a default CLI finding.

## Corpus Redesign

`testdata/sql-corpus` must cover realistic DBA diagnosis scenarios. The corpus should be split into deterministic scenario folders.

Required scenarios:

1. `baseline-small`
   - Small insert/update/delete mix.
   - No expected anomaly.

2. `tps-spike`
   - One minute has many small transactions.
   - Must produce TPS spike and hot interval evidence.

3. `rows-spike`
   - TPS is not high, but affected rows are high.
   - Must produce large transaction or batch-write evidence.

4. `ddl-incident`
   - Includes ALTER, CREATE, DROP, and TRUNCATE examples.
   - Must populate DDL Timeline.

5. `large-transaction`
   - One transaction has highest total rows.
   - Must rank first in Largest by Rows.

6. `long-transaction`
   - One transaction has highest duration but not necessarily most rows.
   - Must rank first in Longest by Duration.

7. `wide-transaction`
   - One transaction touches many tables.
   - Must rank first in Widest by Touched Tables.

8. `multi-file-window`
   - Multiple binlog files with only a subset overlapping the requested window.
   - Must validate selected/skipped Analyzed Files output.

Each scenario should have:

- Text summary golden.
- JSON key golden.
- HTML token golden.
- Chart data golden for key series.

The corpus must be strong enough to fail if the report omits TPS, mislabels transaction rankings, hides DDL, or prints inconsistent top-N output.

## Performance Requirements

The report redesign must include performance gates.

Targets:

```text
1 GB single-binlog analyze:
  target: <= 10s
  warning threshold: > 10s
  failure threshold: > 15s

multi-file one-hour time window:
  target: probe/analyze only files overlapping the requested window
  failure: scanning all files when only 1-2 files overlap
```

Optimization priorities:

1. Run pprof on real or near-real 1 GB input.
2. Split text fast path from HTML full view-model path.
3. Avoid building HTML drilldown structures for default text output.
4. Apply top-N early for high-cardinality structures.
5. Keep DuckDB for high-cardinality persistence and query, not as a parser accelerator.
6. Reduce repeated conversion between parser, normalizer, analyzer, and report view-model layers.

## Implementation Phases

### Phase 1: Spec and Product Contract

- Land this spec.
- Add README language describing the report contract.
- Define metric names and top-N defaults in one place.

### Phase 2: Corpus

- Rebuild `testdata/sql-corpus` with the eight required scenarios.
- Add goldens for text, JSON keys, HTML tokens, and chart data.
- Ensure corpus tests fail on missing TPS, ambiguous transaction labels, missing DDL, and inconsistent top-N.

### Phase 3: Text Report

- Redesign default text output around Summary, Top Findings, Top Tables, and Next Actions.
- Hide minute and pattern detail unless explicitly requested.
- Align top tables in a real table.
- Add `--details`, `--show-minutes`, `--show-patterns`, and consistent `--top N` behavior if missing.

### Phase 4: HTML Report

- Reorder sections into Executive Summary, Timeline, Hotspots, DDL Timeline, Transaction Evidence, Analyzed Files, and Write Shape Patterns.
- Add TPS as the first-class timeline chart.
- Fix tooltip/help behavior.
- Make transaction evidence rankings explicit.
- Rename File Coverage to Analyzed Files.
- Ensure Chinese and English report text stay complete.

### Phase 5: Performance

- Add 1 GB or near-1 GB benchmark input support.
- Add pprof-driven benchmark documentation.
- Split text and HTML computation paths where needed.
- Optimize until the 1 GB target reaches <= 10 seconds on the target environment or until the bottleneck is documented with evidence.

## Acceptance Criteria

Functional acceptance:

- Default text output fits in roughly one to two terminal screens for normal cases.
- Text output includes peak average TPS/min.
- Text output uses aligned tables for top tables.
- CLI and HTML use the same default top-N limit.
- HTML Timeline starts with a TPS chart.
- HTML charts are readable and not obscured by legends or toggles.
- Largest, longest, and widest transaction labels include their ranking metric.
- Transaction evidence clearly distinguishes a ranked transaction list from touched-table details.
- File Coverage is renamed to Analyzed Files and includes explanatory text.
- Pattern help icons either show real tooltip content or are removed.
- Chinese and English reports do not mix unintended hardcoded text.

Performance acceptance:

- 1 GB single-binlog analyze reaches <= 10 seconds on the target environment, or a pprof-backed blocker is documented.
- Multi-file time-window analysis avoids parsing files outside the requested window after discovery/probing.
- Default text output avoids constructing full HTML-only drilldown data.

Testing acceptance:

- `testdata/sql-corpus` contains all eight required scenarios.
- Golden tests cover text summary, JSON key structure, HTML tokens, and chart data.
- Existing `go test ./...` passes.
- Report-specific tests fail if TPS, DDL, transaction rankings, Analyzed Files explanation, or top-N consistency regress.

## Open Decisions

These should be decided before implementation planning:

1. Default top-N value: recommended `10`.
2. Text detail flags: recommended `--details`, `--show-minutes`, `--show-patterns`.
3. 1 GB benchmark input strategy: real private binlog in local validation, synthetic public fixture in CI.
4. Whether text output should support ASCII sparklines for TPS, or only show peak values in this iteration.

