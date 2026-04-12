# Analysis Model

This document explains what BinlogViz means by each report section and how the final analysis result is assembled.

BinlogViz does not try to reconstruct a full database history. It consumes normalized row-based binlog events, aggregates bounded workload signals during the stream, then finalizes a report shaped around operational questions: what wrote the most, which transactions were largest, when activity spiked, and whether any thresholds were crossed.

## Workload Summary

`Workload Summary` is the top-level rollup for the analyzed result window.

It includes:

- total transactions
- total affected rows
- total normalized events
- start time
- end time
- duration

A few details matter for interpretation:

- The time range reflects the timestamps that made it into the analyzed result, not a requested wall-clock schedule.
- If timestamps are unavailable, the renderer falls back to empty or `N/A` style output depending on format.
- Duration is derived from the summary start and end bounds after analysis, not from a user-supplied expectation.

Use this section to answer: "How large was the workload slice I just analyzed?"

## Top Tables

`Top Tables` ranks tables by total affected rows.

Each table entry tracks:

- schema name
- table name
- total rows
- insert rows
- update rows
- delete rows
- transaction count touching that table

This is a write-activity ranking, not a storage-size view and not a read/query profile. A table rises to the top because the analyzed binlog window shows more row activity against it than other tables.

Use this section to answer: "Which tables absorbed the most write load in this input range?"

## Top Transactions

`Top Transactions` ranks reconstructed transactions by total affected rows.

Each transaction entry includes:

- transaction key
- start time
- end time
- duration
- total rows
- event count
- optional per-table counts
- optional per-operation counts
- optional SQL context fields depending on `--sql-context`

A few semantics matter:

- Ranking is based on row count, not duration.
- Duration reflects the time span between the reconstructed transaction boundaries.
- Event count reflects normalized events attributed to that transaction, not raw parser callbacks.
- Table and operation maps are included in JSON only when non-empty.

Use this section to answer: "Which individual transactions dominated the workload or look operationally expensive?"

## Top Patterns

`Top Patterns` groups reconstructed transactions into repeated write shapes.

Each pattern entry represents one class of similar transactions rather than one concrete transaction.

The first version derives pattern identity primarily from:

- touched table set
- operation set
- coarse rows-per-event shape bucket

Optional query summary is used as explanatory context, not as the sole grouping key.

Each pattern entry includes:

- deterministic pattern key
- human-readable label
- total rows
- transaction count
- event count
- share of rows
- share of transactions
- average rows per transaction
- aggregate table and operation maps
- optional sample query summary

This section answers a different question from `Top Tables` and `Top Transactions`:

- `Top Tables`: where the write load landed
- `Top Transactions`: which individual transactions were biggest
- `Top Patterns`: which repeated kinds of write activity dominated the workload

Use this section to answer: "What recurring write shapes made up most of this workload window?"

## Minute Activity

`Minute Activity` aggregates write activity into per-minute buckets.

Each minute bucket contains:

- the minute timestamp
- total rows in that minute
- transaction count in that minute
- optional per-table row totals in JSON output

This section is meant to expose workload shape over time rather than individual transaction details. It is especially useful when you are looking for bursts, ramps, or quiet periods across an input range.

Use this section to answer: "When did write pressure increase or drop during the analyzed window?"

## Alerts

`Alerts` is the analyzer's threshold-based anomaly surface.

Current alert types are centered on:

- `large_transaction`
- `spike`

`large_transaction` alerts come from the large-transaction thresholds in analyzer options. `spike` alerts are only evaluated when spike detection is enabled.

Each alert includes:

- type
- severity
- message
- transaction key or minute when applicable
- optional structured details

Important interpretation rule: an alert is derived from analyzer thresholds and available aggregated context. It is meant to flag operator attention, not to serve as a root-cause explanation by itself.

Use this section to answer: "What crossed an operational threshold strongly enough to be called out?"

## SQL Context

BinlogViz separates transaction workload metrics from optional SQL context display.

`--sql-context` controls how transaction query context is exposed:

- `summary`: include the bounded query summary and query metadata fields when query context exists
- `off`: omit query-related fields entirely
- `full`: include the bounded stored SQL plus metadata when query context exists

The implementation deliberately bounds SQL context:

- stored SQL is capped at `4096` bytes
- query summary is capped at `160` characters
- truncation metadata is preserved when query context exists

This means SQL context is designed for operator orientation, not for lossless archival of original statements.

## Pattern Drilldowns

`Pattern Drilldowns` is an optional explanatory layer that appears only when one or more patterns cross a high-signal threshold.

`Top Patterns` remains the primary summary. Drilldowns do not replace it and do not appear in low-signal windows.

A pattern becomes a drilldown candidate when it satisfies a mixed signal model:

- **dominance**: the pattern materially dominates workload volume or transaction count
- **anomaly**: the pattern is unusually concentrated, spike-aligned, or otherwise operationally suspicious

A candidate is expanded into a drilldown when:

- both dominance and anomaly are present, or
- dominance is extremely strong on its own, or
- anomaly is extremely strong on its own

Each drilldown entry is strictly bounded:

- at most 2 drilldowns per analysis
- at most 2 peak minutes per drilldown
- at most 2 representative transactions per drilldown

Drilldown fields:

- `pattern_key` — links back to the parent Top Patterns entry
- `label` — human-readable pattern description
- `why_selected` — short explanation of which signals triggered selection
- `share_of_rows` — fraction of total rows attributed to this pattern
- `share_of_txns` — fraction of total transactions attributed to this pattern
- `avg_rows_per_txn` — average rows per transaction in this pattern
- `signal_flags` — which signals (dominance, anomaly) qualified this pattern
- `busiest_minutes` — top workload minutes by row volume in the analysis window (window-level context, not pattern-specific)
- `representative_transactions` — largest transactions in the analysis window (window-level context, not pattern-specific)

In JSON output, `pattern_drilldowns` is always present as a top-level array (empty when nothing qualifies).

In text output, selected patterns receive a short indented `drilldown:` block under the pattern line.

In HTML output, selected patterns show a collapsible drilldown card with inline metric help.

In Markdown output, drilldowns are intentionally omitted (Top Patterns is not rendered in Markdown).

Use this section to answer: "Why does this specific top pattern deserve extra operator attention?"

## Final Result Shape

The final analysis result is assembled into six stable report areas, an optional pattern drilldown layer, and a warning count:

- `summary`
- `tables`
- `transactions`
- `patterns`
- `minutes`
- `alerts`
- `pattern_drilldowns`
- `warnings`

Text output always renders the six report sections in a fixed order, even when some sections are empty. JSON output always emits the top-level fields, using empty arrays where a result set is absent.

Nested JSON fields are more selective. Optional per-transaction, per-minute, and alert-detail fields may be omitted when their source data is empty or unavailable.

That stability is intentional: scripts can consume the top-level JSON shape predictably, while operators can rely on the text report layout staying familiar across runs.
