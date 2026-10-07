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

## Top Threads

`Top Threads` ranks sessions so an operator can answer who wrote the most without aggregating transactions by hand.

Each session is one `thread_id` on one `server_id`. A thread id of zero is kept only when the binlog stored `user@host`. Transactions with neither identity are not a session.

The ranking metric is rows when any session wrote rows. Otherwise it is events, then bytes, then transactions. The section shows `server_id`, `user@host`, and schema only when the binlog carried them.

`--top` limits the section in text, Markdown, JSON, and HTML. `--top-threads` overrides that limit. `0` means unlimited.

Use this section to answer: "Which thread or account wrote the most in this window?"

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
- `input_format`

`large_transaction` alerts come from the large-transaction thresholds in analyzer options. `spike` alerts are only evaluated when spike detection is enabled. `input_format` is added on successful MIXED analyze (ROW images and ignored Query-DML both greater than zero) so JSON consumers see that only ROW images were counted.

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

- `summary`: include one bounded query summary and query metadata fields when query context exists. Default text prints that summary on Top Transactions, and `--show-patterns` prints the sample query. A display cut names the original byte length
- `off`: omit query text and DDL statement text in every format, including text `Query:` lines, Markdown, and HTML. Operation, object, and position stay
- `full`: include stored SQL plus metadata when query context exists. Default text prints that SQL on Top Transactions. DDL statement text uses the same 4096-byte cap. A cut ends with `… [truncated: <shown> of <original> bytes]`

The implementation deliberately bounds SQL context:

- stored SQL is capped at `4096` bytes; `query_truncated` means that store cap, not the 160-character summary
- query summary is capped at `160` characters of SQL, then the truncation marker when the text was cut
- credential literals in `CREATE`/`ALTER USER`, `GRANT ... IDENTIFIED`, `SET PASSWORD`, and MariaDB `IDENTIFIED VIA`/`WITH` … `USING`/`AS` forms (including `OR` plugin chains) become `<secret>` before display, in every mode
- `SET PASSWORD` is DDL: it closes its GTID group and is listed on the DDL timeline

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
- at most 2 workload peak minutes per drilldown
- at most 2 workload transactions per drilldown

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

Text output always renders the six report sections in a fixed order, even when some sections are empty. After Busiest Minutes it also renders Replica Apply Delay: a ranking when any counted transaction has a non-zero MySQL 8 commit-timestamp delay, one line when every pair is equal, and `commit timestamps unavailable` when the fields are absent. JSON output always emits the top-level fields above, using empty arrays where a result set is absent.

`replica_apply_delay` is optional. It is omitted when no counted transaction carried both commit timestamps. It is not an empty object, and a missing timestamp is not serialized as a delay of `0`.

Nested JSON fields are more selective. Optional per-transaction, per-minute, and alert-detail fields may be omitted when their source data is empty or unavailable.

That stability is intentional: scripts can consume the top-level JSON shape predictably, while operators can rely on the text report layout staying familiar across runs.
