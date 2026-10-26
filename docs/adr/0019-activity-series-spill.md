# ADR 0019: Spill the per-(table, minute) series

Status: accepted (2026-10-26). Tracks #389.

## Context

`analyze` and `flashback` keep two maps for the same cross product:

- each table's per-minute activity (`TableAggregator.activity`), used by the HTML per-table charts
- each minute's per-table row counts (`MinuteAggregator.tableRows`), used by JSON `minutes[].table_rows`, text and Markdown driving tables, HTML hot intervals, spike detection, and the DuckDB minute-table rows

Together that is about 220 bytes per (table, minute) point, on top of about 2KB for the minute bucket itself. The point count is tables times minutes, not transactions. 500 tables over 7 days is on the order of 1GB of live heap. The event timestamp is not a high-water mark, so a minute cannot be dropped early: a later event can still belong to an earlier minute.

E1 described growth with the time span and with the number of distinct tables, and did not name this product. E2's template writes 4 tables a minute, so the product is folded into the per-minute term and a many-table binlog does not fail the gate.

## Options

1. **Keep the series and name the term in E1.** Honest, and every current byte of output stays. It does not bound the heap. A long many-table binlog still grows without a limit. Rejected: the goal is a bound.

2. **Keep the top K tables per minute and fold the rest into `other`.** Bounds the series at K times minutes. It changes JSON `minutes[].table_rows`, the HTML chart for any table that falls out of the top K, spike alerts for those tables, and the driving-table lines. Rejected: those outputs would be wrong, not just smaller.

3. **Spill the series, and copy it back onto the result only while it is small.** Same shape as the apply-delay spill: an unlinked temp file, and memory if the file cannot be created. The minute currently being updated stays resident, because points are mutated and an earlier minute can be revisited. Past `activityInlineLimit` (8192 points) the result does not hold the cross product. Reports read one minute, or one displayed table, at a time. JSON writes the minutes array one minute at a time so the bytes match `encoding/json`. Under the limit the series is copied onto `MinuteBucket.TableRows` and `TableStats.Activity` and the file is removed, so a small report takes the existing encode path.

## Decision

Option 3.

The two aggregators share one store. Table activity and minute row counts are separate fields in one record, so they are not added twice. Standalone aggregators used by unit tests keep their own maps. The analyzer's store is the one that spills.

At finalize, 8192 points or fewer are inlined and `AnalysisResult.MinuteTables` stays nil. Above that, the buckets keep minute totals only, table `Activity` stays nil, and `MinuteTables` answers `TableRows` and `Activity`. Spike detection and the five hot intervals fill from that source and do not retain every minute's map. HTML loads activity only for the tables it displays (the top-N list, 10 by default). A detail store that is not the no-op records one minute at a time so a DuckDB batch can flush. The default analyze path uses the no-op store.

A failed read fails the analysis. A temp file that cannot be created or written keeps the series in memory, which is the previous behavior for that failure.

Output does not change. `report_version` stays 4. JSON, text, Markdown, and the HTML charts, hot intervals, compare, and trend inputs are the same bytes they were when the series was in memory. Compare and trend read table totals and the timeseries, which were never this series.

What still grows with the input, and is not this fix: one minute bucket per minute (the ~2KB term), one index entry per spilled minute, and the number of distinct tables, threads, and statement shapes. Alerts still grow with the number of alerts.

## Consequences

- 500 tables over 7 days no longer keeps the cross product on the heap. The series is a temp file of 68 bytes per point.
- E2's large file is about 20k points, so it spills. Its small file stays under 8192 and is inlined. The per-minute heap term measured by E2 is the minute bucket plus the index, not 4 table points. The caps were already upper bounds.
- `TestWideActivityHeap` writes the same number of transactions with 4 tables a minute and with 60, and fails if the wide run's live heap is more than 2MiB above the narrow run. On the unspilled code that gap is several megabytes.
