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

3. **Spill the series, and copy it back onto the result only while it is small.** Same shape as the apply-delay spill: an unlinked temp file, and memory if the file cannot be created. The minute currently being updated stays resident, because points are mutated and an earlier minute can be revisited. Past `activityInlineLimit` (8192 points) the result does not hold the cross product. Reports read one minute, or one displayed table, at a time. Under the limit the series is copied onto `MinuteBucket.TableRows` and `TableStats.Activity` and the file is removed, so a small report takes the existing encode path.

## Decision

Option 3.

The two aggregators share one store. Table activity and minute row counts are separate fields in one record, so they are not added twice. Standalone aggregators used by unit tests keep their own maps. The analyzer's store is the one that spills.

A minute can be revisited: a long transaction crosses a minute boundary, or a lagging channel delivers an earlier minute after a later one was already flushed. How the file records that revisit:

1. **Append the whole minute on every flush.** Rejected. The file grows with minutes × tables-per-minute × revisits. On 300 tables alternating between minute m and m+3 the file was about 73× the 68 bytes per point, and RSS went above the in-memory series.

2. **Keep a window of recent minutes as the only bound, and append a fresh copy when a minute leaves the window.** Rejected. The window would have to hold every minute a long transaction or a lagging channel will touch again. Anything smaller evicts the minute, and appending the rewrite grows the file with revisits.

3. **Append a delta for each changed cell and merge the versions on read.** A new cell is one record, so the alternating shape stays near 68 bytes per point. Updating a cell that is already on disk appends another record. A long transaction that keeps returning to the same tables still grows the file with revisits, which misses the bound (one record per distinct point, plus a small constant per revisit).

4. **Overwrite dirty cells in place, append a new run when the minute gains cells, and keep four minutes resident.** Rejected. Two channels a few minutes apart stay in a window of four, and the file stays `points × 68`. Six channels two minutes apart, twelve channels one minute apart, or a random minute inside a ten-minute window do not. Each revisit that adds a cell appends a run, and the reload is one `pread` per run. Past four hot minutes the reads go superlinear (about 16× main at 120k transactions on six channels).

5. **One region per spilled minute, reloaded from disk to update in place, with the resident set capped at 320KiB.** This is the previous decision. Rejected. The counters are additive, so a revisit does not need the bytes already written. Shapes that fit in 320KiB are about 1.1–1.2× main. Once the hot set is larger, every minute switch reads that minute back. On binlogs that is 4–18× main and superlinear: six channels go from 30k to 120k transactions (4×) in 1.00s to 21.39s (21×); a two-channel lag of 8000 tables at 240k transactions is 133.67s against 7.41s on main. The reads are 9–58GB for a file of a few megabytes, and twelve channels of 5000 tables compact the file 1344 times, because packing sets the region's capacity to the bytes just written and the next cell moves it again. Random reload is the root cause. A larger resident budget is not the fix: 800KiB already left the flashback side of `TestWideActivityHeap` about 150KB under its 2MiB cap.

Chosen: write-only runs, then one sequential merge. The in-memory buffer is a hash of (minute, table) to counters, capped at 320KiB of cell bytes so the wide-heap gap still has about 1MiB of room on both sides. An increment of any cell, including a minute that was already written, adds into the buffer. The parse never reads the file. When the buffer is full it is written as one run sorted by (minute, table) and cleared. A revisit after that flush is a new record in the next run. When more than 8 runs exist they are merged in order, duplicate cells summed, the output copied to the front, and the file truncated. That cost is one sequential pass over the file, amortized like a log tier, not a pass per minute switch. The same merge runs once more at the end, and the render or JSON path reads that one run in minute order.

The on-disk record is 8 bytes of minute plus the 68-byte counters. The high water mark, including the merged copy before truncate, stays within 12× points×68. Ten records of 76 bytes is 760 bytes per point; 12×68 is 816. A correct merge holds at most 8 runs plus that copy. Raising the run cap, or never merging, leaves that many copies on disk and crosses 12×. After truncate the file is one 76-byte record per point, which is under 2× points×68. Skipping the truncate, or summing nothing, leaves the high water mark as the file and crosses 2×. Both bounds are checked: the high water mark at every flush and merge, and the size after truncate.

The 320KiB figure is the buffer, not a set of minutes kept hot. Flashback samples the live heap at the end of the parse, before the buffer is flushed, so the buffer shows up on that side of `TestWideActivityHeap`. Analyze's peak is the render after the flush. The cap stays 2MiB. The budget is not raised to hold a hotter set: a hotter set spills another run instead of being read back.

The spill is optional. Before a flush extends the file, the free space on that filesystem is checked. When it would drop below a 32MiB reserve, the series stops spilling and the rest stays in memory, the same path as a failed write. Flashback SQL and the DuckDB store use the same temp filesystem; filling it with this spill must not turn a run that succeeds on main into a failure. A check that itself fails (no `statfs`) allows the write, and a short write still falls back to memory.

At finalize, 8192 points or fewer are inlined and `AnalysisResult.MinuteTables` stays nil. Above that, the buckets keep minute totals only, table `Activity` stays nil, and `MinuteTables` answers `TableRows` and `Activity`. Spike detection and the five hot intervals fill from that source and do not retain every minute's map. HTML loads activity only for the tables it displays (the top-N list, 10 by default). A detail store that is not the no-op records one minute at a time so a DuckDB batch can flush. The default analyze path uses the no-op store.

A failed read fails the analysis. The command reports it as kind `spill` (exit 11) for every spill read: while finishing the analysis, while a detail store that is not the no-op reads one minute (including `--detail-store duckdb`), and while rendering json, text, markdown, or html. It is not kind `error` (exit 1) and not kind `output` (exit 8): nothing was written. A temp file that cannot be created or written, or that would eat the reserve, keeps the minute in memory, including a minute that was already resident when the write failed. Dropping that minute would omit its `table_rows` and still exit 0.

JSON does not stream the minutes. ADR 0017 decision 8 is all-or-nothing: stdout is empty, or it is a document the schema accepts. Streaming checked the spill error after the document was written, so a read error could leave a schema-valid report with null `table_rows` and a non-zero exit, and the reads between the first and last byte widened the SIGTERM window (about 0.09s to about 2s on a large input). The writer now reads every spilled minute before the first stdout byte, copies `table_rows` onto a local minute slice, and encodes with the same path as an in-memory series. A read error returns before any byte is written. The SIGTERM window is that encode, the same as main. The analysis heap still does not hold the cross product. The render holds the minute rows for the encode, which is when main held them too.

Output does not change. `report_version` stays 4. JSON, text, Markdown, and the HTML charts, hot intervals, compare, and trend inputs are the same bytes they were when the series was in memory. Compare and trend read table totals and the timeseries, which were never this series.

What still grows with the input, and is not this fix: one minute bucket per minute (the ~2KB term), the 320KiB buffer, one run descriptor per unmerged run (at most 8), one span per minute after the final merge, and the number of distinct tables, threads, and statement shapes. Alerts still grow with the number of alerts.

## Consequences

- 500 tables over 7 days no longer keeps the cross product on the heap. The series is a temp file whose high water mark stays within `12 × 68` bytes per distinct point. A revisit appends a record into the buffer and, after a flush, into another run; the runs are summed by a sequential merge, not by reading the minute back. The merge cost follows the bytes written, not the number of minute switches.
- E2's large file is about 20k points, so it spills. Its small file stays under 8192 and is inlined. The per-minute heap term measured by E2 is the minute bucket plus the index, not 4 table points. The caps were already upper bounds.
- `TestWideActivityHeap` writes the same number of transactions with 4 tables a minute and with 60, and fails if the wide run's live heap is more than 2MiB above the narrow run. On the unspilled code that gap is several megabytes (analyze 7421024, flashback 5410568). With this spill, on this machine, analyze grew 1019976 bytes and flashback 651624, leaving 1077176 and 1445528 under the cap.
