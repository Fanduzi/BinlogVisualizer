# BinlogViz v0.35.2 Release Notes

Release date: 2026-10-27

## Overview

v0.35.2 is a patch release. The per-(table, minute) activity series no longer grows the heap with tables times minutes (#423, closes #389, ADR 0019). Above 8192 points it spills to an unlinked temp file, written as sorted runs during the parse with a tiered merge and one final merge. Output is byte-identical: `report_version` stays 4, `plan_version` stays 2, and `flashback verify --format json` `schema_version` stays 1. On a 2M-point file, analyze took 24.05s against 27.43s for v0.35.1, with 1372MiB RSS against 2260MiB.

## Fixed

- **Bounded activity series (#423, closes #389).** `analyze` and `flashback` kept each table's per-minute activity (HTML charts) and each minute's per-table row counts (JSON `minutes[].table_rows`, text and Markdown driving tables, HTML hot intervals, spike detection, DuckDB minute-table rows) for every (table, minute) point, about 220 bytes each. 500 tables over 7 days was on the order of 1GB of live heap. See `docs/adr/0019-activity-series-spill.md`.
  - Up to 8192 points the series stays in memory and the report takes the existing path.
  - Past 8192 points the series goes to an unlinked temp file (`binlogviz-activity-*`). A 320KiB in-memory buffer collects counter increments, including revisits of a minute already written, and is written as one sorted run when full. The parse never reads the file back.
  - Eight runs on one level merge into one run on the next level, so each record is rewritten about log8(points / buffer cells) times and temp I/O stays near linear. One final k-way merge sums the remaining runs, and the report reads that run in minute order.
  - JSON, text, Markdown, and the HTML charts, hot intervals, compare, and trend inputs are the same bytes as when the series was in memory.
- **New exit code 11, kind `spill`.** A failed read of the activity spill, while finishing the analysis (the default store and `--detail-store duckdb`) or while rendering json, text, markdown, or html, exits 11 with kind `spill`. Stdout is empty. It is not a generic failure (exit 1) and not a failure to write the report (exit 8). See `docs/reference/exit-codes.md`.
- **The spill is optional.** Before a flush extends the file, free space on the temp filesystem is checked against the pending write plus a 32MiB reserve. When the write would leave less than that, spilling stops and the rest of the series stays in memory. A spill file that cannot be created or written also keeps the series in memory, and the run continues.

## Tests and CI

- `TestWideActivityHeap` compares 4 tables a minute with 60 over 24000 transactions and caps the live-heap gap at 2MiB. On main before #423 the gap was 7.4MB for analyze and 5.4MB for flashback. It runs in CI `verify`.
- `TestSequentialSpillIOScales` (0.25M, 0.5M, 1M points) and `TestLaggingReplicaScales` (6000 and 12000 tables) check that temp I/O stays within the tiered-merge bound and that wall time grows close to linearly. The flat-merge design turned the I/O test red in CI.
- `TestRevisitedSpillStaysBounded`, `TestAlternatingMinuteSpillStaysBounded`, and `TestInterleavedChannelsStayResident` pin the file size, peak, and RSS. `TestSpillBoundFactorsStayTight` keeps the bound factors from being loosened. `TestReserveIncludesPendingWrite` and `TestSpillStopsBeforeFillingTempFS` cover the 32MiB reserve.
- Spill read errors are covered in analysis, DuckDB detail reads, and every render format (`TestSpillReadErrorWritesNoJSON`, `TestAnalysisSpillReadReportsSpillKind`, `TestDuckDBDetailStoreSpillReadFails`).
- `docs/production-readiness.md`: E1 names the per-(table, minute) term. 24 MET / 0 PARTIAL / 0 NOT MET.

## Verification

BinlogQA passed #423 on its sixth round (r6, `d328855f`) and tip QA on main `d328855f` passed with 0 P0, 0 P1, and 0 P2 (one new P3, #439, also in v0.35.1). `v0.35.1..d328855f` is 13 commits, all from #423. This release commit adds only these notes and the CHANGELOG on top of `d328855f`.

- **CI**: PR run 40457750182 and push run 40466867793 at `d328855f` are green, including vet, `-race`, and flashback e2e (73 pass).
- **Corpus** (680 files, 4556 jobs, 7 formats) against the official v0.35.1: 0 exit code mismatches and 0 output diffs, also with a build that forces every series to spill.
- **Large input** (2M points, 8GiB limit): JSON in 24.05s against 27.43s, RSS 1372MiB against 2260MiB, same hash. Temp I/O was 14.7× the final spill file (bound 31.2×). On QA's binlog shapes (interleaved channels, a lagging replica with 8000 tables, sequential 0.5M–3.5M points) wall time is 0.93–1.02× v0.35.1, and RSS is below it everywhere. Both versions run out of memory at 3GiB on this file, as before.
- **Failure paths**: a spill read error exits 11 with empty stdout and leaves no temp file. Write errors and ENOSPC fall back to memory and match v0.35.1. SIGINT, SIGTERM, and SIGKILL give 130, 143, and 137 and leave no temp file.
- **Flashback** on MySQL 8.0.36: 202 rows round-trip with a matching checksum; on 5.7.44 it exits 6, as v0.35.1 does. Live guard jobs on 8.0.36 and 5.7.44: 0 fail-opens.
- **Schemas**: the 13 published schemas are byte-identical; 1276 documents validate with ajv and python-jsonschema. `snapshot`, `compare`, and `trend` (36 jobs) and `--help` (26 nodes, 19 failure cases) are byte-identical.
- **Tests**: `go test` passes; the analyzer DuckDB test runs out of memory at a 3GiB limit (environment) and passes at 8GiB.

## Compatibility

- `report_version` stays 4, `plan_version` stays 2, and `flashback verify --format json` `schema_version` stays 1. No JSON field is added or removed, and every output is byte-identical to v0.35.1.
- New exit code 11 (kind `spill`) for a failed read of the activity spill. Before, the series was never on disk, so this failure could not happen.
- `analyze` and `flashback` may create one unlinked temp file in the temp directory for a large series. The file has no name in the directory and its space is freed when the process exits. With less than 32MiB plus the pending write free, it stays in memory instead.
- Artifact names and supported platforms are unchanged.

## Known issues

- **#439 (P3, also in v0.35.1)**: `docs/examples/analyze-output.json` and `.txt` are a pre-v4 shape and fail the shipped analyze schema, although the quickstart calls them shipped samples.
- **#418**, **#420**, **#421** (P3) carry over. **#399**, **#400**, **#403** (P3) carry over.

See `docs/adr/0019-activity-series-spill.md`, `docs/reference/exit-codes.md`, and `docs/production-readiness.md`.
