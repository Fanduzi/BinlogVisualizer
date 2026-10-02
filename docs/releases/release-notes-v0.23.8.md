# BinlogViz v0.23.8 Release Notes

Release date: 2026-10-02

## Overview

v0.23.8 surfaces critical DBA incident evidence in default analyze output: a DDL occurrence timeline, open uncommitted BEGIN+DML groups, committed transaction duration ranking and distribution buckets, and input file size versus counted event bytes. It also fixes transaction counting for DDL-only groups, handles plain ROLLBACK and XA END group releases cleanly, names open BEGIN / SAVEPOINT / Ignored-only failure causes, and includes the verify-binlogviz skill.

## New Features

- **DDL occurrence timeline (#89)**: Default text output prints a DDL occurrence timeline (timestamp, operation, affected object, binlog position, and statement prefix) to answer schema drift questions quickly without implying lock duration. JSON exposes these on `diagnostics.ddl_events`. A DDL-only binlog file now exits 0 and prints this timeline; an ADMIN-only file still exits 2. `FLUSH TABLES WITH READ LOCK` remains an Unclassified QUERY (exit 1).
- **Open uncommitted BEGIN+DML groups (#89)**: An explicit `BEGIN` that wrote row images but never encountered a `COMMIT` or `ROLLBACK` is now surfaced as an open DML group with duration, affected tables, row counts, and file position span. Groups exceeding `--large-trx-duration` trigger duration warnings. When a later GTID arrives while the group is open, analyze exits 1 with these details preserved in the Error message.
- **Committed duration ranking and buckets (#89)**: Default text output ranks the longest committed transactions alongside largest-by-rows, includes transaction duration in duration alerts, and reports committed duration distribution buckets (`<1s`, `1s-10s`, `10s-30s`, `>=30s`). JSON provides `diagnostics.duration_buckets` whenever committed transactions are present.
- **File size versus counted event bytes (#89)**: Default text reports top byte-contributing transactions and tables, plus per-file size and time span metrics when analyzing multiple files. JSON includes `diagnostics.largest_byte_transactions`.
- **Project-local verify-binlogviz skill (#81)**: Ships the DBA-focused verification skill under `.cursor/skills/verify-binlogviz` to drive CLI analyze, snapshot, compare, trend, and workflow operations against real ROW fixtures.

## Bug Fixes

- **Count row-image transactions in txn_count (#82)**: Table-level and minute-level `txn_count` now count distinct row-image transactions instead of retaining DDL-only groups in transaction sets. A binlog with `CREATE TABLE` no longer reports inconsistent counts compared to summary total transactions.
- **Single Error line for trend and snapshot (#82)**: Missing arguments or execution failures in `binlogviz trend` and `binlogviz snapshot` now print a single `Error:` line without dumping CLI usage, aligning with `analyze` and `compare`.
- **Plain ROLLBACK closes explicit groups (#83)**: Statements matching plain `ROLLBACK` or `ROLLBACK WORK` (with optional semicolon) are classified as group closures, preventing subsequent GTIDs from aborting with fake conflicting GTID errors.
- **XA END group release on subsequent GTID (#83)**: An `XA END` statement records an end boundary for the open XA group, and a subsequent differing GTID or end-of-input finalizes the group rather than triggering a conflicting GTID failure.
- **Specific Error messages for open BEGIN, SAVEPOINT, and Ignored QUERY (#84, #85, #86)**:
  - A subsequent GTID after an unclosed `BEGIN` reports `open BEGIN without close` (exit 1).
  - `ROLLBACK TO SAVEPOINT` does not close a transaction group; if followed by a conflicting GTID, the Error line clarifies that it is not a group close.
  - A subsequent GTID after an Ignored-only group clarifies that Ignored QUERY does not close the transaction group and is not a missing `COMMIT`.
  - JSON reports `diagnostics.open_explicit_groups` when explicit `BEGIN` groups reach end of input without closing.

## Breaking Changes

None.

## Compatibility

- Exit codes 0, 1, and 2 retain their ADR-0001 semantics: exit 0 on counted events, exit 1 on hard failure or unclassified/unclosed query violations, and exit 2 on no data (empty window or ADMIN-only).
- Existing snapshots, workflows, and CLI flags remain backwards compatible.
- Artifact names and supported platforms are unchanged from v0.23.7.
