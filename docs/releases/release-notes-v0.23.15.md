# BinlogViz v0.23.15 Release Notes

Release date: 2026-10-07

## Overview

v0.23.15 can limit an analyze report to chosen ROW kinds and, on request, print the cells those rows changed. `--dml insert,update,delete` is combinable and keeps only those kinds. It composes with the existing table, schema, time, position, and GTID filters. Summary, Top Tables, Top Transactions, Top Threads, and alerts count only the kept kinds, and the report names the filter (`DML filter`). Nothing matched is exit 2, empty stdout, `Error: dml filter matched no events`. `--show-rows` is off by default. On, DELETE prints the before-image, UPDATE prints only changed columns (`before -> after`) plus `(<count> unchanged)`, and INSERT prints the after-image. Column names come from MySQL 8 `binlog_row_metadata=FULL`. Otherwise columns are `@1`..`@N` and the report says names are missing. With FULL metadata, an unsigned integer prints unsigned (`3000000000`). Without signedness metadata, an integer whose two readings differ prints both, the same way `mysqlbinlog -v` does. Bounds are 32 logical rows per transaction and 64 bytes per value. A cut ends with `… [truncated: <shown> of <original> bytes]`, and rows past the cap are counted. `--sql-context off` also omits these cell values and says so. Default text, Markdown, JSON, and HTML are unchanged when neither flag is set. This release does not emit flashback SQL. Workflow and trend behavior are unchanged.

## New Features

- **Filter a report to chosen DML kinds (#131)**: `--dml` accepts `insert`, `update`, and `delete`, in any combination. Tokens are stored uppercase in the order INSERT, UPDATE, DELETE. The report keeps only those ROW kinds and composes with `--include-table`, `--exclude-table`, schema filters, `--start` / `--end`, position selectors, and GTID filters. Summary, Top Tables, Top Transactions, Top Threads, and alerts count only the kept kinds. The text summary names the filter as `DML filter`. JSON field: `scope.dml`. A kind filter that matches nothing is exit 2, empty stdout, `Error: dml filter matched no events`.
- **`--show-rows` prints bounded row images (#131)**: Off by default. On, each listed transaction prints DELETE as the before-image, UPDATE as `column: before -> after` for columns that changed, then `(<count> unchanged)` (or `(no column changed)`), and INSERT as the after-image. Column names come from MySQL 8 `binlog_row_metadata=FULL`. Otherwise the note is `column names unavailable (binlog_row_metadata is not FULL); columns shown as @1..@N`. With FULL metadata, unsigned columns print the unsigned value (`3000000000`). Integers without signedness metadata print `signed (unsigned)` when the two readings differ. At most 32 logical rows are kept per transaction, and each value at most 64 bytes. A cut ends with `… [truncated: <shown> of <original> bytes]`. Further rows print `rows omitted: <count>`. JSON `transactions[].rows` includes `op`, `columns`, and `names` (`full` or `positional`). DELETE sets `before`, INSERT sets `after`, and UPDATE sets `before`, `after`, and `changed`. SQL NULL is JSON `null`. `rows_omitted` counts rows past 32. `column_names_note` and `row_values_note` explain missing names or suppression. Images are kept only when `--show-rows` is on and `--sql-context` is not `off`. Each listed transaction still has `mysqlbinlog_cmd`.
- **`--sql-context off` omits row cells (#131)**: `--sql-context off` also omits these cell values and prints `row values omitted because --sql-context is off`. JSON field: `row_values_note`.

## Bug Fixes

None.

## Verification

Tip QA on `1ac261e` (#131): PASS on 2026-10-07. Known leftover #132 is filed and is not a blocker.

## Breaking Changes

None.

## Compatibility

- Exit codes 0, 1, and 2 keep ADR-0001 meaning. A `--dml` filter that matches nothing is exit 2, empty stdout, `Error: dml filter matched no events`.
- Default text, Markdown, JSON, and HTML are unchanged when neither `--dml` nor `--show-rows` is set. `--show-rows` stays off. The default `--sql-context` remains `summary`.
- `--sql-context off` still omits query text and DDL statement text, and also omits `--show-rows` cell values.
- Auth-DDL credential literals stay `<secret>` in statement text. That rewrite does not apply to row cells.
- This release does not emit flashback or rollback SQL. JSON `before`, `after`, and `changed` are the shape a later release can use.
- Snapshots, workflows, artifact names, and supported platforms are otherwise unchanged from v0.23.14.
- Workflow and trend behavior are unchanged.

## Known issues

- **TIMESTAMP follows the process timezone (#132)**: `--show-rows` formats a `TIMESTAMP` column in the process timezone. `mysqlbinlog -v` shows the UTC instant. On `TZ=Asia/Shanghai`, the MySQL 8.0.46 FULL fixture prints `updated_at='2026-10-06 22:00:01.000000'` where the UTC wall clock is `2026-10-06 14:00:01.000000`. `DATETIME` is unaffected. Unsigned integers under `binlog_row_metadata=FULL` still print unsigned. This is not fixed in v0.23.15 and is not a blocker.
