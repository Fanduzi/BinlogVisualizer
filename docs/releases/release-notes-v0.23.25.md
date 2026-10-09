# BinlogViz v0.23.25 Release Notes

Release date: 2026-10-09

## Overview

v0.23.25 lets a `binlogviz flashback` script restore rows that were written by a non-strict session when the script is applied in a normal strict session (#180). Before, a zero date or a generated column that divided by zero stopped apply partway, after earlier transactions had already committed. `analyze` output and every other flashback statement are unchanged from v0.23.24.

## Changes

- **Zero dates restore under a strict session (#180)**: a `DATE`, `DATETIME` or `TIMESTAMP` value with a zero month or day (`'0000-00-00'`, `'2026-00-15'`) is written by a statement that drops only `NO_ZERO_DATE`, `NO_ZERO_IN_DATE` and `TRADITIONAL` from `@@SESSION.sql_mode`, then restores the saved mode. Strict mode stays on for that statement.
- **Generated columns that divided by zero restore (#180)**: when the script leaves out a generated column (from `--schema-file` or a parsed `CREATE`) and its logged value is `NULL`, the `INSERT` or `UPDATE` drops only `ERROR_FOR_DIVISION_BY_ZERO` and `TRADITIONAL`, so `a / b`, `a DIV b` and `a % b` with `b` = 0 recompute to `NULL` again, as they were stored.
- The `ENUM` index 0 wrap from v0.23.22 is the same mechanism and is unchanged.

## Bug Fixes

- #180: rows written with `sql_mode=''` stopped apply at `ERROR 1292` (zero date) or `ERROR 1365` (division by zero in a generated column) under the server default `sql_mode`, leaving earlier transactions committed. They now restore exactly.

## Verification

- CI on the merge commit, and `TestFlashbackRoundTripMySQL80` plus `TestNumericDecodeMySQL80` against a live MySQL 8.0.46: pass.
- Live fixture on MySQL 8.0.46, MariaDB 10.6 and MariaDB 10.11.19: zero `DATE` / `DATETIME` / `TIMESTAMP` and zero-in-date rows, a table with `a / b` VIRTUAL, `a % b` STORED and `a DIV b` STORED and rows with `b` = 0, a mixed `UPDATE`, and a plain table. After the incident, the script applied in a default strict session gives `CHECKSUM TABLE` identical to before the incident. v0.23.24 stopped at `ERROR 1365` with the tables partly restored.
- The same fixture applied in a `TRADITIONAL` session (MySQL 8.0.46) and a `STRICT_ALL_TABLES,NO_ZERO_DATE,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO` session (MariaDB 10.11) restores identically, and the session `sql_mode` at the end equals the one before the script.
- Strict mode stays on inside the wrap: a zero-date row into a target column that is too narrow fails with `ERROR 1406` instead of truncating.
- Wrong target (the generated columns are plain columns on the target): `mysql` stops at the guard and `mysql --force` hits `ERROR 1792`; in both, rows and the GTID set are unchanged.

## Breaking Changes

None. A script now contains a `SET @binlogviz_sql_mode` / `SET SESSION sql_mode` pair around each statement that writes a zero date or recomputes a generated column logged as `NULL`.

## Compatibility

- Apply is supported on MySQL 5.7 and newer and on MariaDB 10.2 and newer.
- `flashback` never connects to MySQL. Review the script, test it, and apply it in one new session on the primary with `sql_log_bin=1`, with the script header. A statement that fails leaves earlier transactions in the script committed.
- JSON `report_version` stays `3`. Snapshots, workflows, artifact names, and supported platforms are unchanged from v0.23.24.

## Known issues

See `docs/concept/limitations.md` for the full text and workarounds.

- On a correct target, a reconnect in the middle of a block leaves that block not applied. Resume from that block, with the header, in a new session.
- [#167](https://github.com/Fanduzi/BinlogVisualizer/issues/167): an `ALTER` in the parsed binlog is applied again on top of a dump that already contains it. A joined multi-database dump uses only the first `Database:` header.
- [#187](https://github.com/Fanduzi/BinlogVisualizer/issues/187), [#192](https://github.com/Fanduzi/BinlogVisualizer/issues/192), [#193](https://github.com/Fanduzi/BinlogVisualizer/issues/193): a `Database:` header with a space and mixed-case names under `lower_case_table_names`; non-ASCII column names in the guard error; a non-default `div_precision_increment` with `DECIMAL` operands. All fail safe.
- A generated column the script does not know about (no `--schema-file` and no `CREATE` in the parsed binlogs) is still assigned, and apply can stop at `ERROR 3105`.
- Resuming after a failed apply: keep the header (every line before the first `-- gtid:`), delete only the blocks above the failed one, and run the header plus the failed block and everything below it in a new session.
