# BinlogViz v0.23.23 Release Notes

Release date: 2026-10-08

## Overview

v0.23.23 is a `binlogviz flashback` hardening release. A correct incident-time `--schema-file` is accepted for more generated-column expressions, a wrong one is refused or stopped before it writes, and the apply guard fails closed on every server it runs on, including MariaDB and old MySQL. `analyze` output is unchanged from v0.23.22.

## Changes

- **Generated columns are checked against the logged rows (#178, #182)**: before a schema-file generated column is omitted, flashback evaluates `UPPER`, `LOWER`, `CONCAT`, `CONCAT_WS`, `LENGTH` / `CHAR_LENGTH`, simple JSON extraction (`->`, `->>`, `JSON_EXTRACT`, `JSON_UNQUOTE` with a constant path), and integer `+`, `-`, `*`, `/`, `DIV`, `%`, `MOD` on every logged row. A logged value that is not the expression exits 1 with no SQL and names every contradicting table and column with one example row. An expression that cannot be modelled exactly is unverifiable: the column is omitted, stderr warns once, and the script header says the guard checks the target.
- **`DECIMAL` generated division (#179)**: a `DECIMAL(p,s)` or integer column computed with `+`, `-`, `*`, `/`, `DIV`, `%`, or `MOD` is accepted, using MySQL 8.0's division width at the default `div_precision_increment` of 4 (`5 / 2` in `DECIMAL(40,4)` is `2.5000`, `1 / 7` in `DECIMAL(40,9)` is `0.142857142`). A logged endpoint a non-strict `sql_mode` stores (`TINYINT` `127`, `DECIMAL(4,2)` `99.99`) is unverified with a warning; any other contradicting value is refused.
- **Apply-time guard (#183, #184, #186)**: for every schema-file generated column the script omits, one guard after the three `SET` lines and before the first transaction reads `information_schema.COLUMNS.EXTRA` on the target. When a column is not `STORED GENERATED` or `VIRTUAL GENERATED`, apply fails before any transaction, and the error line lists every mismatched `db.table.column` (a long list shows the count and the first names). A no-primary-key `WHERE` keeps generated columns, so an INSERT undo cannot delete a different duplicate row.
- **The guard fails closed (#183, #186, #190, #194, #195, #196)**: the guard locks the session with `SET SESSION TRANSACTION READ ONLY`, which names no server variable, and switches it back only when the check ran in that session, every column matched, and the server is supported. `mysql --force`, an interactive `source` or paste, `mysqlsh --force` / `--interactive`, and a GUI runner set to continue on error then fail every later write with `ERROR 1792` and change no rows. Each transaction block locks again before it starts. A session that was read-only before the script stays read-only.
- **`ENUM` index 0 under `sql_mode=TRADITIONAL` (#168)**: the statement that writes index 0 drops `TRADITIONAL` as well as `STRICT_TRANS_TABLES` and `STRICT_ALL_TABLES`, then restores the saved `@@SESSION.sql_mode`.
- **CI (#176)**: the `flashback e2e` job fails when a MySQL test is skipped or missing; it requires `--- PASS` for `TestFlashbackRoundTripMySQL80` and `TestNumericDecodeMySQL80`.

## Bug Fixes

- #194 (P1): on MariaDB 10.x–11.0 the guard set `transaction_read_only`, which those versions do not have, so `mysql --force` still wrote the wrong rows with exit 0. The lock now uses `SET SESSION TRANSACTION READ ONLY`; a wrong schema file writes no rows and leaves the GTID unchanged.
- #195: on MySQL older than 5.7.0 the guard only failed the next statement. MySQL 5.6 and MariaDB 10.1 now fail the guard with `binlogviz: target server is older than MySQL 5.7.0 or MariaDB 10.2 and is not supported for apply` and stay read-only.
- #196: the lock depended on `PREPARE`, so a server that refused prepared statements (`max_prepared_stmt_count` reached or 0) wrote the wrong rows. It now stays read-only; on a correct target the apply stops at `ERROR 1461` and restores nothing. The prepared statements are named `binlogviz_fb_*_x9q`.
- #190: MySQL 5.7.0 through 5.7.19, which have `tx_read_only` but not `transaction_read_only`, were not locked.
- #186: the `ERROR 1231` line named only the first mismatched column; refusal examples showed `ENUM` / `SET` as index or bitmask instead of the labels.
- #183: a wrong dump whose checkable expression matched the logged rows lost the column with exit 0 and no warning; on a no-primary-key table the INSERT undo could delete a different row.
- #184: `--allow-unverified-generated` help and refusal did not say when it is safe, the zh-CN message had an English prefix, and only the first bad table was reported.
- #182: correct incident-time dumps were refused for JSON null and doubles under `->>`, `ENUM` / `SET`, `ascii` columns, `DECIMAL` / `TIME` / `TIMESTAMP` in `CONCAT`, and `LENGTH` of `BINARY(n)`.
- #179: a correct dump with a `DECIMAL` generated column using `/` was refused.
- #178: a real column the file marked as generated, with an unverifiable expression, was dropped with exit 0 even when the logged values contradicted the expression.
- #168: `ENUM` index 0 under `sql_mode=TRADITIONAL` stopped at `ERROR 1265`.
- #166: a correct `--schema-file` was refused when a generated column used JSON extraction, `UPPER`, `CONCAT`, `DIV`, or integer `/`.

## Verification

- CI job `flashback e2e` on MySQL 8.0.46: `--- PASS: TestFlashbackRoundTripMySQL80` and `--- PASS: TestNumericDecodeMySQL80`, no skips.
- Live guard matrix for #194/#195/#196 (#198) on MariaDB 10.1, 10.6, 10.11, 11.0, 11.4 and MySQL 5.6, 5.7.19, 5.7.44, 8.0: every wrong-schema mode (`mysql --force` with autocommit on and off, a header-less block in a fresh session, `max_prepared_stmt_count=0`) writes 0 rows and leaves the GTID unchanged. On the supported servers a correct schema file restores the same rows and keeps the session read-only flag, autocommit, and `sql_mode` as they were.
- BinlogQA dogfood of the #194 fix: PASS on 2026-10-08, with the two remaining leaks filed as #197 and documented.

## Breaking Changes

None for a correct apply on MySQL 5.7+ or MariaDB 10.2+. A script that contains a guard (a schema-file generated column was omitted) now refuses to apply on MySQL 5.6 and MariaDB 10.1 and leaves that session read-only. On a server that refuses `PREPARE`, it stops at `ERROR 1461` instead of applying.

## Compatibility

- Apply is supported on MySQL 5.7 and newer and on MariaDB 10.2 and newer. The guard is checked live on MySQL 5.7.19, 5.7.44 and 8.0 and on MariaDB 10.6, 10.11, 11.0 and 11.4.
- `flashback` never connects to MySQL. Review the script, test it, and apply it in one new session on the primary with `sql_log_bin=1`, with the script header. A statement that fails leaves earlier transactions in the script committed.
- JSON `report_version` stays `3`. Snapshots, workflows, artifact names, and supported platforms are unchanged from v0.23.22.

## Known issues

See `docs/concept/limitations.md` for the full text and workarounds.

- [#197](https://github.com/Fanduzi/BinlogVisualizer/issues/197): a header-less block pasted into a session that already applied a correct script still writes, with exit 0. An interactive client that reconnects in the middle of a block commits the rest of that block in the new session (see also [#191](https://github.com/Fanduzi/BinlogVisualizer/issues/191)). Batch `mysql --force` is not affected. Apply a cut only with the header, in a new session.
- [#167](https://github.com/Fanduzi/BinlogVisualizer/issues/167): an `ALTER` in the parsed binlog is applied again on top of a dump that already contains it (`reordered (...)`, the same column listed twice). Use a dump taken before that `ALTER`, or leave out the binlog that holds it. A joined multi-database dump uses only the first `Database:` header: add `USE db;` before each dump, or run one flashback per database with that database's own dump.
- [#180](https://github.com/Fanduzi/BinlogVisualizer/issues/180): rows written by a non-strict session (a zero date, a generated `a/b` with `b` = 0) can fail under a strict session with `ERROR 1292` or `ERROR 1365` after earlier transactions committed. Apply with the original session's `sql_mode`.
- [#187](https://github.com/Fanduzi/BinlogVisualizer/issues/187): a `Database:` header with a space in the name, and mixed-case names from a `lower_case_table_names=0` dump against a `lower_case_table_names=1` binlog, leave the table without a definition.
- [#192](https://github.com/Fanduzi/BinlogVisualizer/issues/192): with non-ASCII column names the guard's `ERROR 1231` line shows only the count of mismatched columns.
- [#193](https://github.com/Fanduzi/BinlogVisualizer/issues/193): with a non-default `div_precision_increment`, a correct dump with `DECIMAL` operands in a generated `/` is refused.
- Resuming after a failed apply: keep the header (every line before the first `-- gtid:`), delete only the blocks above the failed one, and run the header plus the failed block and everything below it in a new session.
