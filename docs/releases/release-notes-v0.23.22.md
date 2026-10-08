# BinlogViz v0.23.22 Release Notes

Release date: 2026-10-08

## Overview

v0.23.22 adds `binlogviz flashback` (#153). It prints SQL that undoes selected DELETE, UPDATE, and INSERT row changes from a MySQL 8 binlog, without connecting to a database. Statements are in reverse binlog order, one `START TRANSACTION` / `COMMIT` per original transaction, with the original GTID and `file:start-position` in a comment. The rule is: restore exactly, or refuse (exit 1, one `Error:` line, no SQL) or warn on stderr before anything is applied. Six rounds of tip dogfooding on MySQL 8.0.46 found and fixed #154, #155, #156, #158, #159, #161, #162, #163, #165, #170, and #171 before this release, and #173 fixes the resume-after-failure steps. Three P2 limitations stay open with documented workarounds: #166, #167, and #168.

`analyze` output changes only where v0.23.21 printed a wrong value; see "`analyze` output changes" below. Each new value matches `mysqlbinlog -vv`.

## New Features

- **`binlogviz flashback` (#153)**: selectors are the same as `analyze` (`--include-schema`, `--exclude-schema`, `--include-table`, `--exclude-table`, `--dml`, `--start` / `--end`, `--start-position` / `--stop-position`, `--include-gtids` / `--exclude-gtids`). A DELETE becomes an `INSERT` of the before-image, an INSERT becomes a `DELETE`, and an UPDATE restores the before-image, matching the after-image primary key. A table with no primary key is matched on every column with `LIMIT 1`, and the statement says so. Requires `binlog_row_metadata=FULL` and `binlog_row_image=FULL`. The script header sets `utf8mb4`, `time_zone = '+00:00'` (`TIMESTAMP` literals are the UTC wall clock), and removes `NO_BACKSLASH_ESCAPES` for that session.
- **Exact values**: JSON is rebuilt from the binary document, including inside a `binlog_transaction_compression=ON` transaction, so decimals, datetimes, and `-0.0` round-trip (#154, #158). Non-`utf8mb4` character columns are a charset introducer and hex bytes (#155). `ENUM` is the member index and `SET` the bitmask, so `latin1` / `gbk` values restore under strict and non-strict `sql_mode` (#159). An `ENUM` index of 0 is restored by saving `@@SESSION.sql_mode`, dropping strict mode for that statement only, and restoring the saved mode (#163). `BIT(n)` is a `b'...'` literal of the column width. `FLOAT`, `DOUBLE`, `GEOMETRY`, and `VECTOR` are refused.
- **`--schema-file` and `--schema-file-db`**: generated columns are left out of `INSERT` and `UPDATE` assignments when `CREATE` / `ALTER` in the parsed binlogs, or a `--schema-file` (`mysqldump --no-data`, or `SHOW CREATE TABLE` output including `mysql --batch`), names them (#156, #162). The schema file is checked against the binlog `TABLE_MAP` for every selected event; a mismatch names the table and columns and prints no SQL (#161). A selected table with no definition still prints SQL, with a stderr warning that generated columns cannot be ruled out.
- **Safety warnings in the docs (#156)**: `ON DELETE` / `ON UPDATE CASCADE` child rows are not restored, triggers fire on undo, later changes are overwritten with no conflict check, and the script must be applied in one session on the primary with `sql_log_bin=1`.

## Bug Fixes

All of these were found in pre-release dogfooding of `flashback` and are fixed in this release:

- #154: JSON decimals and datetimes became strings, `10.0` became `10`, `-0.0` became `0`.
- #155: `latin1`, `utf16`, `ucs2`, and `utf32` columns were re-encoded on apply.
- #156: generated columns made the script fail with `ERROR 3105`; DBA safety caveats were missing.
- #158: JSON inside a compressed transaction was refused.
- #159: `ENUM` / `SET` values in non-`utf8mb4` columns were written as `utf8mb4` text.
- #161: a `--schema-file` that did not match the binlog was trusted and could drop a real column.
- #162: single-database `mysqldump` files and several `SHOW CREATE TABLE` results in one file were skipped.
- #163: an `ENUM` index of 0 was rejected by strict `sql_mode` (`ERROR 1265`).
- #165 (P1): numeric columns after a `YEAR` column got the wrong signedness, so the undo matched 0 rows. BinlogViz now walks the TABLE_MAP SIGNEDNESS bitmap itself and counts `YEAR`, as MySQL does.
- #170 (P1): `MEDIUMINT UNSIGNED` values at or above `8388608` came out as 32-bit numbers, so the undo matched 0 rows. The value is now the low 24 bits.
- #171: known-limitation workarounds for #166, #167, and #168, and the resume-after-failure steps, were missing or wrong.
- #173: the resume steps did not say to keep the `SET` header; a literal reading restored `TIMESTAMP` values shifted by the session time zone with exit 0. The steps now keep the header and include a worked example. The #167 per-database workaround now says it needs each database's own dump.

## `analyze` output changes

These are corrected values. Each new value matches `mysqlbinlog -vv`. Fixtures with none of these types are byte-identical to v0.23.21, `FLOAT` and `DOUBLE` output is unchanged, and exit codes are unchanged.

- `YEAR` before an unsigned or signed numeric column (#165): `--show-rows` values and Hot Rows key labels (`id=-294967296` becomes `id=4000000000`).
- `MEDIUMINT UNSIGNED` at or above `8388608`, including `ZEROFILL` (#170): `--show-rows` values and Hot Rows key labels (`id=4287190080` becomes `id=9000000`).
- Hot Rows primary-key labels and ordering in the default text, Markdown, JSON, and HTML output, for the two cases above and for a `BIT(64)` primary key with the high bit set (`b=-1 (18446744073709551615)` becomes `b=18446744073709551615`). A row ranked by its key moves with the corrected label, together with its first and last GTID and position.
- `BIT(64)` values with the high bit set under `--show-rows`: `-1 (18446744073709551615)` becomes `18446744073709551615`.
- `DECIMAL` values longer than 64 bytes under `--show-rows` are printed in full, no longer `[truncated: 64 of 66 bytes]`.

## Verification

- CI job `flashback e2e` on MySQL 8.0.46: `TestFlashbackRoundTripMySQL80` (checksums match the pre-incident state) and `TestNumericDecodeMySQL80` (numeric oracle: `--show-rows` versus `mysqlbinlog -vv` for every integer width signed, unsigned, and `ZEROFILL`, `DECIMAL(10,0)` to `DECIMAL(65,30)`, `BIT(1)` to `BIT(64)`, `FLOAT`, `DOUBLE`, and `YEAR` / `ENUM` / `SET` before numeric columns).
- Tip dogfood on `8e2ef843` (#172): PASS on 2026-10-08 (BinlogQA), with no P0 or P1. Earlier rounds on #153, #157, #160, #164, and #169 found the issues fixed above.

## Breaking Changes

None. `flashback` is a new command. `analyze` changes only corrected values.

## Compatibility

- `flashback` never connects to MySQL. Review the script, test it, and apply it in one session on the primary with `sql_log_bin=1`. A statement that fails leaves earlier transactions in the script committed.
- JSON `report_version` stays `3`. Snapshots, workflows, artifact names, and supported platforms are unchanged from v0.23.21.

## Known issues

See `docs/concept/limitations.md` for the full text and workarounds.

- [#166](https://github.com/Fanduzi/BinlogVisualizer/issues/166): a correct `--schema-file` is refused when a generated column uses an expression the checker cannot verify (JSON extraction, `UPPER`, `CONCAT`, `DIV`, and `/` when MySQL rounded the result). Pass the binlog that holds `CREATE TABLE` (or a later `ALTER`) and select the incident with `--include-gtids`, or delete the generated columns from the script.
- [#167](https://github.com/Fanduzi/BinlogVisualizer/issues/167): an `ALTER` in the parsed binlog is applied again on top of a dump that already contains it (`reordered (...)`, the same column listed twice). Use a dump taken before that `ALTER`, or leave out the binlog that holds it. A joined multi-database dump uses only the first `Database:` header: add `USE db;` before each dump, or run one flashback per database with `--schema-file-db` and that database's own dump. With the joined file, a per-database run refuses or warns and fails safe.
- [#168](https://github.com/Fanduzi/BinlogVisualizer/issues/168): `ENUM` index 0 under `sql_mode=TRADITIONAL` stops at `ERROR 1265`. Set the session to `STRICT_TRANS_TABLES,STRICT_ALL_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION` first.
- Resuming after a failed apply: keep the header (every line before the first `-- gtid:`), delete only the blocks above the failed one, and run the header plus the failed block and everything below it in a new session. The failed block is the last `-- gtid:` line at or before the line number in the client error.
