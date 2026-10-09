# BinlogViz v0.23.28 Release Notes

Release date: 2026-10-09

## Overview

v0.23.28 makes `binlogviz flashback --schema-file` use two kinds of dump it used to skip (#187). A mysqldump `Database:` header whose database name has spaces is now read. A dump table whose names differ from the binlog only in letter case is now used when the binlog names are all lower case, as with a `lower_case_table_names=0` dump and a `lower_case_table_names=1` or `2` binlog. Before, both cases ended in "no table definition", and apply could stop at `ERROR 3105` with earlier transactions committed. `analyze` output is unchanged from v0.23.27.

## Changes

- **`Database:` header with spaces (#187)**: a header such as `-- Host: localhost    Database: p187 sp` binds the dump to `p187 sp`. Names with quotes, backticks inside the name, CJK, and CRLF dumps are read too.
- **Letter-case fold for `lower_case_table_names` (#187)**: when the binlog database and table names are all lower case and the file has the table only under different letter case, flashback uses the file table and prints `warning: <binlog table>: using <file table> from --schema-file, which differs only in letter case ...`. The rows are still checked against that definition. An exact-case match always wins.
- A mixed-case binlog name, or two file tables that fold to the same name, is not guessed: the warning names the near miss and the table is treated as having no definition, as in v0.23.27.

## Bug Fixes

- #187: a correct incident-time dump was ignored when its `Database:` header had a space, or when it came from a server with a different `lower_case_table_names`. Generated columns in that table were then assigned, and apply stopped at `ERROR 3105` after earlier transactions had committed.

## Verification

- CI on the merge commit, `go test ./...`, and the MySQL 8.0 end-to-end tests including `TestFlashbackRoundTripMySQL80` on a fresh MySQL 8.0.46: pass.
- Live restores on MySQL 8.0.46 (`lower_case_table_names=1` incident server, dumps from a `lower_case_table_names=0` server): 21/21 restored identically, and 20/20 of those scripts restored identically on a MySQL 5.7.44 `lower_case_table_names=1` target. Cases: headers with spaces, quotes, backticks, CJK and `Database:` inside the name; CRLF dumps; primary-key and no-primary-key tables with stored and virtual generated columns; multi-table and two-database dumps; Unicode names; unqualified `SHOW CREATE TABLE` with `--schema-file-db`. v0.23.27 restored 4/21.
- Wrong dumps reached through the fold (different expression, extra column, one good and one bad table) exit 1 with no SQL. v0.23.27 printed scripts for these that partly applied before `ERROR 3105`.
- Offline flashback differential against v0.23.27 over 4722 cases: 4672 byte-identical, 5 differ only in progress-bar whitespace, 45 are the intended #187 changes. `analyze` is byte-identical to v0.23.27 on 1698 cases. The #192 and #208 guard cases (34) give identical `ERROR 1231` lines.

## Breaking Changes

None. A run that used to warn "no table definition" for a case-folded or space-named table now uses the file definition, prints a fold warning, and can refuse a dump that does not match the binlog.

## Compatibility

- Apply is supported on MySQL 5.7 and newer and on MariaDB 10.2 and newer.
- `flashback` never connects to MySQL. Review the script, test it, and apply it in one new session on the primary with `sql_log_bin=1`, with the script header. A statement that fails leaves earlier transactions in the script committed.
- JSON `report_version` stays `3`. Snapshots, workflows, artifact names, and supported platforms are unchanged from v0.23.27.

## Known issues

See `docs/concept/limitations.md` for the full text and workarounds.

- [#215](https://github.com/Fanduzi/BinlogVisualizer/issues/215): the fold assumes an all-lower-case binlog name comes from a `lower_case_table_names=1` or `2` server. On a `lower_case_table_names=0` server with both `t` and `T`, a dump that has only `T` is used for `t`, and the guard stops the apply before any write (or flashback exits 1) where v0.23.27 restored `t`. When a folded table is refused, the error names the binlog table, not the file table, and the fold warning is not printed. Both fail safe. Use a dump with the exact binlog name.
- [#167](https://github.com/Fanduzi/BinlogVisualizer/issues/167): an `ALTER` in the parsed binlog is applied again on top of a dump that already contains it. A joined multi-database dump uses only the first `Database:` header.
- [#193](https://github.com/Fanduzi/BinlogVisualizer/issues/193): a non-default `div_precision_increment` with `DECIMAL` operands is refused with a message that blames the dump. It fails safe.
- A database name that starts and ends with a literal backtick, or starts with a space, loses that character in the `Database:` header and gets no definition, as in v0.23.27.
- In the guard error line, a `|` in a column name is shown as `/`. The guard's `SELECT` on stdout and the SQL keep the real names.
- A generated column the script does not know about (no `--schema-file` and no `CREATE` in the parsed binlogs) is still assigned, and apply can stop at `ERROR 3105`.
- Resuming after a failed apply: keep the header (every line before the first `-- gtid:`), delete only the blocks above the failed one, and run the header plus the failed block and everything below it in a new session.
