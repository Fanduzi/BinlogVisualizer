# BinlogViz v0.23.30 Release Notes

Release date: 2026-10-10

## Overview

v0.23.30 changes the error `binlogviz flashback --schema-file` prints when a generated column with `/` was stored under a non-default `div_precision_increment` (#193). The binlog does not record that variable, so the table is still refused, but the error now names the increments that fit the logged values, says the dump may be right, and points at `--include-table` / `--exclude-table` instead of only telling the DBA to find another dump. Which rows are accepted or refused is unchanged, and so is every script. `analyze` is unchanged from v0.23.29.

## Changes

- **Non-default `div_precision_increment` (#193)**: when a generated column with `/` does not match at MySQL's default increment of 4, flashback re-checks it at increments 0 to 30. If every logged value fits some of them, the error says so, for example `the logged values match it only at div_precision_increment 0-3`. It also keeps the other explanation: if the dump's expression is wrong, for example `/` where the table has `DIV`, use a dump from incident time. Flashback still exits 1 and prints no SQL. A value no increment produces keeps the previous message.
- **Docs**: `docs/concept/limitations.md` (EN and zh-CN) now says that with `DECIMAL` operands, or a quotient that is multiplied or added afterwards, any non-default increment can change the stored digits, including in a narrow column such as `DECIMAL(12,4)`, and that flashback refuses those rows.

## Bug Fixes

- #193: a correct incident-time dump refused because of a non-default `div_precision_increment` was reported as a wrong dump.

## Verification

- CI on the merge commit (`verify`, `flashback e2e`) and `go test ./...`, including the new `TestDivIncrementHint` with the issue's exact values: pass.
- The issue's real MySQL fixtures: `div_precision_increment` 0 and 2 exit 1 with `match it only at div_precision_increment 0-3` in English and Chinese; 4 and 8 exit 0 with SQL identical to v0.23.29.
- Differential against real MySQL from #193 (2,328,777 cells and 14,436,632 probes over 27 expressions, 11 target types, and increments 0 to 30): output identical to v0.23.29, with 0 false accepts and 0 false refusals.

## Breaking Changes

None. Only the text of this one error changes.

## Compatibility

- Apply is supported on MySQL 5.7 and newer and on MariaDB 10.2 and newer.
- `flashback` never connects to MySQL. Review the script, test it, and apply it in one new session on the primary with `sql_log_bin=1`, with the script header. A statement that fails leaves earlier transactions in the script committed.
- JSON `report_version` stays `3`. Snapshots, workflows, artifact names, and supported platforms are unchanged from v0.23.29.

## Known issues

See `docs/concept/limitations.md` for the full text and workarounds.

- [#215](https://github.com/Fanduzi/BinlogVisualizer/issues/215): the letter-case fold assumes an all-lower-case binlog name comes from a `lower_case_table_names=1` or `2` server. On a `lower_case_table_names=0` server with both `t` and `T`, a dump that has only `T` is used for `t`, and the guard stops the apply before any write (or flashback exits 1). A refused folded table is named by its binlog name, and the fold warning is not printed. Both fail safe. Use a dump with the exact binlog name.
- A column added in the parsed binlogs and later modified or renamed there (`MODIFY`, `CHANGE`, `RENAME COLUMN`) still makes a correct incident-time dump fail with `reordered ... c, c`. It fails safe. Parse only the incident binlog, or use a dump taken between those `ALTER`s.
- When `--schema-file-db` conflicts with several `Database:` headers, the warning names only the last conflicting header, and its ending "unqualified tables were not used" is not exact: tables of a dump whose header matches `--schema-file-db` are used.
- A database name that starts and ends with a literal backtick, or starts with a space, loses that character in the `Database:` header and gets no definition.
- In the guard error line, a `|` in a column name is shown as `/`. The guard's `SELECT` on stdout and the SQL keep the real names.
- A generated column the script does not know about (no `--schema-file` and no `CREATE` in the parsed binlogs) is still assigned, and apply can stop at `ERROR 3105`.
- Resuming after a failed apply: keep the header (every line before the first `-- gtid:`), delete only the blocks above the failed one, and run the header plus the failed block and everything below it in a new session.
