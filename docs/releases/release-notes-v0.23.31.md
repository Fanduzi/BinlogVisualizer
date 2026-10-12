# BinlogViz v0.23.31 Release Notes

Release date: 2026-10-12

## Overview

v0.23.31 makes two `binlogviz flashback --schema-file` messages clearer when a table is matched through the letter-case fold from v0.23.28 (#187, #215). A refused folded table used to be named only by its binlog name, which the schema file does not contain. The error now names both tables. The fold warning now says it assumes the binlog server had `lower_case_table_names=1` or `2`. Which rows are accepted or refused is unchanged, and so is every script. `analyze` is unchanged from v0.23.30.

## Changes

- **Refused folded table (#215)**: the error reads `p187x.mixt (schema file table P187X.MixT, letter case differs): --schema-file does not match the binlog columns: ...`, so the DBA can find the table in the dump.
- **Fold warning (#215)**: the warning says that using `P187X.MixT` for `p187x.mixt` assumes `lower_case_table_names=1` or `2` on the binlog server, that on a `lower_case_table_names=0` server they can be two different tables, and that a dump with the exact binlog name avoids the guess. Rows are still checked against the file table.
- **Docs**: `docs/concept/limitations.md` describes the new wording.

## Bug Fixes

- #215: a refused table matched through the letter-case fold was named only by its binlog name, and the fold warning did not state its `lower_case_table_names` assumption.

## Verification

- CI on the merge commit (`verify`, `flashback e2e`) and `go test ./...`, including a new unit test for the label: pass.
- Live QA on MySQL 8.0.46 (`lower_case_table_names` 0 and 1) and 5.7.44, 59 cases, each run with this build and v0.23.30, every exit-0 script applied and compared with the pre-incident rows: scripts byte-identical in 59/59, exit and apply results identical in 59/59. A new hunt on a `lower_case_table_names=0` server where `mixt` and `MixT` are two tables found no case that restores wrong data with exit 0.
- Offline flashback corpus: SQL byte-identical to v0.23.30 in 4897/4897 cases; only the fold wording differs. `analyze`: 1500/1500 the same.

## Breaking Changes

None. Only message text changes.

## Compatibility

- Apply is supported on MySQL 5.7 and newer and on MariaDB 10.2 and newer.
- `flashback` never connects to MySQL. Review the script, test it, and apply it in one new session on the primary with `sql_log_bin=1`, with the script header. A statement that fails leaves earlier transactions in the script committed.
- JSON `report_version` stays `3`. Snapshots, workflows, artifact names, and supported platforms are unchanged from v0.23.30.

## Known issues

See `docs/concept/limitations.md` for the full text and workarounds.

- [#225](https://github.com/Fanduzi/BinlogVisualizer/issues/225): the schema-file table label is added only in English and only to the first refused table. A zh-CN refusal, and every refused folded table after the first, is still named by its binlog name. Message only: flashback exits 1 and prints no SQL.
- [#226](https://github.com/Fanduzi/BinlogVisualizer/issues/226): when a generated `DECIMAL` column with `/` is refused, the #193 hint re-checks it at every `div_precision_increment` from 0 to 30, which can make a refused run on a large binlog many times slower. Exit codes and scripts are unaffected. Narrow the run with `--include-table` / `--exclude-table` if a refusal is slow.
- [#227](https://github.com/Fanduzi/BinlogVisualizer/issues/227): the #193 range (`only at div_precision_increment 10-27`) can miss high increments that MySQL also produces, and is sometimes left out. No increment it names is wrong. Read `only` as "at least".
- The letter-case fold assumes an all-lower-case binlog name comes from a `lower_case_table_names=1` or `2` server. On a `lower_case_table_names=0` server with both `t` and `T`, a dump that has only `T` is used for `t`; the guard stops the apply before any write, or flashback exits 1. Use a dump with the exact binlog name.
- A column added in the parsed binlogs and later modified or renamed there (`MODIFY`, `CHANGE`, `RENAME COLUMN`) still makes a correct incident-time dump fail with `reordered ... c, c`. It fails safe. Parse only the incident binlog, or use a dump taken between those `ALTER`s.
- When `--schema-file-db` conflicts with several `Database:` headers, the warning names only the last conflicting header.
- A generated column the script does not know about (no `--schema-file` and no `CREATE` in the parsed binlogs) is still assigned, and apply can stop at `ERROR 3105`.
- Resuming after a failed apply: keep the header (every line before the first `-- gtid:`), delete only the blocks above the failed one, and run the header plus the failed block and everything below it in a new session.
