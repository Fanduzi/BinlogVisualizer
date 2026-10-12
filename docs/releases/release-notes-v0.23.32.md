# BinlogViz v0.23.32 Release Notes

Release date: 2026-10-12

## Overview

v0.23.32 makes the `binlogviz flashback` `div_precision_increment` hint (#193) fast again and makes its increment list honest (#226, part of #227). On a 19.5 MB, 186-table binlog where the schema file is refused, the run takes 27.2 s, against 407.7 s on v0.23.29 and about 10 h on v0.23.30 and v0.23.31. The hint now also names the increments it could not check, instead of printing a cut-short range or no hint. Which columns and tables are accepted or refused is unchanged, and so is every script. `analyze` is unchanged from v0.23.31.

## Changes

- **Faster refused runs (#226)**: when a generated `/` column is refused, the hint re-checks it at every `div_precision_increment` from 0 to 30. Each table's row images are now parsed once and shared by all its generated columns and increments, the input-clash scan runs once, examples are formatted only when printed, and the image that refused one increment is tried first at the next. Memory use is unchanged.
- **Honest increment list (#227)**: increments where nothing contradicted the logged values but some image could not be evaluated (a quotient past 30 fractional digits, a value clipped to the column range) are named as not checked, for example `match it at div_precision_increment 10-27, and flashback could not check them at 28-30`. A column with no checked fit but some unchecked increments keeps the #193 wording with both explanations. Before, these increments were dropped silently, so `only at 10-27` read as exact and some columns got no hint at all.
- **Advice (#227)**: the error now says to leave the table out with `--exclude-table` (or to list only the other tables with `--include-table`), in English and Chinese.

## Bug Fixes

- #226: a refused run with the #193 hint was many times slower than v0.23.29 on a large binlog (about 10 h instead of 7 min on one corpus).
- #227 (part): the increment range could be cut short or left out, and the include-table advice was misleading. The remaining #227 items are listed under Known issues.

## Verification

- CI on the merge commit (`verify`, `flashback e2e`), `go test ./...`, and the MySQL 8.0 end-to-end tests on a fresh MySQL 8.0.46: pass.
- Timing on a quiet 8-core box, 19.5 MB binlog with 186 tables and the incident-time dump (a refused run), median of 3: **27.2 s** with v0.23.32, 407.7 s with v0.23.29. On a 16-table subset: 2.9 s, against 39.7 s on v0.23.29, 107.0 s on v0.23.30 and 109.7 s on v0.23.31. The full run on v0.23.30/31 took about 10 h. A wrong dump on the full binlog: 28.6 s. Peak memory is within 3% of v0.23.29.
- Increment lists checked against real MySQL 8.0.46 and 5.7.44 at increments 0–30, over 4608 refused columns with the correct dump and 17154 with a wrong dump: no increment is named as fitting when MySQL does not produce it, and no increment that fits is missing from both the fitting and the not-checked lists. English and Chinese give identical counts.
- Decisions: the refused column sets equal v0.23.29 and v0.23.30 (4608/4608 correct dump, 17154/17154 wrong dump), and accepted scripts are byte-identical to v0.23.31. Offline flashback corpus: SQL byte-identical to v0.23.31 in 4897/4897 cases; only refusal messages differ. `analyze`: 1500/1500 the same.
- Regressions: #187/#215 (59 live cases), #167, the #192/#208 guard cases, the #183 bypass set, correct apply and #180 on MySQL 8.0 and 5.7 match v0.23.31.

## Breaking Changes

None. Only refusal message text and speed change.

## Compatibility

- Apply is supported on MySQL 5.7 and newer and on MariaDB 10.2 and newer.
- `flashback` never connects to MySQL. Review the script, test it, and apply it in one new session on the primary with `sql_log_bin=1`, with the script header. A statement that fails leaves earlier transactions in the script committed.
- JSON `report_version` stays `3`. Snapshots, workflows, artifact names, and supported platforms are unchanged from v0.23.31.

## Known issues

See `docs/concept/limitations.md` for the full text and workarounds.

- [#225](https://github.com/Fanduzi/BinlogVisualizer/issues/225): the schema-file table label for a refused folded table is added only in English and only to the first refused table. Message only: flashback exits 1 and prints no SQL.
- [#227](https://github.com/Fanduzi/BinlogVisualizer/issues/227) (remaining): a zh-CN refusal still starts with the English `Error:` prefix, and the hint does not mention `FLUSH TABLES`.
- [#231](https://github.com/Fanduzi/BinlogVisualizer/issues/231): a column of a genuinely wrong dump that fits no increment now often gets the `div_precision_increment` wording (with a could-not-check list) instead of a plain "the dump is wrong" message. Still refused: flashback exits 1 and prints no SQL. A correct-dump refusal also always lists a few increments it could not check, usually `28-30`.
- The letter-case fold assumes an all-lower-case binlog name comes from a `lower_case_table_names=1` or `2` server. On a `lower_case_table_names=0` server with both `t` and `T`, a dump that has only `T` is used for `t`; the guard stops the apply before any write, or flashback exits 1. Use a dump with the exact binlog name.
- A column added in the parsed binlogs and later modified or renamed there (`MODIFY`, `CHANGE`, `RENAME COLUMN`) still makes a correct incident-time dump fail with `reordered ... c, c`. It fails safe. Parse only the incident binlog, or use a dump taken between those `ALTER`s.
- When `--schema-file-db` conflicts with several `Database:` headers, the warning names only the last conflicting header.
- A generated column the script does not know about (no `--schema-file` and no `CREATE` in the parsed binlogs) is still assigned, and apply can stop at `ERROR 3105`.
- Resuming after a failed apply: keep the header (every line before the first `-- gtid:`), delete only the blocks above the failed one, and run the header plus the failed block and everything below it in a new session.
