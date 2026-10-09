# BinlogViz v0.23.27 Release Notes

Release date: 2026-10-09

## Overview

v0.23.27 fixes a cut in the `binlogviz flashback` guard error that could show a column that does not exist (#208). A column name ending in ` |` (or starting with `| `) formed the ` | ` list separator together with the real one, so the 200-byte cut could keep part of a name. Every `|` in a guard label is now shown as `/`. The guard still refuses in the same cases and writes no rows. `analyze` output and every other flashback statement are unchanged from v0.23.26.

## Changes

- **Guard labels rewrite every `|` (#208)**: in the `ERROR 1231` line, a `|` anywhere in a database, table or column name is shown as `/`, so no name can form the ` | ` separator and the byte cut always keeps whole names.
- The `SELECT` that the guard prints on stdout, and the `information_schema` checks in the SQL, keep the real names.

## Bug Fixes

- #208: with a column name ending in ` |`, the `ERROR 1231` line could keep that name without its trailing ` |` and present a column that does not exist. The #208 repro now keeps only the first whole name (106 bytes).

## Verification

- CI on the merge commit, `go test ./...` (including `TestGuardSafeLabel` and `TestGuardErrorValuePipeSuffix`), and the MySQL 8.0 end-to-end tests including `TestFlashbackRoundTripMySQL80` on a fresh MySQL 8.0.46: pass.
- 13 live `|` cases on MySQL 8.0.46 and 5.7.44 (the #208 repro, `|` as prefix, suffix and inside names, `|`-only names, names that already contain `/`, CJK table and column names with `|`, `|` straddling and exactly at the cut), in `mysql --force` and a continue-on-error runner: 13/13 match the expected line on both servers, at most 200 bytes, valid UTF-8, rows and GTID set unchanged, later writes fail with `ERROR 1792`. v0.23.26 passed 0/13.
- The 20 #192 cases still pass on both servers, byte-identical to v0.23.26. SQL-level fuzz of the cut: 0 mismatches in 3000 cases per server in each mode, including names with ` |` suffixes, `| ` prefixes and `|` anywhere.
- Wrong target: 21/21 safe on each server. Correct target: 6/6 restored identically on each server. #180, #191 and #197 behave as in v0.23.26.
- Generated scripts without `|` in any name are byte-identical to v0.23.26. The offline flashback differential over 596 cases shows 0 real differences, and refusals are unchanged. `analyze` is byte-identical to v0.23.26 on 1509 cases.

## Breaking Changes

None. The guard error shows `|` in names as `/`.

## Compatibility

- Apply is supported on MySQL 5.7 and newer and on MariaDB 10.2 and newer.
- `flashback` never connects to MySQL. Review the script, test it, and apply it in one new session on the primary with `sql_log_bin=1`, with the script header. A statement that fails leaves earlier transactions in the script committed.
- JSON `report_version` stays `3`. Snapshots, workflows, artifact names, and supported platforms are unchanged from v0.23.26.

## Known issues

See `docs/concept/limitations.md` for the full text and workarounds.

- In the guard error line, a `|` in a column name is shown as `/`, so `a|b` and `a/b` look the same there. The guard's `SELECT` on stdout and the SQL keep the real names.
- On a correct target, a reconnect in the middle of a block leaves that block not applied. Resume from that block, with the header, in a new session.
- [#167](https://github.com/Fanduzi/BinlogVisualizer/issues/167): an `ALTER` in the parsed binlog is applied again on top of a dump that already contains it. A joined multi-database dump uses only the first `Database:` header.
- [#187](https://github.com/Fanduzi/BinlogVisualizer/issues/187), [#193](https://github.com/Fanduzi/BinlogVisualizer/issues/193): a `Database:` header with a space and mixed-case names under `lower_case_table_names`; a non-default `div_precision_increment` with `DECIMAL` operands. Both fail safe.
- A generated column the script does not know about (no `--schema-file` and no `CREATE` in the parsed binlogs) is still assigned, and apply can stop at `ERROR 3105`.
- Resuming after a failed apply: keep the header (every line before the first `-- gtid:`), delete only the blocks above the failed one, and run the header plus the failed block and everything below it in a new session.
