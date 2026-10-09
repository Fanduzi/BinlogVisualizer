# BinlogViz v0.23.26 Release Notes

Release date: 2026-10-09

## Overview

v0.23.26 keeps non-ASCII column names in the `binlogviz flashback` guard error (#192). MySQL cuts the `ERROR 1231` value at 200 bytes. The guard picked whole names by characters, so with CJK names the list went over 200 bytes and the line fell back to the bare column count. It now picks names by bytes. The guard still refuses in the same cases and writes no rows. `analyze` output and every other flashback statement are unchanged from v0.23.25.

## Changes

- **Guard error lists names by bytes (#192)**: the guard SQL takes the room as `200 - LENGTH(head)`, picks whole names on a binary copy of the list, then takes that many characters from the original. The separator ` | ` is ASCII, so the cut always lands on a character boundary. The value is valid UTF-8 and at most 200 bytes, so it reaches the client intact on MySQL 8.0 and 5.7.
- When no whole name fits, the value is the count form, for example `(1 columns)`, with no trailing `: `.
- The full list is still printed by the guard's `SELECT` on stdout.

## Bug Fixes

- #192: with non-ASCII column names, the `ERROR 1231` line showed only the count and no names, although the 200-byte limit fits several. The #192 repro (six `生成列_客户全名_N` columns) now lists 4 names in 199 bytes.

## Verification

- CI on the merge commit, `go test ./...`, and the MySQL 8.0 end-to-end tests including `TestFlashbackRoundTripMySQL80` on a fresh MySQL 8.0.46: pass.
- 20 live guard-error cases on MySQL 8.0.46 and 5.7.44 (CJK, Latin-1, ASCII and mixed names; lists of exactly 199, 200 and 201 bytes; a character straddling the cut; a single 64-character CJK name; 40 and 60 columns across tables; names containing `,` and ` | `), in `mysql --force` and a continue-on-error runner: 20/20 match the expected value on both servers. All 80 applies wrote 0 rows and left the GTID set unchanged. v0.23.25 printed only the count in 10 of these cases.
- SQL-level fuzz of the cut (1- to 4-byte names, 1–80 names): 0 mismatches in 3000 cases per server, apart from #208 below.
- Wrong target: 21/21 safe on each server across `mysql`, `--force`, `source`, tty paste, `mysqlsh`, a continue-on-error runner, header-less blocks, stale or forged guard state, XA and reconnects. Correct target: 6/6 restored identically on each server.
- Correct apply: generated scripts equal v0.23.25 apart from the guard cut lines and the per-script token. The offline flashback differential over 570 cases shows 0 real differences, and refusals are unchanged. `analyze` is byte-identical to v0.23.25 on 1473 cases.

## Breaking Changes

None. The guard error text can now contain names that v0.23.25 replaced with the count.

## Compatibility

- Apply is supported on MySQL 5.7 and newer and on MariaDB 10.2 and newer.
- `flashback` never connects to MySQL. Review the script, test it, and apply it in one new session on the primary with `sql_log_bin=1`, with the script header. A statement that fails leaves earlier transactions in the script committed.
- JSON `report_version` stays `3`. Snapshots, workflows, artifact names, and supported platforms are unchanged from v0.23.25.

## Known issues

See `docs/concept/limitations.md` for the full text and workarounds.

- On a correct target, a reconnect in the middle of a block leaves that block not applied. Resume from that block, with the header, in a new session.
- [#167](https://github.com/Fanduzi/BinlogVisualizer/issues/167): an `ALTER` in the parsed binlog is applied again on top of a dump that already contains it. A joined multi-database dump uses only the first `Database:` header.
- [#187](https://github.com/Fanduzi/BinlogVisualizer/issues/187), [#193](https://github.com/Fanduzi/BinlogVisualizer/issues/193): a `Database:` header with a space and mixed-case names under `lower_case_table_names`; a non-default `div_precision_increment` with `DECIMAL` operands. Both fail safe.
- [#208](https://github.com/Fanduzi/BinlogVisualizer/issues/208) (P2): a column name that ends in ` |` can be cut mid-name in the `ERROR 1231` line, so the line shows a name that does not exist. It is cosmetic and fails safe: the guard still refuses, the session is locked, 0 rows are written, and the `SELECT` on stdout has the full list.
- A generated column the script does not know about (no `--schema-file` and no `CREATE` in the parsed binlogs) is still assigned, and apply can stop at `ERROR 3105`.
- Resuming after a failed apply: keep the header (every line before the first `-- gtid:`), delete only the blocks above the failed one, and run the header plus the failed block and everything below it in a new session.
