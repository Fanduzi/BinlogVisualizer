# BinlogViz v0.23.16 Release Notes

Release date: 2026-10-07

## Overview

v0.23.16 prints a `--show-rows` MySQL `TIMESTAMP` as the UTC wall clock of the stored instant, including fractional seconds. The printed time is independent of the process timezone. `DATETIME` stays the wall clock stored in the binlog. This fixes #132.

## New Features

None.

## Bug Fixes

- **`--show-rows` TIMESTAMP is UTC (#132)**: A `TIMESTAMP` cell, including fractional seconds, is the UTC wall clock of the stored instant. On `TZ=Asia/Shanghai` and on `TZ=UTC`, the MySQL 8.0.46 FULL fixture prints `updated_at='2026-10-06 14:00:01.000000'`. v0.23.15 printed `2026-10-06 22:00:01.000000` under `Asia/Shanghai`. The string matches `mysqlbinlog` `@6=1791295201` (14:00:01 UTC). A `DATETIME` cell is unchanged. Column names, unsigned integers, the 32-row and 64-byte bounds, and `--dml` are unchanged.

## Verification

Tip dogfood on `33a3f50` (#135): PASS on 2026-10-07 (BinlogQA). `Asia/Shanghai` and `UTC` both show `updated_at='2026-10-06 14:00:01.000000'`, matching `mysqlbinlog` `@6=1791295201`. `go test` is green under both timezones, including `TestShowRowsTimestampIsUTCInEveryZone`.

## Breaking Changes

None.

## Compatibility

- A process timezone other than UTC changes the `--show-rows` `TIMESTAMP` text. The same stored instant that printed `22:00:01` on `Asia/Shanghai` in v0.23.15 prints `14:00:01`. `DATETIME` text is unchanged.
- `--show-rows` stays off by default. `--dml`, column names, unsigned FULL integers, and the 32-row / 64-byte bounds are unchanged from v0.23.15.
- Exit codes 0, 1, and 2 keep ADR-0001 meaning.
- Snapshots, workflows, artifact names, and supported platforms are otherwise unchanged from v0.23.15.
- Workflow and trend behavior are unchanged.

## Known issues

None.
