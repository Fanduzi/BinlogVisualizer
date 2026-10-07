# BinlogViz v0.23.18 Release Notes

Release date: 2026-10-07

## Overview

v0.23.18 names, in text, Markdown, and HTML, the tables that produced each busiest minute. The window's hottest table and the table behind the peak minute can differ. On `mysql-8.0.46-busiest-minute.binlog`, Activity peaks at `Rows/min: 32.0 at 2026-03-15 14:05`, and that minute is `shop.orders` 30 and `shop.catalog` 2, while Top Tables stays led by `shop.catalog` (82). JSON already had `diagnostics.hot_intervals[].table_rows` and `minutes[].table_rows`; the shape is unchanged. There is no new CLI flag. `--detect-spikes` stays opt-in. This is #141.

## New Features

- **Busiest Minutes names the tables in that minute (#141)**: `binlogviz analyze` lists those tables after Activity, in text, Markdown, and HTML. The section is the existing hot intervals, at most five, busiest first. Text heading: `=== Busiest Minutes ===`, led by `Rows in that minute, by table.` Each minute is one line, `2026-03-15 14:05:00 UTC  rows=32  txns=16`, then one indented line per table, ranked by rows and then by name (`shop.orders  30`, then `shop.catalog  2`). On `internal/binlog/testdata/mysql-8.0.46-busiest-minute.binlog`, Activity peaks at `Rows/min: 32.0 at 2026-03-15 14:05`. That minute is `shop.orders` 30 and `shop.catalog` 2, across 16 transactions. Top Tables stays `shop.catalog` (82) ahead of `shop.orders` (30). The earlier catalog minutes are 20 rows each (`shop.catalog  20`). A minute with no rows is omitted. A table with no rows in that minute is omitted. `--top` limits how many of those minutes are shown and how many tables are named under each; further tables use `… and <count> more tables`. `--show-minutes` stays off by default. On, each minute line appends the same tables (`shop.orders 30, shop.catalog 2`). Markdown adds `## Busiest Minutes` and a `Driving tables` column on that table and on the chronological minute table. HTML hot-interval cards list the same tables. JSON fields `diagnostics.hot_intervals[].table_rows` and `minutes[].table_rows` are unchanged. Top Transactions stays a 20-row `shop.catalog` insert. The 14:05 `shop.orders` writes are 15 two-row inserts, so they do not outrank those batches. The section is evidence. It does not add a write-spike finding. `--detect-spikes` stays opt-in.

## Bug Fixes

None.

## Verification

Tip dogfood on `b5b8c284` (#141): PASS on 2026-10-07 (BinlogQA).

## Breaking Changes

None.

## Compatibility

- Default text, Markdown, and HTML grow when a minute has rows. Text adds `Busiest Minutes` after Activity. Markdown adds `## Busiest Minutes` and a `Driving tables` column on the chronological minute table. HTML hot-interval cards list each minute's tables. JSON shape is unchanged.
- `--include-table`, `--exclude-table`, schema filters, `--dml`, and time, position, and GTID filters apply to this section the same way they apply to Top Tables. On the fixture, `--include-table shop.orders` keeps only `shop.orders  30`. `--exclude-table shop.orders` drops the 32-row minute and leaves the catalog minutes (`shop.catalog  20`).
- `--top` limits the minutes and the tables named under each. `--show-minutes` stays off by default; when on, each minute line names its tables.
- There is no new CLI flag. `--detect-spikes` stays opt-in. Exit codes 0, 1, and 2 keep ADR-0001 meaning.
- Snapshots, workflows, artifact names, and supported platforms are otherwise unchanged from v0.23.17.
- Workflow, trend, compare, and snapshot behavior are unchanged. This release does not emit rollback or flashback SQL.

## Known issues

None.
