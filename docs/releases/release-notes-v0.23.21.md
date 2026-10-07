# BinlogViz v0.23.21 Release Notes

Release date: 2026-10-07

## Overview

v0.23.21 ranks the primary keys touched by the most UPDATE and DELETE row images. The section is Hot Rows, after No Primary Key and before Top Threads, in text, Markdown, and HTML. Identity comes only from MySQL 8 `binlog_row_metadata=FULL` (`SIMPLE_PRIMARY_KEY` or `PRIMARY_KEY_WITH_PREFIX`). On `mysql-8.0.46-hot-rows-full.binlog`, `shop.counters` `id=7` has 7 touches across 6 transactions. The first transaction is `87adba67-c25d-11f1-ba98-822b383dbcd0:11` at byte 2791 (2026-10-06 14:00:01 UTC) and the last is `87adba67-c25d-11f1-ba98-822b383dbcd0:16` at byte 4605 (2026-10-06 14:00:06 UTC). Those bytes are the GTID event's `# at` start, so `mysqlbinlog` or BinlogServer can open the group. The primary key is `id`, column `@2`. The first column is `label`. The report prints `id=7` and does not guess `@1`. `shop.heap` has no primary key and twelve UPDATE images, and it is not ranked. `--top` limits the list. `--top-rows` overrides it (`0` keeps every tracked key). `--sql-context off` hides the key values and keeps the counts. This is #150.

## New Features

- **Hot Rows ranks UPDATE and DELETE primary keys (#150)**: `binlogviz analyze` adds that section after No Primary Key and before Top Threads in text, Markdown, and HTML. `--lang zh-CN` translates the heading, the lead, and the sentences. JSON adds `hot_rows`. Text heading: `=== Hot Rows ===`. The lead is `UPDATE and DELETE row images, by primary key.` INSERT images are not counted. Each listed key is `schema.table` plus the key (`id=7`, or `sku=BOLT, wh=1` for a composite key), then `touches=` and `transactions=`, then `first` and `last` with the UTC event time and the GTID plus `file:byte` of that transaction. The byte is where the transaction starts. On `mysql-8.0.46-hot-rows-full.binlog` the first line is `1. shop.counters id=7`, then `touches=7  transactions=6`, then `first 2026-10-06 14:00:01 UTC  87adba67-c25d-11f1-ba98-822b383dbcd0:11 mysql-8.0.46-hot-rows-full.binlog:2791` and `last 2026-10-06 14:00:06 UTC  87adba67-c25d-11f1-ba98-822b383dbcd0:16 mysql-8.0.46-hot-rows-full.binlog:4605`. The first of those six transactions holds two UPDATE images, so the touch count is one higher than the transaction count. The same file also ranks `shop.inventory` `sku=BOLT, wh=1` (3 touches, 3 transactions), `shop.counters` `id=8` (2 touches, 2 transactions), the DELETE `shop.counters` `id=9` (1 touch), and `shop.inventory` `sku=NUT, wh=2` (1 touch). `--top 1` lists `id=7` and prints `4 more hot rows omitted`. `--top-rows 2` lists `id=7` and then `sku=BOLT, wh=1`. `--include-table shop.inventory` keeps the two inventory keys. `--dml delete` keeps only `id=9`. `--start 2026-10-06T14:00:04Z --end 2026-10-06T14:00:06Z` leaves `id=7` at 3 touches across 3 transactions. `--include-gtids` of the first GTID leaves `id=7` at 2 touches in 1 transaction. Markdown heading: `## Hot Rows`, with columns `#`, Table, Primary key, Touches, Transactions, First, Last, First transaction, and Last transaction. Those column titles stay English under `--lang zh-CN`. HTML uses `id="hot-rows-table"`. JSON `hot_rows[]` fields: `schema`, `table`, `primary_key`, `touches`, `transactions`, `first_time`, `last_time`, `first_gtid`, `first_file`, `first_pos`, `last_gtid`, `last_file`, `last_pos`. Times are RFC3339 (`2026-10-06T14:00:01Z`). `first_file` is the basename. `hot_rows_listed` and `hot_rows_omitted` count the `--top` / `--top-rows` limit. `hot_row_track_limit` is `8192`. A missing value is omitted. `report_version` stays `3`.
- **No key in the binlog is unavailable, and a replaced key is approximate (#150)**: On `mysql-8.0.46-hot-rows-minimal.binlog` the section does not rank a key. It prints `hot-row tracking unavailable for shop.counters: primary key is not in the binlog (binlog_row_metadata is not FULL)`, and the same sentence for `shop.inventory` and `shop.heap`. JSON `hot_rows` is `[]`. `hot_row_unavailable[]` has `schema`, `table`, `reason` (`metadata`), and `message`. The report does not print `@1`, `@2`, or `id=7`, and it does not say the table has no primary key. When FULL metadata named a primary key but a row image omitted those columns, the sentence is `hot-row tracking unavailable for <schema.table>: the primary key was not in the row image` and `reason` is `values`. A table with no primary key is not ranked and is not listed as unavailable. On the FULL fixture, `shop.heap` stays in No Primary Key. The INSERT of `shop.counters` `id=4` is not a hot row. The section is omitted when there are no ranked keys, no unavailable tables, and the cap was not hit. Tracking keeps at most 8192 primary keys. A new key after that replaces the least-touched key and starts from that dropped count plus one. The report prints `Hot-row tracking keeps 8192 primary keys. A new key replaced the least-touched key and may count touches that belonged to the dropped key; rows marked approximate include those inherited touches. A key that was never replaced keeps an exact count.` Text appends `approximate` on that key's count line. JSON sets `hot_rows_overflow` to true, sets `hot_rows_note` to that sentence, and sets `approximate` on the replaced key. A key that stayed in the table keeps an exact count. `--sql-context off` prints `primary key values hidden (--sql-context off)`, drops the key from the text line, and keeps the counts, times, GTIDs, and positions. Markdown and HTML print `[hidden]` in the key cell. JSON omits `primary_key` and sets `key_hidden` to true. `--lang zh-CN` prints `=== 热点行 ===`. The lead is `按主键统计 UPDATE 和 DELETE 行镜像。` The unavailable line is `shop.counters 无法追踪热点行：binlog 里没有主键列（binlog_row_metadata 不是 FULL）`. The hidden line is `主键值已隐藏（--sql-context off）`. The hidden key is `[已隐藏]`. JSON field names and `reason` stay English.

## Bug Fixes

None.

## Verification

Tip dogfood on `843e2c80` (#150): PASS on 2026-10-07 (BinlogQA).

## Breaking Changes

None.

## Compatibility

- Default text, Markdown, and HTML grow by a Hot Rows section after No Primary Key when a primary key was ranked, a table's key was unavailable, or the 8192 cap was hit. A file with nothing to say omits the section. JSON always adds `hot_rows` (an array), `hot_rows_listed`, `hot_rows_omitted`, and `hot_row_track_limit` (`8192`). `hot_rows_overflow`, `hot_rows_note`, and `hot_row_unavailable` are omitted when they do not apply. `report_version` stays `3`.
- `--top` limits how many keys are listed. `--top-rows` overrides it. `0` keeps every tracked key. The tracked set stays at most 8192 either way. The omitted line counts keys past the list limit, not keys the cap already dropped.
- `--include-table`, `--exclude-table`, schema filters, `--dml`, and time, position, and GTID filters decide which row images count, the same way they decide Top Tables.
- `--sql-context off` hides primary-key values in every format and keeps the counts, times, GTIDs, and positions.
- `--lang zh-CN` translates the section. JSON field names and `hot_row_unavailable[].reason` stay `metadata` or `values`. Markdown and HTML column titles stay English.
- `--top-rows` is new. Exit codes 0, 1, and 2 keep ADR-0001 meaning. This section is not an alert. The `no_primary_key` warning is unchanged.
- Snapshots, workflows, artifact names, and supported platforms are otherwise unchanged from v0.23.20.
- Workflow, trend, compare, and snapshot behavior are unchanged. This release does not emit rollback or flashback SQL.

## Known issues

None.
