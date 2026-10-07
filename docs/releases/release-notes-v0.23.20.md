# BinlogViz v0.23.20 Release Notes

Release date: 2026-10-07

## Overview

v0.23.20 reports, after Busiest Minutes, how far behind a replica was when it applied each transaction. MySQL 8 GTID events carry `original_commit_timestamp` and `immediate_commit_timestamp`. Delay is immediate minus original. On `mysql-8.0.46-replica-apply.binlog` the max delay is 5426519 µs (5.426519s), p95 is 5208595 µs (5.208595s), the peak minute is 2026-10-07 11:41:00 UTC, and the slowest transaction is `11458b63-c244-11f1-a0d2-822b383dbcd0:7` at byte 197 (`shop.audit`, 1 row). The lead sentence says this assumes the source and replica clocks agree. That sentence is not a finding. There is no new CLI flag. This is #147.

## New Features

- **Replica Apply Delay ranks MySQL 8 commit timestamps (#147)**: `binlogviz analyze` adds that section after Busiest Minutes in text, Markdown, and HTML. `--lang zh-CN` translates the headings and sentences. JSON adds optional `replica_apply_delay`. Text heading: `=== Replica Apply Delay ===`. When any counted delay is non-zero, the lead sentence is `Delay is immediate commit time minus original commit time. This assumes the source and replica clocks agree.` That sentence is not a finding, and a negative delay is not an alert. The next line is `Replica: at least one transaction committed here after it committed on the source.` Then `max 5.426519s  p95 5.208595s  peak minute 2026-10-07 11:41:00 UTC`. Each listed transaction is one line: `gtid=` when a GTID exists, the transaction start `file:byte` (the GTID event's `# at` byte, the same convention as the DDL Timeline), `original=`, `immediate=`, and `delay=`, then one indented line per table. On `mysql-8.0.46-replica-apply.binlog` the slowest transaction is `gtid=11458b63-c244-11f1-a0d2-822b383dbcd0:7` at byte 197, and the table line is `shop.audit  1`. Max is 5426519 µs (5.426519s). Nearest-rank p95 is 5208595 µs (5.208595s), rank 21 of 22 (`11458b63-c244-11f1-a0d2-822b383dbcd0:8`). The peak minute is the immediate commit time of the slowest transaction, 2026-10-07 11:41:00 UTC. `--top` limits how many transactions are listed. The max, the p95, and the peak minute stay on every counted transaction. Counted transactions are the same retained row transactions as Top Tables. Table, schema, time, position, GTID, and `--dml` filters apply. A DDL-only or zero-row group does not count. An anonymous GTID omits `gtid`. `--sql-context off` hides statements and keeps the timestamps and positions. Markdown heading: `## Replica Apply Delay`, with columns GTID, txn start, original commit, immediate commit, delay, and driving tables. HTML uses `id="section-replica-delay"`, after Activity. JSON `replica_apply_delay.origin` is `replica`. Fields: `max_delay_us`, `p95_delay_us`, `peak_minute` (RFC3339), and `transactions` (`gtid`, `txn_start_file`, `txn_start_pos`, `original_commit_us`, `immediate_commit_us`, `original_commit_time`, `immediate_commit_time`, `delay_us`, `tables`). A missing value is omitted. It is never an empty string, and a missing timestamp is never stored as `0`. `origin` stays the English token under `--lang zh-CN`. There is no new flag.
- **Equal timestamps are a source; missing timestamps are not a delay of 0 (#147)**: On `mysql-8.0.46-source-apply.binlog` every counted pair is equal. Text, Markdown, and HTML print the clock sentence and one line, `Source: original and immediate commit timestamps are equal.` There is no transaction table and no `gtid=` line. JSON sets `origin` to `source` and `max_delay_us` and `p95_delay_us` to `0`, omits `transactions`, and keeps `peak_minute` when that time is non-zero. A file without these fields (MySQL 5.7, MariaDB, or a zero timestamp), including `minimal.binlog` and `mariadb-10.11.14-dml.binlog`, prints one line, `commit timestamps unavailable`, and does not print the clock sentence. JSON omits `replica_apply_delay`. A missing timestamp is not a ranking and is not a delay of `0`. `--lang zh-CN` prints `=== 副本应用延迟 ===`. The unavailable line is `提交时间戳不可用`. The source line is `源库：原始提交时间与本机提交时间相同。`

## Bug Fixes

None.

## Verification

Tip dogfood on `766dc6f7` (#147): PASS on 2026-10-07 (BinlogQA).

## Breaking Changes

None.

## Compatibility

- Default text, Markdown, and HTML grow by a Replica Apply Delay section after Busiest Minutes. A file without commit timestamps is one line, `commit timestamps unavailable`. JSON adds optional `replica_apply_delay` and omits it when no counted transaction carried both timestamps.
- `--include-table`, `--exclude-table`, schema filters, `--dml`, and time, position, and GTID filters decide which transactions count, the same way they decide Top Tables. `--top` limits the listed transactions. When `--top-transactions` is set explicitly, that flag limits how many the analyzer keeps. The max, the p95, and the peak minute stay on the full counted set. `--sql-context off` still omits statements and keeps the timestamps and positions.
- `--lang zh-CN` translates the section. JSON `origin` stays `replica` or `source`.
- There is no new CLI flag. Exit codes 0, 1, and 2 keep ADR-0001 meaning. This section is not an alert.
- Snapshots, workflows, artifact names, and supported platforms are otherwise unchanged from v0.23.19.
- Workflow, trend, compare, and snapshot behavior are unchanged. This release does not emit rollback or flashback SQL.

## Known issues

None.
