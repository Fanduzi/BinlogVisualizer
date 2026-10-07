# BinlogViz v0.23.21 发布说明

发布日期：2026-10-07

## 概述

v0.23.21 按主键给被碰到次数最多的 UPDATE 和 DELETE 行镜像排名。这一节叫热点行，在「无主键」之后、Top Threads 之前，文本、Markdown、HTML 都有。身份只来自 MySQL 8 `binlog_row_metadata=FULL`（`SIMPLE_PRIMARY_KEY` 或 `PRIMARY_KEY_WITH_PREFIX`）。在 `mysql-8.0.46-hot-rows-full.binlog` 上，`shop.counters` 的 `id=7` 被碰到 7 次，分布在 6 个事务里。最早的事务是 `87adba67-c25d-11f1-ba98-822b383dbcd0:11`，字节 2791（2026-10-06 14:00:01 UTC），最晚的是 `87adba67-c25d-11f1-ba98-822b383dbcd0:16`，字节 4605（2026-10-06 14:00:06 UTC）。这两个字节是 GTID 事件的 `# at` 起点，可以直接拿去打开 `mysqlbinlog` 或 BinlogServer。主键是 `id`，也就是第 2 列 `@2`。第 1 列是 `label`。报告写出 `id=7`，不会拿 `@1` 来猜。`shop.heap` 没有主键，有 12 次 UPDATE，不参与排名。`--top` 限制列出几条。`--top-rows` 覆盖它（`0` 保留已追踪的全部键）。`--sql-context off` 隐藏键值，次数仍在。本版本是 #150。

## 新功能

- **热点行按 UPDATE 和 DELETE 的主键排名（#150）**：`binlogviz analyze` 在「无主键」之后、Top Threads 之前增加这一节，文本、Markdown、HTML 都有。`--lang zh-CN` 翻译标题、导语和句子。JSON 增加 `hot_rows`。文本标题是 `=== 热点行 ===`。导语是 `按主键统计 UPDATE 和 DELETE 行镜像。` INSERT 不计入。每一条是 `schema.table` 加上键（`id=7`，复合键为 `sku=BOLT, wh=1`），然后是 `次数=` 和 `事务=`，再是 `最早` 和 `最晚`，带上 UTC 事件时间，以及该事务的 GTID 和 `file:byte`。这个字节是事务起点。在 `mysql-8.0.46-hot-rows-full.binlog` 上，第一行是 `1. shop.counters id=7`，然后是 `次数=7  事务=6`，然后是 `最早 2026-10-06 14:00:01 UTC  87adba67-c25d-11f1-ba98-822b383dbcd0:11 mysql-8.0.46-hot-rows-full.binlog:2791` 和 `最晚 2026-10-06 14:00:06 UTC  87adba67-c25d-11f1-ba98-822b383dbcd0:16 mysql-8.0.46-hot-rows-full.binlog:4605`。这 6 个事务里的第一个含有两次 UPDATE，所以次数比事务数多 1。同一份文件还排出 `shop.inventory` 的 `sku=BOLT, wh=1`（3 次，3 个事务）、`shop.counters` 的 `id=8`（2 次，2 个事务）、DELETE 的 `shop.counters` `id=9`（1 次），以及 `shop.inventory` 的 `sku=NUT, wh=2`（1 次）。`--top 1` 只列出 `id=7`，并写出 `另有 4 个热点行未列出`。英文报告里这一行是 `4 more hot rows omitted`。`--top-rows 2` 先列出 `id=7`，再列出 `sku=BOLT, wh=1`。`--include-table shop.inventory` 只留下这两个库存键。`--dml delete` 只留下 `id=9`。`--start 2026-10-06T14:00:04Z --end 2026-10-06T14:00:06Z` 之后，`id=7` 是 3 次、3 个事务。`--include-gtids` 只选最早那个 GTID 时，`id=7` 是 2 次、1 个事务。Markdown 标题是 `## 热点行`。列名仍是英文：`#`、Table、Primary key、Touches、Transactions、First、Last、First transaction、Last transaction。`--lang zh-CN` 不翻译这些列名。HTML 使用 `id="hot-rows-table"`。JSON `hot_rows[]` 的字段有 `schema`、`table`、`primary_key`、`touches`、`transactions`、`first_time`、`last_time`、`first_gtid`、`first_file`、`first_pos`、`last_gtid`、`last_file`、`last_pos`。时间是 RFC3339（`2026-10-06T14:00:01Z`）。`first_file` 是文件名。`hot_rows_listed` 和 `hot_rows_omitted` 记录 `--top` / `--top-rows` 的上限。`hot_row_track_limit` 是 `8192`。缺的值省略。`report_version` 仍是 `3`。
- **binlog 里没有主键就是无法追踪；被替换的键标成近似（#150）**：在 `mysql-8.0.46-hot-rows-minimal.binlog` 上，这一节不给任何键排名。它写出 `shop.counters 无法追踪热点行：binlog 里没有主键列（binlog_row_metadata 不是 FULL）`，`shop.inventory` 和 `shop.heap` 同样一句。英文报告里这一行是 `hot-row tracking unavailable for shop.counters: primary key is not in the binlog (binlog_row_metadata is not FULL)`。JSON 的 `hot_rows` 是空数组。`hot_row_unavailable[]` 有 `schema`、`table`、`reason`（`metadata`）和 `message`。报告不写 `@1`、`@2` 或 `id=7`，也不把这个缺口说成没有主键。FULL 元数据已经点名主键、但某次行镜像没带上这些列时，句子是 `{{表}} 无法追踪热点行：行镜像里没有主键列`，英文是 `hot-row tracking unavailable for <schema.table>: the primary key was not in the row image`，`reason` 是 `values`。没有主键的表不参与排名，也不会被写成无法追踪。在 FULL 这份 fixture 上，`shop.heap` 仍在「无主键」一节。`shop.counters` 的 INSERT `id=4` 不是热点行。没有排名、没有无法追踪的表、也没有触及追踪上限时，这一节省略。追踪最多保留 8192 个主键。满了之后，新键替换被碰到次数最少的键，次数从被丢掉的计数加一开始。报告写出 `热点行追踪最多保留 8192 个主键。新的键会替换被碰到次数最少的键，计数可能带上被丢掉的键的次数；标成近似的行包含这些继承来的次数。一直留在表里的键计数是精确的。` 英文是 `Hot-row tracking keeps 8192 primary keys. A new key replaced the least-touched key and may count touches that belonged to the dropped key; rows marked approximate include those inherited touches. A key that was never replaced keeps an exact count.` 文本在该键的计数行末尾加上 `近似`。JSON 把 `hot_rows_overflow` 设为 true，把 `hot_rows_note` 设为这句，并在被替换的键上设 `approximate`。一直留在表里的键计数是精确的。`--sql-context off` 写出 `主键值已隐藏（--sql-context off）`，文本行不再带键，次数、时间、GTID 和位置仍在。英文这一行是 `primary key values hidden (--sql-context off)`。Markdown 和 HTML 的键那一格写 `[已隐藏]`，英文是 `[hidden]`。JSON 省略 `primary_key`，并把 `key_hidden` 设为 true。JSON 字段名和 `reason` 不翻译。

## Bug 修复

无。

## 验证

tip `843e2c80`（#150）的 BinlogQA dogfood：2026-10-07 PASS。

## 破坏性变更

无。

## 兼容性说明

- 默认文本、Markdown、HTML 会变长：有主键被排名、有表的键无法追踪，或触及 8192 上限时，在「无主键」之后增加「热点行」。没有可说的内容时，这一节省略。JSON 总会增加 `hot_rows`（数组）、`hot_rows_listed`、`hot_rows_omitted` 和 `hot_row_track_limit`（`8192`）。`hot_rows_overflow`、`hot_rows_note`、`hot_row_unavailable` 不适用时省略。`report_version` 仍是 `3`。
- `--top` 限制列出几条。`--top-rows` 覆盖它。`0` 保留已追踪的全部键。无论哪个上限，追踪集合最多仍是 8192。省略那一行数的是列表上限之外的键，不是上限已经丢掉的键。
- `--include-table`、`--exclude-table`、schema 过滤、`--dml`，以及时间、位点、GTID 过滤，决定哪些行镜像计入，作用与对热点表相同。
- `--sql-context off` 在每种格式里隐藏主键值，次数、时间、GTID 和位置仍在。
- `--lang zh-CN` 翻译这一节。JSON 字段名和 `hot_row_unavailable[].reason` 仍是 `metadata` 或 `values`。Markdown 和 HTML 的列名仍是英文。
- `--top-rows` 是新参数。退出码 0 / 1 / 2 仍按 ADR-0001。这一节不是告警。`no_primary_key` 告警不变。
- 快照、工作流、产物命名和支持平台除此之外与 v0.23.20 一致。
- workflow、trend、compare 与 snapshot 行为不变。本版本不生成回滚或 flashback SQL。

## 已知问题

无。
