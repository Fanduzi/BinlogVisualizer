# BinlogViz v0.23.20 发布说明

发布日期：2026-10-07

## 概述

v0.23.20 在最忙分钟之后报告副本应用每条事务时落后多少。MySQL 8 的 GTID 事件带有 `original_commit_timestamp` 和 `immediate_commit_timestamp`。延迟是本机提交时间减去原始提交时间。在 `mysql-8.0.46-replica-apply.binlog` 上，最大延迟是 5426519 µs（5.426519s），p95 是 5208595 µs（5.208595s），峰值分钟是 2026-10-07 11:41:00 UTC，最慢的事务是 `11458b63-c244-11f1-a0d2-822b383dbcd0:7`，字节 197（`shop.audit`，1 行）。导语说明这假设源库和副本的时钟一致。这句话不是发现。没有新的 CLI 参数。本版本是 #147。

## 新功能

- **副本应用延迟按 MySQL 8 提交时间戳排名（#147）**：`binlogviz analyze` 在最忙分钟之后增加这一节，文本、Markdown、HTML 都有。`--lang zh-CN` 翻译标题和句子。JSON 增加可选的 `replica_apply_delay`。文本标题是 `=== 副本应用延迟 ===`。任一计入延迟不为 0 时，导语是 `延迟是本机提交时间减去原始提交时间。这假设源库和副本的时钟一致。` 这句话不是发现，负延迟也不会变成告警。下一行是 `副本：至少有一个事务在源库提交之后才在这里提交。` 然后是 `最大 5.426519s  p95 5.208595s  峰值分钟 2026-10-07 11:41:00 UTC`。列出的每个事务一行：有 GTID 时写 `gtid=`，事务起点 `file:byte`（GTID 事件的 `# at` 字节，和 DDL 时间线同一约定），以及 `original=`、`immediate=`、`delay=`，下面按表缩进一行。在 `mysql-8.0.46-replica-apply.binlog` 上，最慢的事务是 `gtid=11458b63-c244-11f1-a0d2-822b383dbcd0:7`，字节 197，表那一行是 `shop.audit  1`。最大是 5426519 µs（5.426519s）。最近秩 p95 是 5208595 µs（5.208595s），22 条里的第 21 名（`11458b63-c244-11f1-a0d2-822b383dbcd0:8`）。峰值分钟是这条最慢事务的本机提交时间，2026-10-07 11:41:00 UTC。`--top` 限制列出几条事务。最大、p95 和峰值分钟按全部计入事务计算。计入的事务与热点表相同，都是保留下来的行事务。表、库、时间、位点、GTID 和 `--dml` 过滤都生效。只有 DDL 或零行的组不计入。匿名 GTID 省略 `gtid`。`--sql-context off` 隐藏语句，保留时间戳和位置。Markdown 标题是 `## 副本应用延迟`，列是 GTID、事务起点、原始提交、本机提交、延迟和驱动表。HTML 使用 `id="section-replica-delay"`，在活动概览之后。JSON 的 `replica_apply_delay.origin` 是 `replica`。这两个 `origin` 标记不翻译。字段有 `max_delay_us`、`p95_delay_us`、`peak_minute`（RFC3339），以及 `transactions`（`gtid`、`txn_start_file`、`txn_start_pos`、`original_commit_us`、`immediate_commit_us`、`original_commit_time`、`immediate_commit_time`、`delay_us`、`tables`）。缺的值省略。不会写成空字符串，缺时间戳也不会写成 `0`。没有新的参数。
- **时间戳相同是源库；缺时间戳不是延迟 0（#147）**：在 `mysql-8.0.46-source-apply.binlog` 上，每一对计入的时间戳都相等。文本、Markdown、HTML 写出时钟那句，再加一行 `源库：原始提交时间与本机提交时间相同。` 没有事务表，也不写 `gtid=`。JSON 把 `origin` 设为 `source`，`max_delay_us` 和 `p95_delay_us` 设为 `0`，省略 `transactions`；本机提交时间不为零时仍保留 `peak_minute`。没有这些字段的文件（MySQL 5.7、MariaDB，或时间戳为 0），包括 `minimal.binlog` 和 `mariadb-10.11.14-dml.binlog`，只打印一行 `提交时间戳不可用`，不打印时钟那句。英文报告里这一行是 `commit timestamps unavailable`。JSON 省略 `replica_apply_delay`。缺时间戳不会进入排名，也不会被写成延迟 `0`。

## Bug 修复

无。

## 验证

tip `766dc6f7`（#147）的 BinlogQA dogfood：2026-10-07 PASS。

## 破坏性变更

无。

## 兼容性说明

- 默认文本、Markdown、HTML 会变长，在最忙分钟之后增加「副本应用延迟」。没有提交时间戳的文件只有一行 `提交时间戳不可用`（英文是 `commit timestamps unavailable`）。JSON 增加可选的 `replica_apply_delay`；没有计入的事务同时带上两个时间戳时省略。
- `--include-table`、`--exclude-table`、schema 过滤、`--dml`，以及时间、位点、GTID 过滤，决定哪些事务计入，作用与对热点表相同。`--top` 限制列出的事务数。显式设置 `--top-transactions` 时，由它限制分析器保留的条数。最大、p95 和峰值分钟按全部计入事务计算。`--sql-context off` 仍省略语句，保留时间戳和位置。
- `--lang zh-CN` 翻译这一节。JSON 的 `origin` 仍是 `replica` 或 `source`。
- 没有新的 CLI 参数。退出码 0 / 1 / 2 仍按 ADR-0001。这一节不是告警。
- 快照、工作流、产物命名和支持平台除此之外与 v0.23.19 一致。
- workflow、trend、compare 与 snapshot 行为不变。本版本不生成回滚或 flashback SQL。

## 已知问题

无。
