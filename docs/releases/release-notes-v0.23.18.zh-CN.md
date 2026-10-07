# BinlogViz v0.23.18 发布说明

发布日期：2026-10-07

## 概述

v0.23.18 在文本、Markdown、HTML 里点名每个最忙分钟是哪些表写出来的。整个窗口最热的表，和峰值那一分钟背后的表，可以不是同一张。在 `mysql-8.0.46-busiest-minute.binlog` 上，活动概览峰值是 `Rows/min: 32.0 at 2026-03-15 14:05`（中文标签是 `每分钟行数`），这一分钟是 `shop.orders` 30 行和 `shop.catalog` 2 行，热点表仍由 `shop.catalog`（82）领头。JSON 本来就有 `diagnostics.hot_intervals[].table_rows` 和 `minutes[].table_rows`，形状不变。没有新的 CLI 参数。`--detect-spikes` 仍是显式打开。本版本是 #141。

## 新功能

- **最忙分钟点名这一分钟的表（#141）**：`binlogviz analyze` 在活动概览之后列出这些表，文本、Markdown、HTML 都有。这一节就是原有的热点时段，最多五个，最忙的在前。文本标题是 `=== 最忙分钟 ===`，导语是 `这一分钟里的行，按表列出。` 每一分钟一行，例如 `2026-03-15 14:05:00 UTC  rows=32  txns=16`，下面按行数再按表名缩进列出（`shop.orders  30`，然后 `shop.catalog  2`）。在 `internal/binlog/testdata/mysql-8.0.46-busiest-minute.binlog` 上，活动概览峰值是 `Rows/min: 32.0 at 2026-03-15 14:05`。这一分钟是 `shop.orders` 30 行和 `shop.catalog` 2 行，共 16 个事务。热点表仍是 `shop.catalog`（82）排在 `shop.orders`（30）前面。更早的 catalog 分钟各 20 行（`shop.catalog  20`）。没有行的分钟不列出。这一分钟里行数为 0 的表不列出。`--top` 限制列出几分钟、每分钟点名几张表；超出的表写成 `… 还有 <count> 张表未显示`。`--show-minutes` 仍默认关闭。打开后，每一分钟那一行末尾接上同样的表（`shop.orders 30, shop.catalog 2`）。Markdown 增加 `## 最忙分钟`，并在这张表和按时间排列的分钟表上增加 `驱动表` 列。HTML 热点时段卡片列出同样的表。JSON 的 `diagnostics.hot_intervals[].table_rows` 和 `minutes[].table_rows` 不变。最大事务仍是 20 行的 `shop.catalog` 插入。14:05 的 `shop.orders` 是 15 次两行插入，不会压过那些批次。这一节是证据，不会变成写入尖峰发现。`--detect-spikes` 仍是显式打开。

## Bug 修复

无。

## 验证

tip `b5b8c284`（#141）的 BinlogQA dogfood：2026-10-07 PASS。

## 破坏性变更

无。

## 兼容性说明

- 某一分钟有行时，默认文本、Markdown、HTML 都会变长。文本在活动概览之后增加「最忙分钟」。Markdown 增加 `## 最忙分钟`，并在按时间排列的分钟表上增加 `驱动表` 列。HTML 热点时段卡片列出这一分钟的表。JSON 形状不变。
- `--include-table`、`--exclude-table`、schema 过滤、`--dml`，以及时间、位点、GTID 过滤，对这一节的作用与对热点表相同。在这份 fixture 上，`--include-table shop.orders` 只留下 `shop.orders  30`。`--exclude-table shop.orders` 去掉 32 行那一分钟，留下 catalog 的分钟（`shop.catalog  20`）。
- `--top` 限制列出的分钟数，以及每分钟点名的表数。`--show-minutes` 仍默认关闭；打开后，每一分钟那一行会点名表。
- 没有新的 CLI 参数。`--detect-spikes` 仍是显式打开。退出码 0 / 1 / 2 仍按 ADR-0001。
- 快照、工作流、产物命名和支持平台除此之外与 v0.23.17 一致。
- workflow、trend、compare 与 snapshot 行为不变。本版本不生成回滚或 flashback SQL。

## 已知问题

无。
