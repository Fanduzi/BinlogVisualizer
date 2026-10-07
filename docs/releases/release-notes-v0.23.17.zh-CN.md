# BinlogViz v0.23.17 发布说明

发布日期：2026-10-07

## 概述

v0.23.17 会报告收到 UPDATE 或 DELETE 行事件、并且没有主键的表。副本在应用 ROW 事件时，每一行都可能全表扫描，这是复制延迟的常见原因。MySQL 8 `binlog_row_metadata=FULL` 的 TABLE_MAP 元数据把每张表标成 `has_pk`、`no_pk` 或 `unknown`。`SIMPLE_PRIMARY_KEY` 和 `PRIMARY_KEY_WITH_PREFIX` 是 `has_pk`。有 FULL 元数据但这两个字段都没有，是 `no_pk`。没有 FULL 元数据时，主键是否存在是 `unknown`；报告不会写出 `no_pk`。没有主键但只有 INSERT 的表会点名，不会被排成延迟风险。Top Findings 增加 `no_primary_key` 警告。没有新的 CLI 参数。本版本是 #138。

## 新功能

- **无主键表上的 UPDATE/DELETE 记为复制延迟发现（#138）**：`binlogviz analyze` 把 `key_status` 为 `no_pk`、并且至少收到一行 UPDATE 或 DELETE 的表，按这些行数排序，文本、Markdown、JSON、HTML 都有。这一节紧跟 Top Tables。文本标题是 `=== 无主键 ===`，导语是 `这些表上的 UPDATE/DELETE 会让副本全表扫描。` 在 `mysql-8.0.46-no-pk-full.binlog` 上，`shop.heap`（2 行 UPDATE、1 行 DELETE）排在 `shop.log`（1 行 UPDATE、0 行 DELETE）前面。`shop.scratch`（2 行 INSERT）写成 `仅 INSERT，不是延迟风险：shop.scratch (2 INSERT)`，不编号。`shop.orders` 和 `shop.prefixed`（`PRIMARY KEY (sku(8))`）是 `has_pk`，不进入这一节。Top Findings 增加 `[warning] shop.heap 没有主键，收到 2 行 UPDATE 和 1 行 DELETE；副本应用时可能全表扫描`。JSON 字段是 `tables[].key_status`（`has_pk`、`no_pk` 或 `unknown`）。一行就够。没有单独的阈值参数。
- **没有 FULL 元数据时保持未知（#138）**：在 `mysql-8.0.46-no-pk-minimal.binlog`（`binlog_row_metadata=MINIMAL`）上，每张有行变更的表都是 `unknown`。摘要多一行：`主键是否存在未知（binlog_row_metadata 不是 FULL）`。JSON 的 `primary_key_note` 就是这句话。没有「无主键」一节，也没有 `no_primary_key` 告警。每张有行变更的表都有主键时（`mysql-8.0.46-dml-full.binlog`），摘要一行是 `每张表都有主键`，并省略 `primary_key_note`。没有延迟排名、且只有 INSERT 的无主键表写成 `没有主键，且只有 INSERT（不是复制延迟风险）：…`。

## Bug 修复

无。

## 验证

tip `cca873f`（#138）的 BinlogQA dogfood：2026-10-07 PASS。

## 破坏性变更

无。

## 兼容性说明

- binlog 里有行事件时，默认文本、Markdown、JSON、HTML 都会变长。每张表都有主键时多一行 `每张表都有主键`。元数据不是 FULL 时多一行 `主键是否存在未知（binlog_row_metadata 不是 FULL）`。无主键表收到 UPDATE 或 DELETE 行时，多一节「无主键」，并多一条 `no_primary_key` 警告。JSON 增加 `tables[].key_status`；主键是否存在未知时还有 `primary_key_note`。
- `--include-table`、`--exclude-table`、schema 过滤、`--dml`，以及时间、位点、GTID 过滤，对这一节的作用与对 Top Tables 相同。`--dml insert` 会点名只有 INSERT 的无主键表，不把它们排成延迟风险。
- 没有新的 CLI 参数。退出码 0 / 1 / 2 仍按 ADR-0001。
- 快照、工作流、产物命名和支持平台除此之外与 v0.23.16 一致。
- workflow、trend、compare 与 snapshot 行为不变。本版本不生成回滚或 flashback SQL。

## 已知问题

无。
