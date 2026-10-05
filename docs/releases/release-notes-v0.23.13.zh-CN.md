# BinlogViz v0.23.13 发布说明

发布日期：2026-10-05

## 概述

v0.23.13 把索引变更记在语句改动的表上。`CREATE INDEX`、`CREATE UNIQUE INDEX`、`CREATE FULLTEXT INDEX`、`CREATE SPATIAL INDEX` 和 `DROP INDEX` 在 DDL 时间线、按表列表和 `--include-table` 里显示 `ON` 后面的那张表。`CREATE UNIQUE INDEX` 和 `CREATE FULLTEXT INDEX` 会留在 DDL 时间线上，`CREATE SPATIAL INDEX` 同样留下。语句里写的 schema 优先于 `USE` 选中的库。没有空格的 `ON t(col)` 和 `CREATE TABLE t(id INT)` 会解析成那张表。workflow 与 trend 行为不变。

## Bug 修复

- **索引 DDL 记在 `ON` 后面的表上（#112）**：`CREATE INDEX`、`CREATE UNIQUE INDEX`、`CREATE FULLTEXT INDEX`、`CREATE SPATIAL INDEX` 和 `DROP INDEX` 记在 `ON` 后面的表上。DDL 时间线、按表列表和 `--include-table` 用的都是这张表。`CREATE UNIQUE INDEX` 和 `CREATE FULLTEXT INDEX` 留在 DDL 时间线上。`CREATE SPATIAL INDEX` 同样留在时间线上。过滤名是这张表时，`--include-table` 会保留这些索引语句。
- **语句里的 schema 优先于 `USE`（#112）**：语句写了 `schema.table` 时，报告按那张表计数，包括会话里选了另一个库的情况。
- **`(` 前面没有空格时仍能认出表名（#112）**：`ON t(col)` 和 `CREATE TABLE t(id INT)` 解析成表 `t`。反引号标识符本身含有括号时保持原样。

## 破坏性变更

无。

## 兼容性说明

- 退出码 0 / 1 / 2 仍按 ADR-0001。
- CLI 参数不变。`--include-table` 在过滤名是 `ON` 后面的表时，会保留这些索引语句。
- `CREATE INDEX`、`CREATE UNIQUE INDEX`、`CREATE FULLTEXT INDEX`、`CREATE SPATIAL INDEX` 和 `DROP INDEX` 在 DDL 时间线、按表列表和 `--include-table` 里显示 `ON` 后面的表。`CREATE UNIQUE INDEX`、`CREATE FULLTEXT INDEX` 和 `CREATE SPATIAL INDEX` 会出现在 DDL 时间线上。
- 语句里写了 schema 时，这条语句按该 schema 计数。
- 快照、工作流、产物命名和支持平台除此之外与 v0.23.12 一致。
- workflow 与 trend 行为不变。
