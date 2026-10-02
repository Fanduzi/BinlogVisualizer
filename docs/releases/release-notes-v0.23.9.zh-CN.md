# BinlogViz v0.23.9 发布说明

发布日期：2026-10-02

## 概述

v0.23.9 把 `CHECK TABLE` 和 `SET ROLE` 收为 ADMIN。维护窗口里原先会因此失败的 analyze 现在以 exit 0 结束。准入依据是已提交的 MySQL 8.0.46 ROW+GTID 方言 fixture。精确 `FLUSH TABLES` 不变。`FLUSH TABLES WITH READ LOCK` 仍是 Unclassified QUERY。workflow 与 trend 行为不变。

## Bug 修复

- **`CHECK TABLE` 是 ADMIN（#87）**：双词前缀 `CHECK TABLE` 会关闭「该语句是唯一工作」的 GTID 开启非显式组，包括 `CHECK TABLE schema.table`。它不是 DDL，不会出现在 DDL 时间线上。准入 fixture 是 `mysql-8.0.46-check-table.binlog`：一个仅含维护语句的 GTID 组，随后一条业务 INSERT。分析该文件 exit 0，并报告那笔业务事务。
- **`SET ROLE` 是 ADMIN（#87）**：双词前缀 `SET ROLE` 覆盖 `SET ROLE ALL` 和 `SET ROLE <name>`。它不匹配 `SET DEFAULT ROLE`；后者仍是原有的三词 ADMIN。`SET timestamp`、`SET NAMES`，以及其他不是 `SET ROLE` / `SET DEFAULT ROLE` 的 `SET`，仍是 Ignored QUERY。准入 fixture 是 `mysql-8.0.46-set-role.binlog`（`SET ROLE ALL`，随后一条业务 INSERT）。分析该文件 exit 0。
- **精确 `FLUSH TABLES` 不变**：成员资格仍是现有 MySQL 8.0.46 fixture 上的精确相等。`FLUSH TABLES WITH READ LOCK` 和 `FLUSH TABLES <table>` 仍是 Unclassified QUERY。当它是 GTID 开启的非显式组里唯一工作时，analyze 仍 exit 1。

## 验证

BinlogQA 在 tip `d43513e`（#96 的合并提交）上的 dogfood：PASS。

## 破坏性变更

无。

## 兼容性说明

- 退出码 0 / 1 / 2 仍按 ADR-0001。只有这些 ADMIN 语句、没有 ROW 图的文件仍是无数据结果（exit 2）。Unclassified QUERY（含 `FLUSH TABLES WITH READ LOCK`）仍是 exit 1。
- CLI 参数、快照、工作流、产物命名和支持平台与 v0.23.8 一致。
