# BinlogViz v0.23.4 发布说明

发布日期：2026-09-06

## 概述

v0.23.4 恢复对含连续 GRANT / CREATE USER `ddl` GTID 的 MariaDB 初始化 binlog 的 analyze，并修好值班会踩到的表过滤、prefix 与 export CLI 不一致。

## Bug 修复

- **连续 MariaDB GRANT DDL GTID 可正常分析（#67）**：`GRANT` / `REVOKE` Query 现在会结束所属 GTID 组，`--from-dir` 扫到 `mysql-bin.000001` 不再 `conflicting GTID`。真事务内 GTID 冲突仍会拒绝。
- **`--include-table` 接受 `SCHEMA.TABLE`（#65）**：`dogfood.orders` 按 schema `dogfood` + table `orders` 匹配，不再当成字面表名滤空。
- **`--prefix` 接受完整文件名（#69）**：`--prefix mysql-bin.000008` 只选该文件。看起来像文件名的无匹配错误会提示公共前缀或直接 `analyze DIR/file`。
- **`workflow export` 接受 `-o`（#68）**：短选项与 `analyze --output` 对齐。

## 文档

- 中文 README 补上英文已有的 SHOW MASTER STATUS / GTID 快速开始（#66）。

## 破坏性变更

无。

## 兼容性说明

- CLI 参数、退出码、报告 schema、产物命名和支持平台均与 v0.23.3 保持一致。
