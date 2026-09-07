# BinlogViz v0.23.5 发布说明

发布日期：2026-09-07

## 概述

v0.23.5 让 MariaDB XA ROLLBACK 结束所属 GTID 组，后续 GTID 不再让 analyze 失败；默认 analyze 也不再需要 CGO 或 DuckDB。

## Bug 修复

- **XA ROLLBACK 会结束所属 GTID 组**：Query `XA ROLLBACK` 关闭当前事务组。XA END 仍继续。下一个合法 GTID 不再被当成冲突。
- **零行 XA 只有带文件位置才进报告**：保留条件是 XA 身份、已记录的起始文件路径，以及 `PositionEnd > PositionStart`。纯 DDL 组仍只出现在 DDL 时间线。
- **默认 analyze 可在无 CGO 下构建和运行**：DuckDB 只是可选的 `--detail-store duckdb` 适配器。无 CGO 时 `NewDuckDBStore` 返回 `ErrDuckDBRequiresCGO`。workflow analyze 与 CLI 一样默认走 `none`。
- **未映射的解析事件会计数**：没有规范 kind 的事件（ROTATE 等）计入 JSON `unmapped_events`，不再从诊断里消失。

## 改进

- 解析器 `EventType` 使用单一规范 kind（`QUERY`、`WRITE_ROWS`、`GTID`、`XA_PREPARE` 等），go-mysql 的 `String()` 名字不再进入分析。
- analyze / compare / trend HTML 共用同一套五主题 CSS token。

## 破坏性变更

无。

## 兼容性说明

- CLI 参数、退出码、报告 schema、产物命名和支持平台均与 v0.23.4 保持一致。JSON 可能多一个可选字段 `unmapped_events`。
