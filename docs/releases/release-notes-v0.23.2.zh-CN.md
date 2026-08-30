# BinlogViz v0.23.2 发布说明

发布日期：2026-08-30

## 概述

v0.23.2 是面向 MariaDB 值班场景的补丁版本。它恢复了包含已 PREPARE XA 事务的 binlog 分析能力；此前下一条 GTID 出现时，分析会在生成报告前失败。

## Bug 修复

- **物理 MariaDB XA PREPARE 事件会结束所属 GTID 组（#62）**：`XAPrepareLogEvent` 现在归一化为已有的 `XA_PREPARE` 边界，后续合法 GTID 会开启新事务，不再报 `conflicting GTID`。
- **位点与 GTID 选择器可跨越已 PREPARE 的 XA 事务**：选择器仍会消费建立事务上下文所需的前缀，但不会再被 XA PREPARE 后的合法 GTID 阻断。
- **真实的事务内 GTID 冲突仍会失败**：来源完整性校验保持不变；本补丁补齐缺失的物理 XA 边界，没有放松冲突检测。

## 破坏性变更

无。

## 兼容性说明

- CLI 参数、退出码、报告 schema、产物命名和支持平台均与 v0.23.1 保持一致。
- Query 形式的 XA 语句与 MariaDB 物理 XA PREPARE 日志事件均受支持。
