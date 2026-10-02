# BinlogViz v0.23.10 发布说明

发布日期：2026-10-02

## 概述

v0.23.10 把已提交事务的时长记为窗口内最早到最晚的非零时间戳。MySQL 8 在提交时写入开头的 GTID 事件，并与 XID 盖上同一时刻，所以 `SLEEP` 之后按文件顺序算出的时长是 0s。`BEGIN` 和行事件保留语句开始的那一秒，这段墙上时钟跨度才是时长。一份已提交的 MySQL 8.0.46 fixture 同时锁住了开放 BEGIN+DML 路径。workflow 与 trend 行为不变。

## Bug 修复

- **已提交事务时长按服务器时钟（#99、#92）**：组时长是窗口内最早到最晚的非零时间戳。MySQL 在提交时给开头的 GTID 和 XID 盖戳；`BEGIN` 和行图保留语句开始时刻。准入 fixture 是 `mysql-8.0.46-committed-duration.binlog`：`INSERT` 与 `COMMIT` 之间的 `SELECT SLEEP(2)`（睡眠本身不记入 binlog）落入 `1s-10s` 桶，随后一条自动提交的 INSERT 仍在 `<1s`。`--large-trx-duration 1s` 会带着这段时长告警。默认阈值 `30s` 不会。文件里没有锁等待。
- **开放 BEGIN+DML 方言 fixture（#98、#91）**：`mysql-8.0.46-open-begin-dml.binlog` 是显式 `BEGIN`、两条 `INSERT` 行图、没有 `COMMIT` 或 `ROLLBACK`，随后一个更晚的业务 GTID。分析完整文件 exit 1，stdout 为空，唯一的 `Error:` 行写明 `open BEGIN without close`、时长、行数、表和文件跨度，并说明这段跨度不是锁争用证据。停在后一个 GTID 的前缀 exit 0，JSON 保留 `diagnostics.open_dml_groups`。该开放组不进入已提交时长排行。Error 行上的时长是开放组第一个事件到后一个 GTID 的墙上时钟（本文件为 `2s`）。EOF 前缀报告的组时长是 `0s`，因为该组自己的开始时刻和最后一行落在同一秒。

## 验证

BinlogQA 在 tip `1c2d527`（#99 的合并提交）上的 dogfood：已提交时长 fixture PASS。#98 的开放 BEGIN fixture 在同一 tip 上也通过。

## 破坏性变更

无。

## 兼容性说明

- 退出码 0 / 1 / 2 仍按 ADR-0001。写过行的开放 `BEGIN` 之后又来了 GTID 仍是 exit 1。这种形状的 EOF 开放前缀仍是 exit 0。
- MySQL 8 GTID 组的报告时长可以长于 v0.23.9 按文件顺序打印的跨度；当时开头的 GTID 与提交戳相同。
- CLI 参数、快照、工作流、产物命名和支持平台与 v0.23.9 一致。
