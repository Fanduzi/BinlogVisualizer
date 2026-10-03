# BinlogViz v0.23.12 发布说明

发布日期：2026-10-03

## 概述

v0.23.12 在默认文本、Markdown 和 HTML 的 analyze 报告里按事务写出身份。事件里有的 `server_id`、`thread_id`、GTID，以及 `xid` 或 XA xid，会写出来。`user@host` 只在 binlog 存了调用方时出现。事件里没有的字段保持缺席：不写 `0`，也不写空字符串。JSON 本来就有这些字段。`--sql-context full` 不变。workflow 与 trend 行为不变。

## 新功能

- **文本、Markdown 和 HTML 写出事务身份（#104）**：默认文本、Markdown 和 HTML 的 analyze 报告按事务写出事件里有的 `server_id`、`thread_id`、GTID，以及 `xid` 或 XA xid。记号是事务实际带上的 `server_id=`、`thread_id=`、`gtid=`、`xid=` 和 `xa_xid=`。`user@host=` 只在 binlog 存了调用方时出现。缺的字段保持缺席（不写 `0`，不写空字符串）。MySQL 8.0.46 的 `FLUSH TABLES` fixture 会写出 `server_id=1`、`thread_id=12`、它的 GTID 和 `xid=16`，不写 `user@host`。`minimal.binlog` 写出 `server_id`、`thread_id` 和 `xid`，不写 `gtid=` 或 `user@host`。JSON 本来就有这些字段，空的仍然省略。`--sql-context full` 不变。

## 验证

BinlogQA 在 tip `72cd660`（#104）上的 dogfood：2026-10-03 PASS。

## 破坏性变更

无。

## 兼容性说明

- 退出码 0 / 1 / 2 仍按 ADR-0001。
- JSON 的事务身份不变。`--sql-context full` 不变。
- CLI 参数、快照、工作流、产物命名和支持平台除此之外与 v0.23.11 一致。
- workflow 与 trend 行为不变。
