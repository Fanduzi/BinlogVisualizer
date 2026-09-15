# BinlogViz v0.23.6 发布说明

发布日期：2026-09-15

## 概述

v0.23.6 让独立管理 QUERY 结束所属 GTID 组，后续 GTID 不再让 analyze 失败；`--start`/`--end` 也接受 DBA 常用的空格时间格式。

## Bug 修复

- **独立管理 QUERY 会结束所属 GTID 组**：`ANALYZE TABLE`、`OPTIMIZE TABLE`、`FLUSH PRIVILEGES`、`SET DEFAULT ROLE` 归一化为 `ADMIN`，不是 `DDL`。由 GTID 开始且没有 BEGIN/XA 的组在该 QUERY 结束位置关闭。下一个合法 GTID 不再被当成冲突。未知 QUERY（包括其他 `SET`/`FLUSH` 形式）仍会跳过。显式 `BEGIN`/`XA START` 后再遇到不同 GTID，analyze 仍失败。
- **`--start`/`--end` 接受 `YYYY-MM-DD HH:MM:SS`**：先去掉首尾空白。带 `Z`、`+08:00` 等显式偏移的 RFC3339 仍决定该时刻。空格格式按运行 `binlogviz` 的机器本地时区解释。包含端点语义和报告的 UTC 展示不变。

## 破坏性变更

无。

## 兼容性说明

- CLI 参数、退出码、报告 schema、产物命名和支持平台均与 v0.23.5 保持一致。零行管理语句组仍不会变成报告中的业务事务。
