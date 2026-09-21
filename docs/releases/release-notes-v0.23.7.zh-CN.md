# BinlogViz v0.23.7 发布说明

发布日期：2026-09-21

## 概述

v0.23.7 不再悄悄丢掉独立管理 QUERY。Analyze 把 QUERY 分成 ADMIN、Ignored QUERY、Unclassified QUERY。未分类语句如果是 GTID 开启的非显式组里唯一工作，会带着语句前缀失败，而不是伪装成 conflicting GTID。Transaction payload 会展开成内部 ROW 图；精确的 `FLUSH TABLES` 由已提交的 MySQL 8.0.46 方言 fixture 升为 ADMIN。

## Bug 修复

- **Unclassified QUERY 会让 analyze 失败**：`CHECK TABLE`、`SET ROLE` 以及其他未归类的独立语句不再被跳过。若它是 GTID 开启的非显式组（具名或匿名）里唯一工作，analyze 以 exit 1、空 stdout、一条带语句前缀的 `Error:` 失败，文案不是 “conflicting GTID”。
- **Ignored QUERY 会计数且从不关组**：`SET timestamp`、`SET NAMES` 以及不是 `SET ROLE` / `SET DEFAULT ROLE` 的其他 `SET` 仍是会话前缀。JSON 诊断用单独的可选字段计数，不和 Query-DML、未映射事件混用。只有 Ignored QUERY 的组后面再来一个 GTID，仍是 conflicting GTID。
- **窗口外的 Unclassified QUERY 不会毁掉窗口内报告**：裁剪标志把从未落入所选窗口的 Unclassified QUERY 当作边界观察。与窗口相交的未分类唯一工作组仍会失败。
- **Transaction payload 内部 ROW 图会计数**：压缩 wrapper 在分类前展开；成功展开后 wrapper 不计入未映射事件。内部图像共用 wrapper 在文件上的跨度一次，`--start-position` / `--stop-position` 看到的是磁盘范围。部分更新计为 UPDATE。匿名 GTID 仍以空身份开组。
- **精确 `FLUSH TABLES` 是 ADMIN**：已提交的 MySQL 8.0.46 ROW+GTID fixture 把它收成 ADMIN。`FLUSH TABLES WITH READ LOCK` 和 `FLUSH TABLES tbl` 仍是 Unclassified QUERY。原有四条 ADMIN 不变。

## 破坏性变更

无。

## 兼容性说明

- 退出码 0 / 1 / 2 仍按 ADR-0001。Unclassified QUERY 是 exit 1。只有 ADMIN、没有 ROW 图的文件仍是无数据结果（exit 2）。
- 新增可选 Ignored QUERY 计数字段的快照仍可加载。
- CLI 参数、产物命名和支持平台与 v0.23.6 一致。
