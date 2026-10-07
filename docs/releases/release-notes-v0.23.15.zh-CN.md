# BinlogViz v0.23.15 发布说明

发布日期：2026-10-07

## 概述

v0.23.15 可以把 analyze 报告限制到选定的 ROW 类型，并在需要时打印这些行改过的单元格。`--dml insert,update,delete` 可组合，只保留这些类型。它和已有的表、schema、时间、位置、GTID 过滤一起生效。摘要、Top Tables、Top Transactions、Top Threads 和告警只统计保留下来的类型，报告会写出这个过滤（`DML filter`）。什么都匹配不到时退出 2，stdout 为空，`Error: dml filter matched no events`。`--show-rows` 默认关闭。打开后，DELETE 打印前镜像，UPDATE 只打印变化的列（`before -> after`）以及 `(<count> unchanged)`，INSERT 打印后镜像。列名来自 MySQL 8 `binlog_row_metadata=FULL`。否则列是 `@1`..`@N`，报告说明没有列名。FULL 元数据下，无符号整数按无符号打印（`3000000000`）。没有符号信息时，两种读数不同的整数两种都打印，和 `mysqlbinlog -v` 一样。上限是每个事务 32 条逻辑行、每个值 64 字节。被截断时以 `… [truncated: <shown> of <original> bytes]` 结尾，超出上限的行有计数。`--sql-context off` 也会省略这些单元格，并说明已省略。两个标志都没开时，默认文本、Markdown、JSON、HTML 不变。本版本不生成 flashback SQL。workflow 与 trend 行为不变。

## 新功能

- **按选定的 DML 类型过滤报告（#131）**：`--dml` 接受 `insert`、`update`、`delete`，任意组合。记号按 INSERT、UPDATE、DELETE 的顺序存成大写。报告只保留这些 ROW 类型，并与 `--include-table`、`--exclude-table`、schema 过滤、`--start` / `--end`、位置选择器和 GTID 过滤一起生效。摘要、Top Tables、Top Transactions、Top Threads 和告警只统计保留下来的类型。文本摘要把过滤写成 `DML filter`。JSON 字段是 `scope.dml`。什么都匹配不到时退出 2，stdout 为空，`Error: dml filter matched no events`。
- **`--show-rows` 打印有界行镜像（#131）**：默认关闭。打开后，列出的每个事务里，DELETE 是前镜像，UPDATE 对变化的列写成 `column: before -> after`，然后是 `(<count> unchanged)`（或 `(no column changed)`），INSERT 是后镜像。列名来自 MySQL 8 `binlog_row_metadata=FULL`。否则说明是 `column names unavailable (binlog_row_metadata is not FULL); columns shown as @1..@N`。FULL 元数据下，无符号列打印无符号值（`3000000000`）。没有符号信息、两种读数不同的整数打印成 `signed (unsigned)`。每个事务最多保留 32 条逻辑行，每个值最多 64 字节。被截断时以 `… [truncated: <shown> of <original> bytes]` 结尾。超出的行写成 `rows omitted: <count>`。JSON 的 `transactions[].rows` 含 `op`、`columns`、`names`（`full` 或 `positional`）。DELETE 写 `before`，INSERT 写 `after`，UPDATE 写 `before`、`after` 和 `changed`。SQL NULL 是 JSON `null`。`rows_omitted` 是超过 32 的条数。`column_names_note` 和 `row_values_note` 说明缺列名或被省略。只有 `--show-rows` 打开且 `--sql-context` 不是 `off` 时才保留镜像。列出的事务仍有 `mysqlbinlog_cmd`。
- **`--sql-context off` 省略行值（#131）**：`--sql-context off` 也省略这些单元格，并写 `row values omitted because --sql-context is off`。JSON 字段是 `row_values_note`。

## Bug 修复

无。

## 验证

tip `1ac261e`（#131）的 QA：2026-10-07 PASS。已知遗留 #132 已建单，不是阻塞项。

## 破坏性变更

无。

## 兼容性说明

- 退出码 0 / 1 / 2 仍按 ADR-0001。`--dml` 什么都匹配不到时退出 2，stdout 为空，`Error: dml filter matched no events`。
- 既不设 `--dml` 也不设 `--show-rows` 时，默认文本、Markdown、JSON、HTML 不变。`--show-rows` 仍默认关闭。`--sql-context` 默认仍是 `summary`。
- `--sql-context off` 仍去掉查询文本和 DDL 语句文本，同时省略 `--show-rows` 的单元格。
- 账号 DDL 的口令字面量在语句文本里仍是 `<secret>`。这个改写不作用于行单元格。
- 本版本不生成 flashback 或回滚 SQL。JSON 的 `before`、`after`、`changed` 是后续版本可以用的形状。
- 快照、工作流、产物命名和支持平台除此之外与 v0.23.14 一致。
- workflow 与 trend 行为不变。

## 已知问题

- **TIMESTAMP 跟随进程时区（#132）**：`--show-rows` 按进程时区格式化 `TIMESTAMP` 列。`mysqlbinlog -v` 显示的是 UTC 时刻。在 `TZ=Asia/Shanghai` 下，MySQL 8.0.46 FULL fixture 会打印 `updated_at='2026-10-06 22:00:01.000000'`，而 UTC 墙钟是 `2026-10-06 14:00:01.000000`。`DATETIME` 不受影响。`binlog_row_metadata=FULL` 下的无符号整数仍按无符号打印。v0.23.15 没有修复这一点，也不是阻塞项。
