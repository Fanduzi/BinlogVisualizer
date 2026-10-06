# BinlogViz v0.23.14 发布说明

发布日期：2026-10-06

## 概述

v0.23.14 在默认 analyze 报告里给会话排序，限制并打码语句文本，并在 analyze 结束时删掉 stdin 临时文件。默认文本、Markdown、JSON 和 HTML 都有 Top Threads/Sessions。`--top` 限制这一节；`--top-threads` 覆盖它。`--sql-context off` 去掉查询文本和 DDL 语句文本。`summary` 保留一行。`full` 打印存储 SQL，DDL 语句也一样，上限 4096 字节。被截断时以 `… [truncated: <shown> of <original> bytes]` 结尾。MySQL 和 MariaDB 账号 DDL 里的口令字面量写成 `<secret>`。MariaDB `SET PASSWORD` 是 DDL：它结束所属 GTID 组，留在 DDL 时间线上，口令已打码。时间线保留 VIEW、TRIGGER、routine 和 EVENT。`CREATE TRIGGER` 和 `DROP TRIGGER` 使用触发器名字。`--include-table` 和 `--exclude-table` 按这些对象名匹配。匹配到时即使没有行变更也退出 0。什么都匹配不到时仍是 exit 2。`binlogviz analyze -` 把 stdin 复制到临时文件，并在 SIGHUP、SIGINT、SIGQUIT、SIGTERM 时删掉这份副本。只有真正的终端才报终端。`/dev/null` 和空管道说 `stdin has no data`。stdin 的回放提示只给出位置，不编造文件路径。workflow 与 trend 行为不变。

## 新功能

- **Top Threads/Sessions（#117）**：默认文本、Markdown、JSON 和 HTML 的 analyze 报告包含 Top Threads/Sessions。任一会话写过行时按行数排序，否则按事件、字节、事务。每一行显示 thread id；binlog 里有 server id、user@host、schema 时一并显示。`--top` 限制这一节。`--top-threads` 覆盖这个上限。`0` 表示不限制。JSON 字段是 `threads`。`threads_ranked_by` 为 `rows`、`events`、`bytes` 或 `transactions`；没有任何会话带 thread id 或调用方时省略。
- **`--sql-context` 同时约束查询文本和 DDL 文本（#117）**：`off` 从所有格式去掉查询文本和 DDL 语句文本，包括默认文本和 `--show-patterns`。`summary` 保留一行空白归一化的摘要，SQL 正文最多 160 个字符。`full` 打印存储 SQL，上限 4096 字节。任何格式被截断时都以 `… [truncated: <shown> of <original> bytes]` 结尾。`query_truncated` 表示这个 4096 字节存储上限，不是 160 字符摘要。
- **从 stdin 分析一份二进制 binlog（#117）**：`binlogviz analyze -` 以及管道这类不可寻址路径从 stdin 读一份二进制 binlog。解析需要 seek，所以字节会先复制到临时文件。`mysqlbinlog` 文本不是 binlog，仍然过不了 magic header 检查。

## Bug 修复

- **账号 DDL 的口令写成 `<secret>`（#114、#120、#124）**：展示之前，在任何 `--sql-context` 模式下，口令字面量都改成 `<secret>`。MySQL：`IDENTIFIED BY`、`IDENTIFIED WITH … AS` 或 `BY`、`GRANT … IDENTIFIED`、`SET PASSWORD`。MariaDB：`IDENTIFIED VIA` 或 `WITH` 再加上 `USING`、`AS` 或 `BY`，包括 `PASSWORD('…')` 和哈希字面量，以及 `OR` 插件链。`IDENTIFIED BY RANDOM PASSWORD` 没有字面量，保持原样。`--sql-context off` 省略整句，所以既不打印口令，也不打印 `<secret>`。
- **MariaDB `SET PASSWORD` 是 DDL（#125）**：`SET PASSWORD` 结束所属 GTID 组，出现在 DDL 时间线上，口令为 `<secret>`。它不是 Ignored QUERY。后面的语句是自己的组。
- **`--sql-context full` 把 DDL 限在 4096 字节（#126）**：DDL 语句文本和查询文本使用同一个 4096 字节存储上限。被截断时追加 `… [truncated: <shown> of <original> bytes]`。`summary` 仍用 160 字符那一行，以及原始字节长度。
- **DDL 时间线保留视图、触发器、例程和事件（#122）**：`CREATE`、`ALTER`、`DROP` 的 `VIEW`、`TRIGGER`、`PROCEDURE`、`FUNCTION`、`EVENT` 留在时间线上。对象类型为 `view`、`trigger`、`routine` 或 `event`。`CREATE TRIGGER` 和 `DROP TRIGGER` 都使用触发器名字，不是 `ON` 后面的表。无法识别的 DDL 仍以通用 `DDL` 列出，不会被丢掉。
- **表过滤匹配这些对象名（#128）**：`--include-table` 和 `--exclude-table` 用和表一样的方式匹配视图、事件、函数、存储过程、触发器名字（`TABLE` 或 `SCHEMA.TABLE`）。过滤命中其中一个对象时退出 0，并打印这条 DDL，即使没有行变更。`CREATE TRIGGER … ON table` 按触发器名字保留，不按 `ON` 后面的表。什么都匹配不到时仍是 exit 2，`Error: schema/table filter matched no events`。
- **stdin 临时文件清理和提示（#118、#119、#127）**：命令结束时删除临时副本，包括 SIGHUP（exit 129）、SIGINT（exit 130，`Error: interrupted`）、SIGQUIT（exit 131）和 SIGTERM（exit 143）。只有真正的终端才报终端（`stdin is a terminal and has no binlog data`）。`/dev/null` 和空管道说 `stdin has no data`。stdin 的回放提示只给出位置（`input came from stdin; no replay path (start-position=… stop-position=…)`），不编造文件路径。

## 破坏性变更

无。

## 兼容性说明

- 退出码 0 / 1 / 2 仍按 ADR-0001。过滤命中视图、事件、函数、存储过程或触发器时，即使该对象没有行变更也退出 0。什么都匹配不到时仍是 exit 2。
- `--top` 仍限制排行节。`--top-threads` 覆盖 Top Threads 的上限。`0` 表示不限制。`--sql-context` 默认仍是 `summary`。
- `--sql-context off` 去掉查询文本和 DDL 语句文本。`full` 把存储 SQL 和 DDL 语句文本限在 4096 字节。`query_truncated` 表示这个存储上限。
- 会打印语句的模式下，账号 DDL 的口令字面量是 `<secret>`。`IDENTIFIED BY RANDOM PASSWORD` 不变。
- 在 Unix 上，SIGHUP 退出 129，SIGINT 退出 130（`Error: interrupted`），SIGQUIT 退出 131，SIGTERM 退出 143，退出前会删掉 stdin 临时副本。
- 快照、工作流、产物命名和支持平台除此之外与 v0.23.13 一致。
- workflow 与 trend 行为不变。
