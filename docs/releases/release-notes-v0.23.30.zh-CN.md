# BinlogViz v0.23.30 发布说明

发布日期：2026-10-10

## 概述

v0.23.30 修改了 `binlogviz flashback --schema-file` 在一种情况下的报错：含 `/` 的生成列是在非默认 `div_precision_increment` 下写入的（#193）。binlog 不记录这个变量，所以这张表仍会被拒绝，但报错现在会写出能对上记下值的增量，说明转储可能没错，并提示用 `--include-table` / `--exclude-table`，而不是只让 DBA 去找另一份转储。哪些行被接受或拒绝没有变化，生成的脚本也完全相同。`analyze` 与 v0.23.29 相同。

## 变更

- **非默认 `div_precision_increment`（#193）**：含 `/` 的生成列按 MySQL 默认增量 4 对不上时，flashback 会在增量 0 到 30 下重新核对。如果每个记下的值都能在其中某些增量下对上，报错会写出来，例如 `记下的值只在 div_precision_increment 为 0-3 时成立`。报错也保留另一种可能：如果是转储里的表达式不对（例如表里是 `DIV` 而转储写成 `/`），请使用事故当时的转储。flashback 仍然退出 1，不输出 SQL。没有任何增量能产生的值，仍是原来的报错。
- **文档**：`docs/concept/limitations.zh-CN.md`（及英文版）补充说明：操作数是 `DECIMAL`，或商之后又参与乘法或加法时，任何非默认增量都可能改变存下来的数字，`DECIMAL(12,4)` 这样的窄列也一样，flashback 会拒绝这些行。

## Bug 修复

- #193：因为非默认 `div_precision_increment` 被拒绝的正确事故当时转储，报错却说转储不对。

## 验证

- 合并提交上的 CI（`verify`、`flashback e2e`）和 `go test ./...`，包括使用 issue 原始数值的新测试 `TestDivIncrementHint`：通过。
- issue 里的真实 MySQL 样本：`div_precision_increment` 为 0 和 2 时退出 1，中英文报错都写出 `0-3`；为 4 和 8 时退出 0，SQL 与 v0.23.29 相同。
- #193 的真实 MySQL 差分（27 个表达式、11 种目标类型、增量 0 到 30，共 2,328,777 个单元格、14,436,632 次探测）：输出与 v0.23.29 完全一致，误接受 0，误拒绝 0。

## 破坏性变更

无。只改了这一条报错的文字。

## 兼容性说明

- 执行支持 MySQL 5.7 及以上、MariaDB 10.2 及以上。
- `flashback` 从不连接 MySQL。先审阅、测试脚本，再在主库上新开一个会话、`sql_log_bin=1`、带上脚本头执行。某条语句失败时，脚本里更早的事务已经提交。
- JSON `report_version` 仍是 `3`。快照、工作流、产物名和支持的平台与 v0.23.29 相同。

## 已知问题

完整说明和绕过办法见 `docs/concept/limitations.zh-CN.md`。

- [#215](https://github.com/Fanduzi/BinlogVisualizer/issues/215)：大小写匹配假定全小写的 binlog 名字来自 `lower_case_table_names=1` 或 `2` 的服务器。在同时有 `t` 和 `T` 的 `lower_case_table_names=0` 服务器上，只含 `T` 的转储会被用在 `t` 上，守卫在任何写入之前停止执行（或 flashback 退出 1）。折叠后的表被拒绝时，错误里写的是 binlog 表名，也不打印折叠警告。两者都会安全失败。请用表名与 binlog 完全一致的转储。
- 某一列在解析到的 binlog 里先被加上、之后又在其中被修改或改名（`MODIFY`、`CHANGE`、`RENAME COLUMN`）时，正确的事故当时转储仍会失败，报 `reordered ... c, c`。会安全失败。只解析包含事故的 binlog，或者用这两次 `ALTER` 之间导出的转储。
- `--schema-file-db` 与好几个 `Database:` 头冲突时，警告只写出最后一个冲突的头，结尾的「未带库名的表没有使用」也不准确：转储头与 `--schema-file-db` 一致的那一段里的表会被使用。
- 库名首尾都是反引号，或以空格开头时，`Database:` 头会丢掉这个字符，这张表没有定义。
- 守卫报错那一行里，列名中的 `|` 显示为 `/`。守卫在 stdout 上的 `SELECT` 和 SQL 里仍是真实列名。
- 脚本不知道的生成列（没有 `--schema-file`，解析到的 binlog 里也没有 `CREATE`）仍会被赋值，执行可能停在 `ERROR 3105`。
- 执行失败后接着跑：保留脚本头（第一个 `-- gtid:` 之前的所有行），只删掉失败块上面的块，在新会话里执行脚本头加上失败块及其后的全部内容。
