# BinlogViz v0.23.31 发布说明

发布日期：2026-10-12

## 概述

v0.23.31 让 `binlogviz flashback --schema-file` 在按大小写折叠匹配表时（v0.23.28 起，#187、#215）的两条提示更清楚。以前折叠匹配的表被拒绝时，报错只写 binlog 里的表名，而转储里并没有这个名字；现在报错会同时写出两张表。折叠警告现在会说明它假定产生 binlog 的服务器是 `lower_case_table_names=1` 或 `2`。哪些行被接受或拒绝没有变化，生成的脚本也完全相同。`analyze` 与 v0.23.30 相同。

## 变更

- **折叠匹配的表被拒绝（#215）**：英文报错写成 `p187x.mixt (schema file table P187X.MixT, letter case differs): --schema-file does not match the binlog columns: ...`，DBA 可以直接在转储里找到这张表。
- **折叠警告（#215）**：警告说明把 `P187X.MixT` 用于 `p187x.mixt` 的前提是 binlog 服务器为 `lower_case_table_names=1` 或 `2`；在 `lower_case_table_names=0` 的服务器上它们可能是两张不同的表，使用表名与 binlog 完全一致的转储即可避免这个推断。行数据仍会按转储中的表校验。
- **文档**：`docs/concept/limitations.md` 已更新为新的提示文字。

## Bug 修复

- #215：按大小写折叠匹配的表被拒绝时只写 binlog 表名，折叠警告也没有说明它对 `lower_case_table_names` 的假定。

## 验证

- 合并提交上的 CI（`verify`、`flashback e2e`）和 `go test ./...`（含新增的标注单元测试）：通过。
- 在 MySQL 8.0.46（`lower_case_table_names` 为 0 和 1）及 5.7.44 上实测 59 个用例，本版本与 v0.23.30 各跑一次，所有退出 0 的脚本都实际执行并与事故前数据比对：脚本 59/59 字节一致，退出码与执行结果 59/59 一致。在 `mixt` 和 `MixT` 是两张表的 `lower_case_table_names=0` 服务器上专门找问题，没有发现退出 0 却恢复出错误数据的情况。
- 离线 flashback 用例集：4897/4897 个用例 SQL 与 v0.23.30 字节一致，只有折叠相关的提示文字不同。`analyze`：1500/1500 相同。

## 破坏性变更

无。只改了提示文字。

## 兼容性说明

- 执行支持 MySQL 5.7 及以上、MariaDB 10.2 及以上。
- `flashback` 从不连接 MySQL。先审阅、测试脚本，再在主库上新开一个会话、`sql_log_bin=1`、带上脚本头执行。某条语句失败时，脚本里更早的事务已经提交。
- JSON `report_version` 仍是 `3`。快照、工作流、产物名和支持的平台与 v0.23.30 相同。

## 已知问题

完整说明和绕过办法见 `docs/concept/limitations.zh-CN.md`。

- [#225](https://github.com/Fanduzi/BinlogVisualizer/issues/225)：转储表名标注只在英文报错里有，而且只加在第一张被拒绝的表上。中文报错，以及第一张之后被拒绝的折叠表，仍只写 binlog 表名。只影响提示：flashback 仍退出 1，不输出 SQL。
- [#226](https://github.com/Fanduzi/BinlogVisualizer/issues/226)：含 `/` 的 `DECIMAL` 生成列被拒绝时，#193 的提示会在 `div_precision_increment` 0 到 30 下逐个重算，大 binlog 上被拒绝的运行可能慢很多倍。退出码和脚本不受影响。被拒绝的运行很慢时，可以用 `--include-table` / `--exclude-table` 缩小范围。
- [#227](https://github.com/Fanduzi/BinlogVisualizer/issues/227)：#193 给出的范围（例如 `0-3`、`10-27`）可能漏掉 MySQL 同样会产生的高位增量，有时干脆不给出。给出的增量都不会错，请把「只在」理解为「至少在」。
- 大小写折叠假定全小写的 binlog 表名来自 `lower_case_table_names=1` 或 `2` 的服务器。在同时有 `t` 和 `T` 的 `lower_case_table_names=0` 服务器上，只含 `T` 的转储会被用在 `t` 上；守卫会在任何写入之前停止执行，或 flashback 退出 1。请使用表名与 binlog 完全一致的转储。
- 某一列在解析到的 binlog 里先被加上、之后又在其中被修改或改名（`MODIFY`、`CHANGE`、`RENAME COLUMN`）时，正确的事故当时转储仍会失败，报 `reordered ... c, c`。会安全失败。只解析包含事故的 binlog，或者用这两次 `ALTER` 之间导出的转储。
- `--schema-file-db` 与好几个 `Database:` 头冲突时，警告只写出最后一个冲突的头。
- 脚本不知道的生成列（没有 `--schema-file`，解析到的 binlog 里也没有 `CREATE`）仍会被赋值，执行可能停在 `ERROR 3105`。
- 执行失败后接着跑：保留脚本头（第一个 `-- gtid:` 之前的所有行），只删掉失败块上面的块，在新会话里执行脚本头加上失败块及其后的全部内容。
