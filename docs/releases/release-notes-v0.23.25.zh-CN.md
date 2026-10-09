# BinlogViz v0.23.25 发布说明

发布日期：2026-10-09

## 概述

v0.23.25 让 `binlogviz flashback` 脚本在普通的严格会话里，也能还原当初由非严格会话写下的行（#180）。以前零日期或除以零的生成列会让执行中途停下，而更早的事务已经提交。`analyze` 的输出和其他 flashback 语句与 v0.23.24 相同。

## 变更

- **零日期在严格会话里能还原（#180）**：`DATE`、`DATETIME`、`TIMESTAMP` 的月或日为零（`'0000-00-00'`、`'2026-00-15'`）时，写它的那一条语句只从 `@@SESSION.sql_mode` 去掉 `NO_ZERO_DATE`、`NO_ZERO_IN_DATE` 和 `TRADITIONAL`，执行完把保存的模式设回去。这条语句仍是严格模式。
- **除以零的生成列能还原（#180）**：脚本略去某个生成列（来自 `--schema-file` 或解析到的 `CREATE`），且它记下的值是 `NULL` 时，那条 `INSERT` 或 `UPDATE` 只去掉 `ERROR_FOR_DIVISION_BY_ZERO` 和 `TRADITIONAL`，这样 `b` 为 0 时 `a / b`、`a DIV b`、`a % b` 重新算出来仍是当初存下的 `NULL`。
- v0.23.22 起 `ENUM` 序号 0 用的也是这个机制，行为不变。

## Bug 修复

- #180：在 `sql_mode=''` 下写入的行，在服务器默认 `sql_mode` 下执行会停在 `ERROR 1292`（零日期）或 `ERROR 1365`（生成列除以零），更早的事务已经提交。现在能原样还原。

## 验证

- 合并提交上的 CI，以及在真实 MySQL 8.0.46 上跑的 `TestFlashbackRoundTripMySQL80`、`TestNumericDecodeMySQL80`：通过。
- 在 MySQL 8.0.46、MariaDB 10.6、MariaDB 10.11.19 上用同一套数据：零值和月日为零的 `DATE` / `DATETIME` / `TIMESTAMP`，含 `a / b` VIRTUAL、`a % b` STORED、`a DIV b` STORED 且有 `b` = 0 行的表，一个混合 `UPDATE`，以及一张普通表。事故之后在默认严格会话里执行脚本，`CHECKSUM TABLE` 与事故前一致。v0.23.24 停在 `ERROR 1365`，表只还原了一部分。
- 同一套数据在 `TRADITIONAL` 会话（MySQL 8.0.46）和 `STRICT_ALL_TABLES,NO_ZERO_DATE,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO` 会话（MariaDB 10.11）里执行，同样还原一致，结束时会话的 `sql_mode` 和执行前相同。
- 包裹内仍是严格模式：零日期行写进过窄的目标列时报 `ERROR 1406`，不会截断。
- 目标不对（目标上这些生成列是普通列）：`mysql` 停在守卫，`mysql --force` 报 `ERROR 1792`；两种情况下行和 GTID 集合都没变。

## 破坏性变更

无。写零日期、或重新计算记下值为 `NULL` 的生成列的语句，前后会多一对 `SET @binlogviz_sql_mode` / `SET SESSION sql_mode`。

## 兼容性说明

- 执行支持 MySQL 5.7 及以上、MariaDB 10.2 及以上。
- `flashback` 从不连接 MySQL。先审阅、测试脚本，再在主库上新开一个会话、`sql_log_bin=1`、带上脚本头执行。某条语句失败时，脚本里更早的事务已经提交。
- JSON `report_version` 仍是 `3`。快照、工作流、产物名和支持的平台与 v0.23.24 相同。

## 已知问题

完整说明和绕过办法见 `docs/concept/limitations.zh-CN.md`。

- 目标正确时，块中间重连会让这个块没有应用。在新会话里带上脚本头，从这个块接着执行。
- [#167](https://github.com/Fanduzi/BinlogVisualizer/issues/167)：解析到的 binlog 里的 `ALTER` 会再应用到已经包含它的转储上。拼接的多库转储只用第一个 `Database:` 头。
- [#187](https://github.com/Fanduzi/BinlogVisualizer/issues/187)、[#192](https://github.com/Fanduzi/BinlogVisualizer/issues/192)、[#193](https://github.com/Fanduzi/BinlogVisualizer/issues/193)：带空格的 `Database:` 头和 `lower_case_table_names` 下的大小写混用；守卫报错里的非 ASCII 列名；非默认 `div_precision_increment` 加 `DECIMAL` 操作数。都会安全失败。
- 脚本不知道的生成列（没有 `--schema-file`，解析到的 binlog 里也没有 `CREATE`）仍会被赋值，执行可能停在 `ERROR 3105`。
- 执行失败后接着跑：保留脚本头（第一个 `-- gtid:` 之前的所有行），只删掉失败块上面的块，在新会话里执行脚本头加上失败块及其后的全部内容。
