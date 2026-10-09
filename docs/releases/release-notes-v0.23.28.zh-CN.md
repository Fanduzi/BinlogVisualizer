# BinlogViz v0.23.28 发布说明

发布日期：2026-10-09

## 概述

v0.23.28 让 `binlogviz flashback --schema-file` 能用上两类以前会被跳过的转储（#187）。mysqldump 的 `Database:` 头里库名带空格时，现在能读出来。binlog 里的库名、表名全是小写，而转储里的表只有大小写不同时（例如 `lower_case_table_names=0` 的转储配 `lower_case_table_names=1` 或 `2` 的 binlog），现在会采用这张表。以前这两种情况都会提示"没有表定义"，执行可能停在 `ERROR 3105`，而更早的事务已经提交。`analyze` 的输出与 v0.23.27 相同。

## 变更

- **`Database:` 头带空格（#187）**：`-- Host: localhost    Database: p187 sp` 这样的头会把转储绑定到 `p187 sp`。库名带引号、名字中间有反引号、中文，以及 CRLF 换行的转储也能读。
- **按 `lower_case_table_names` 忽略大小写匹配（#187）**：binlog 里的库名和表名全是小写、而文件里只有大小写不同的同名表时，flashback 采用文件里的表，并打印 `warning: <binlog 表>：使用 --schema-file 中的 <文件表> ...`。行数据仍按这份定义校验。大小写完全一致的表总是优先。
- binlog 名字含大写，或文件里有两张只差大小写的表时，不做猜测：警告里写出那张相近的表，这张表按没有定义处理，与 v0.23.27 相同。

## Bug 修复

- #187：事故当时的正确转储，如果 `Database:` 头带空格，或来自 `lower_case_table_names` 不同的服务器，会被忽略。这张表的生成列于是被赋值，执行停在 `ERROR 3105`，而更早的事务已经提交。

## 验证

- 合并提交上的 CI、`go test ./...`，以及在全新 MySQL 8.0.46 上跑的 MySQL 8.0 端到端测试（含 `TestFlashbackRoundTripMySQL80`）：通过。
- 在 MySQL 8.0.46 上真实还原（事故库 `lower_case_table_names=1`，转储来自 `lower_case_table_names=0` 的库）：21/21 还原一致；其中 20/20 的脚本在 MySQL 5.7.44 `lower_case_table_names=1` 的目标上也还原一致。用例包括：带空格、引号、反引号、中文以及名字里含 `Database:` 的头；CRLF 转储；含存储和虚拟生成列的有主键表和无主键表；多表和两个库的转储；Unicode 名字；不带库名的 `SHOW CREATE TABLE` 配 `--schema-file-db`。v0.23.27 为 4/21。
- 通过大小写匹配用上的错误转储（表达式不同、多一列、一张对一张错）退出 1，不打印 SQL。v0.23.27 对这些情况会打印脚本，执行到 `ERROR 3105` 前已部分应用。
- 与 v0.23.27 的离线 flashback 差异对比共 4722 个用例：4672 个逐字节一致，5 个只差进度条空白，45 个是 #187 预期的变化。`analyze` 在 1698 个用例上与 v0.23.27 逐字节一致。#192 和 #208 的 34 个守卫用例 `ERROR 1231` 行一致。

## 破坏性变更

无。以前对大小写不同或库名带空格的表提示"没有表定义"的运行，现在会采用文件里的定义、打印大小写警告，并可能拒绝与 binlog 对不上的转储。

## 兼容性说明

- 执行支持 MySQL 5.7 及以上、MariaDB 10.2 及以上。
- `flashback` 从不连接 MySQL。先审阅、测试脚本，再在主库上新开一个会话、`sql_log_bin=1`、带上脚本头执行。某条语句失败时，脚本里更早的事务已经提交。
- JSON `report_version` 仍是 `3`。快照、工作流、产物名和支持的平台与 v0.23.27 相同。

## 已知问题

完整说明和绕过办法见 `docs/concept/limitations.zh-CN.md`。

- [#215](https://github.com/Fanduzi/BinlogVisualizer/issues/215)：大小写匹配假定全小写的 binlog 名字来自 `lower_case_table_names=1` 或 `2` 的服务器。在同时有 `t` 和 `T` 的 `lower_case_table_names=0` 服务器上，只含 `T` 的转储会被用在 `t` 上，守卫在任何写入之前停止执行（或 flashback 退出 1），而 v0.23.27 能还原 `t`。折叠后的表被拒绝时，错误里写的是 binlog 表名而不是文件表名，也不打印折叠警告。两者都会安全失败。请用表名与 binlog 完全一致的转储。
- [#167](https://github.com/Fanduzi/BinlogVisualizer/issues/167)：解析到的 binlog 里的 `ALTER` 会再应用到已经包含它的转储上。拼接的多库转储只用第一个 `Database:` 头。
- [#193](https://github.com/Fanduzi/BinlogVisualizer/issues/193)：非默认 `div_precision_increment` 加 `DECIMAL` 操作数会被拒绝，报错指向转储。会安全失败。
- 库名首尾都是反引号，或以空格开头时，`Database:` 头会丢掉这个字符，这张表没有定义，与 v0.23.27 相同。
- 守卫报错那一行里，列名中的 `|` 显示为 `/`。守卫在 stdout 上的 `SELECT` 和 SQL 里仍是真实列名。
- 脚本不知道的生成列（没有 `--schema-file`，解析到的 binlog 里也没有 `CREATE`）仍会被赋值，执行可能停在 `ERROR 3105`。
- 执行失败后接着跑：保留脚本头（第一个 `-- gtid:` 之前的所有行），只删掉失败块上面的块，在新会话里执行脚本头加上失败块及其后的全部内容。
