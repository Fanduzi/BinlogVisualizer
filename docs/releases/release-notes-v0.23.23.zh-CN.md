# BinlogViz v0.23.23 发布说明

发布日期：2026-10-09

## 概述

v0.23.23 是 `binlogviz flashback` 的加固版本。更多生成列表达式下，正确的事故时 `--schema-file` 会被接受；错误的 schema 文件会被拒绝，或在写入前被拦下；执行时的守卫在所有服务器上都失败即锁定（fail closed），包括 MariaDB 和老版本 MySQL。`analyze` 的输出与 v0.23.22 相同。

## 变更

- **生成列按 binlog 里的行核对（#178、#182）**：省略 schema 文件里的生成列之前，flashback 对每一行计算 `UPPER`、`LOWER`、`CONCAT`、`CONCAT_WS`、`LENGTH` / `CHAR_LENGTH`、简单 JSON 提取（`->`、`->>`、`JSON_EXTRACT`、常量路径的 `JSON_UNQUOTE`），以及整数 `+`、`-`、`*`、`/`、`DIV`、`%`、`MOD`。记录的值与表达式不符时退出 1、不打印 SQL，并列出所有不符的表和列及一行示例。无法精确建模的表达式视为无法核对：该列被省略，stderr 警告一次，脚本头注明由守卫检查目标库。
- **`DECIMAL` 生成列除法（#179）**：用 `+`、`-`、`*`、`/`、`DIV`、`%`、`MOD` 计算的 `DECIMAL(p,s)` 或整数列会被接受，除法宽度按 MySQL 8.0 默认 `div_precision_increment` = 4 计算（`DECIMAL(40,4)` 里 `5 / 2` 是 `2.5000`，`DECIMAL(40,9)` 里 `1 / 7` 是 `0.142857142`）。非严格 `sql_mode` 截断到边界的值（`TINYINT` `127`、`DECIMAL(4,2)` `99.99`）视为未核对并给出警告；其他不符的值一律拒绝。
- **执行时守卫（#183、#184、#186）**：脚本省略的每个 schema 文件生成列，都在三行 `SET` 之后、第一个事务之前有一个守卫，读取目标库的 `information_schema.COLUMNS.EXTRA`。某列不是 `STORED GENERATED` 或 `VIRTUAL GENERATED` 时，执行在任何事务之前失败，报错行列出所有不符的 `db.table.column`（列表太长时给出数量和前几个名字）。无主键表的 `WHERE` 保留生成列，INSERT 的撤销不会删掉另一条重复行。
- **守卫失败即锁定（#183、#186、#190、#194、#195、#196）**：守卫先用 `SET SESSION TRANSACTION READ ONLY`（不引用任何服务器变量）把会话设为只读，只有在本会话里完成检查、所有列都匹配、并且服务器受支持时才切回读写。`mysql --force`、交互式 `source` 或粘贴、`mysqlsh --force` / `--interactive`、以及设置为出错继续的 GUI 工具，之后的每次写入都会以 `ERROR 1792` 失败，不改任何行。每个事务块开始前都会重新加锁。执行脚本前已经是只读的会话保持只读。
- **`sql_mode=TRADITIONAL` 下的 `ENUM` 序号 0（#168）**：写入序号 0 的那条语句除了 `STRICT_TRANS_TABLES` 和 `STRICT_ALL_TABLES`，也去掉 `TRADITIONAL`，之后恢复保存的 `@@SESSION.sql_mode`。
- **CI（#176）**：`flashback e2e` 在 MySQL 测试被跳过或缺失时失败；要求 `TestFlashbackRoundTripMySQL80` 和 `TestNumericDecodeMySQL80` 都有 `--- PASS`。

## Bug 修复

- #194（P1）：MariaDB 10.x–11.0 上守卫设置的是这些版本没有的 `transaction_read_only`，`mysql --force` 仍然写入错误的行，退出码 0。现在用 `SET SESSION TRANSACTION READ ONLY` 加锁；错误的 schema 文件不写入任何行，GTID 不变。
- #195：MySQL 5.7.0 之前的版本上，守卫只让下一条语句失败。现在 MySQL 5.6 和 MariaDB 10.1 在守卫处报 `binlogviz: target server is older than MySQL 5.7.0 or MariaDB 10.2 and is not supported for apply`，并保持只读。
- #196：加锁依赖 `PREPARE`，服务器拒绝预处理语句（`max_prepared_stmt_count` 用满或为 0）时会写入错误的行。现在保持只读；在正确的目标库上执行停在 `ERROR 1461`，不还原任何行。预处理语句命名为 `binlogviz_fb_*_x9q`。
- #190：MySQL 5.7.0 到 5.7.19 只有 `tx_read_only`，没有 `transaction_read_only`，此前没有被锁定。
- #186：`ERROR 1231` 报错行只列出第一个不符的列；拒绝时的示例把 `ENUM` / `SET` 显示成序号或位掩码，而不是标签。
- #183：表达式恰好与记录的行一致的错误转储会丢掉该列，退出码 0 且没有警告；无主键表上 INSERT 的撤销可能删掉另一行。
- #184：`--allow-unverified-generated` 的帮助和拒绝信息没说明何时安全，zh-CN 信息带英文前缀，只报告第一个有问题的表。
- #182：JSON null 和 `->>` 下的浮点数、`ENUM` / `SET`、`ascii` 列、`CONCAT` 里的 `DECIMAL` / `TIME` / `TIMESTAMP`、`BINARY(n)` 的 `LENGTH` 会让正确的事故时转储被拒绝。
- #179：生成列用 `/` 的 `DECIMAL` 列让正确的转储被拒绝。
- #178：文件把真实列标成生成列且表达式无法核对时，即使记录的值与表达式矛盾，该列也被丢掉，退出码 0。
- #168：`sql_mode=TRADITIONAL` 下还原 `ENUM` 序号 0 停在 `ERROR 1265`。
- #166：生成列使用 JSON 提取、`UPPER`、`CONCAT`、`DIV` 或整数 `/` 时，正确的 `--schema-file` 被拒绝。

## 验证

- CI 任务 `flashback e2e`（MySQL 8.0.46）：`--- PASS: TestFlashbackRoundTripMySQL80` 和 `--- PASS: TestNumericDecodeMySQL80`，没有跳过。
- #194/#195/#196 的守卫实机矩阵（#198），覆盖 MariaDB 10.1、10.6、10.11、11.0、11.4 和 MySQL 5.6、5.7.19、5.7.44、8.0：所有错误 schema 场景（autocommit 开和关下的 `mysql --force`、新会话里无脚本头的块、`max_prepared_stmt_count=0`）写入 0 行，GTID 不变。在受支持的服务器上，正确的 schema 文件还原出相同的行，会话只读标志、autocommit 和 `sql_mode` 保持原样。
- #194 修复的 BinlogQA dogfood：2026-10-08 PASS，剩下的两个泄漏记为 #197 并写入文档。

## 破坏性变更

在 MySQL 5.7+ 或 MariaDB 10.2+ 上正确执行时没有。含守卫的脚本（省略了 schema 文件里的生成列）现在在 MySQL 5.6 和 MariaDB 10.1 上拒绝执行，并让该会话保持只读；在拒绝 `PREPARE` 的服务器上停在 `ERROR 1461`，不再执行。

## 兼容性说明

- 执行支持 MySQL 5.7 及以上和 MariaDB 10.2 及以上。守卫已在 MySQL 5.7.19、5.7.44、8.0 和 MariaDB 10.6、10.11、11.0、11.4 上实机验证。
- `flashback` 从不连接 MySQL。先审阅并测试脚本，再带着脚本头在主库的一个新会话里执行，并保持 `sql_log_bin=1`。某条语句失败时，脚本里更早的事务已经提交。
- JSON `report_version` 仍是 `3`。快照、工作流、产物命名和支持平台与 v0.23.22 一致。

## 已知问题

完整说明和变通办法见 `docs/concept/limitations.zh-CN.md`。

- [#197](https://github.com/Fanduzi/BinlogVisualizer/issues/197)：在已经执行过正确脚本的会话里粘贴没有脚本头的块，仍会写入，退出码 0。交互式客户端在块中间重连时，该块剩下的语句在新会话里提交（另见 [#191](https://github.com/Fanduzi/BinlogVisualizer/issues/191)）。批量 `mysql --force` 不受影响。截取部分脚本执行时，务必带上脚本头并使用新会话。
- [#167](https://github.com/Fanduzi/BinlogVisualizer/issues/167)：解析到的 binlog 里的 `ALTER` 会再应用到已经包含它的转储上（`顺序不同（...）`，同一列出现两次）。用这次 `ALTER` 之前的转储，或者不要传入包含它的 binlog。拼接的多库转储只使用第一个 `Database:` 头：在每份转储前加 `USE db;`，或者每个库用自己的转储单独跑一次。
- [#180](https://github.com/Fanduzi/BinlogVisualizer/issues/180)：非严格会话写入的行（零日期、`b` = 0 时的生成列 `a/b`）在严格会话下可能以 `ERROR 1292` 或 `ERROR 1365` 失败，此时更早的事务已经提交。用原会话的 `sql_mode` 执行。
- [#187](https://github.com/Fanduzi/BinlogVisualizer/issues/187)：库名带空格的 `Database:` 头，以及 `lower_case_table_names=0` 的转储配 `lower_case_table_names=1` 的 binlog 时的大小写混用名字，会让表没有定义。
- [#192](https://github.com/Fanduzi/BinlogVisualizer/issues/192)：列名含非 ASCII 字符时，守卫的 `ERROR 1231` 报错行只显示不符列的数量。
- [#193](https://github.com/Fanduzi/BinlogVisualizer/issues/193)：`div_precision_increment` 不是默认值时，生成列 `/` 用 `DECIMAL` 操作数的正确转储会被拒绝。
- 执行失败后接着跑：保留脚本头（第一个 `-- gtid:` 之前的所有行），只删掉失败那一块上面的事务块，在新会话里执行「脚本头 + 失败的那一块及其下面的全部」。
