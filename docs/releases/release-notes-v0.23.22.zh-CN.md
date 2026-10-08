# BinlogViz v0.23.22 发布说明

发布日期：2026-10-08

## 概述

v0.23.22 新增 `binlogviz flashback`（#153）。它从 MySQL 8 binlog 里选出 DELETE、UPDATE、INSERT 行变更，打印撤销它们的 SQL，不连接数据库。语句按 binlog 逆序排列，每个原事务一对 `START TRANSACTION` / `COMMIT`，注释里写出原 GTID 和 `file:start-position`。原则是：要么精确还原，要么在执行之前拒绝（退出码 1，一行 `Error:`，不输出 SQL）或在 stderr 警告。在 MySQL 8.0.46 上做了六轮 tip dogfood，发版前已修复 #154、#155、#156、#158、#159、#161、#162、#163、#165、#170、#171，#173 修正了「执行失败后接着跑」的步骤。三个 P2 限制仍开着，文档里写了变通办法：#166、#167、#168。

`analyze` 的输出只在 v0.23.21 打错值的地方变化，见下面的「`analyze` 输出变化」。每个新值都与 `mysqlbinlog -vv` 一致。

## 新功能

- **`binlogviz flashback`（#153）**：选择条件与 `analyze` 相同（`--include-schema`、`--exclude-schema`、`--include-table`、`--exclude-table`、`--dml`、`--start` / `--end`、`--start-position` / `--stop-position`、`--include-gtids` / `--exclude-gtids`）。DELETE 变成插回前镜像的 `INSERT`，INSERT 变成 `DELETE`，UPDATE 按后镜像的主键把行改回前镜像。没有主键的表按每一列匹配并加 `LIMIT 1`，语句上方有注释说明。要求 `binlog_row_metadata=FULL` 和 `binlog_row_image=FULL`。脚本头设置 `utf8mb4`、`time_zone = '+00:00'`（`TIMESTAMP` 字面量是 UTC 墙钟），并在该会话去掉 `NO_BACKSLASH_ESCAPES`。
- **精确的值**：JSON 按二进制文档重建，包括 `binlog_transaction_compression=ON` 的压缩事务，小数、日期时间和 `-0.0` 都能原样回去（#154、#158）。非 `utf8mb4` 的字符列写成字符集前缀加十六进制字节（#155）。`ENUM` 写成成员序号，`SET` 写成位掩码，`latin1` / `gbk` 的值在严格和非严格 `sql_mode` 下都能还原（#159）。`ENUM` 序号 0 的还原方式是：保存 `@@SESSION.sql_mode`，只在这一条语句去掉严格模式，再设回保存的值（#163）。`BIT(n)` 写成该宽度的 `b'...'` 字面量。`FLOAT`、`DOUBLE`、`GEOMETRY`、`VECTOR` 会被拒绝。
- **`--schema-file` 和 `--schema-file-db`**：解析到的 binlog 里的 `CREATE` / `ALTER`，或者 `--schema-file`（`mysqldump --no-data`，或 `SHOW CREATE TABLE` 输出，包括 `mysql --batch`）标出生成列时，`INSERT` 和 `UPDATE` 的赋值里不写这些列（#156、#162）。schema 文件会和每个选中事件的 binlog `TABLE_MAP` 核对；不一致时点名表和列，不输出 SQL（#161）。选中的表没有定义时仍输出 SQL，stderr 警告无法排除生成列。
- **文档里的安全提示（#156）**：`ON DELETE` / `ON UPDATE CASCADE` 的子表行不会被还原，撤销时触发器会执行，事故之后的修改会被覆盖且没有冲突检查，脚本要在主库的同一个会话里执行并保持 `sql_log_bin=1`。

## Bug 修复

以下问题都是发版前 dogfood `flashback` 时发现的，本版本已修复：

- #154：JSON 里的小数和日期时间变成字符串，`10.0` 变成 `10`，`-0.0` 变成 `0`。
- #155：`latin1`、`utf16`、`ucs2`、`utf32` 列在执行时被重新编码。
- #156：生成列让脚本停在 `ERROR 3105`；缺少 DBA 安全提示。
- #158：压缩事务里的 JSON 被拒绝。
- #159：非 `utf8mb4` 列的 `ENUM` / `SET` 值被写成 `utf8mb4` 文本。
- #161：与 binlog 不一致的 `--schema-file` 也被采信，可能丢掉真实的列。
- #162：单库 `mysqldump` 文件，以及一个文件里放多份 `SHOW CREATE TABLE` 结果时，表被跳过。
- #163：`ENUM` 序号 0 被严格 `sql_mode` 拒绝（`ERROR 1265`）。
- #165（P1）：`YEAR` 列后面的数值列有无符号判断错误，撤销语句匹配 0 行。现在 BinlogViz 自己遍历 TABLE_MAP 的 SIGNEDNESS 位图，并像 MySQL 一样把 `YEAR` 算进去。
- #170（P1）：`MEDIUMINT UNSIGNED` 大于等于 `8388608` 的值被当成 32 位数，撤销语句匹配 0 行。现在取低 24 位。
- #171：#166、#167、#168 的已知限制变通办法，以及执行失败后接着跑的步骤，缺失或写错。
- #173：接着跑的步骤没说要保留脚本开头的 `SET` 几行；照字面执行时 `TIMESTAMP` 按会话时区偏移，退出码仍是 0。现在步骤明确保留脚本头，并给出完整例子。#167 按库单独跑的变通办法写明要用每个库自己的转储。

## `analyze` 输出变化

这些都是改正后的值，每个新值都与 `mysqlbinlog -vv` 一致。不含这些类型的 fixture 与 v0.23.21 逐字节相同，`FLOAT` 和 `DOUBLE` 的输出不变，退出码不变。

- `YEAR` 后面的有符号或无符号数值列（#165）：`--show-rows` 的值和热点行的键标签（`id=-294967296` 变成 `id=4000000000`）。
- `MEDIUMINT UNSIGNED` 大于等于 `8388608`，包括 `ZEROFILL`（#170）：`--show-rows` 的值和热点行的键标签（`id=4287190080` 变成 `id=9000000`）。
- 默认文本、Markdown、JSON、HTML 输出里热点行的主键标签和排序：上面两种情况，以及最高位为 1 的 `BIT(64)` 主键（`b=-1 (18446744073709551615)` 变成 `b=18446744073709551615`）。按键排名的行随改正后的标签移动，最早和最晚的 GTID 与位置一起移动。
- `--show-rows` 下最高位为 1 的 `BIT(64)` 值：`-1 (18446744073709551615)` 变成 `18446744073709551615`。
- `--show-rows` 下超过 64 字节的 `DECIMAL` 值完整打印，不再是 `[truncated: 64 of 66 bytes]`。

## 验证

- CI 任务 `flashback e2e`（MySQL 8.0.46）：`TestFlashbackRoundTripMySQL80`（校验和回到事故前）和 `TestNumericDecodeMySQL80`（数值对照：`--show-rows` 对比 `mysqlbinlog -vv`，覆盖每种整数宽度的有符号、无符号和 `ZEROFILL`，`DECIMAL(10,0)` 到 `DECIMAL(65,30)`，`BIT(1)` 到 `BIT(64)`，`FLOAT`、`DOUBLE`，以及放在数值列前面的 `YEAR` / `ENUM` / `SET`）。
- tip `8e2ef843`（#172）的 BinlogQA dogfood：2026-10-08 PASS，没有 P0 或 P1。之前在 #153、#157、#160、#164、#169 上的几轮发现了上面修复的问题。

## 破坏性变更

无。`flashback` 是新命令。`analyze` 只改正了错误的值。

## 兼容性说明

- `flashback` 从不连接 MySQL。先审阅并测试脚本，再在主库的同一个会话里执行，并保持 `sql_log_bin=1`。某条语句失败时，脚本里更早的事务已经提交。
- JSON `report_version` 仍是 `3`。快照、工作流、产物命名和支持平台与 v0.23.21 一致。

## 已知问题

完整说明和变通办法见 `docs/concept/limitations.zh-CN.md`。

- [#166](https://github.com/Fanduzi/BinlogVisualizer/issues/166)：生成列的表达式核对不了时（JSON 提取、`UPPER`、`CONCAT`、`DIV`，以及 MySQL 对结果做了舍入的 `/`），正确的 `--schema-file` 也会被拒绝。把包含 `CREATE TABLE`（或之后的 `ALTER`）的 binlog 一起传入并用 `--include-gtids` 选择事故，或从脚本里删掉生成列。
- [#167](https://github.com/Fanduzi/BinlogVisualizer/issues/167)：解析到的 binlog 里的 `ALTER` 会再应用到已经包含它的转储上（`顺序不同（...）`，同一列出现两次）。用这次 `ALTER` 之前的转储，或者不要传入包含它的 binlog。拼接的多库转储只用第一个 `Database:` 头：每份转储前面加 `USE db;`，或者每个库单独跑一次 flashback，带上 `--schema-file-db` 和这个库自己的转储。拿拼接后的文件按库单独跑会拒绝或警告，属于安全失败。
- [#168](https://github.com/Fanduzi/BinlogVisualizer/issues/168)：`sql_mode=TRADITIONAL` 下还原 `ENUM` 序号 0 会停在 `ERROR 1265`。先把会话设成 `STRICT_TRANS_TABLES,STRICT_ALL_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION`。
- 执行失败后接着跑：保留脚本头（第一个 `-- gtid:` 之前的所有行），只删掉失败那一块上面的事务块，在新会话里执行「脚本头 + 失败的那一块及其下面的全部」。失败的那一块，就是行号小于等于客户端报错行号的最后一个 `-- gtid:` 行。
