# 限制与边界

本文说明 BinlogViz 的产品边界，帮助运维人员理解这个工具被设计成做什么，以及它刻意不做什么。

## 支持的 Binlog 格式

BinlogViz 面向本地 MySQL `ROW` 格式 binlog 文件。

这个边界非常重要，因为分析器的设计前提是消费规范化后的行级事件，并从行级写入活动中推导负载统计。如果你的运维问题依赖 statement-based replay 语义，那么应该选择另一类工具链。

MIXED 文件会少计：只统计 ROW image，没有行镜像的 Query-DML 会被忽略。不要把 MIXED / STATEMENT 报告当成全量写入。分析会在 stderr 警告 `binlog appears STATEMENT or MIXED; only ROW images are counted`；文件完全没有 row image 时以非 0 退出。

换句话说，当你的输入是本地 `ROW` 格式 binlog 序列，并且目标是做负载分析而不是逻辑回放时，BinlogViz 才是合适的工具。

## 输入范围

BinlogViz 只分析本地文件。

命令只接受两种输入方式之一：

- 显式位置参数 binlog 路径
- 使用 `--from-dir` 加 `--prefix` 的 discovery 模式

它不会帮你拉取远程 binlog，也不会连接在线 MySQL 实例，更不会替你管理复制位点。discovery 模式本身也被刻意限定为窄契约：扫描单个目录、按前缀加纯数字后缀过滤，再把结果排序后送去分析。

这种设计让输入契约保持可预测，但也意味着运维人员需要先把本地文件集准备好。

## 运行时模型

BinlogViz 采用单次流式命令路径：

```text
parse -> normalize -> consume -> finalize -> render
```

这个设计让运行过程中的常驻内存状态保持有界，但并不意味着分析在命令末尾没有成本。

几个重要的运行时影响：

- 解析阶段是流式的
- 最终报告组装仍然发生在解析完成之后
- 使用 `--detail-store duckdb` 时每次命令都会创建一个临时 DuckDB 存储（默认：none）
- 输入越大，finalize 阶段越可能表现出明显耗时和临时磁盘占用

因此，这个产品优化的是有界流式分析，而不是零磁盘、也不是完全增量式的交互探索。

## SQL 上下文边界

SQL 上下文是有界的，而且面向展示。

当前限制包括：

- 存储 SQL 最大 `4096` 字节，被这个上限截断时以 `… [truncated: <shown> of <original> bytes]` 结尾
- 查询摘要的 SQL 正文最大 `160` 个字符，被截断时后面再加同一标记
- 查询字段和 DDL 语句文本是否展示由 `--sql-context` 控制（`off` 两者都省略）
- `--sql-context off` 也会省略 `--show-rows` 的单元格，报告会写明这些值被省略
- `query_truncated` 表示 4096 字节存储上限，不是 160 字符摘要

这意味着：

- BinlogViz 不承诺无损保存原始 SQL 文本
- `summary` 模式用于帮助运维快速建立判断上下文
- 即便是 `full` 模式，也只会暴露有界存储后的 SQL，而不是无限长度的原始语句

如果你的流程需要完整长 SQL 的归档或取证保存，就不应把 BinlogViz 当作那个系统。

## 行值

`--show-rows` 默认关闭。打开后，列出的事务带有界行镜像：DELETE 是前镜像，UPDATE 只列出变化的列，INSERT 是后镜像。每个事务最多保留 32 个逻辑行。字符串和二进制值最多 64 字节。整数、小数和 `BIT` 会完整打印，因此 `DECIMAL(65)` 不会被截成一个错误的数。被截断时使用 `… [truncated: shown of original bytes]`，事务上会写明省略了多少行。

列名来自 binlog，前提是 `binlog_row_metadata=FULL`（MySQL 8.0.1+）。否则列是 `@1`..`@N`，报告会说明没有列名。没有这份元数据时，有符号和无符号读数不同的整数会两种都打印，和 `mysqlbinlog -v` 一样。有 FULL 元数据时，按 binlog 记录的有无符号打印。

`--sql-context off` 不打印这些单元格。账号 DDL 的 `<secret>` 打码不变；它作用于语句文本，不是行单元格。同一事务上的 `mysqlbinlog_cmd` 用来对照。analyze 报告不打印撤销 SQL。`binlogviz flashback` 会打印，见下一节。

## Flashback SQL

`binlogviz flashback` 打印撤销选定行变更的 SQL。它只读本地 binlog，不连接数据库。先审阅并测试脚本，再在主库的同一个会话里执行。

DELETE 变成前镜像的 `INSERT`。INSERT 变成后镜像的 `DELETE`。UPDATE 把每一列设回前镜像，`WHERE` 用后镜像的主键。语句按 binlog 逆序。每个原事务是一个 `START TRANSACTION` / `COMMIT`。注释写原 GTID，没有则写 `GTID unavailable`，以及 `file:start-position`（文件名和该事务起点字节）。

表、schema、`--dml`、时间、位点、GTID 与 `analyze` 是同一套选择条件。binlog 必须带列名（`binlog_row_metadata=FULL`）和完整行镜像（`binlog_row_image=FULL`）。脚本会设置 `utf8mb4`、`time_zone='+00:00'`，并在该会话去掉 `NO_BACKSLASH_ESCAPES`。`TIMESTAMP` 字面量是存储时刻的 UTC 墙钟。

没有主键的表仍然可以撤销：`DELETE` 或 `UPDATE` 匹配每一列并加 `LIMIT 1`，注释会说明。把被删行插回去的 `INSERT` 不用 `LIMIT 1`。

JSON 按二进制文档重建（`JSON_OBJECT` / `JSON_ARRAY`，标量则用 `CAST(... AS JSON)`），小数仍是小数，日期时间仍是日期时间，`-0.0` 保留符号。`binlog_transaction_compression=ON` 记下的事务也一样。非 `utf8mb4` 的字符列写成字符集引导符加原始字节（`_latin1 0xE9`、`_utf16 0x00410042`）。`binary` 校对仍是 `X'...'`。`utf8mb4` 仍是带引号的字符串。`ENUM` 写成从 1 开始的成员序号（`0` 是空成员），`SET` 写成位掩码（bit 0 是第一个成员）。带引号的成员名是列自己的字符集，`latin1` 或 `gbk` 在 `SET NAMES utf8mb4` 下并不精确：严格 `sql_mode` 会返回 `ERROR 1265`，非严格模式可能写成空值。数字在两种模式下都还原同一个成员。序号 0 不是成员。严格 `sql_mode` 会报 `ERROR 1265`。flashback 还原这一行时先保存 `@@SESSION.sql_mode`，只在这一条语句去掉 `STRICT_TRANS_TABLES`、`STRICT_ALL_TABLES` 和 `TRADITIONAL`，然后把保存的模式设回去。`TRADITIONAL` 也要去掉，因为 MySQL 会把它展开回那两个严格模式。其他语句仍是严格模式。空的 `sql_mode` 以及会话一开始带 `NO_BACKSLASH_ESCAPES` 时，这一行仍然能还原。脚本头会在整个会话去掉 `NO_BACKSLASH_ESCAPES`，并且不会设回去。

生成列（VIRTUAL 或 STORED）在定义已知时不写入 `INSERT` 列清单和 `UPDATE` 赋值。没有主键时，只要还剩基列，就从 `WHERE` 里去掉生成列。生成列若属于主键，仍留在 `WHERE` 中。列名来自你传入文件里的 `CREATE TABLE` 和 `ALTER TABLE`，包括被 `--exclude-gtids` 或时间窗口排除的事务，也来自 `--schema-file`（`mysqldump --no-data`，或 `SHOW CREATE TABLE`，含 `mysql --batch` 把语句里的换行写成 `\n` 的输出）。flashback 不连接 MySQL。MySQL 8 的 `TABLE_MAP` 可选元数据止于 `COLUMN_VISIBILITY`（不可见列，不是生成列），`binlog_row_image=FULL` 同时存下虚拟列和存储列的值，所以缺一个单元格并不是信号。`FULL` 镜像仍然记下了这个值，flashback 会拿它和 schema 文件里的表达式核对。定义出现过但读不出来时，flashback 拒绝该表且不打印 SQL。选中的表从未有过定义时，flashback 仍打印脚本，列出每一列，并在脚本之前把警告写到 stderr。警告点名这张表，说明不能排除生成列，并且执行可能在 `ERROR 3105` 停下，更早的事务已经提交。

`--schema-file` 必须是行被写入时的表结构。flashback 拿这份定义和每个选中事件的 binlog `TABLE_MAP` 比较：列数、列名、顺序，以及 binlog 里有的类型、有无符号、字符集、`ENUM`/`SET` 成员、小数精度和小数秒。不一致时点名这张表和有差异的列，说明文件对不上，并且不打印 SQL。错误里还会说明下一步：用事故当时的转储；若之后有 `ALTER`，用更早的转储；或用 `--include-table` 把这张表排除。生成列在每一行的记录值都等于表达式时才省略。能核对的是整数 `+`、`-`、`*`、`/`、`DIV`、`%`、`MOD`、括号、列名和 `NULL`，以及 `UPPER`、`LOWER`、`CONCAT`、`CONCAT_WS`、`LENGTH`、`CHAR_LENGTH` 和简单的 JSON 提取（`->`、`->>`、带常量路径的 `JSON_EXTRACT`、`JSON_UNQUOTE`）。`/` 按 MySQL 赋给整数的方式舍入，远离零的一半进位，所以整数列里的 `5 / 2` 是 `3`，`-5 / 2` 是 `-3`。`DIV` 向零截断。`UPPER` 和 `LOWER` 按字符集转换。`NULL` 仍是 `NULL`，但 `CONCAT_WS` 会跳过 `NULL` 参数。记下的值对不上这个表达式时，退出 1，并且不打印 SQL。错误会点名表、列、表达式，并给出一行示例。两份镜像里表达式引用的列相同、生成列不同，也一样拒绝，包括只改了这一列的 `UPDATE`，以及一对 `INSERT`/`DELETE`。表达式无法计算、且记下的值没有矛盾时，除非传入 `--allow-unverified-generated`，否则拒绝。错误会说明这个选项，并说明如果这一列其实不是生成列，省略它会丢掉已存储的值。带上这个选项时，脚本省略该列，stderr 给出警告，脚本头写入 `-- WARNING: generated column db.tbl.col not verified`。这个选项不能覆盖矛盾的值。记下的值恰好等于能核对的表达式时，真实列和生成列仍然分不开。解析到的文件里若有 `CREATE`，就用它替换文件里的这张表。解析到的 `ALTER`（含被时间或 GTID 排除的语句）会更新之后事件使用的定义。选定范围内的 `ALTER` 仍然拒绝，因为 flashback 不撤销 DDL。普通的 `mysqldump --no-data db` 没有 `USE`。库名来自 `-- Host: ... Database:` 头、`--schema-file-db`，或这张表在 binlog 里只出现在一个库时的那个库。多份 `SHOW CREATE TABLE` 可以放在同一个文件里，中间没有 `;`，包括 `mysql --batch` 的输出。转储头和 `--schema-file-db` 不一致，或表名出现在多个库时，flashback 警告并且不用这份定义。

即使字面量精确，这些限制仍然在：

- `ON DELETE CASCADE` 和 `ON UPDATE CASCADE` 改动的子表行不在 binlog 里，flashback 不会把它们改回去。
- 撤销执行时，表上的触发器会触发。
- 匹配只用主键，或剩余每一列加 `LIMIT 1`。没有冲突检查。脚本会覆盖事故之后的修改。
- 表过滤或 `--dml` 可能只撤销一个事务里的一部分行变更。这时 stderr 打出警告，stdout 仍是保留行的 SQL。时间、位点、GTID 选择丢掉的是整个事务，不警告。
- 先审阅并测试脚本。在主库的同一个会话里执行，并保持 `sql_log_bin=1`，副本才会在新 GTID 下跟上。不要在副本上执行。某条语句失败时，脚本里更早的事务已经提交。

### 已知限制

先审阅并测试脚本，再在主库的同一个会话里执行。某条语句失败时，脚本里更早的事务已经提交。

- `--schema-file` 把真实列标成生成列时，若记下的值恰好等于 binlogviz 能核对的表达式（`c` 一直被写成 `a + 1`、`UPPER(x)`，或 `first` 与 `last` 拼出来的名字），这一列仍会被省略，命令以退出码 0 结束（[#166](https://github.com/Fanduzi/BinlogVisualizer/issues/166)）。MySQL 8 的行元数据不标记生成列，这种情况和正确的转储分不开。记下的值与表达式矛盾时，包括只改了这个所谓生成列的 `UPDATE`，退出 1，并且不打印 SQL。表达式无法计算时同样拒绝，除非传入 `--allow-unverified-generated`。这个选项会省略该列，并在脚本头写警告；它不能覆盖矛盾的值。表里确实有生成列时，不传 `--schema-file` 解决不了问题：脚本仍会给这些列赋值，执行停在 `ERROR 3105`，更早的事务已经提交。
- 非严格会话写下的行，在严格会话里执行可能失败（[#180](https://github.com/Fanduzi/BinlogVisualizer/issues/180)）。零日期报 `ERROR 1292`（错误的日期值）。生成列 `a/b` 在 `b` 为 0 时写入，严格会话报 `ERROR 1365`（除以零）。脚本里更早的事务已经提交。用当初那个会话的 `sql_mode` 执行。
- [#167](https://github.com/Fanduzi/BinlogVisualizer/issues/167)：解析到的 binlog 里若有 `ALTER`，会再应用到已经包含这次变更的 schema 文件上。ALTER 之后导出的文件（ALTER 早于事故，这份文件正是事故当时的结构）也可能被拒绝。怎么认出来：错误里同一列出现两次。英文是 `reordered (schema file ...; binlog ...)`，例如 `id, a, b, c, c`。中文是 `--schema-file 与 binlog 的列不一致`，后面跟着 `顺序不同（schema 文件 …；binlog …）`。用这次 `ALTER` 之前的转储（实测可以还原），或者不要传入包含这次 `ALTER` 的那个 binlog 文件。把多份 `mysqldump --no-data` 拼成一个文件时，只用第一个 `Database:` 头，后面的转储都绑到那个库。每份转储前面加上 `USE db;`，或者每个库单独跑一次 flashback，带上 `--schema-file-db` 和这个库自己的那份转储。拿拼接后的文件按库单独跑不行：要么拒绝（`--schema-file 与 binlog 的列不一致`），要么警告 `Database` 头和 `--schema-file-db` 不一致、不使用这份定义。两种都会安全失败，但都还原不了。未带库名的表出现在多个库时，逐表警告只点名其中一张没有定义的表。给表加上库名，或补上 `USE`，这份定义才会被用上。

### 执行失败后接着跑

某条语句失败时，脚本里更早的事务已经提交。把整个脚本再跑一遍，没有主键的表里会出现重复行。有主键的表会停在 `ERROR 1062`。

脚本分两部分。第一个 `-- gtid:` 行之前的都是脚本头：两行注释、使用 `--allow-unverified-generated` 时每个被省略的生成列各一行 `-- WARNING: generated column ... not verified`，以及三条会话语句。

```sql
SET NAMES utf8mb4;
SET time_zone = '+00:00';
SET SESSION sql_mode = REPLACE(@@SESSION.sql_mode, 'NO_BACKSLASH_ESCAPES', '');
```

后面是事务块，按 binlog 逆序：最后发生的事务排在最前面。每一块以 `-- gtid:` 注释开头（没有 GTID 时是 `-- gtid: GTID unavailable`），下一行是 `-- binlog: file:pos`，以 `COMMIT;` 结束。块里面的 `SET @binlogviz_sql_mode` / `SET SESSION sql_mode` 属于这一块。

失败位置上面的块已经执行过，下面的块还没有。客户端会报出失败的行号（`ERROR 1265 (01000) at line 20: ...`）。失败的那一块，就是行号小于等于这个数的最后一个 `-- gtid:` 行。接着跑的步骤：

1. 保留脚本头。脚本里的 `TIMESTAMP` 字面量都是 UTC 墙钟，必须有 `SET time_zone = '+00:00'`。脚本头被删掉时，剩下的部分按会话自己的时区执行：退出码仍是 0，MySQL 不报任何错，还原出来的每个 `TIMESTAMP` 都偏了这个时差（`+08:00` 的服务器上，`09:00:00.123` 会变成 `01:00:00.123`）。
2. 只删掉失败那一块上面的事务块。
3. 保留失败的那一块和它下面的全部。
4. 原会话已经中断，在新会话里执行「脚本头 + 剩下的部分」。失败原因需要会话设置时，把那条 `SET SESSION` 放在脚本头之前，作为接着跑的文件的第一行。

已经提交的事务不要再跑。

例子。下面这个脚本停在 `ERROR 1265 (01000) at line 20`。行号小于等于 20 的最后一个 `-- gtid:` 行是第 13 行。第 1–6 行是脚本头，第 7–12 行是已经提交的块，第 13 行起还没有执行：

```text
 1  -- flashback reverses the selected row changes, last transaction first.
 2  -- TIMESTAMP literals are the UTC wall clock of the stored instant. Review this script before applying it.
 3  SET NAMES utf8mb4;                                   -- 脚本头：保留
 4  SET time_zone = '+00:00';                            -- 脚本头：保留
 5  SET SESSION sql_mode = REPLACE(@@SESSION.sql_mode, 'NO_BACKSLASH_ESCAPES', '');  -- 脚本头：保留
 6
 7  -- gtid: 3528e50c-c289-11f1-8861-0242ac110003:986    -- 已提交：删掉第 7-12 行
 8  -- binlog: mysql-bin.000124:542
 9  START TRANSACTION;
10  INSERT INTO `shop`.`r_nopk` (`n`, `ts`, `s`) VALUES (1, '2026-10-08 00:00:00', 'one');
11  COMMIT;
12
13  -- gtid: 3528e50c-c289-11f1-8861-0242ac110003:985    -- 失败的块：从这里保留到结尾
14  -- binlog: mysql-bin.000124:197
15  START TRANSACTION;
...
```

接着跑的文件是第 1–6 行加上第 13 行到结尾。下面的命令保留脚本头，删掉第 13 行之前开始的每一块，其余保留：

```bash
awk -v n=13 'NR >= n || !seen { if (NR < n && /^-- gtid:/) { seen = 1; next } print }' flashback.sql > resume.sql
mysql --default-character-set=utf8mb4 < resume.sql
```

执行前检查 `resume.sql` 开头是那三条 `SET`，第一个 `-- gtid:` 行是失败的那一块。

无法精确还原时拒绝，不猜测。退出 1，一行 `Error:` 点名表和原因，stdout 没有 SQL，出现在：

- 没有列名
- 前镜像或后镜像不完整（`binlog_row_image` 为 `MINIMAL` 或 `NOBLOB`）
- 某一列无法精确写成字面量（`FLOAT`、`DOUBLE`、`GEOMETRY`、`VECTOR`、不完整的 JSON、无法精确表示的 JSON、`utf8mb4` 列里的非法 UTF-8、未知校对，或缺少有无符号、字符集、ENUM/SET 成员）。`BIT` 是列宽的 `b'...'` 字面量。MySQL 无法转回的 `FLOAT` 或 `DOUBLE` 在 `--show-rows` 里写成 `<FLOAT>` 或 `<DOUBLE>`，而不是一个错误的数。
- binlog 或 `--schema-file` 里出现过 `CREATE` 或 `ALTER`，但语句读不出来
- `--schema-file` 和某个选中事件的 binlog 列不一致
- 选定范围内有 DDL（不生成反向 DDL）
- 使用了 `--sql-context off`，因为脚本就是行值

什么都没选中时，退出码和 `Error:` 与 `analyze` 相同（`schema/table filter matched no events`、`dml filter matched no events` 或 `window matched 0 events`），stdout 为空。选定范围内的账号 DDL 按 DDL 拒绝，密钥不会被抄进脚本。单元格的值不打码。不使用 flashback 时，analyze 的文本、Markdown、JSON 和 HTML 不变。

## 输出与契约边界

BinlogViz 有意把输出通道分离：

- 最终报告写到 `stdout`
- 进度、解析出的文件列表、finalize 状态以及命令错误写到 `stderr`

这个契约便于 shell 重定向和自动化，但也意味着运维人员不应把 `stderr` 内容误当成报告载荷的一部分。如果你只重定向 `stdout`，得到的是报告；如果你也想保留运行状态日志，就要单独捕获 `stderr`。

## 产品焦点

BinlogViz 聚焦于负载分析，而不是完整的数据库运维控制面。

它被设计来回答的问题包括：

- 哪些表最热
- 哪些事务最大
- 什么时候出现活动尖峰
- 整体写入负载长什么样

它并不定位为：

- MySQL 复制管理器
- 实时 binlog tailing 服务
- statement replay 引擎（flashback 只为选定范围生成反向行 DML，不重放原始语句）
- 完整历史数据重建工具
- 通用 SQL 可观测性平台

## 明确的非目标

为了保持产品聚焦，以下内容是当前文档范围内的明确非目标：

- 管理或修改 MySQL 服务器状态
- 在分析过程中直接连接远程 MySQL 实例读取数据
- 在报告中无限制保存原始 SQL 文本
- 将进度输出并入机器可读报告流
- 替代更深入的复制、取证或可观测性系统

当你需要的是本地 `ROW` binlog 工作负载的快速运维总结，或撤销一组选定行变更的 SQL 时，使用 BinlogViz。若你需要远程采集、重放原始语句，或更广泛的数据库运维平台，则应该选择其他工具。
