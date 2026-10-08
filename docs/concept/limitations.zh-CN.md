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

JSON 按二进制文档重建（`JSON_OBJECT` / `JSON_ARRAY`，标量则用 `CAST(... AS JSON)`），小数仍是小数，日期时间仍是日期时间，`-0.0` 保留符号。`binlog_transaction_compression=ON` 记下的事务也一样。非 `utf8mb4` 的字符列写成字符集引导符加原始字节（`_latin1 0xE9`、`_utf16 0x00410042`）。`binary` 校对仍是 `X'...'`。`utf8mb4` 仍是带引号的字符串。`ENUM` 写成从 1 开始的成员序号（`0` 是空成员），`SET` 写成位掩码（bit 0 是第一个成员）。带引号的成员名是列自己的字符集，`latin1` 或 `gbk` 在 `SET NAMES utf8mb4` 下并不精确：严格 `sql_mode` 会返回 `ERROR 1265`，非严格模式可能写成空值。数字在两种模式下都还原同一个成员。序号 0 不是成员。严格 `sql_mode` 会报 `ERROR 1265`。flashback 还原这一行时先保存 `@@SESSION.sql_mode`，只在这一条语句去掉 `STRICT_TRANS_TABLES` 和 `STRICT_ALL_TABLES`，然后把保存的模式设回去。其他语句仍是严格模式。

生成列（VIRTUAL 或 STORED）在定义已知时不写入 `INSERT` 列清单和 `UPDATE` 赋值。没有主键时，只要还剩基列，就从 `WHERE` 里去掉生成列。生成列若属于主键，仍留在 `WHERE` 中。列名来自你传入文件里的 `CREATE TABLE` 和 `ALTER TABLE`，包括被 `--exclude-gtids` 或时间窗口排除的事务，也来自 `--schema-file`（`mysqldump --no-data`，或 `SHOW CREATE TABLE`，含 `mysql --batch` 把语句里的换行写成 `\n` 的输出）。flashback 不连接 MySQL。MySQL 8 的 `TABLE_MAP` 可选元数据止于 `COLUMN_VISIBILITY`（不可见列，不是生成列），`binlog_row_image=FULL` 同时存下虚拟列和存储列的值，所以缺一个单元格并不是信号，只看 binlog 无法区分。定义出现过但读不出来时，flashback 拒绝该表且不打印 SQL。选中的表从未有过定义时，flashback 仍打印脚本，列出每一列，并在脚本之前把警告写到 stderr。警告点名这张表，说明不能排除生成列，并且执行可能在 `ERROR 3105` 停下，更早的事务已经提交。

`--schema-file` 必须是行被写入时的表结构。flashback 拿这份定义和每个选中事件的 binlog `TABLE_MAP` 比较：列数、列名、顺序，以及 binlog 里有的类型、有无符号、字符集、`ENUM`/`SET` 成员、小数精度和小数秒。不一致时点名这张表和有差异的列，并且不打印 SQL。生成列只有每一行的记录值都等于表达式时才省略。能核对的是整数 `+`、`-`、`*`、括号、列名和 `NULL`。`/` 按截断计算，MySQL 赋给整数时会四舍五入，所以正确的文件也可能对不上。`DIV` 和其他表达式一律拒绝，否则把真实列标成生成列会丢掉记下的值。解析到的文件里若有 `CREATE`，就用它替换文件里的这张表。解析到的 `ALTER`（含被时间或 GTID 排除的语句）会更新之后事件使用的定义。选定范围内的 `ALTER` 仍然拒绝，因为 flashback 不撤销 DDL。普通的 `mysqldump --no-data db` 没有 `USE`。库名来自 `-- Host: ... Database:` 头、`--schema-file-db`，或这张表在 binlog 里只出现在一个库时的那个库。多份 `SHOW CREATE TABLE` 可以放在同一个文件里，中间没有 `;`，包括 `mysql --batch` 的输出。转储头和 `--schema-file-db` 不一致，或表名出现在多个库时，flashback 警告并且不用这份定义。

即使字面量精确，这些限制仍然在：

- `ON DELETE CASCADE` 和 `ON UPDATE CASCADE` 改动的子表行不在 binlog 里，flashback 不会把它们改回去。
- 撤销执行时，表上的触发器会触发。
- 匹配只用主键，或剩余每一列加 `LIMIT 1`。没有冲突检查。脚本会覆盖事故之后的修改。
- 表过滤或 `--dml` 可能只撤销一个事务里的一部分行变更。这时 stderr 打出警告，stdout 仍是保留行的 SQL。时间、位点、GTID 选择丢掉的是整个事务，不警告。
- 先审阅并测试脚本。在主库的同一个会话里执行，并保持 `sql_log_bin=1`，副本才会在新 GTID 下跟上。不要在副本上执行。某条语句失败时，脚本里更早的事务已经提交。

### 已知限制

先审阅并测试脚本，再在主库的同一个会话里执行。某条语句失败时，脚本里更早的事务已经提交。

- [#166](https://github.com/Fanduzi/BinlogVisualizer/issues/166)：生成列的表达式核对不了时，正确的 `--schema-file` 也会被拒绝。这包括 JSON 提取（`j->>'$.k'`）、`UPPER`、`CONCAT` 和 `DIV`。`/` 只在 MySQL 对结果做了舍入时才会对不上，因为核对按截断计算。英文错误是 `--schema-file does not match the binlog columns`。`--lang zh-CN` 下是 `--schema-file 与 binlog 的列不一致`。不传 `--schema-file` 不是真有生成列时的办法：脚本仍会给这些列赋值，执行停在 `ERROR 3105`，更早的事务已经提交。把记下 `CREATE TABLE`（或之后的 `ALTER`）的 binlog 和事故 binlog 一起传入，并用 `--include-gtids` 只选事故。从解析到的 binlog 学到的定义不按 schema 文件核对，生成列会从脚本里去掉，校验和可以回到事故前。也可以手工编辑脚本，从 `INSERT` 列清单和 `UPDATE` 赋值里删掉生成列。`--include-table` / `--exclude-table` 只是不选这些表，行还得用别的办法还原。
- [#167](https://github.com/Fanduzi/BinlogVisualizer/issues/167)：解析到的 binlog 里若有 `ALTER`，会再应用到已经包含这次变更的 schema 文件上。ALTER 之后导出的文件（ALTER 早于事故，这份文件正是事故当时的结构）也可能被拒绝。怎么认出来：错误里同一列出现两次。英文是 `reordered (schema file ...; binlog ...)`，例如 `id, a, b, c, c`。中文是 `--schema-file 与 binlog 的列不一致`，后面跟着 `顺序不同（schema 文件 …；binlog …）`。用这次 `ALTER` 之前的转储（已核对可以还原），或者不要传入记下这次 `ALTER` 的那个 binlog 文件。把多份 `mysqldump --no-data` 拼成一个文件时，只用第一个 `Database:` 头，后面的转储都绑到那个库。每份转储前面加上 `USE db;`，或者每个库单独跑一次 flashback 并带上 `--schema-file-db`。未带库名的表出现在多个库时，逐表警告只点名其中一张没有定义的表。给表加上库名，或补上 `USE`，这份定义才会被用上。
- [#168](https://github.com/Fanduzi/BinlogVisualizer/issues/168)：还原 `ENUM` 序号 0 时，只在这一条语句去掉 `STRICT_TRANS_TABLES` 和 `STRICT_ALL_TABLES`。`sql_mode=TRADITIONAL` 会把这两个严格模式加回来，执行停在 `ERROR 1265`，脚本里更早的事务已经提交。怎么认出来：`SELECT @@SESSION.sql_mode` 里有 `TRADITIONAL`，脚本里有 `@binlogviz_sql_mode`。执行脚本之前，把会话设成 TRADITIONAL 的展开形式，但不要带 `TRADITIONAL` 这个词：

  ```sql
  SET SESSION sql_mode = 'STRICT_TRANS_TABLES,STRICT_ALL_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';
  ```

  这样 `ENUM` 序号 0 的行能写回去，脚本结尾也会把保存的模式设回去。把整个脚本包进一个外层事务没有用。每个原事务自己有 `START TRANSACTION`，新开一个事务会把上一个提交掉。

### 执行失败后接着跑

某条语句失败时，脚本里更早的事务已经提交。把整个脚本再跑一遍，没有主键的表会多出一份行。有主键的表会停在 `ERROR 1062`。

脚本按 binlog 逆序：最后发生的事务排在最前面。失败位置上面的块已经执行过，下面的块还没有。找到 MySQL 拒绝的那一块。它以 `-- gtid:` 注释开头（没有 GTID 时是 `-- gtid: GTID unavailable`），下一行是 `-- binlog: file:pos`。最后提交成功的事务是它上面的那一块。删掉失败位置上面的块，保留失败的那一块和它下面的全部，再执行剩下的部分。已经提交的事务不要再跑。

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
