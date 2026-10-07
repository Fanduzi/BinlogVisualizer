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

`--show-rows` 默认关闭。打开后，列出的事务带有界行镜像：DELETE 是前镜像，UPDATE 只列出变化的列，INSERT 是后镜像。每个事务最多保留 32 个逻辑行，每个值最多 64 字节。被截断时使用 `… [truncated: shown of original bytes]`，事务上会写明省略了多少行。

列名来自 binlog，前提是 `binlog_row_metadata=FULL`（MySQL 8.0.1+）。否则列是 `@1`..`@N`，报告会说明没有列名。没有这份元数据时，有符号和无符号读数不同的整数会两种都打印，和 `mysqlbinlog -v` 一样。有 FULL 元数据时，按 binlog 记录的有无符号打印。

`--sql-context off` 不打印这些单元格。账号 DDL 的 `<secret>` 打码不变；它作用于语句文本，不是行单元格。同一事务上的 `mysqlbinlog_cmd` 用来对照。analyze 报告不打印撤销 SQL。`binlogviz flashback` 会打印，见下一节。

## Flashback SQL

`binlogviz flashback` 打印撤销选定行变更的 SQL。它只读本地 binlog，不连接数据库。审完脚本后自己执行。

DELETE 变成前镜像的 `INSERT`。INSERT 变成后镜像的 `DELETE`。UPDATE 把每一列设回前镜像，`WHERE` 用后镜像的主键。语句按 binlog 逆序。每个原事务是一个 `START TRANSACTION` / `COMMIT`。注释写原 GTID，没有则写 `GTID unavailable`，以及 `file:start-position`（文件名和该事务起点字节）。

表、schema、`--dml`、时间、位点、GTID 与 `analyze` 是同一套选择条件。binlog 必须带列名（`binlog_row_metadata=FULL`）和完整行镜像（`binlog_row_image=FULL`）。脚本会设置 `utf8mb4`、`time_zone='+00:00'`，并在该会话去掉 `NO_BACKSLASH_ESCAPES`。`TIMESTAMP` 字面量是存储时刻的 UTC 墙钟。

没有主键的表仍然可以撤销：`DELETE` 或 `UPDATE` 匹配每一列并加 `LIMIT 1`，注释会说明。把被删行插回去的 `INSERT` 不用 `LIMIT 1`。

JSON 按二进制文档重建（`JSON_OBJECT` / `JSON_ARRAY`，标量则用 `CAST(... AS JSON)`），小数仍是小数，日期时间仍是日期时间，`-0.0` 保留符号。`binlog_transaction_compression=ON` 记下的事务也一样。非 `utf8mb4` 的字符列写成字符集引导符加原始字节（`_latin1 0xE9`、`_utf16 0x00410042`）。`binary` 校对仍是 `X'...'`。`utf8mb4` 仍是带引号的字符串。`ENUM` 写成从 1 开始的成员序号（`0` 是空成员），`SET` 写成位掩码（bit 0 是第一个成员）。带引号的成员名是列自己的字符集，`latin1` 或 `gbk` 在 `SET NAMES utf8mb4` 下并不精确：严格 `sql_mode` 会返回 `ERROR 1265`，非严格模式可能写成空值。数字在两种模式下都还原同一个成员。

生成列（VIRTUAL 或 STORED）在定义已知时不写入 `INSERT` 列清单和 `UPDATE` 赋值。没有主键时，只要还剩基列，就从 `WHERE` 里去掉生成列。生成列若属于主键，仍留在 `WHERE` 中。列名来自你传入文件里的 `CREATE TABLE` 和 `ALTER TABLE`，包括被 `--exclude-gtids` 或时间窗口排除的事务，也来自 `--schema-file`（`mysqldump --no-data`，或 `USE` 之后的 `SHOW CREATE TABLE`，含 `mysql --batch` 把语句里的换行写成 `\n` 的输出）。flashback 不连接 MySQL。MySQL 8 的 `TABLE_MAP` 可选元数据止于 `COLUMN_VISIBILITY`（不可见列，不是生成列），`binlog_row_image=FULL` 同时存下虚拟列和存储列的值，所以缺一个单元格并不是信号，只看 binlog 无法区分。定义出现过但读不出来时，flashback 拒绝该表且不打印 SQL。选中的表从未有过定义时，flashback 仍打印脚本，列出每一列，并在脚本之前把警告写到 stderr。警告点名这张表，说明不能排除生成列，并且执行可能在 `ERROR 3105` 停下，更早的事务已经提交。

即使字面量精确，这些限制仍然在：

- `ON DELETE CASCADE` 和 `ON UPDATE CASCADE` 改动的子表行不在 binlog 里，flashback 不会把它们改回去。
- 撤销执行时，表上的触发器会触发。
- 匹配只用主键，或剩余每一列加 `LIMIT 1`。没有冲突检查。脚本会覆盖事故之后的修改。
- 表过滤或 `--dml` 可能只撤销一个事务里的一部分行变更。这时 stderr 打出警告，stdout 仍是保留行的 SQL。时间、位点、GTID 选择丢掉的是整个事务，不警告。
- 在主库上执行，并保持 `sql_log_bin=1`，副本才会在新 GTID 下跟上。不要在副本上执行。

无法精确还原时拒绝，不猜测。退出 1，一行 `Error:` 点名表和原因，stdout 没有 SQL，出现在：

- 没有列名
- 前镜像或后镜像不完整（`binlog_row_image` 为 `MINIMAL` 或 `NOBLOB`）
- 某一列无法精确写成字面量（`FLOAT`、`DOUBLE`、`BIT`、`GEOMETRY`、`VECTOR`、不完整的 JSON、无法精确表示的 JSON、`utf8mb4` 列里的非法 UTF-8、未知校对，或缺少有无符号、字符集、ENUM/SET 成员）
- binlog 或 `--schema-file` 里出现过 `CREATE` 或 `ALTER`，但语句读不出来
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
