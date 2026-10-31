# CLI 参考

本文档定义 `binlogviz` 根命令、`binlogviz analyze`、`binlogviz flashback`、`binlogviz redo`、`binlogviz compare`、`binlogviz trend`、`binlogviz snapshot`、`binlogviz workflow run`、`binlogviz workflow resume`、`binlogviz workflow status`、`binlogviz workflow clean`、`binlogviz workflow export`、`binlogviz workflow validate` 和 `binlogviz workflow describe` 的用户可见契约。

如果你想先走最短运维路径，而不是直接看完整契约，请先阅读[快速开始](../recipe/quickstart.zh-CN.md)或[分析本地 Binlog](../recipe/analyze-local-binlogs.zh-CN.md)。

## 命令语法

```bash
binlogviz --version
binlogviz version --format json
binlogviz --lang zh-CN analyze <binlog files...>
binlogviz analyze <binlog files...>
binlogviz analyze --from-dir DIR --prefix PREFIX
binlogviz analyze --from-dir DIR --prefix PREFIX --format json --snapshot-name NAME
binlogviz flashback <binlog files...>
binlogviz flashback --from-dir DIR --prefix PREFIX
binlogviz flashback --from-dir DIR --index-file FILE
binlogviz flashback --host HOST --user USER
binlogviz redo <binlog files...>
binlogviz redo --from-dir DIR --prefix PREFIX
binlogviz redo --host HOST --user USER
binlogviz compare <current.json> <baseline.json>
binlogviz compare --current-snapshot CURRENT --baseline-snapshot BASELINE
binlogviz trend <snapshot...>
binlogviz trend --from-snapshots 'incident-*'
binlogviz snapshot save <report.json> --name NAME
binlogviz snapshot list
binlogviz snapshot show <name>
binlogviz workflow run <plan.yaml>
binlogviz workflow run <plan.yaml> --output-dir ./artifacts
binlogviz workflow resume <output_dir>
binlogviz workflow resume <output_dir> --rerun analyze:week2
binlogviz workflow status <output_dir>
binlogviz workflow status <output_dir> --format json
binlogviz workflow clean <output_dir>
binlogviz workflow clean <output_dir> --format json
binlogviz workflow clean <output_dir> --apply --include-snapshots
binlogviz workflow export <output_dir>
binlogviz workflow export <output_dir> --output ./incident.zip
binlogviz workflow export <output_dir> -o ./incident.zip
binlogviz workflow export <output_dir> --include-snapshots --format json
binlogviz workflow validate <plan.yaml>
binlogviz workflow validate <plan.yaml> --format json
binlogviz workflow describe <plan.yaml>
binlogviz workflow describe <plan.yaml> --format json
```

## 全局参数

这些参数定义在根命令上，会在子命令执行前生效：

| Flag | 默认值 | 说明 |
|------|--------|------|
| `--lang` | 按环境变量检测 | 运行时输出语言，例如 `en` 或 `zh-CN`。 |
| `--tz` | 历史 UTC 写法 | 渲染时间戳的 IANA 时区，例如 `Asia/Shanghai`。不传或 `UTC` 在文本、Markdown、HTML 里保持 ` UTC`，在 JSON 里保持 `Z`。没有时区的 --start 或 --end，在设置了 --tz 时按 --tz 读取，包括 --tz UTC；没有设置 --tz 时，按运行 binlogviz 的机器的本地时区读取。空名、未知名或 `Local` 退出码是 3。 |
| `--version`, `-v` | `false` | 仅输出版本号并退出。`binlogviz version` 在构建带有 git 信息时还会输出 commit；`binlogviz version --format json` 输出 `{"schema_version": 1, "version", "commit", "commit_time", "modified", "go_version"}`，供脚本和 agent 使用（`commit` 优先取 Go 构建信息，其次取 `-ldflags -X binlogviz/internal/version.Commit=…`/`CommitTime=…` 注入的值，两者都没有时为 `""`）。其他 `--format` 退出码为 3。 |

## `analyze` 命令语法

每次调用 `analyze` 只能使用一种输入模式：

- **位置参数文件模式**：通过位置参数传入一个或多个本地 binlog 文件路径。`-` 从 stdin 读取一份二进制 binlog。
- **discovery 模式**：同时提供 `--from-dir` 和 `--prefix`，由 BinlogViz 从目录中解析匹配文件；或用 `--from-dir` 加 `--index-file` 按服务器的 `binlog.index` 清单读取（顺序即索引顺序，所列文件必须都存在）。
- **远程模式**：传入 `--host`，以伪副本身份从一台存活的 MySQL 或 MariaDB 服务器上流式读取 binlog 事件，无需先复制文件。远程参数与位置参数文件、`-`、`--from-dir`、`--prefix`、`--index-file` 以及 `--start-position`、`--stop-position` 互斥。

默认文本和 JSON 报告会在每条事务上写出事件里实际有的 `server_id`、`thread_id`、GTID，以及 `xid` 或 XA xid。只有 binlog 存了调用者时才出现 `user@host`。事件里没有的字段保持缺席，报告不会补 `0` 或空字符串。Markdown 和 HTML 使用同一规则。

## 输入规则

### 位置参数文件模式

当你已经知道要分析哪些精确文件时，使用位置参数即可。

```bash
binlogviz analyze mysql-bin.000123
binlogviz analyze mysql-bin.000123 mysql-bin.000124
cat mysql-bin.000123 | binlogviz analyze -
```

`-` 从管道或其他不可 seek 的输入读取一份二进制 binlog。解析需要 seek，因此字节会先复制到临时文件，命令结束后删除。在终端上执行 `binlogviz analyze -`，或管道为空，会在解析前以 `Error:` 失败。命名管道路径同样处理。`mysqlbinlog` 的文本输出不是 binlog，会在魔数检查处失败。

### Discovery 模式

当你希望 BinlogViz 帮你解析一个有序文件集合时，使用 discovery 模式。

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin.
```

### 位置参数与 discovery 的行为关系

- 位置参数文件与 discovery flags 互斥。
- `--from-dir` 需要且只能搭配 `--prefix` 或 `--index-file` 之一。
- 如果既没有位置参数文件，也没有完整的 discovery 参数对，命令会失败。

关于精确的 discovery 匹配、排序、已解析文件报告以及非法组合契约，请参见[输入发现参考](input-discovery.zh-CN.md)。

### 远程模式

当 binlog 文件无法复制时使用远程模式——托管数据库、不允许落盘的主机，或空间与权限受限的场景。

```bash
export BINLOGVIZ_MYSQL_PASSWORD='secret'
binlogviz analyze --host db.internal --user replica
binlogviz analyze --host db.internal:3307 --user replica --start-file binlog.000041 --start-pos 12345
binlogviz analyze --host db.internal --user replica --start-gtid '3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5'
binlogviz analyze --host mariadb.internal --user replica --start-gtid '0-1-300'
```

客户端注册为伪副本（`COM_BINLOG_DUMP`，MySQL GTID 起点用 `COM_BINLOG_DUMP_GTID`，MariaDB 用 `@slave_connect_state`），每个事件都经过与文件输入相同的字节级校验。账号需要 `REPLICATION SLAVE` 和 `REPLICATION CLIENT`。

读取是有界的：连接时客户端会快照 `SHOW BINARY LOGS`，并在最后列出的文件结尾处停止。探测之后新写入的事件、以及越过探测清单的服务器轮换，都会被排除，而不是让报告去追赶。没有起点选择器时，dump 覆盖服务器从首个 binlog 起的全部保留历史。`--start-file` 必须在探测到的清单中（已清除的名字会以 `reason: start_file` 报 `input` 失败），`--start-pos` 不得超过探测到的文件大小（`usage`）。`--start-gtid` 的语义是“已消费的事务集”：流从其后第一个事务开始；裸写 `uuid:N` 展开为 `uuid:1-N`。GTID 集合的格式自行选择线路 flavor——MySQL 的 `uuid:intervals` 与 MariaDB 的 `domain-server-seq`，不需要单独的 `--flavor` 参数。

`--user` 默认取操作系统登录名。密码来自 `--password` 或 `BINLOGVIZ_MYSQL_PASSWORD`（flag 优先；环境变量可使其不出现在命令行里），绝不写入日志或输出。`--server-id` 默认生成密码学随机的伪副本 id。`--ssl-mode` 默认 `preferred`（服务器支持则启用 TLS，与 `mysql` 客户端一致）：`preferred` 只在服务器握手时拒绝 TLS 时回退明文——发生在发送凭据之前；`required` 加密但不校验证书（MySQL `REQUIRED` 语义），否则失败。读取中途连接断开会以 `input`（`reason: stream`）失败，而不是产出一份截短的报告。

## Flags

| Flag | 默认值 | 说明 |
|------|--------|------|
| `--start` | none | 开始时间，包含边界。RFC3339，或 `YYYY-MM-DD HH:MM:SS`（可带 `±HH:MM` 或以 ` UTC` 结尾）。显式偏移量定义该时刻。没有时区的 --start 或 --end，在设置了 --tz 时按 --tz 读取，包括 --tz UTC；没有设置 --tz 时，按运行 binlogviz 的机器的本地时区读取。 |
| `--end` | none | 结束时间，包含边界。格式与 `--start` 相同。没有时区的 --start 或 --end，在设置了 --tz 时按 --tz 读取，包括 --tz UTC；没有设置 --tz 时，按运行 binlogviz 的机器的本地时区读取。 |
| `--start-position` | none | 单个显式 binlog 文件上的精确事件起始边界（包含）。 |
| `--stop-position` | none | 单个显式 binlog 文件上的精确事件结束边界（不包含），也可为 EOF。 |
| `--include-gtids` | none | 包含匹配 MySQL UUID range set，或 MariaDB 身份 `domain-server-seq` 与区间 `domain-server-first..last`（`..` 两侧不能有空白）的完整事务组。报告里的 `selection.matched_gtid_set` 可以直接作为输入。 |
| `--exclude-gtids` | none | 在 include 之后排除匹配的完整事务组；exclude 优先。 |
| `--from-dir` | none | 从该目录自动发现 binlog 文件。必须与 `--prefix` 一起使用。 |
| `--prefix` | none | 配合 `--from-dir` 使用的文件名前缀。必须与 `--from-dir` 一起使用。数字后缀前可省略 `.`；完整文件名只匹配该文件；单文件请直接传路径。 |
| `--index-file` | none | `--from-dir` 目录中的 binlog 索引清单（例如 `binlog.index`）。相对条目按目录解析，按列出顺序读取，所列文件必须都存在。与 `--prefix` 互斥。 |
| `--host` | none | 从存活服务器读取 binlog 事件：`host` 或 `host:port`。传入该参数即选择远程输入模式。 |
| `--port` | `3306` | 远程输入的服务器端口；`--host` 内携带的端口优先，除非同时显式传入 `--port`。 |
| `--user` | 操作系统登录名 | 远程输入的复制账号。 |
| `--password` | none | 远程输入的账号密码；未传该 flag 时由 `BINLOGVIZ_MYSQL_PASSWORD` 补齐（推荐用法——使密码不出现在命令行）。绝不写入日志或输出。 |
| `--server-id` | `0` = 随机 | 伪副本注册时使用的 `server_id`；必须非零，缺省时为密码学随机值。 |
| `--start-file` | none | 服务器上开始读取的 binlog 文件名；缺省时读取服务器全部保留历史。 |
| `--start-pos` | `4` | `--start-file` 内的字节位置；必须与 `--start-file` 一起使用且不小于 4。 |
| `--start-gtid` | none | 已执行 GTID 集合，从其后继续读取（MySQL `uuid:intervals` 或 MariaDB `domain-server-seq`）；与 `--start-file`/`--start-pos` 互斥。 |
| `--ssl-mode` | `preferred` | 远程输入的 TLS 模式：`preferred`（服务器支持时使用 TLS）、`required`（必须 TLS，否则失败）或 `disabled`。 |
| `--format` | `text` | 报告输出格式：`text`、`json`、`markdown`（别名 `md`）、`html`、`csv` 或 `tsv`。 |
| `--output`, `-o` | auto | HTML 输出文件路径。仅支持 `--format html`。默认：TTY 下写入推导出的 cwd 文件；stdout 被重定向时写入 stdout。使用 `-` 强制 stdout。 |
| `--csv-dataset` | `tables` | `--format csv`/`tsv` 写出的数据集：`tables`、`transactions`、`patterns` 或 `fingerprints`。在其他格式下使用该标志是用法错误（exit 3）。 |
| `--snapshot-name` | none | 把本次 JSON analyze 输出保存成 `<name>.json`。要求同时使用 `--format json`。 |
| `--snapshot-dir` | home-based default | 保存快照时使用的目录。默认值：`~/.binlogviz/snapshots`。 |
| `--sql-context` | `summary` | 所有格式中的查询文本，包括默认文本、DDL 语句和 `--show-patterns`：`summary`（一行有界摘要）、`off`（不输出查询文本，也不输出 `--show-rows` 的单元格）或 `full`（存储 SQL，含 DDL 语句文本，上限 4096 字节，截断时带标记）。MySQL 和 MariaDB 账号 DDL 里的口令字面量在任何模式下都会打码。 |
| `--diagnostics` | `full` | `analyze --format json` 里 `diagnostics` 写多少。`full` 是现在的对象。`summary` 保留覆盖、计数、未提交组和时长分桶，把 `ddl_events` 限制为前 `--top` 条（默认 10）并写出 `ddl_events_omitted`，并把事务排行、文件分段、热点时段和 findings 置空（这些已经在 `transactions`、`timeseries`、`minutes` 和 `alerts` 里）。`off` 省略 `diagnostics`。文本、Markdown 和 HTML 忽略此标志。显式传入空字符串是用法错误（exit 3）。裁剪后的文档带 `"diagnostics_mode": "summary"` 或 `"off"`。JSON `null` 视为没有该字段。非字符串是损坏文档（exit 5）。它只限制 stdout 的大小：`--snapshot-name` 保存的快照始终是完整的 diagnostics；compare、trend 和 `snapshot save` 会拒绝裁剪过的文档（exit 9，`reduced_diagnostics`）。`snapshot list` 会标出这种文件，而不是拒绝它。 |
| `--top-tables` | `10` | 人类可读报告中显示的 Top 表数量；JSON 保留全部表聚合结果。 |
| `--top-transactions` | `10` | 报告中包含的 Top 事务数量；`0` 表示不限制。 |
| `--top-threads` | 继承 `--top` | 所有格式中的热点线程或会话数量；`0` 表示不限制。 |
| `--top-rows` | 继承 `--top` | 所有格式中的热点主键数量；`0` 表示不限制。 |
| `--top-fingerprints` | 继承 `--top` | JSON 列表、文本章节和 `fingerprints` CSV/TSV 数据集里的语句指纹组数量；`0` 表示不限制。聚合本身始终覆盖整个已选工作负载。 |
| `--top` | `10` | 文本明细章节（分钟、写入形态）以及热点线程、热点行、语句指纹的默认 Top-N。`0` 就是这个默认值，不是不限制。 |
| `--details` | `false` | 在文本报告中同时展开分钟明细、写入形态和语句指纹。 |
| `--show-minutes` | `false` | 在文本报告中展示分钟级活动。 |
| `--show-patterns` | `false` | 在文本报告中展示写入形态。 |
| `--show-fingerprints` | `false` | 在文本报告中展示语句指纹排名。 |
| `--detail-store` | `none` | 可选事务明细后端：`none` 或 `duckdb`。 |
| `--detect-spikes` | `false` | 启用写入尖峰检测。 |
| `--large-trx-rows` | `1000` | 大事务告警的行数阈值。 |
| `--large-trx-duration` | `30s` | 大事务告警的持续时间阈值。 |
| `--top-minutes` | `60` | 报告中包含的最活跃分钟数。 |
| `--spike-window` | `5` | 尖峰检测的滑动基线窗口（分钟）。 |
| `--spike-factor` | `3.0` | 触发尖峰告警的基线倍数。 |
| `--spike-min-rows` | `100` | 纳入尖峰检测候选的最小行数。 |
| `--include-schema` | none | 仅分析指定 schema（逗号分隔，其余均排除）。 |
| `--exclude-schema` | none | 跳过指定 schema（逗号分隔）。 |
| `--include-table` | none | 仅分析指定对象（逗号分隔，其余均排除）。`TABLE` 或 `SCHEMA.TABLE`，也包括 VIEW、EVENT、FUNCTION、PROCEDURE、TRIGGER 的名字。 |
| `--exclude-table` | none | 跳过指定对象（逗号分隔）。`TABLE` 或 `SCHEMA.TABLE`，也包括 VIEW、EVENT、FUNCTION、PROCEDURE、TRIGGER 的名字。 |
| `--dml` | none | 只统计这些 ROW 类型（逗号分隔）：`insert`、`update`、`delete`，可组合。与 schema、table、时间、位点、GTID 过滤一起生效。没有匹配时退出 2：`Error: dml filter matched no events`。 |
| `--show-rows` | `false` | 为列出的事务打印有界行值（DELETE 前镜像，UPDATE 变化列，INSERT 后镜像）。列名需要 `binlog_row_metadata=FULL`；否则列是 `@1`..`@N`。`--sql-context off` 省略这些值。 |

binlog 是 MySQL 8 且 `binlog_row_metadata=FULL` 时，TABLE_MAP 可选元数据会给出每张表的主键（`SIMPLE_PRIMARY_KEY` 或 `PRIMARY_KEY_WITH_PREFIX`），或者说明这张表没有主键。每种输出都会列出没有主键、并且收到了 UPDATE 或 DELETE 行的表，按这些行数排序。副本应用这些行时可能全表扫描。没有主键但只有 INSERT 的表会点名，但不会被排成这种延迟风险。只要有一行 UPDATE 或 DELETE，就会在大事务和尖峰告警旁边给出 `no_primary_key` 警告；没有单独的阈值参数。没有 FULL 元数据时报告不猜测：这些表是 `unknown`，并说明主键是否存在未知，因为 `binlog_row_metadata` 不是 FULL。`--include-table`、schema 过滤、`--dml`，以及时间、位点、GTID 选择，对这一节的作用与对 Top Tables 相同。analyze 报告不生成撤销 SQL。`binlogviz flashback` 会生成。

热点行按主键被 UPDATE 和 DELETE 行镜像碰到的次数排序。每一条有次数、不同事务数、第一次和最后一次事件时间，以及最早和最晚那个事务的 GTID 和 file:byte（事务起点，用来打开 `mysqlbinlog` 或 BinlogServer）。身份只来自 MySQL 8 `binlog_row_metadata=FULL`（`SIMPLE_PRIMARY_KEY` 或 `PRIMARY_KEY_WITH_PREFIX` 点名的列，不是第 1 列 `@1`）。binlog 里没有主键时，这张表不参与排名，报告说明无法追踪热点行，因为 `binlog_row_metadata` 不是 FULL。没有主键的表不在这一节排名。`--top` 限制条数；`--top-rows` 覆盖它；`0` 保留已追踪的全部键。`--sql-context off` 隐藏键值，次数仍在。过滤与 Top Tables 相同。追踪最多保留 8192 个主键。满了之后，新键替换被碰到次数最少的键，次数从被丢掉的计数加一开始；报告说明达到了上限，并把这一行标成近似。从未被替换的键保持精确。JSON 字段：`hot_rows`、`hot_rows_listed`、`hot_rows_omitted`、`hot_row_track_limit`、`hot_rows_overflow`、`hot_rows_note`、`hot_row_unavailable`。

语句指纹把 MySQL 在每个行块前记录的 `Rows_query` 语句文本（`binlog_rows_query_log_events=ON`）分组：注释被剥离，字符串/数字/十六进制/二进制字面量和占位符变成 `?`，空白折叠，字面量列表折叠成 `(?)`，所以 `DELETE FROM t WHERE id IN (1, 2, 3)` 和 `DELETE FROM t WHERE id IN (7)` 归为同一个形态。每组报告语句发生次数、归因行数、行占比、每表计数和一条有界样例。聚合覆盖整个已选工作负载，包括没有列入 `--top-transactions` 的事务，并遵循同样的过滤条件。组按行数、语句次数、文本排序。追踪最多保留 8192 个不同组；超出的发生次数计入 `statement_fingerprints_overflow` 而不再分组。文本章节由 `--show-fingerprints` 或 `--details` 开启，并在 binlog 没有携带 `Rows_query` 文本时明确说明。`--sql-context off` 隐藏样例，计数仍在。JSON 字段：`statement_fingerprints`、`statement_fingerprints_listed`、`statement_fingerprints_omitted`、`statement_fingerprints_overflow`。`fingerprints` CSV/TSV 数据集输出同一组有界排名。

position selector 使用 `[start, stop)` 语义；discovery、多显式文件、反向/越界/事件中间位置都会失败。position 与时间条件取交集。GTID selector 在有序 rotation 上完成事务组重建后生效；匿名组不匹配任何 active selector（包括仅 exclude 的 selector）。独立的匿名 DDL 和无键上下文会被丢弃，但不会阻止后续匹配的有键事务组被保留。混合/冲突/无法解析的 flavor 会失败；合法但无保留事件的选择以 exit 2 结束且不输出报告。

DDL 时间线在 binlog 有 GTID 时给出持有该 DDL 的事务（MySQL `uuid:seq` 或 MariaDB `domain-server-seq`）、该事务起点的文件字节（GTID 事件的起点，不是 Query 事件，也不是 `end_log_pos`），以及两段可复制的值：`mysqlbinlog --stop-position=<N> <file>` 和 BinlogServer `stop_gtid=<gtid>`。两者都回放更早的事件，并且不包含这条 DDL。没有 GTID（`GTID_MODE=OFF` 或匿名）时省略 GTID 并写明不可用，位置提示仍然在。`--sql-context off` 仍然只省略语句。退出码不变。

### 保存快照时的行为

`analyze` 可以选择把写到 `stdout` 的同一份 JSON 载荷持久化到快照目录。

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. \
  --format json \
  --snapshot-name incident_current
```

规则：

- `--snapshot-name` 必须与 `--format json` 一起使用
- 快照名必须是单个文件名 stem，只允许字母、数字、`-`、`_`
- `--snapshot-dir` 可以覆盖默认的 home-based 快照目录
- 只要设置了 `--snapshot-name`，报告仍然照常写到 `stdout`
- 保存成功提示写到 `stderr`

如果省略 `--snapshot-dir`，BinlogViz 会保存到 `~/.binlogviz/snapshots/<name>.json`。

文本报告明细参数示例：

```bash
binlogviz analyze mysql-bin.000123 --details --show-minutes --show-patterns
```

## `flashback` 命令语法

```bash
binlogviz flashback <binlog files...>
binlogviz flashback --from-dir DIR --prefix PREFIX
binlogviz flashback --from-dir DIR --index-file FILE
binlogviz flashback --host db.internal --user replica --start-file binlog.000041 --start-pos 4
binlogviz flashback mysql-bin.000123 --include-table shop.orders --dml delete
binlogviz flashback mysql-bin.000123 --include-table shop.orders --where "user_id < 400"
binlogviz flashback mysql-bin.000123 --schema-file schema.sql
binlogviz flashback mysql-bin.000123 --schema-file schema.sql --schema-file-db shop
binlogviz flashback mysql-bin.000123 --output-dir undo/ --file-per-table
binlogviz flashback mysql-bin.000123 --output-dir undo/ --max-statements-per-file 500
```

`flashback` 打印撤销选定行变更的 SQL。stdout 是脚本。除了用 `--host` 读取远程 binlog 之外不连接数据库。输入规则与 `analyze` 一致，包括远程模式：位置参数、stdin `-`、`--from-dir` 加 `--prefix` 或 `--index-file`，或复制协议输入 `--host`/`--port`/`--user`/`--password`/`--server-id`/`--start-file`/`--start-pos`/`--start-gtid`/`--ssl-mode`（凭据、TLS 和起点选择的约定见「远程模式」一节）。远程读取以连接时拿到的 binlog 清单为界——不会跟随。远程模式拒绝位置/发现输入和本地位置选择；`--start-pos` 需要 `--start-file`，`--start-gtid` 与 `--start-file`/`--start-pos` 互斥。远程读取从事务中间开始或结束时，撤销已送达的部分；`partial_transaction` 警告标记的是选择切分，不是窗口截断。flashback 是实验功能：执行生成的 SQL 之前必须先审阅。

选择条件与 `analyze` 相同：`--include-schema`、`--exclude-schema`、`--include-table`、`--exclude-table`、`--dml`、`--start`、`--end`、`--start-position`、`--stop-position`（仅对单个显式文件）、`--include-gtids`、`--exclude-gtids` 和 `--where`。`--schema-file` 是可选的表定义 SQL（`mysqldump --no-data`，或一份或多份 `SHOW CREATE TABLE`，含 `mysql --batch` 把语句内部换行写成 `\n` 的输出），不是过滤器。文件必须是事故当时的表结构。普通转储不必带 `USE`：库名来自 `-- Host: ... Database:` 头、`--schema-file-db`，或 binlog 里该表唯一的库。多份 `SHOW CREATE TABLE` 可以放在同一个文件里，中间没有 `;`。库名有歧义时只警告，不用这份定义。`Database:` 头里的库名可以带空格。binlog 里的名字全是小写、而文件里只有大小写不同的同名表时（例如转储来自 `lower_case_table_names=0` 的库，binlog 来自 `lower_case_table_names=1` 或 `2` 的库），flashback 采用这张表，警告里写出文件中的表名，行数据仍按它校验。binlog 名字含大写，或文件里有两张只差大小写的表时，不做这种匹配。DELETE 变成前镜像的 `INSERT`。INSERT 变成后镜像的 `DELETE`。UPDATE 把每一列设回前镜像，并用后镜像的主键匹配。顺序是 binlog 逆序。整个撤销是一个 XA 事务，文件本身不会提交。`mysql --force` 不会提交前缀，同一个会话里后面的 `BEGIN`、`START TRANSACTION` 或 DDL 也不会（ADR 0014）。执行需要在 `binlogviz.*` 上具备 `CREATE`、`CREATE ROUTINE` 和 `ALTER ROUTINE`，以及官方 `mysql` 客户端。`verify` 每次执行都会新建一个结束过程，并在结束时删掉它。残留过程用 `DROP DATABASE binlogviz` 清掉。应用方式是 `binlogviz flashback verify <file> | mysql --default-character-set=utf8mb4`。注释写原 GTID，没有则写 `GTID unavailable`，以及 `file:start-position`。

binlog 需要 `binlog_row_metadata=FULL` 和 `binlog_row_image=FULL`。没有主键的表按每一列匹配并加 `LIMIT 1`，注释会说明。被删行的 `INSERT` 不用 `LIMIT 1`。

`--where EXPR` 只保留谓词可证为真的行，与其余选择条件是逻辑与（ADR 0024）。谓词读的是该操作用来匹配行的镜像：INSERT 的后镜像、DELETE 和 UPDATE 的前镜像。支持的语法是 MySQL 的子集：列名（需要时加反引号）、整数和小数字面量、`'引号'` 字符串、`X'..'` 和 `b'..'` 字面量、`NULL`、`TRUE`、`FALSE`、比较符 `=`、`<=>`、`<>`、`!=`、`<`、`<=`、`>`、`>=`、`IS [NOT] NULL|TRUE|FALSE`、`[NOT] IN (...)`、`AND`、`OR`、`XOR`、`NOT`、括号、算术 `+ - * / DIV % MOD`，以及生成列函数 `UPPER`、`LOWER`、`CONCAT`、`CONCAT_WS`、`LENGTH`、`CHAR_LENGTH`、`OCTET_LENGTH`、`CHARACTER_LENGTH`、`JSON_EXTRACT`、`JSON_UNQUOTE`、`MOD`。其余一切——`BETWEEN`、`LIKE`、`REGEXP`、限定的 `t.c` 名字、`COLLATE`、子查询、其他函数——都在读输入之前作为用法错误拒绝（`invalid --where ...`）。求值遵循 MySQL 三值逻辑：谓词为 `NULL`、为假、或无法精确验证时不选该行——binlog 带字符集但没有排序规则，字节序和折叠大小写后的顺序不一致的文本比较不可验证，数字与非数值文本的比较同样不可验证。谓词命名了行里不存在的列时退出 6，`details.reason` 为 `where_column`。谓词只保留一个事务的一部分时在 stderr 警告，与表过滤或 `--dml` 一样；谓词之后没有任何行被选中时退出 2，与空选择相同。

JSON 按二进制文档重建，包括 `binlog_transaction_compression=ON` 的事务，小数、日期时间和 `-0.0` 的符号可以原样还原。非 `utf8mb4` 字符列是字符集引导符加十六进制字节（`_latin1 0xE9`）。`ENUM` 写成成员序号，`SET` 写成位掩码。`ENUM` 序号 0 会先保存 `@@SESSION.sql_mode`，只在这一条语句去掉 `STRICT_TRANS_TABLES`、`STRICT_ALL_TABLES` 和 `TRADITIONAL`，然后把保存的模式设回去。日期的月或日为零（`'0000-00-00'`）时同样包一层，去掉的是 `NO_ZERO_DATE` 和 `NO_ZERO_IN_DATE`；略去的生成列记下的值是 `NULL` 时去掉 `ERROR_FOR_DIVISION_BY_ZERO`。这样非严格会话写下的行在严格会话里也能还原，严格模式仍然开着（#180）。解析到的文件里若有 `CREATE` 或 `ALTER`（含被 `--exclude-gtids` 排除的事件）点名了生成列，或 `--schema-file` 点名了生成列，这些列不会出现在 `INSERT` 和 `UPDATE` 赋值里。schema 文件里的整数和 `DECIMAL` 表达式（`+`、`-`、`*`、`/`、`DIV`、`%`、`MOD`），以及 `UPPER`、`LOWER`、`CONCAT`、`CONCAT_WS`、`LENGTH`、`CHAR_LENGTH` 和简单的 JSON 提取，都会核对。`/` 赋给整数时远离零的一半进位。`/` 赋给 `DECIMAL(p,s)` 时，整数操作数按 MySQL 默认保留 9 位小数，再按列的标度远离零的一半进位（`DECIMAL(40,4)` 里的 `5 / 2` 是 `2.5000`，`DECIMAL(40,9)` 里的 `1 / 7` 是 `0.142857142`）。记下的值与表达式矛盾时退出 1，不打印 SQL，包括 `div_precision_increment = 0` 时写下的商。结果放不进列类型、且记下的值是非严格模式的截断端点时，省略该列并给出警告。表达式无法核对时省略该列。stderr 给出警告，三条 `SET` 之后的守卫会在目标上这一列不是生成列时让执行失败。错误行会列出每一个对不上的列。遇错继续的客户端会被改成只读会话，后面的写入都会失败。目标上这一列是生成列但表达式不同时，守卫会放过，由目标重新计算。MySQL 8 的 `TABLE_MAP` 可选元数据不标记生成列，`FULL` 镜像同时包含虚拟列和存储列的值，核对用的就是这些值。记下的值恰好等于能核对的表达式时，真实列和生成列仍然分不开。schema 文件和这些列对不上时不打印 SQL。错误会说明文件对不上，并给出下一步：事故当时的转储、`ALTER` 之后则用更早的转储，或用 `--include-table` 把这张表排除。定义出现过但读不出来时，拒绝该表且不打印 SQL。选中的表没有定义时，脚本仍列出每一列，stderr 警告不能排除生成列，并且执行可能在 `ERROR 3105` 停下，撤销会回滚。

`ON DELETE` / `ON UPDATE CASCADE` 的子表行不在 binlog 里，不会被还原。撤销时触发器会执行。选中的行在扫描的 binlog 中之后又被修改时，脚本以退出码 7 拒绝（`--conflict refuse`，默认）。`--conflict guard` 让每条撤销语句匹配日志里的后镜像而不只是主键，DELETE 的撤销只在主键不存在时插入，把主键改回去的 UPDATE 撤销在旧主键已被别的行占用时不改任何行（不再报 `ERROR 1062`）。`--plan` 输出 JSON 计划（`plan_version` 2：事务数、每张表的行数、冲突、加了 guard 的语句数、`would_refuse`、`refusal`、`warnings`），退出码与真正执行时一致。被拒绝时也输出计划：退出码 7 或 6 时 `refusal` 是 `{"kind": "conflict"|"refused", "message": ...}`，否则为 `null`；`refused` 时各计数为 0。`warnings` 每项是 `{"code", "message"}`，代码有 `partial_transaction`、`table_definition_missing`、`generated_column_unverified`、`schema_file_db_mismatch`、`schema_file_table_ambiguous`、`schema_file_case_folded`、`schema_file_case_mismatch` 和 `mariadb_apply_unsupported`，不随 `--lang` 变化。`--conflict-limit N`（默认 20，`0` 表示全部）决定计划和退出码 7 的错误里列出多少处冲突；`count` 始终是完整总数。扫描范围之外的修改看不到。表过滤或 `--dml` 只保留一个事务的部分行时，stderr 打出警告。先审阅并测试脚本，再在主库的同一个会话里执行（`sql_log_bin=1`），副本才会在新 GTID 下跟上。某条语句失败时，撤销会回滚，`mysql --force` 也一样。不要把脚本切成块：`binlogviz flashback verify` 会拒绝缺了一段或没有结尾的文件，`mysql < flashback.sql` 不会提交任何修改。截断的前缀后面再跟 `BEGIN` 或 DDL，也不会把它提交（ADR 0014）。`TIMESTAMP` 字面量是 UTC，脚本会设置 `time_zone='+00:00'`。剩下的限制，包括记下的值恰好等于能核对表达式的真实列（#166）、在解析到的 binlog 里先加上又被修改或改名的列，见 `docs/concept/limitations.zh-CN.md`。`sql_mode=TRADITIONAL` 下的 `ENUM` 序号 0 已经处理（#168）。

`--output-dir DIR` 把一个目录的拆分 SQL 文件写到 `DIR`，而不是向 stdout 输出一份脚本（ADR 0025）。它要求 `--file-per-table` 或 `--max-statements-per-file N` 至少给一个，这两个标志也要求 `--output-dir`。`--file-per-table` 按撤销时倒序扫描首次见到表的顺序为每张选中的表写一组文件，命名为 `<schema>.<table>.sql`（同时按条数拆分时为 `<schema>.<table>-NNNNNN.sql`）。`--max-statements-per-file N` 把撤销流按每个 `part-NNNNNN.sql` 最多 N 条切块；与 `--file-per-table` 组合时限定每张表的文件集。`DIR` 必须不存在或为空，且父目录必须存在——否则是用法错误。每个文件本身就是一份完整的密封脚本：自带过程、XA 事务、摘要和结尾标记，可以独立用 `binlogviz flashback verify` 校验并执行。原子应用单位是单个文件而不是整个运行——不同表的文件顺序无所谓（注意外键），同一流的条数切块必须按 `MANIFEST` 顺序执行。`MANIFEST` 按执行顺序列出文件。目录原子发布：文件先写进同级临时目录，全部 fsync 且 `MANIFEST` 写完后才改名就位，失败不会留下半个文件集。`--max-statements-per-file` 下一个事务的撤销落到多个文件时按 GTID 在 stderr 警告（仅按表拆分时一个事务天然分到各表文件，不警告）。`--plan` 不能与拆分输出组合。

列名缺失、行镜像不完整、某一列无法精确写成字面量、出现过的表定义读不出来、`--schema-file` 和 binlog 的列不一致、生成列的值与表达式矛盾、选定范围内有 DDL，或 `--sql-context off` 时，退出 1，一行 `Error:`，没有 SQL。什么都没选中时退出 2，`Error:` 与 `analyze` 相同，stdout 为空。除此之外 `--sql-context` 不影响脚本：flashback 不打印原始语句，单元格的值不打码。选定范围内的账号 DDL 按 DDL 拒绝。

## `redo` 命令语法

```bash
binlogviz redo <binlog files...>
binlogviz redo --from-dir DIR --prefix PREFIX
binlogviz redo --from-dir DIR --index-file FILE
binlogviz redo --host db.internal --user replica --start-file binlog.000041 --start-pos 4
binlogviz redo mysql-bin.000123 --include-table shop.orders --dml insert
binlogviz redo mysql-bin.000123 --include-table shop.orders --where "user_id < 400"
binlogviz redo mysql-bin.000123 --schema-file schema.sql
binlogviz redo mysql-bin.000123 --output-dir replay/ --max-statements-per-file 500
cat mysql-bin.000123 | binlogviz redo -
```

`redo` 按 binlog 顺序打印重演所选变更的 SQL——即 `2sql` 方向。stdout 是脚本，在解析和规划全部完成之前一直是暂存的，拒绝或文件损坏时什么都不留下。除了读取远端 binlog 之外不连接数据库。请自行审阅脚本并执行——用于时间点恢复的副本回放、选择性重演或审计。redo 是实验功能。

输入规则与 `analyze` 一致，包括远端模式：位置参数、stdin `-`、`--from-dir` 加 `--prefix` 或 `--index-file`，或复制协议输入 `--host`/`--port`/`--user`/`--password`/`--server-id`/`--start-file`/`--start-pos`/`--start-gtid`/`--ssl-mode`。远端读取以连接时拿到的 binlog 清单为界——不会跟随。远端模式拒绝位置/发现输入和本地位置选择；`--start-pos` 需要 `--start-file`，`--start-gtid` 与 `--start-file`/`--start-pos` 互斥。选择条件与 `analyze` 相同：`--include-schema`、`--exclude-schema`、`--include-table`、`--exclude-table`、`--dml`、`--start`、`--end`、`--start-position`、`--stop-position`（仅对单个显式文件）、`--include-gtids`、`--exclude-gtids` 和 `--where`。`--schema-file` 和 `--schema-file-db` 遵循 flashback 的契约——必须是事故当时的表结构，且不是过滤器。

`--where EXPR` 只保留谓词可证为真的行，与其余选择条件是逻辑与。谓词读 INSERT 的后镜像、DELETE 和 UPDATE 的前镜像——与上文 `flashback --where` 使用相同的匹配镜像、表达式子集、三值结果、`where_column` 拒绝和部分事务警告（ADR 0024）。`binlog_row_metadata=MINIMAL` 时，谓词里的列名与输出的列名一样通过 `--schema-file` 或解析到的 DDL 解析；没有可解析的定义时仍是 `names` 拒绝。

日志里的 INSERT 输出后镜像的 `INSERT`，DELETE 输出按前镜像匹配的 `DELETE`，UPDATE 输出把后镜像设为值、按前镜像匹配的 `UPDATE`——表映射给出主键时按主键匹配，否则按全部列匹配并加 `LIMIT 1` 和 `no primary key` 注释，匹配一律用 NULL 安全的 `<=>`。定义点名生成列时写入里省略它们，与 flashback 相同。值的渲染与撤销走同一套字面量机制：JSON 按二进制文档重建，非 `utf8mb4` 文本用字符集引导符，二进制用十六进制，BIT 用 `b'...'`，ENUM/SET 用成员标签；ENUM 序号 0、零值日期、生成列里记下的 NULL 都会包一层 sql_mode 放松。

事务边界如实反映源流。记了 `BEGIN` 的组输出 `BEGIN;`；XID 输出 `COMMIT;`，记了 ROLLBACK 的输出 `ROLLBACK;`。DDL、ADMIN 和其他保留的 QUERY 语句原样透传——源里本就没有事务包装的隐式提交工作——语句的默认库与上次输出不同时会先输出 `USE`。`ROWS_QUERY` 注解变成 `--` 注释，GTID 事件变成 `-- gtid:`/`-- binlog:` 注释。输入结束时仍未关闭的组补一条 `COMMIT;` 并在 stderr 给出警告。XA 动词原样透传；服务器生成的 XA PREPARE 标记展开成回放所需的 `XA END`/`XA PREPARE` 一对。脚本开头是与 flashback 一致的会话 `SET` 行（sql_mode 放松、`SET NAMES utf8mb4`、UTC `time_zone`）。

binlog 需要 `binlog_row_image=FULL`。`binlog_row_metadata=MINIMAL` 时，列名和类型事实来自 `--schema-file` 或扫描文件里解析到的 DDL：先按行本身核对每一项可比事实（基础类型、符号、字符集、成员表、精度、小数秒位），事实矛盾、列数不一致或没有可解析的定义都拒绝脚本——只有列名不够。STATEMENT 或 MIXED 格式输入被拒绝：扫描到的输入里只要有一条语句格式 DML 就以退出码 9、`details.reason` 为 `statement_format` 结束，因为输出脚本会重演行选择之外的变更。

`--output-dir DIR` 把重演拆成 `DIR` 下的 `part-NNNNNN.sql` 文件，而不是向 stdout 输出一份脚本（ADR 0025）。它要求 `--max-statements-per-file N`——每个 part 最多 N 条语句（行语句和事务动词计数，注释不计），该标志也要求 `--output-dir`。`DIR` 必须不存在或为空，且父目录必须存在。part 保持 binlog 顺序，按 `MANIFEST` 顺序执行。每个 part 都可独立执行：开头是会话 `SET` 行和当前的 `USE` 上下文；边界落在事务中间时该 part 以 `COMMIT;` 收尾，下一个 part 用续接注释加 `BEGIN;` 重开事务——跨文件事务按 GTID 在 stderr 警告，且不会有只含 `BEGIN;`/`COMMIT;` 的空 part。目录与 flashback 一样经暂存改名原子发布：失败不会留下半个文件集。`redo` 没有 `--file-per-table`——按表拆分会把一个事务的重演散到多个文件，破坏 binlog 顺序。

拒绝以退出码 6 结束并带 `details.reason`——`capture`（行镜像不完整）、`column_type`（单元格无法精确渲染）、`names`（MINIMAL 且无定义）、`schema`（定义与行矛盾）、`generated_unknown`、`truncated_query`——在指向单张表时附带 `details.table`，不输出 SQL。资源上限（`BINLOGVIZ_SPILL_BYTES`、`BINLOGVIZ_MEMORY_BYTES`）退出 10。什么都没选中时退出 2，stdout 为空。警告——表没有定义、过滤只保留事务的一部分、截断事务的补写提交——打在 stderr，不随 `--lang` 变化。redo 没有 `verify`/`apply`，没有 `--follow`，没有断线重连/续传，没有快照输入，也没有按表或按事务拆分输出（ADR 0022；`--output-dir` 的按条数拆分见 ADR 0025）。

## `compare` 命令语法

```bash
binlogviz compare <current.json> <baseline.json>
binlogviz compare <current.json> <baseline.json> --format text
binlogviz compare <current.json> <baseline.json> --format json
binlogviz compare <current.json> <baseline.json> --format html
binlogviz compare --current-snapshot current --baseline-snapshot baseline
binlogviz compare --current-snapshot current --baseline-snapshot baseline --snapshot-dir /tmp/binlogviz-snapshots
```

`compare` 每次调用支持两种输入模式：

- **文件模式**：两个位置参数 JSON 报告
- **快照模式**：`--current-snapshot` 加 `--baseline-snapshot`

### 文件模式

在文件模式下，`compare` 只接受两个位置参数：

- `current.json`：当前窗口对应的 BinlogViz 分析报告
- `baseline.json`：基线窗口对应的 BinlogViz 分析报告

### 快照模式

快照模式按名字加载之前保存的 analyze JSON 报告：

- `--current-snapshot`：作为 current report 使用的快照名
- `--baseline-snapshot`：作为 baseline report 使用的快照名
- `--snapshot-dir`：可选快照目录覆盖；默认 `~/.binlogviz/snapshots`

该命令不支持 discovery 模式、不接受 binlog 原文件、不支持 Markdown 输出，也不允许把文件模式和快照模式混用。

## `compare` 输入规则

`compare` 只接受由 `binlogviz analyze --format json` 生成的 JSON 报告，无论这些报告是通过显式文件加载，还是通过快照目录按名字加载。

校验规则：

- 文件模式必须同时提供两个位置参数
- 快照模式必须同时提供 `--current-snapshot` 和 `--baseline-snapshot`
- 文件模式和快照模式不能混用
- 每个输入都必须是可读取的本地 JSON 文件
- 每个输入都必须符合 BinlogViz analyze JSON 报告 shape
- 对 malformed JSON 或 foreign / 非 BinlogViz JSON，会在渲染前直接拒绝

支持的输出格式：

| Flag | 默认值 | 说明 |
|------|--------|------|
| `--current-snapshot` | none | 快照模式下作为 current report 使用的快照名。 |
| `--baseline-snapshot` | none | 快照模式下作为 baseline report 使用的快照名。 |
| `--snapshot-dir` | home-based default | 快照模式下使用的快照目录。默认值：`~/.binlogviz/snapshots`。 |
| `--format` | `text` | compare 报告输出格式：`text`、`json`、`html`。 |

代表性用法：

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. --format json --snapshot-name current > current.json
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. --format json --snapshot-name baseline > baseline.json

binlogviz compare --current-snapshot current --baseline-snapshot baseline
binlogviz compare --current-snapshot current --baseline-snapshot baseline --format json > compare.json
binlogviz compare --current-snapshot current --baseline-snapshot baseline --format html > compare.html

# 旧的文件模式仍然兼容
binlogviz compare current.json baseline.json
binlogviz compare current.json baseline.json --format json > compare.json
binlogviz compare current.json baseline.json --format html > compare.html
```

## `trend` 命令语法

```bash
binlogviz trend <snapshot-a> <snapshot-b> [<snapshot-c> ...]
binlogviz trend <snapshot-a> <snapshot-b> --baseline-snapshot baseline
binlogviz trend --from-snapshots 'incident-*'
binlogviz trend --from-snapshots 'incident-*' --baseline-snapshot baseline --format html > trend.html
```

`trend` 是一个面向 snapshot store 的历史趋势命令，每次调用支持两种互斥输入模式：

- **显式快照模式**：通过位置参数传入两个或更多 snapshot 名称
- **模式匹配模式**：通过 `--from-snapshots <pattern>` 从 snapshot store 中按名字选取

规则：

- 显式快照模式和模式匹配模式不能混用
- 最终解析出的 trend 集合至少要有两个 snapshot
- 显式 `trend A B` 默认保持 A→B（`--order cli`）；模式匹配保持 snapshot store 的选取顺序
- `--order time` 按有效窗口开始时间升序排序；如果因此改写了故事，会在 stderr 打印 `trend: reordered by window start_time: ...`
- trend 优先使用 `snapshot.window.start_time`；对于较旧的快照，会回退到 `summary.start_time`
- `--baseline-snapshot` 是可选的；除非 baseline 本身也被单独选中，否则它不会自动成为 trend 点
- 所有 trend 输出都会包含 pattern trends；`text` 和 `json` 会暴露 `Top Pattern Trends` / `pattern_trends`，`html` 会增加一个交互式 `Pattern Trends` 分区
- HTML trend 输出默认展示 `share of rows` 视角，并可以切换到绝对 `rows`

支持的 flags：

| Flag | 默认值 | 说明 |
|------|--------|------|
| `--format` | `text` | trend 报告输出格式：`text`、`json`、`html`。 |
| `--from-snapshots` | none | 从 snapshot store 中按名字选取快照的模式。 |
| `--baseline-snapshot` | none | 用于计算每个 trend 点 delta 的可选 baseline snapshot。 |
| `--snapshot-dir` | home-based default | 读取 snapshot 时使用的目录。默认值：`~/.binlogviz/snapshots`。 |
| `--top-tables` | `10` | trend 输出中包含的热点表趋势序列数量。 |
| `--order` | `cli` | 趋势点顺序：`cli` 保持参数或选取顺序；`time` 按窗口开始时间排序。 |

## `snapshot` 命令语法

```bash
binlogviz snapshot save <report.json> --name NAME
binlogviz snapshot save <report.json> --name NAME --snapshot-dir /tmp/binlogviz-snapshots
binlogviz snapshot list
binlogviz snapshot list --format json
binlogviz snapshot show <name>
binlogviz snapshot show <name> --format json
binlogviz snapshot rename <old-name> <new-name>
binlogviz snapshot delete <name>
```

`snapshot` 命令用于按名字管理 analyze JSON 报告。

### `snapshot save`

`snapshot save` 会把一份 analyze JSON 报告复制到快照目录。

规则：

- `<report.json>` 必须是本地 JSON 文件，且 shape 必须符合 analyze 报告契约
- `--name` 是必填参数
- `--snapshot-dir` 可以覆盖默认的 `~/.binlogviz/snapshots`
- 成功保存时 `stdout` 不输出报告内容
- 成功保存时 `stderr` 输出 `Saved snapshot "<name>" to <path>`

支持的 flags：

| Flag | 默认值 | 说明 |
|------|--------|------|
| `--name` | none | 必填的快照名，会作为保存后的 snapshot 标识符。 |
| `--snapshot-dir` | home-based default | 保存快照时使用的目录。默认值：`~/.binlogviz/snapshots`。 |

### `snapshot list`

`snapshot list` 支持两种输出模式：

- text 模式（默认）：输出面向人的表格，包含 `name`、`label`、`created_at`、`input_mode`、`window` 和 `diagnostics`（标记缺失、null、`full` 或不是字符串时为 `-`；是 `""` 时为 `(empty)`；否则是模式字符串）。`""` 仍保留 label、created_at 和 window。
- JSON 模式：输出包含 `snapshot_dir` 和 `snapshots` 的机器可读对象。裁剪过的文件在该条目上带 `diagnostics_mode`，包括 `""`。没有裁剪标记时省略该键。`schema_version` 仍是 1。两种模式 list 都以 exit 0 结束。show、compare、trend 和 `snapshot save` 仍以 exit 9 拒绝 `""`。

可用参数：

- `--format text`
- `--format json`
- `--snapshot-dir /path/to/store`

### `snapshot show`

`snapshot show <name>` 支持两种输出模式：

- text 模式（默认）：把简短摘要打印到 `stdout`，包括快照名、解析后的路径、identity 元数据、过滤条件和顶层汇总
- JSON 模式：输出一个机器可读对象，并把规范化后的 snapshot descriptor 放在 `snapshot` 字段下

可用参数：

- `--format text`
- `--format json`
- `--snapshot-dir /path/to/store`

### `snapshot rename`

`snapshot rename <old-name> <new-name>` 用于重命名快照目录中的一个已存储快照。

规则：

- 两个名字都必须满足和 `snapshot save` 相同的快照命名校验
- 命令会在重命名文件的同时保持快照内部 identity 与新文件名一致
- 成功重命名时 `stderr` 输出 `Renamed snapshot "<old>" to "<new>" at <path>`

### `snapshot delete`

`snapshot delete <name>` 会从快照目录中删除一个已存储快照。

规则：

- 名字必须满足快照命名校验
- 成功删除时 `stderr` 输出 `Deleted snapshot "<name>" at <path>`

## 时间与校验行为

### 时间过滤

`--start` 和 `--end` 接受 RFC3339（跨机器推荐）、`YYYY-MM-DD HH:MM:SS`、带 `±HH:MM` 的空格形式，以及结尾的 ` UTC`。偏移量或结尾 ` UTC` 定义该时刻。没有时区的 --start 或 --end，在设置了 --tz 时按 --tz 读取，包括 --tz UTC；没有设置 --tz 时，按运行 binlogviz 的机器的本地时区读取。`--tz UTC` 和 `--tz Etc/UTC` 把这个值解析成同一个时刻。不传 `--tz` 时，报告时间戳仍为 UTC。快照文件始终是 UTC，包括 `snapshot save` 一份带 `--tz` 的报告。落在夏令时空档或重叠时段、且没有时区的值，退出码是 3，错误会写出该值、时区和原因，并要求补上显式偏移。绝对值大于 14:00 的偏移（例如 `+24:00`、`+14:30`），或分钟不是 00、30、45 的偏移，同样是退出码 3。

```bash
binlogviz analyze mysql-bin.000123 \
  --start "2026-03-15T10:00:00Z" \
  --end "2026-03-15T10:30:00Z"

binlogviz analyze mysql-bin.000123 \
  --start "2026-09-12T00:00:00+08:00" \
  --end "2026-09-14T15:00:00+08:00"

binlogviz analyze mysql-bin.000123 \
  --start "2026-09-12 00:00:00" \
  --end "2026-09-14 15:00:00"
```

校验规则：

- 非法的 `--start` 值会以 `invalid start time format` 错误失败。
- 非法的 `--end` 值会以 `invalid end time format` 错误失败。
- 如果同时提供二者，`--end` 不能早于 `--start`。

### 输入校验

分析开始前会先进行输入校验：

- 缺失文件会以 `file not found: <path>` 失败。
- 无法读取的 discovery 目录会以目录读取错误失败。
- discovery 模式下如果没有匹配项，会以 `no matching binlog files found under <dir> with prefix "<prefix>"` 失败。
- 非法输入模式组合会在开始解析前快速失败。

## 错误与非法组合行为

命令会拒绝以下非法组合：

- 位置参数文件与 `--from-dir`、`--prefix` 或 `--index-file` 同时出现
- 位置参数文件与 `--host` 同时出现
- `--host` 与 `--from-dir`、`--prefix`、`--index-file`、`--start-position` 或 `--stop-position` 同时出现
- 只提供 `--from-dir`，却没有 `--prefix` 或 `--index-file` 其中之一
- 只提供 `--prefix` 或 `--index-file`，不提供 `--from-dir`
- 提供了 `--port`、`--user`、`--password`、`--server-id`、`--start-file`、`--start-pos`、`--start-gtid` 或 `--ssl-mode`，却没有 `--host`
- `--start-gtid` 与 `--start-file` 或显式 `--start-pos` 同时出现
- `--start-pos` 没有搭配 `--start-file`（单独使用 `--start-file` 是允许的，`--start-pos` 默认 `4`）
- `--host` 的 host 部分为空（`:3306`）或 port 部分为空（`db:`）
- `--start-pos` 小于 `4`、`--port 0` 或端口超过 `65535`、空的 `--start-gtid`，或 `--ssl-mode` 不是 `disabled`/`preferred`/`required`
- 既没有位置参数文件，也没有完整的 discovery 参数对，也没有 `--host`
- 使用 `--snapshot-name` 但没有同时设置 `--format json`

代表性的失败案例：

```bash
# 非法：混用输入模式
binlogviz analyze mysql-bin.000123 --from-dir /var/lib/mysql --prefix mysql-bin.

# 非法：discovery 模式参数不完整
binlogviz analyze --from-dir /var/lib/mysql
```

## 输出通道

BinlogViz 会刻意把机器可消费的报告输出，与面向操作者的运行时状态输出分开。

### 标准输出（`stdout`）

`stdout` 专门用于最终分析报告：

- 默认输出文本报告
- 设置 `--format json` 时输出 JSON 报告
- 设置 `--format markdown`（或 `--format md`）时输出 Markdown 报告
- 设置 `--format html` 时输出 HTML 报告

这种行为便于安全地做 shell 重定向和脚本处理。

```bash
binlogviz analyze mysql-bin.000123 --format json > report.json
binlogviz analyze mysql-bin.000123 --format markdown > report.md
binlogviz analyze mysql-bin.000123 --format html > report.html
```

**HTML 输出说明：** 交互式 TTY 下，`analyze --format html` 不带 `--output` 时写入推导出的 cwd 文件（例如 `mysql-bin.000123.html`，多文件或 `--from-dir` 时为 `binlogviz-report.html`）。stdout 为管道或重定向时，HTML 文档写入 stdout，因此 `analyze --format html > report.html` 会得到完整报告。`--output <path>` 写入该路径；`--output -` 强制 stdout。`compare` 和 `trend` 的 HTML 本来就写 stdout。

### 标准错误（`stderr`）

`stderr` 用于面向操作者的运行时状态信息：

- 解析进度输出
- `Finalizing analysis...`
- discovery 模式下的已解析文件列表
- 使用 `--snapshot-name` 时的快照保存确认
- 命令错误

这样可以保持 `stdout` 在管道和重定向场景下的纯净。

关于精确的输出通道契约和 JSON 字段级行为，请参见[输出格式参考](output-format.zh-CN.md)。

## `compare` 输出通道

`compare` 同样把最终报告写到 `stdout`，但不会像 `analyze` 那样输出解析进度：

- `stdout` 承载最终 compare 报告
- `stderr` 承载命令错误

`compare` 按格式输出：

- `text`：适合终端阅读的差异摘要
- `json`：机器可读的 compare 结果
- `html`：自包含的可视化 compare 报告

示例：

```bash
binlogviz compare current.json baseline.json > compare.txt
binlogviz compare current.json baseline.json --format json > compare.json
binlogviz compare current.json baseline.json --format html > compare.html
```

关于 compare 输出结构和用户可见内容，请参见[输出格式参考](output-format.zh-CN.md)。

## `snapshot` 输出通道

`snapshot` 子命令的输出通道约定如下：

- `snapshot save`：`stdout` 不输出报告；保存确认写到 `stderr`
- `snapshot list`：快照名写到 `stdout`
- `snapshot show`：快照元数据和摘要写到 `stdout`
- 命令失败：沿用 CLI 的 `stderr` 错误路径

## 示例

### 分析单个文件

```bash
binlogviz analyze mysql-bin.000123
```

### 分析多个文件

```bash
binlogviz analyze mysql-bin.000123 mysql-bin.000124
```

### 使用 discovery 模式分析

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin.
```

### 输出 JSON 供下游处理

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. --format json > analyze.json
```

### 调整报告大小与告警阈值

```bash
binlogviz analyze mysql-bin.000123 mysql-bin.000124 \
  --top-tables 20 \
  --top-transactions 20 \
  --top-minutes 30 \
  --detect-spikes \
  --spike-window 10 \
  --spike-factor 5.0 \
  --large-trx-rows 5000 \
  --large-trx-duration 60s
```

### 按 schema 或表过滤

```bash
# 仅分析指定 schema
binlogviz analyze mysql-bin.000123 --include-schema mydb

# 排除系统 schema
binlogviz analyze mysql-bin.000123 --exclude-schema mysql,sys,information_schema

# 仅分析指定表
binlogviz analyze mysql-bin.000123 \
  --include-schema mydb \
  --include-table orders,payments
```

### 对比两份 JSON 报告

```bash
binlogviz compare current.json baseline.json
```

### 输出 compare JSON 供下游处理

```bash
binlogviz compare current.json baseline.json --format json > compare.json
```

### 输出可视化 compare 报告

```bash
binlogviz compare current.json baseline.json --format html > compare.html
```

## `workflow run` 命令语法

```bash
binlogviz workflow run <plan.yaml>
binlogviz workflow run <plan.yaml> --output-dir ./artifacts
binlogviz workflow run <plan.yaml> --snapshot-dir /tmp/snapshots
```

`workflow run` 执行一份声明式 YAML plan，描述一个或多个分析窗口、可选的 compare 作业和可选的 trend 作业。它会产生一个确定性的 artifact 目录树以及一份 `manifest.json`。Manifest 会始终包含一个规范化后的 `workflow_summary` 对象，其中带有 `findings`、`recommendations` 和 `warnings` 三个数组。这份 summary 只会基于成功 compare/trend 步骤的 JSON artifact 以 best-effort 方式重建，因此 summary warnings 不会改变 workflow 或步骤的状态语义。

### Plan 格式

Plan 文件使用 YAML 格式，`version: 1`。根级段落包括：

- `version` — 必填，必须为 `1`
- `workflow` — workflow 名称和输出目录
- `defaults` — 共享输入源、分析选项和快照设置
- `windows` — 一个或多个命名时间窗口
- `compare` — 可选 compare 作业，引用命名窗口
- `trend` — 可选 trend 作业，引用命名窗口

仓库根目录的 `incident.yaml` 指向 `cmd/binlogviz/testdata/sample-binlog`，因此 `binlogviz workflow run incident.yaml` 不需要本机 MySQL datadir。GitHub Release 归档会带上一份 `from_dir: testdata/sample-binlog` 的 `incident.yaml`，解压后可以跑同一条命令。

示例 plan：

```yaml
version: 1
workflow:
  name: incident-investigation
  output_dir: ./artifacts/incident
defaults:
  input:
    from_dir: /var/lib/mysql
    prefix: mysql-bin.
  analyze:
    format: json
    top_tables: 10
  snapshot:
    save: true
windows:
  - name: baseline
    start: 2026-04-09T10:00:00Z
    end: 2026-04-09T10:30:00Z
  - name: incident
    start: 2026-04-09T11:00:00Z
    end: 2026-04-09T11:30:00Z
compare:
  - name: incident_vs_baseline
    current: incident
    baseline: baseline
    formats: [json, html]
trend:
  - name: incident_series
    snapshots: [baseline, incident]
    formats: [json, html]
```

### 参数

| Flag | 默认值 | 说明 |
|------|--------|------|
| `--output-dir` | plan 中定义 | 覆盖 plan 中定义的输出目录。 |
| `--snapshot-dir` | home-based default | 覆盖快照存储目录。 |

### 输出目录布局

```
<output_dir>/
  index.html
  manifest.json
  analyze/
    baseline.json
    incident.json
  compare/
    incident_vs_baseline.json
    incident_vs_baseline.html
  trend/
    incident_series.json
    incident_series.html
```

### 执行顺序

1. 验证并加载 plan
2. 创建输出目录结构
3. 按 plan 顺序运行所有 analyze 窗口
4. 按 plan 顺序运行 compare 作业
5. 按 plan 顺序运行 trend 作业
6. 写入 `manifest.json`
7. 写入 `index.html`

### `workflow_summary` 重建行为

`workflow run` 会把一份紧凑的 workflow 级汇总持久化到 `manifest.json`：

- `workflow_summary.findings`、`workflow_summary.recommendations` 和 `workflow_summary.warnings` 始终以规范化数组形式出现
- 只有成功的 `compare` 和 `trend` 步骤才会贡献 summary 项
- summary 提取只读取 JSON artifact
- findings 和 recommendations 会按确定性规则去重，并且各自最多保留 5 条
- 缺少必需顶层数组时会追加 warning；字段存在但为空数组属于有效低信号输入，不会产生 warning
- `index.html` 会优先为 workflow summary 项链接到 HTML 源报告；如果没有 HTML artifact，则回退到 JSON
- workflow summary 的 evidence link 只有在 HTML 源报告上才会追加 `#anchor`；JSON 回退链接不会带锚点
- summary 重建是 best-effort 的：缺失、不可读或无效的 summary 来源只会追加 warning 字符串，不会让 workflow 失败
- summary warnings 永远不会改变 workflow 或步骤的状态语义

### 错误处理

- Plan 验证错误在任何 artifact 写入之前就会失败
- 运行时步骤失败会在第一个失败步骤处停止
- 已写入的 artifact 保留在磁盘上
- `manifest.json` 总会被写入，`status: failed` 并记录失败步骤的错误信息
- `index.html` 在成功和失败时都会写入

### 输出通道

- v1 中 `stdout` 不使用
- `stderr` 承载进度行和最终的 manifest 路径
- `index.html` 写入 `<output_dir>/index.html`，是自包含的 workflow 落地页，展示 workflow 元数据、步骤状态和 artifact 链接

## `workflow status` 命令语法

```bash
binlogviz workflow status <output_dir>
binlogviz workflow status <output_dir> --format text
binlogviz workflow status <output_dir> --format json
```

`workflow status` 以只读方式检查一个已有的 workflow 输出目录。它会读取 `manifest.json`，检查 manifest 中记录的每个 artifact 当前是否存在，直接带出已持久化的 `workflow_summary`，判断该 workflow root 是否可 resume，并在保存的 plan 可以成功加载时给出 dry-run resume preview。

该命令严格只读：

- 不会执行任何 workflow 步骤
- 不会重写 `manifest.json`、`index.html` 或任何 artifact
- 不会修复缺失的 artifact 或 snapshot
- 不会修改磁盘上的运行时状态

### 参数

| Flag | 默认值 | 说明 |
|------|--------|------|
| `--format` | `text` | 状态输出格式：`text` 或 `json`。 |

### 运行时检查行为

`workflow status` 会报告以下顶层运行时事实：

- workflow 名称、输出根目录、manifest 版本、mode、attempt 和 manifest status
- `runtime_state`：仅当 manifest 有 `resolved_input_files`、至少有一个成功 step、所有已记录 artifact 都存在，且在保存的 plan 可以成功加载时 resume 所需的可复用 snapshot 也完整时为 `complete`；否则为 `incomplete`。输入发现失败、steps 为空时是 `incomplete`，不是 `complete`。
- `resumable`：只有在该 workflow root 通过 resume 校验时才为 `true`
- `resume_error`：当 legacy manifest、plan 文件缺失、plan 哈希不匹配、plan 加载失败或其他 resume guard 失败时，用于解释为什么不能 resume
- 已持久化的 `workflow_summary`，其中 `findings`、`recommendations` 和 `warnings` 会以规范化数组形式直接从 manifest 透传，不会在 status 中重建
- 每个步骤的 artifact presence，使用 manifest 中记录的 artifact 路径
- `resume_preview`：当保存的 plan 成功加载且可以推导出 dry resume 计划时出现

Legacy manifest 仍然可被检查。对于 pre-v2 格式的 manifest，该命令仍会输出状态，但会报告 `resumable: false`，并提供非空的 `resume_error`。

### 信任边界行为

`workflow status` 仅信任 workflow 本地内生根（rooted）的 plan 引用。当 `manifest.plan_path` 解析到 workflow root 之外或通过符号链接逃逸时，plan 会被视为不可信：命令仍然成功并报告完整状态，但会将 `resumable` 设为 `false`，并在 `resume_error` 中填入信任边界说明。位于 root 之外和符号链接逃逸的 plan 路径会在文件打开前被拒绝。信任检查由 `ValidateWorkflowPlanPath(outputDir, planPath)` 执行。

### 输出行为

- 只支持 `text` 和 `json`
- 当 `<output_dir>/manifest.json` 无法读取时，会在渲染前直接失败
- manifest 记录的 compare 或 trend JSON 产物带有本构建不认识的 `schema_version` 时，以退出码 9 退出（`unsupported`，reason 为 `unsupported_artifact_version`），stdout 为空（ADR 0010）
- 所有输出都写到 `stdout`
- 不使用 `stderr` 输出进度
- 当 plan 不可用或无法加载时，省略 `resume_preview`

## `workflow clean` 命令语法

```bash
binlogviz workflow clean <output_dir>
binlogviz workflow clean <output_dir> --format text
binlogviz workflow clean <output_dir> --format json
binlogviz workflow clean <output_dir> --apply
binlogviz workflow clean <output_dir> --apply --include-snapshots
```

`workflow clean` 用于检查一个已有 workflow root，并报告或删除当前 `manifest.json` 已不再引用的孤儿生成文件。

### 参数

| Flag | 默认值 | 说明 |
|------|--------|------|
| `--apply` | `false` | 实际删除已发现的清理候选，而不只是预览。 |
| `--include-snapshots` | `false` | 把 `manifest.snapshot_dir` 下的孤儿 snapshot JSON 一并纳入清理。 |
| `--format` | `text` | 清理结果输出格式：`text` 或 `json`。 |

### 清理范围与安全保证

`workflow clean` 的范围被刻意限制得很窄：

- 只扫描 workflow 生成目录：`analyze/`、`compare/`、`trend/`
- 只处理这些目录下已知的生成文件扩展名
- 以当前 manifest 中的 `steps[].artifacts` 作为 live artifact 集合
- 以成功 analyze 步骤的 `snapshot_name` 作为 live snapshot 集合
- 绝不会删除 `manifest.json`
- 绝不会删除 `index.html`
- 绝不会删除 plan 文件
- 绝不会删除确定性 workflow artifact 集合之外的未知文件

当前纳入范围的生成扩展名：

- `analyze`: `.json`
- `compare`: `.json`, `.html`
- `trend`: `.json`, `.html`

### 错误行为

只有在无法有意义地评估 cleanup 时，`workflow clean` 才会在渲染前失败：

- `<output_dir>/manifest.json` 缺失
- manifest 不可读或非法
- 某个 workflow artifact 目录不可读
- manifest 记录的 compare 或 trend JSON 产物带有本构建不认识的 `schema_version`：退出码 9（`unsupported`，reason 为 `unsupported_artifact_version`），不删除任何文件，避免旧版 binlogviz 替新版的运行判断哪些文件属于它（ADR 0010）

补充规则：

- snapshot 目录缺失不是错误，只会得到零个 snapshot 候选
- `--apply` 模式下，单个文件删除失败不会中断整个清理过程
- 删除失败的路径会记录到 `skipped`
- 只要出现 skipped 删除，命令会在写出结果后以非零状态退出

### 输出行为

- `text` 模式输出摘要块，以及 orphan、deleted、skipped 列表
- `json` 模式输出稳定的机器可读对象，包含 `workflow_name`、`output_dir`、`mode`、`include_snapshots`、`artifact_orphans`、`snapshot_orphans`、`deleted`、`skipped` 和 `counts`
- 输出写到 `stdout`
- 命令错误继续沿用 CLI 默认失败链路

### 非目标

`workflow clean` 不会：

- 修复 workflow 状态
- 重写 manifest 内容
- 决定 `resume` 下一步应执行什么
- 对单个 workflow root 之外做全局清理
- 实现 retention、TTL 或基于时间的 pruning

## `workflow export` 命令语法

```bash
binlogviz workflow export <output_dir>
binlogviz workflow export <output_dir> --output ./incident.zip
binlogviz workflow export <output_dir> -o ./incident.zip
binlogviz workflow export <output_dir> --include-snapshots
binlogviz workflow export <output_dir> --format json
```

`workflow export` 会把一个已有 workflow root 打包成确定性、只读的 zip archive。它会读取 `manifest.json`，始终包含 `manifest.json` 本身，以 best-effort 方式包含 `index.html`，只包含 manifest 中声明的 workflow artifacts，并在 `manifest.plan_path` 存在时以 best-effort 方式把 plan 文件写入归档中的 `plan.yaml`。它不会重跑 workflow 步骤、不会重建 workflow summary，也不会修改 workflow root 下的任何文件。

### 参数

| Flag | 默认值 | 说明 |
|------|--------|------|
| `--output`, `-o` | `<output_dir>.zip` | 输出 archive 路径。默认值来自 `filepath.Clean(output_dir) + ".zip"`。 |
| `--include-snapshots` | `false` | 仅把 manifest 引用到的 snapshot JSON 文件纳入归档。 |
| `--format` | `text` | export 结果输出格式：`text` 或 `json`。 |

### 导出规则与安全保证

- `manifest.json` 是必需输入，并且始终被纳入归档
- `index.html` 以 best-effort 方式纳入；缺失时只会生成 warning
- workflow artifacts 只会从 manifest 的 `steps[].artifacts` 读取
- 位于 workflow root 之外的 artifact 会被跳过并记录 warning
- 只有当 `manifest.plan_path` 存在、解析后仍位于 workflow root 内、可读、内容仍然匹配 `manifest.plan_sha256`，并且仍然能解析为与 manifest 记录元数据一致的 workflow plan 时，plan 文件才会以 `plan.yaml` 纳入归档；否则只会生成 warning
- snapshots 默认不纳入归档
- 使用 `--include-snapshots` 时，只会考虑被 manifest 引用到的 snapshot JSON 文件；如果 `manifest.snapshot_dir` 为空，则只记录 warning，不会回退去读取当前工作目录
- 缺失的 manifest artifact、snapshot、plan 或 `index.html` 会转成 warning，而不是 fatal error
- archive 输出路径必须位于 workflow root 之外；位于 root 内部的路径会被拒绝
- zip entry 的顺序、时间戳和文件 mode 都会被规范化，以保证重复导出结果确定性一致

### 失败行为

出现以下情况时，`workflow export` 会在成功写出结果前直接失败：

- `<output_dir>/manifest.json` 缺失、不可读或非法
- 某个需要纳入的 artifact 因可选 best-effort 缺失以外的原因无法读取
- archive 创建或写入失败
- archive 输出路径解析到 workflow root 内部

### 输出行为

- 所有结果输出都写到 `stdout`
- 命令不会用 `stderr` 输出进度
- `text` 模式输出紧凑的运维摘要，并在需要时追加 `Warnings` 分区
- `json` 模式输出机器可读对象，包含 `workflow_name`、`output_dir`、`archive_path`、`format`、`included_files`、`included_snapshots` 和 `warnings`

## `workflow validate` 命令语法

```bash
binlogviz workflow validate <plan.yaml>
binlogviz workflow validate <plan.yaml> --format text
binlogviz workflow validate <plan.yaml> --format json
```

`workflow validate` 用于回答一份 workflow plan 在静态层面是否可运行。它只读取 `plan.yaml`，使用严格 YAML 字段校验加载 plan，并复用 `workflow run` 在真正执行前使用的静态校验逻辑。

校验覆盖 workflow 元数据、window 定义、命名引用、重复 compare / trend 作业名，以及 compare / trend 作业内重复的 format 条目。该命令不会检查 `output_dir`、`manifest.json`、`index.html` 或任何已有运行产物。

如果 plan 结构合法，但 `defaults.input.from_dir` 看起来像占位符（例如 `PLACEHOLDER/binlog`）或不存在，命令仍以零状态退出，并在文本 / JSON 输出中给出 `warnings`。这些 warning 不会把 plan 判为 invalid。

### 参数

| Flag | 默认值 | 说明 |
|------|--------|------|
| `--format` | `text` | 校验结果输出格式：`text` 或 `json`。 |

### 成功契约

- plan 合法时以零状态退出
- 向 `stdout` 写出文本或 JSON 摘要
- 输出 workflow 名称、window 数量、compare 作业数量、trend 作业数量、output root，以及任何 `from_dir` warning

### 失败契约

- plan 非法或不可读取时以非零状态退出
- 向 `stdout` 写出文本或 JSON 错误载荷
- 同时沿用 CLI 正常失败链路返回命令错误，因此默认 CLI 执行下还可能在 `stderr` 再输出一行错误信息

## `workflow describe` 命令语法

```bash
binlogviz workflow describe <plan.yaml>
binlogviz workflow describe <plan.yaml> --format text
binlogviz workflow describe <plan.yaml> --format json
```

`workflow describe` 用于回答一份 workflow plan 会如何运行，但不会真正执行。它只读取 `plan.yaml`，要求 plan 先通过静态校验，然后基于 plan 本身渲染确定性的执行预览。

预览内容包括 workflow 元数据、analyze windows、compare 作业、trend 作业、声明的依赖关系、计划中的 artifact 路径，以及在 `defaults.snapshot.save` 开启时暴露的 snapshot 名称。该命令不会检查 `output_dir`、`manifest.json`、`index.html` 或任何已有输出。

### 参数

| Flag | 默认值 | 说明 |
|------|--------|------|
| `--format` | `text` | 描述输出格式：`text` 或 `json`。 |

### 输出行为

- 只支持 `text` 和 `json`
- 不渲染 HTML
- plan 非法或不可读取时，在渲染前直接失败
- 失败时会先把错误载荷写到 `stdout`，同时继续返回命令错误，因此默认 CLI 执行下还可能在 `stderr` 再输出一行错误信息
- 保持 analyze windows、compare 作业和 trend 作业在 plan 中声明的顺序

## `workflow resume` 命令语法

```bash
binlogviz workflow resume <output_dir>
binlogviz workflow resume <output_dir> --snapshot-dir /tmp/snapshots
binlogviz workflow resume <output_dir> --rerun analyze:week2 --rerun compare:incident_vs_baseline
```

`workflow resume` 从已有的输出目录继续执行之前运行过的 workflow。它会读取已有的 `manifest.json`，复用成功步骤，并重跑失败或缺失的步骤。如果所有步骤都已成功、产物完整，且没有指定 `--rerun`，命令以 0 退出，并在 `stderr` 打一行 `nothing to resume`。需要强制重跑时加 `--rerun`。

### 参数

| Flag | 默认值 | 说明 |
|------|--------|------|
| `--snapshot-dir` | home-based default | 覆盖快照存储目录。 |
| `--rerun` | none | 可重复指定的显式步骤选择器。强制重新执行指定步骤，无论其之前的状态如何。 |

### 选择器语法

`--rerun` 参数接受 `<kind>:<name>` 格式的步骤选择器：

| Kind | Name 匹配目标 | 示例 |
|------|--------------|------|
| `analyze` | plan 中的窗口名 | `analyze:week2` |
| `compare` | plan 中的 compare 作业名 | `compare:incident_vs_baseline` |
| `trend` | plan 中的 trend 作业名 | `trend:incident_series` |

可以组合多个 `--rerun` 参数，在一次调用中强制重跑多个步骤。

### Resume 行为

1. 从 `<output_dir>` 加载已有的 `manifest.json`
2. 校验 manifest 版本（必须为 v2；旧版 pre-v2 manifest 会被拒绝）
3. 校验 plan 文件哈希是否与原始运行一致（plan 文件变更则拒绝执行）
4. 对每个 plan 步骤：
   - 如果步骤已成功且不在 `--rerun` 列表中，标记为 reused
   - 如果步骤失败、缺失或在 `--rerun` 列表中，重新执行
5. 依赖感知重跑：重跑 `analyze` 步骤会使引用它的下游 `compare` 和 `trend` 步骤失效
6. 写入更新后的 `manifest.json`，包含每个步骤的执行状态（`executed` 或 `reused`）
7. 写入更新后的 `index.html`，展示 mode、attempt 编号和每个步骤的执行标签

### 前置校验条件

Resume 会在以下情况拒绝执行：

- `<output_dir>` 中不存在 `manifest.json`
- manifest 是旧版 pre-v2 产物（缺少 `manifest_version` 字段）
- plan 文件 SHA-256 与 manifest 中记录的 `plan_sha256` 不匹配
- manifest 中记录的 plan 路径解析到 workflow root 之外或通过符号链接逃逸（信任边界硬拒绝）
- manifest 记录的 compare 或 trend JSON 产物带有本构建不认识的 `schema_version`：退出码 9（`unsupported`，reason 为 `unsupported_artifact_version`），不执行也不改写任何文件（ADR 0010）

`ValidateWorkflowPlanPath(outputDir, planPath)` 会在 plan 文件打开前被调用。位于 root 之外和符号链接逃逸的路径会被无条件拒绝。`ValidateResumableManifest` 现在接受四个参数 `(m Manifest, outputDir string, planPath string, planSHA256 string)`，以在 resume 校验期间强制执行信任边界。

### 输出目录布局

输出目录布局与 `workflow run` 完全一致。Resume 会覆盖重跑步骤的 artifact，保留复用步骤的 artifact 不变。

### 输出通道

- `stdout` 不使用
- `stderr` 承载进度行和最终的 manifest 路径
- `index.html` 写入 `<output_dir>/index.html`，包含 resume mode、attempt 编号，以及每个步骤的执行标签（`executed` / `reused`）
