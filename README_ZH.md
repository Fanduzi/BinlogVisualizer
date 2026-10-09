<div align="center">

# BinlogViz

[![Release](https://img.shields.io/github/v/release/Fanduzi/BinlogVisualizer?display_name=tag)](https://github.com/Fanduzi/BinlogVisualizer/releases)
![Platform](https://img.shields.io/badge/platform-darwin%20amd64%20%7C%20darwin%20arm64%20%7C%20linux%20amd64%20%7C%20linux%20arm64-blue)
![Go Version](https://img.shields.io/badge/go-1.26.1-00ADD8?logo=go)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](#license)

[![English](https://img.shields.io/badge/docs-English-blue)](README.md) [![简体中文](https://img.shields.io/badge/docs-简体中文-yellow)](README_ZH.md)

[![变更记录](https://img.shields.io/badge/变更记录-informational)](CHANGELOG.md) [![安全策略](https://img.shields.io/badge/安全策略-important)](SECURITY.md) [![发行说明](https://img.shields.io/badge/发行说明-success)](docs/releases/)
</div>

BinlogViz 是一个面向 DBA 和运维人员的本地 MySQL `ROW` binlog 分析 CLI。它专门用于回答真实运维问题：哪些表写入最重、哪些事务异常大、尖峰发生在哪些分钟、某个故障窗口内的负载究竟发生了什么。

## 截图

### Analyze HTML 报告

![Analyze HTML 报告](docs/images/analyze-html.png)

### Compare HTML 报告

![Compare HTML 报告](docs/images/compare-html.png)

## 从这里开始

BinlogViz 做的是 **ROW binlog 的快速摘要**：热表、写入形态、上线前后 compare。510 MB 级文件通常只要数秒。

它**不是** STATEMENT / MIXED 全量分析器——这类文件会空成功或只统计到 ROW 子集。报告里的位置是文件证据；只有当跨度覆盖事务事件、而不是只有 XID 区间时，才把它当作 `mysqlbinlog --start-position`。

### 用样例 ROW binlog 验证安装

```bash
curl -fsSLO https://raw.githubusercontent.com/Fanduzi/BinlogVisualizer/main/cmd/binlogviz/testdata/minimal.binlog
binlogviz analyze minimal.binlog
```

同一份 1500 字节样本在仓库的 `cmd/binlogviz/testdata/minimal.binlog`。GitHub Release 的每个 tar.gz 也会带上这份文件（`testdata/minimal.binlog`）、discovery 布局副本 `testdata/sample-binlog/mysql-bin.000001`，以及 `from_dir` 指向该目录的 `incident.yaml`。解压后执行 `./binlogviz analyze testdata/minimal.binlog` 和 `./binlogviz workflow run incident.yaml`，不需要再 clone 仓库。

### 检查你自己的文件

```bash
binlogviz analyze mysql-bin.000123
cat mysql-bin.000123 | binlogviz analyze -
```

`analyze -` 从管道读取一份二进制 binlog。解析需要可 seek 的文件，所以会把 stdin 复制到临时文件，命令结束时删除，包括 SIGHUP（退出码 129）、Ctrl-C（退出码 130）、SIGQUIT（退出码 131）和 SIGTERM（退出码 143）。终端会在解析前失败。`/dev/null` 和空管道报 `stdin 没有数据`。这种输入的回放提示会说明来自 stdin，不会编造文件路径。`mysqlbinlog` 的文本输出不是 binlog。

默认文本报告包含热点线程，有行变更时按行数排序（否则按事件数、字节或事务数）。binlog 里有的 `thread_id`、`server_id`、`user@host` 和 schema 会写出来，回答「谁写最多」不必再 `jq`。`--top` 限制这一节；`--top-threads 0` 保留全部会话。JSON 的同一排名在 `threads`。

同一份报告还有热点行：被 UPDATE 和 DELETE 行镜像碰到次数最多的主键。每一条给出次数、碰到它的事务数、第一次和最后一次的事件时间，以及最早和最晚那个事务的 GTID 和 file:byte，可以直接拿去跑 `mysqlbinlog` 或 BinlogServer。主键只来自 MySQL 8 `binlog_row_metadata=FULL`。binlog 没有记下主键时，报告会说明这张表无法追踪热点行，不会拿第 1 列 `@1` 来猜。没有主键的表不进这个排名。`--top` 限制这一节；`--top-rows` 覆盖它（`0` 保留已追踪的全部键）。`--sql-context off` 隐藏键值，次数仍在。JSON 字段是 `hot_rows`。追踪最多保留 8192 个主键。超出后，新键替换被碰到次数最少的键；报告会说明达到了上限，继承了被丢掉键的计数的行标成近似。一直留在表里的键计数是精确的。

`analyze` 在计入至少 1 个事件时退出 **0**；无法分析（损坏、截断、没有 Format Description）时退出 **1**；完整 binlog 解析成功但计入 0 个事件（空的 `--start`/`--end` 窗口，或仅 Format Description / rotate）时退出 **2**。schema 或 table 过滤没有匹配到事件同样是 exit 2，`Error:` 会写明过滤没有匹配。`--dml` 没有匹配到事件也是 exit 2，`Error: dml filter matched no events`。过滤匹配到 view、event、function、procedure 或 trigger 时退出 0，即使没有行变更也会打印这条 DDL。exit 2 不写 `stdout`，只在 `stderr` 打一行 `Error:`。如果进度条还停在当前 stderr 行，打印 `Error:` 之前会先清掉那一行。

### 按 binlog 顺序分析整个目录

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin.
```

### 聚焦某个故障时间窗口

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. \
  --start "2026-03-15T10:00:00Z" \
  --end "2026-03-15T10:30:00Z"
```

`--start`/`--end` 也接受运行 `binlogviz` 的机器本地时区下的 `YYYY-MM-DD HH:MM:SS`。跨机器请优先使用带显式偏移的 RFC3339。

### 从 `SHOW MASTER STATUS` 位点或 GTID 起

位点是单个显式文件上的精确事件边界，使用半开区间 `[start, stop)`。也可以同时给时间条件；各谓词取交集。

```bash
binlogviz analyze mysql-bin.000015 --start-position 1651 --stop-position 4096
binlogviz analyze mysql-bin.000015 --include-gtids '24bc7850-2c16-11e6-a073-0242ac110002:7-12'
binlogviz analyze mariadb-bin.000015 --include-gtids '0-7-1857,0-7-1859' --exclude-gtids '0-7-1859'
```

### 只看某个 schema 或表

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. \
  --include-schema orders \
  --include-table payments
```

`--include-table` / `--exclude-table` 接受 `TABLE` 或 `SCHEMA.TABLE`，view、event、function、procedure、trigger 也是同样的写法。`CREATE TRIGGER` 和 `DROP TRIGGER` 都用触发器名字，过滤时传这个名字。

### 找到误删并看到被删的行

```bash
binlogviz analyze mysql-bin.000123 \
  --include-table shop.orders \
  --dml delete \
  --start "2026-10-06 14:00:00" \
  --end "2026-10-06 14:10:00" \
  --show-rows
```

`--dml` 接受 `insert`、`update`、`delete`，用逗号组合。它和 `--include-table` / `--exclude-table`、`--include-schema`、`--start` / `--end`、位点、GTID 过滤一起生效。Summary、Top Tables、Top Transactions、Top Threads 和告警只统计保留下来的类型，报告里会写明这个过滤。类型过滤没有匹配时退出 2，`Error: dml filter matched no events`。

`--show-rows` 默认关闭。打开后，列出的每个事务会打印 DELETE 的前镜像、UPDATE 里发生变化的列（`before -> after`），以及 INSERT 的后镜像。MySQL 8 且 `binlog_row_metadata=FULL` 时显示列名；否则列是 `@1`..`@N`，报告会说明为什么没有列名。值有上限（每个事务 32 行；字符串和二进制值最多 64 字节；整数、小数和 `BIT` 完整打印），截断会标明，并给出省略的行数。`--sql-context off` 会一并省略这些值，并在报告里说明。事务上的 `mysqlbinlog_cmd` 仍然在，用来对照。报告本身不打印撤销 SQL；同样的选择条件交给 `binlogviz flashback` 才会打印。`TIMESTAMP` 列是存储时刻的 UTC 墙钟（含小数秒），不跟随本机时区。`DATETIME` 仍是 binlog 里写下的墙钟。

### 找到误操作的 DROP

```bash
binlogviz analyze mysql-bin.000123
```

DDL 时间线列出 `DROP TABLE`、`TRUNCATE` 和 `ALTER`，并带上该事务的 GTID，以及事务起点的文件字节。复制 `BinlogServer stop_gtid=<gtid>` 或 `mysqlbinlog --stop-position=<N> <file>`。这个停止点回放更早的事件，并且不包含这条 DDL。binlog 里没有 GTID 时，中文界面写「GTID 不可用」，英文界面写 `GTID unavailable`，位置提示仍然在。时间线不生成撤销 SQL。选定范围内有 DDL 时，`binlogviz flashback` 会拒绝。

### 撤销误改的行

先用 `analyze`、DDL 时间线或 `--show-rows` 找到那个事务，再打印只撤销这些行变更的 SQL。先审脚本，再自己执行。flashback 不连接数据库。

```bash
binlogviz analyze mysql-bin.000123 \
  --include-table shop.orders \
  --dml delete \
  --show-rows

binlogviz flashback mysql-bin.000123 \
  --include-table shop.orders \
  --dml delete \
  > flashback.sql

mysql --default-character-set=utf8mb4 < flashback.sql
```

`--dml`、`--include-table`、`--include-schema`、`--start` / `--end`、`--start-position` / `--stop-position`、`--include-gtids` / `--exclude-gtids` 与 `analyze` 是同一套选择条件。`--schema-file` 不是过滤器：它提供表定义，flashback 仍然离线。DELETE 变成前镜像的 `INSERT`。INSERT 变成 `DELETE`。UPDATE 把行设回前镜像，`WHERE` 用后镜像的主键，这样改过主键的行仍能对上当前行。语句按 binlog 逆序：最后一个事务在前，事务内最后一行在前。每个原事务包成一个 `START TRANSACTION` / `COMMIT`。注释里有原 GTID（binlog 没有 GTID 时写 `GTID unavailable`）和 `file:start-position`，即文件名和该事务起点字节，用来和 `mysqlbinlog` 对照。

binlog 必须是 `binlog_row_metadata=FULL` 且 `binlog_row_image=FULL`。脚本会设置 `utf8mb4`、`time_zone='+00:00'`（`TIMESTAMP` 字面量是 UTC 墙钟），并在该会话去掉 `NO_BACKSLASH_ESCAPES`。没有主键的表按每一列匹配并加 `LIMIT 1`，语句上方的注释会说明。被删行的 `INSERT` 不用 `LIMIT 1`。JSON 按二进制文档重建，包括 `binlog_transaction_compression=ON` 记下的事务。非 `utf8mb4` 字符串是字符集引导符加十六进制字节。`ENUM` 写成成员序号，`SET` 写成位掩码，因此 `latin1` 或 `gbk` 的值在严格和非严格 `sql_mode` 下都精确。`ENUM` 序号 0 是非严格插入留下的错误成员，严格模式会报 `ERROR 1265`。脚本先保存 `@@SESSION.sql_mode`，只在这一条语句去掉 `STRICT_TRANS_TABLES`、`STRICT_ALL_TABLES` 和 `TRADITIONAL`，然后把保存的模式设回去。`TRADITIONAL` 也要去掉，因为 MySQL 会把它展开回那两个严格模式。解析到的 `CREATE` 或 `ALTER`，或者 `--schema-file`（`mysqldump --no-data`，或 `SHOW CREATE TABLE`）点名的生成列，只有定义和 binlog 对得上时才不写入 `INSERT` 和 `UPDATE`。MySQL 8 的行元数据不标记生成列，`FULL` 镜像同时包含虚拟列和存储列的值，因此会拿记下的值和表达式核对。schema 文件必须是事故当时的表结构。flashback 核对列数、列名、顺序，以及 binlog 里有的类型，不一致就不打印 SQL。生成列在每一行的记录值都等于表达式时省略。能核对的是整数和 `DECIMAL` 的 `+`、`-`、`*`、`/`、`DIV`、`%`、`MOD`，以及 `UPPER`、`LOWER`、`CONCAT`、`CONCAT_WS`、`LENGTH`、`CHAR_LENGTH` 和简单的 JSON 提取。`/` 赋给整数时远离零的一半进位，所以 `5 / 2` 是 `3`，`-5 / 2` 是 `-3`。`/` 赋给 `DECIMAL(p,s)` 时按 MySQL 默认的除法宽度（整数操作数保留 9 位小数），再按列的标度远离零的一半进位，所以 `DECIMAL(40,4)` 里的 `5 / 2` 是 `2.5000`，`DECIMAL(40,9)` 里的 `1 / 7` 是 `0.142857142`。记下的值与表达式矛盾时退出 1，不打印 SQL，包括 `div_precision_increment = 0` 时写下的商。结果放不进列类型、且记下的值是非严格模式的截断端点时，省略该列并给出警告。表达式无法核对时省略该列，stderr 给出警告，三条 `SET` 之后的守卫会在目标上这一列不是生成列时让执行失败。遇错继续的客户端会被改成只读会话，后面的写入都会失败。记下的值恰好等于能核对的表达式时，要等这个守卫跑起来才分得开真实列和生成列。没有主键的 `WHERE` 仍比较生成列。普通的 `mysqldump --no-data db` 不必带 `USE`：库名来自转储头的 `Database:`、`--schema-file-db`，或 binlog 里该表唯一的库。多份 `SHOW CREATE TABLE` 可以放在同一个文件里，中间没有 `;`。库名有歧义时只警告，不用这份定义。选中的表没有定义时，脚本仍列出每一列，stderr 警告不能排除生成列，并且执行可能在 `ERROR 3105` 停下，更早的事务已经提交。

`ON DELETE` / `ON UPDATE CASCADE` 的子表行不会被还原。撤销时触发器会执行。之后对同一主键的修改会被覆盖，没有冲突检查。表过滤或 `--dml` 只撤销事务的一部分时，stderr 打出警告。先审阅并测试脚本，再在主库的同一个会话里执行，并保持 `sql_log_bin=1`。某条语句失败时，脚本里更早的事务已经提交。已知限制见 [docs/concept/limitations.zh-CN.md](docs/concept/limitations.zh-CN.md)：schema 文件把真实列标成生成列时，若记下的值恰好等于能核对的表达式，这一列仍会被省略并以退出码 0 结束（[#166](https://github.com/Fanduzi/BinlogVisualizer/issues/166)）。矛盾的值会拒绝。仍然以退出码 0 结束的错误转储，会在任何事务之前被执行守卫拦住。遇错继续的客户端会被改成只读会话，后面的写入都会失败；断开连接，改好 schema 文件，在新会话里重新执行。事故之后如果把这一列改成了生成列，原来的值写不回去。非严格 `sql_mode` 下写入的行（零日期，或生成列 `a/b` 且 `b` 为 0）在严格会话里也能还原：只有那一条语句去掉会拒绝存下的值的那个标志（[#180](https://github.com/Fanduzi/BinlogVisualizer/issues/180)）。不传 `--schema-file` 仍会给真正的生成列赋值，执行可能停在 `ERROR 3105`。解析到的 `ALTER` 会再次应用到已经包含它的转储上，拼接的多库转储只用第一个 `Database:` 头（按库单独跑时要用这个库自己的转储），歧义表的警告只点名一张表（[#167](https://github.com/Fanduzi/BinlogVisualizer/issues/167)）。还原 `ENUM` 序号 0 时，这一条语句会去掉 `TRADITIONAL` 和严格模式，然后再把保存的 `sql_mode` 设回去，包括 `sql_mode=TRADITIONAL`（[#168](https://github.com/Fanduzi/BinlogVisualizer/issues/168)）。执行失败后把整个脚本再跑一遍，没有主键的表里会出现重复行。从失败的 `-- gtid:` 块接着跑，并保留脚本开头的 `SET NAMES`、`SET time_zone`、`SET SESSION sql_mode` 三行；删掉它们时 `TIMESTAMP` 会按会话时区偏移，退出码仍是 0。步骤写在那份文档里。

列名缺失、前镜像或后镜像不完整、某一列无法精确写成字面量（`FLOAT`、`DOUBLE`、`GEOMETRY`、`VECTOR`、不完整或无法精确表示的 JSON、未知字符集，或缺少有无符号、字符集或 ENUM/SET 成员）、表定义出现过但读不出来、`--schema-file` 和 binlog 的列不一致、生成列的值与表达式矛盾，或者选定范围内有 DDL 时，flashback 退出 1，只打一行 `Error:`，不打印任何 SQL。`--sql-context off` 同样拒绝：脚本就是行值。什么都没选中时退出 2，`Error:` 与 `analyze` 相同（`schema/table filter matched no events`、`dml filter matched no events` 或 `window matched 0 events`），stdout 为空。账号 DDL 的 `<secret>` 打码仍作用于 `analyze` 里的语句文本。flashback 不打印这些语句；选定范围内的账号 DDL 会按 DDL 拒绝。单元格的值不打码。不运行 `flashback` 时，`analyze` 的文本、Markdown、JSON 和 HTML 不变。

### 把机器可读结果交给脚本或其他工具

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. --format json > analyze.json
```

### 把两个故障窗口保存成快照后再做对比

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. \
  --start "2026-03-15T10:00:00Z" \
  --end "2026-03-15T10:30:00Z" \
  --format json \
  --snapshot-name incident_current

binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. \
  --start "2026-03-08T10:00:00Z" \
  --end "2026-03-08T10:30:00Z" \
  --format json \
  --snapshot-name incident_baseline

binlogviz snapshot list
binlogviz snapshot list --format json
binlogviz snapshot show incident_current
binlogviz snapshot show incident_current --format json
binlogviz snapshot rename incident_current incident_current_renamed
binlogviz snapshot delete incident_current_renamed
binlogviz compare \
  --current-snapshot incident_current \
  --baseline-snapshot incident_baseline \
  --format html > compare.html
```

当设置 `--snapshot-name` 时，`analyze --format json` 仍然会把 JSON 报告写到 `stdout`，同时把同一份载荷保存到 `~/.binlogviz/snapshots/<name>.json`。保存成功提示会打印到 `stderr`。

`snapshot list` 现在会输出面向人的表格，包含 `name`、`label`、`created_at`、`input_mode` 和 `window`。`snapshot list --format json` 和 `snapshot show --format json` 仍然为脚本和外部工具提供稳定的机器可读输出。`snapshot rename` 会在重命名文件的同时保持快照内部 identity 一致，`snapshot delete` 则用于删除单个快照而不影响其余历史。

`compare` 既可以加载已保存的快照，也可以继续加载两份由 `binlogviz analyze --format json` 生成的 JSON 文件。输出格式支持 `text`、`json`、`html`。其中 text 和 HTML 输出会带出 input mode、来源摘要、过滤条件和请求时间窗口。除了 summary 差异、热点表变化、操作类型分布和告警新增/移除之外，compare 现在还会通过一等结果区 `pattern_changes` 直接展示写入模式漂移。`key_findings` 部分会以最多 5 条确定性、基于证据的发现来突出变化的主要驱动因素（体量变化、模式驱动、表趋势、操作漂移、新模式），并通过 `evidence_refs` 把发现链接回支撑它的报告章节。`recommendations` 数组提供基于关键发现的保守、有据可依的后续建议。高信号跨窗口模式变化现在还会在 text、JSON 和 HTML 输出中携带有界 `pattern_drilldowns`。

compare 生成的最终报告写到 `stdout`。如果 compare 命令失败，CLI 会通过 `stderr` 输出错误。

### 把多个快照当作一条时间线查看趋势

```bash
binlogviz trend incident_week1 incident_week2 incident_week3 --format text

binlogviz trend --from-snapshots 'incident_week*' \
  --baseline-snapshot baseline_weekly \
  --format html > trend.html
```

`trend` 会加载已保存的 snapshots，按有效窗口开始时间排序，并输出 `text`、`json` 或 `html` 趋势报告。新快照优先使用 `snapshot.window.start_time`；较旧的快照则可以回退到 `summary.start_time`，不需要手工重写历史文件。除了总量和热点表变化之外，trend 现在还会输出 `pattern_trends` 用于跨多个窗口查看重复写入模式的演进，以及 `trend_summary` 部分以最多 5 条确定性发现突出上升/下降模式、表趋势、集中度偏移和体量尖峰。这些发现可以包含 `evidence_refs`，指回相关模式、表和 ordered point 证据。`recommendations` 数组提供基于发现的保守、有据可依的后续建议。高信号跨窗口模式趋势现在还会在 text、JSON 和 HTML 输出中携带有界 `pattern_drilldowns`。HTML 报告中的 `Pattern Trends` 分区默认展示 `share of rows`，也可以切换到绝对 `rows`；`text` 和 `json` 则会以终端友好和机器可读的形式暴露同一组模式序列。

### 用一份 plan 文件跑多步调查

仓库自带一份可运行的 `incident.yaml`，`from_dir` 指向 `cmd/binlogviz/testdata/sample-binlog` 里的 1500 字节 ROW 样本。在仓库根目录执行下面第一行命令，不需要本机 `/var/lib/mysql`。Release 归档里另有一份 `incident.yaml`，`from_dir` 是 `testdata/sample-binlog`，解压后也可以跑同一条命令：

```bash
binlogviz workflow run incident.yaml
tree artifacts/incident
```

同一份 plan 和样本也可以通过 raw URL 下载：

- plan：https://raw.githubusercontent.com/Fanduzi/BinlogVisualizer/main/incident.yaml
- 样本 binlog：https://raw.githubusercontent.com/Fanduzi/BinlogVisualizer/main/cmd/binlogviz/testdata/sample-binlog/mysql-bin.000001

`workflow run` 执行一份声明式 YAML plan，定义分析窗口、可选 compare 作业和可选 trend 作业。它会产生一个确定性的 artifact 目录，包含 `analyze/`、`compare/`、`trend/`、一份 `manifest.json` 和一个 `index.html` 落地页。`manifest.json` 会始终持久化一个 `workflow_summary` 对象，其中包含 `findings`、`recommendations` 和 `warnings` 三个数组。BinlogViz 只会基于成功 compare/trend 步骤产出的 JSON artifact 以 best-effort 方式重建这份 summary；如果 summary 输入缺失或不可读，只会追加 warnings，不会改变 workflow 或步骤的状态语义。只要 summary 中存在内容，`index.html` 就会渲染 `Workflow Recommendations`、`Workflow Findings` 和 `Workflow Summary Warnings` 分区，并优先链接到 HTML 源报告，必要时回退到 JSON。v1 中 `stdout` 留空，所有状态走 `stderr`。plan schema 和参数请参见 [CLI 参考](docs/reference/cli.zh-CN.md)。

如果 workflow 运行中途失败，`workflow resume` 可以从已有的输出目录继续执行：复用成功步骤，重跑失败或缺失的步骤，并支持通过 `--rerun` 选择器强制重跑指定步骤。一次已经全部成功的 run 之后再执行 `workflow resume` 会以 0 退出，并在 `stderr` 打一行 `nothing to resume`；需要强制重跑时加 `--rerun`。Resume 会在 plan 文件变更或 manifest 是旧版 pre-v2 产物时拒绝执行。Resume 还会拒绝解析到 workflow root 之外或通过符号链接逃逸的 plan 路径（信任边界加固）。`workflow status` 会报告同样的信任检查：不可信的 plan 仍会产生完整状态输出，但会将 `resumable` 设为 `false`，并在 `resume_error` 中给出信任边界说明。

在真正执行之前，`workflow validate` 会只基于 `plan.yaml` 做静态可运行性检查，`workflow describe` 则会预览该 plan 将产生的 analyze / compare / trend artifact 布局。两个命令都支持 `--format text` 和 `--format json`，且只读取 plan 文件，不会检查 `output_dir`、`manifest.json` 或 `index.html`。如果 `defaults.input.from_dir` 看起来像占位符，或不存在，`workflow validate` 仍会以 0 退出，但会给出 warning。

`workflow status` 是针对已有 workflow root 的只读运行时检查命令。它会读取 `manifest.json`，检查 artifact presence，报告 `runtime_state`、`resumable` 和 `resume_error`，在 `--format json` 下直接带出已持久化的 `workflow_summary`，并在保存的 plan 仍可加载时给出 dry `resume_preview`。文本输出只会在这些已持久化数组非空时渲染 `Workflow Recommendations`、`Workflow Findings` 和 `Workflow Summary Warnings`。它不会执行步骤、不会重建 workflow summary，也不会重写任何 workflow 输出。

`workflow clean` 则是这一生命周期中的最终 maintenance 命令。它以当前 manifest 为唯一真相源，默认 dry-run，报告 `analyze/`、`compare/`、`trend/` 下已不再被引用的孤儿生成物，并且只有显式加 `--include-snapshots` 时才会把孤儿 snapshot JSON 纳入候选。`--apply` 会执行 best-effort 删除，但仍然不会触碰 `manifest.json`、`index.html`、plan 文件以及任何超出范围的未知文件。

`workflow export` 是已完成 workflow root 的只读打包命令。它会读取 `manifest.json`，把 manifest 声明的 artifacts 打包成确定性的 zip archive，始终包含 `manifest.json`，并以 best-effort 方式包含 `index.html`；使用 `--include-snapshots` 时还可以把被引用的 snapshots 一并纳入。它不会重跑步骤，并且会拒绝位于 workflow root 内部的 archive 输出路径。

```bash
binlogviz workflow resume ./artifacts/incident
binlogviz workflow resume ./artifacts/incident --rerun analyze:week2
binlogviz workflow status ./artifacts/incident
binlogviz workflow status ./artifacts/incident --format json
binlogviz workflow clean ./artifacts/incident
binlogviz workflow clean ./artifacts/incident --apply --include-snapshots
binlogviz workflow export ./artifacts/incident
binlogviz workflow export ./artifacts/incident --include-snapshots --format json
binlogviz workflow validate incident.yaml
binlogviz workflow validate incident.yaml --format json
binlogviz workflow describe incident.yaml
binlogviz workflow describe incident.yaml --format json
```

### 生成 Markdown 或 HTML 报告

```bash
# Markdown — 粘贴到 GitHub issue、wiki 或文档
binlogviz analyze mysql-bin.000123 --format markdown > report.md

# HTML — 默认写入文件（例如 mysql-bin.000123.html）
binlogviz analyze mysql-bin.000123 --format html

# HTML — 指定输出路径
binlogviz analyze mysql-bin.000123 --format html --output report.html

# HTML — 输出到 stdout 以供管道使用（旧行为）
binlogviz analyze mysql-bin.000123 --format html --output -
```

HTML 报告包含交互式图表（每分钟行数/事务数、热点表、操作类型分布）、高信号写入模式的可选模式钻取（Pattern Drilldowns），以及五主题切换器。

## Analyze 性能门槛

面向故障排查时，单个 1 GB binlog 的 `analyze` 目标耗时是目标 DBA 环境上的 10 秒。超过 15 秒应视为性能失败，并用 `pprof` 做剖析。热点行追踪不会为文件里的每一行建一张表：最多保留 8192 个主键。超出后，新键替换被碰到次数最少的键，报告把继承来的计数标成近似。

默认 `--detail-store none` 与 `--detail-store duckdb` 生成的 JSON 等价，同时峰值 RSS 降低约 38%（在 988 MB MySQL 8.0 ROW binlog 上测量）。总耗时仍主要受 parser/流式聚合限制。

建议的人工检查命令：

```bash
time binlogviz analyze /path/to/mysql-bin.000044 --format text > /tmp/binlogviz-text.txt
time binlogviz analyze /path/to/mysql-bin.000044 --format html --output /tmp/binlogviz.html
```

文本输出应保持在快速诊断路径上；HTML 输出则构建完整的可视化证据报告。

## BinlogViz 适合回答什么问题

BinlogViz 重点服务这些 DBA 常见问题：

- **哪些表承受了最重的写入负载？**
- **哪个主键被 UPDATE 或 DELETE 的次数最多？**
- **哪些 UPDATE 或 DELETE 打到了没有主键的表？**
- **这份副本当时落后多少，是哪些事务造成的？**
- **哪些事务大到值得优先排查？**
- **某个分钟级尖峰是否真实发生过？**
- **指定故障窗口内到底发生了什么变化？**
- **当前窗口和可信基线相比，负载差异到底在哪里？**
- **结果能否安全交给脚本、管道或其他工具？**

### 为什么副本在延迟？

```bash
binlogviz analyze mysql-bin.000123
```

「无主键」一节列出收到了 UPDATE 或 DELETE、并且没有主键的表，按这些行数排序。副本应用这些行时可能全表扫描。`主键是否存在未知（binlog_row_metadata 不是 FULL）` 表示 binlog 没有记下主键信息，不是在说某张表没有主键。没有主键但只有 INSERT 的表会点名，但不会被排成这种风险。

MySQL 8.0 及以上、并且打开了 `log_replica_updates` 的副本 binlog，在最忙分钟之后有「副本应用延迟」。延迟是本机提交时间减去原始提交时间，并假设源库和副本的时钟一致。这一节给出最大延迟、p95、延迟达到峰值的分钟，以及最慢的事务：GTID、事务起点 file:byte、两个提交时间、延迟，以及该事务里的表。`--top` 限制列出几条。两个时间戳相同的源库 binlog 只说一行，没有表。MySQL 5.7 和 MariaDB 打印 `提交时间戳不可用`。

## 安装

### macOS 首选：Homebrew Cask

Homebrew **仅适用于 macOS**。Linux 请用下面的 tarball 或 `install.sh`。

```bash
brew tap Fanduzi/binlogviz
brew install --cask binlogviz
```

这条路径会安装预编译 release artifact，并在安装时移除 macOS quarantine 属性；用户不需要额外安装 DuckDB。

### Linux：tarball 或 install.sh

```bash
# install.sh（当前版本）
curl -fsSLO https://raw.githubusercontent.com/Fanduzi/BinlogVisualizer/v0.23.24/install.sh
sh ./install.sh --version v0.23.24

# 或 linux/amd64 tarball
curl -fsSLO https://github.com/Fanduzi/BinlogVisualizer/releases/download/v0.23.24/binlogviz_0.23.24_linux_amd64.tar.gz
tar -xzf binlogviz_0.23.24_linux_amd64.tar.gz
install ./binlogviz /usr/local/bin/binlogviz
```

### 通用备选：下载 Release Artifact

从 GitHub Releases 下载与你平台匹配的归档文件，校验 checksum 后再把二进制放到 `PATH` 中。

权威 release artifact 由 GitHub Actions release workflow 产出。macOS 产物在原生 runner 上构建，Linux 产物则在 manylinux2014 用户态中构建，以保持对 CentOS 7 / glibc 2.17 的兼容基线。本地 `goreleaser` 更适合做配置校验和当前宿主机的可选验证，不是主要发布路径。

下面是 `darwin/arm64` 和当前版本 `v0.23.24` 的示例：

```bash
curl -fsSLO https://github.com/Fanduzi/BinlogVisualizer/releases/download/v0.23.24/binlogviz_0.23.24_darwin_arm64.tar.gz
curl -fsSLO https://github.com/Fanduzi/BinlogVisualizer/releases/download/v0.23.24/binlogviz_0.23.24_checksums.txt
shasum -a 256 -c binlogviz_0.23.24_checksums.txt 2>/dev/null | grep "binlogviz_0.23.24_darwin_arm64.tar.gz: OK"
tar -xzf binlogviz_0.23.24_darwin_arm64.tar.gz
install ./binlogviz /usr/local/bin/binlogviz
```

也可以先从同一个 release tag 下载仓库内置安装脚本，再执行它：

```bash
curl -fsSLO https://raw.githubusercontent.com/Fanduzi/BinlogVisualizer/v0.23.24/install.sh
sh ./install.sh --version v0.23.24
```

如果只想预览将要解析出的 artifact，而不实际下载：

```bash
./install.sh --version v0.23.24 --dry-run
```

### 备选：从源码构建

```bash
git clone https://github.com/Fanduzi/BinlogVisualizer.git
cd BinlogVisualizer

go build -o binlogviz .
go install .
go run . analyze <binlog files...>
```

如果你从源码构建且没有注入 release ldflags，那么 `binlogviz --version` 会显示 `dev`，而不是某个发布版本号。

### 验证二进制

```bash
binlogviz --version
binlogviz version
```

- `binlogviz --version` 只输出版本号
- `binlogviz version` 输出 ASCII Logo 加 `binlogviz <version>`

## 常见 DBA 工作流

### 1. 先用一个文件验证分析链路

```bash
curl -fsSLO https://raw.githubusercontent.com/Fanduzi/BinlogVisualizer/main/cmd/binlogviz/testdata/minimal.binlog
binlogviz analyze minimal.binlog
```

适用场景：

- 先确认文件可读
- 先确认解析成功
- 先看默认文本报告是否已经足够支撑第一轮判断

### 2. 分析整个目录时优先用 discovery 模式

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin.
```

discovery 模式通常是目录分析时最稳妥的运维路径。BinlogViz 会：

1. 扫描目录下的直接子项
2. 只保留前缀之后是纯数字后缀的文件
3. 按数字后缀排序
4. 把最终解析出的有序文件列表打印到 `stderr`
5. 按该顺序执行分析

### 3. 用时间和对象过滤减少噪音

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. \
  --start "2026-03-15T10:00:00Z" \
  --end "2026-03-15T10:30:00Z" \
  --exclude-schema mysql,sys,information_schema,performance_schema
```

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. \
  --include-schema orders \
  --include-table payments,refunds
```

适合用于已知故障时间窗口、特定服务 schema，或者一组明确的热点表排查。

### 4. 安全地输出 JSON

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. --format json > analyze.json
```

这样机器可读结果保留在 `stdout`，进度和运行状态保留在 `stderr`。

### 5. 默认 Top-N 不够时扩大报告范围

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. \
  --top-tables 20 \
  --top-transactions 20 \
  --top-threads 20 \
  --top-minutes 30
```

### 6. 关注异常时打开告警检测

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. \
  --detect-spikes \
  --large-trx-rows 5000 \
  --large-trx-duration 60s
```

### 7. 把当前结果和基线结果做对比

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. \
  --start "2026-03-15T10:00:00Z" \
  --end "2026-03-15T10:30:00Z" \
  --format json \
  --snapshot-name incident_current > /tmp/incident_current.json

binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. \
  --start "2026-03-08T10:00:00Z" \
  --end "2026-03-08T10:30:00Z" \
  --format json \
  --snapshot-name incident_baseline > /tmp/incident_baseline.json

binlogviz snapshot list
binlogviz snapshot list --format json
binlogviz snapshot show incident_current
binlogviz snapshot show incident_current --format json
binlogviz snapshot rename incident_current incident_current_renamed
binlogviz snapshot delete incident_current_renamed
binlogviz compare --current-snapshot incident_current --baseline-snapshot incident_baseline
binlogviz compare --current-snapshot incident_current --baseline-snapshot incident_baseline --format json > compare.json
binlogviz compare --current-snapshot incident_current --baseline-snapshot incident_baseline --format html > compare.html
```

默认快照目录是 `~/.binlogviz/snapshots`。如果你已经有导出的 analyze JSON 文件，之后也可以通过 `binlogviz snapshot save <report.json> --name <name>` 把它补充保存进去。`snapshot list` 是查看整个快照库的最快入口；对于长期维护的快照库，优先使用 `snapshot rename` 和 `snapshot delete`，而不是直接手工改文件名或删文件。

如果你已经在自己管理导出的 JSON 文件，旧的文件对比模式仍然可用：

```bash
binlogviz compare /tmp/incident_current.json /tmp/incident_baseline.json
```

compare 会把工作负载总量变化、热点表变化、操作类型变化，以及告警新增/移除情况集中呈现，便于 DBA 快速判断当前窗口是否比基线更重、更分散、或者更异常。

## 多语言支持

BinlogViz 支持多语言运行时输出，包括错误信息、报告和进度提示。

```bash
binlogviz --lang zh-CN analyze mysql-bin.000123
LANG=zh_CN.UTF-8 binlogviz analyze mysql-bin.000123
```

支持的语言：

- `en` - English（默认）
- `zh-CN` - 简体中文

`--help` 当前仍然是英文，因为 help 文本生成早于语言初始化；运行时输出已支持本地化。

## 按任务阅读文档

### 建议先读这些

- [快速开始](docs/recipe/quickstart.zh-CN.md)
- [分析本地 Binlog](docs/recipe/analyze-local-binlogs.zh-CN.md)
- [常见错误排查](docs/recipe/troubleshoot-common-errors.zh-CN.md)

### 需要精确契约和参数行为时

- [CLI 参考](docs/reference/cli.zh-CN.md)
- [输入发现参考](docs/reference/input-discovery.zh-CN.md)
- [输出格式参考](docs/reference/output-format.zh-CN.md)

### 需要理解内部设计或分析模型时

- [产品架构](docs/concept/architecture.zh-CN.md)
- [DuckDB 临时存储](docs/concept/duckdb-temp-store.zh-CN.md)
- [分析模型](docs/concept/analysis-model.zh-CN.md)
- [限制与边界](docs/concept/limitations.zh-CN.md)

### 发布与补充资料

- [示例输出](docs/examples/)
- [发行说明](docs/releases/)
- [变更记录](CHANGELOG.md)
- [安全策略](SECURITY.md)

## 环境要求

- 本地 MySQL `ROW` 格式 binlog 文件
- 如果从源码构建，需要 Go 1.26.1+

## License

Apache 2.0
