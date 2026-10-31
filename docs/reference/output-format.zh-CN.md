# 输出格式参考

本文档说明 `binlogviz analyze`、`binlogviz compare`、`binlogviz trend`、`binlogviz workflow run`、`binlogviz workflow resume`、`binlogviz workflow status`、`binlogviz workflow clean`、`binlogviz workflow export`、`binlogviz workflow validate` 和 `binlogviz workflow describe` 会向 `stdout` 和 `stderr` 分别写入什么内容。

如果你想先看最短运维路径，请先阅读[快速开始](../recipe/quickstart.zh-CN.md)或[分析本地 Binlog](../recipe/analyze-local-binlogs.zh-CN.md)。

## 输出通道契约

BinlogViz 会把不同用途的输出写到不同通道：

- `analyze`：`stdout` 承载最终分析报告；`stderr` 承载进度、discovery 解析出的文件列表、最终组装状态、快照保存确认以及运行时错误。
- `compare`：`stdout` 承载最终 compare 报告；命令失败时由 CLI 通过 `stderr` 输出错误。
- `trend`：`stdout` 承载最终 trend 报告；命令失败时由 CLI 通过 `stderr` 输出错误。
- `workflow run`：v1 中 `stdout` 不使用；`stderr` 承载进度行和最终 manifest 路径。所有报告写到 `<output_dir>/` 下的 artifact 目录树中。无论成功或失败，都会写入 `manifest.json` 和 `index.html`。`manifest.json` 会始终包含一个规范化后的 `workflow_summary` 对象，而 `index.html` 会在 summary 有内容时渲染 `Workflow Recommendations`、`Workflow Findings` 和 `Workflow Summary Warnings` 分区。
- `workflow resume`：`stdout` 不使用；`stderr` 承载进度行和最终 manifest 路径。Resume 会复用成功步骤的 artifact，并重跑失败、缺失或被显式选中的步骤。更新后的 `manifest.json` 会记录每个步骤的执行状态（`executed` 或 `reused`）。`index.html` 会包含 resume mode、attempt 编号和每个步骤的执行标签。
- `workflow status`：`stdout` 输出文本或 JSON 形式的运行时检查结果。命令会读取 `manifest.json`，检查 artifact presence，报告 `runtime_state`、`resumable`、`resume_error` 和每个步骤的状态，并且在可行时包含 dry `resume_preview`。它是严格只读的，也不会用 `stderr` 输出进度。
- `workflow clean`：`stdout` 输出文本或 JSON 形式的清理摘要。命令会读取 `manifest.json`，报告 orphaned workflow artifacts 以及可选的 orphaned snapshots；在 `--apply` 模式下还会报告 `deleted` 和 `skipped`。它不会用 `stderr` 输出进度，但只要存在 skipped 删除，仍会在写完 `stdout` 后以非零状态退出。
- `workflow export`：`stdout` 输出文本或 JSON 形式的导出摘要。命令会读取 `manifest.json`，在 workflow root 之外写入确定性的 zip archive，并报告纳入文件计数和 warnings。它不会用 `stderr` 输出进度。
- `workflow validate`：`stdout` 输出文本或 JSON 校验结果。命令只读取 `plan.yaml`，执行静态 plan 校验；如果 plan 非法则以非零状态退出。失败时还会继续返回命令错误，因此默认 CLI 执行下可能在 `stdout` 载荷之后再向 `stderr` 输出一行错误信息。
- `workflow describe`：`stdout` 输出文本或 JSON 的静态执行预览，包括步骤顺序和 artifact 路径。命令只读取 `plan.yaml`，不会渲染 HTML，也不会检查任何运行时产物。失败时，JSON stdout 与 `workflow validate` 相同，是 `{schema_version, valid: false, error}`；命令还会继续返回错误，因此默认 CLI 执行下可能在 `stdout` 载荷之后再向 `stderr` 输出一行错误信息。

这种分离很重要，因为它能让报告输出保持适合重定向和自动化处理。

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. --format json > analyze.json
```

在上面的例子中，JSON 报告会写入 `analyze.json`，而进度和 discovery 状态仍然通过 `stderr` 显示在终端上。

如果再加上 `--snapshot-name`，JSON 载荷仍然写到 `stdout`，同时 BinlogViz 会在 `stderr` 上打印类似 `Saved snapshot "incident_current" to /home/user/.binlogviz/snapshots/incident_current.json` 的保存确认。

## JSON 版本

`stdout` 上的每个 JSON 文档顶层都有一个整数版本字段（ADR 0010）。使用方应先读它，遇到不认识的版本就拒绝处理。

| 输出 | 版本字段 | 当前 |
|---|---|---|
| `analyze --format json`、已保存的快照 | `report_version` | 4 |
| `flashback --plan` | `plan_version` | 2 |
| `compare`、`trend`、`snapshot list`、`snapshot show` 加 `--format json` | `schema_version` | 1 |
| `workflow validate`、`describe`、`status`、`clean`、`export` 加 `--format json` | `schema_version` | 1 |
| `version --format json` | `schema_version` | 1 |

`schema_version` 总是第一个字段。每种输出只在不兼容变更（删除或改名字段、改变字段类型或含义）时提升自己的版本号，CHANGELOG 会把这次提升标为不兼容。新增字段不提升版本。`workflow run` 写出的 compare 和 trend 产物带同样的字段。`workflow status`、`resume` 和 `clean` 对这些产物遵守同一规则：manifest 记录的 compare 或 trend JSON 产物，如果 `schema_version` 不是 1 到本构建所写版本之间的整数，就以退出码 9 退出（`unsupported`，`details.reason` 为 `unsupported_artifact_version`，并带 `artifact`、`step_kind`、`step_name`、`schema_version` 和 `supported_version`），stdout 不输出，磁盘上不做任何改动。没有这个字段的产物是 ADR 0010 之前写的，仍然可以读取。`stderr` 上单行的 `{"error":…}` 和 `{"warning":…}` 不是带版本的文档，格式由 ADR 0009 固定。`workflow status` 另外报告 `manifest_version`，那是它读到的 manifest 的版本。

`flashback --plan` 文档在 flashback 处于实验阶段时带 `experimental: true`（ADR 0020）。它是新增字段，按版本规则 `plan_version` 保持 2。

## 显示时区

不传 `--tz`，或 `--tz UTC`，渲染出的时间保持原来的写法：文本、Markdown 和 HTML 以 ` UTC` 结尾，JSON 使用带 `Z` 的 RFC3339。`--tz` 接受 IANA 名，例如 `Asia/Shanghai`。其他时区使用数字偏移，不用缩写。时间戳行写出这个 IANA 名。

`--tz` 作用于 analyze 的文本、Markdown、HTML 和 JSON，compare 与 trend HTML 的生成时间、Current/Baseline 窗口、trend 起点，以及 compare HTML 里 hot-interval 的 Minute 单元格，snapshot list/show 的 `created_at` 和窗口时间，workflow describe 的窗口时间，能解析为 RFC3339 的 `version` `commit_time`，以及 `workflow run` 和 `workflow resume` 写出的 analyze 产物与 compare/trend HTML。它不改写 compare 或 trend 的 JSON 与文本，workflow status/clean/export 的簿记时间，行单元格里的 `TIMESTAMP` 或 `DATETIME`，以及 flashback SQL。快照文件始终写成 UTC，包括把一份带 `--tz` 的报告交给 `snapshot save`。list 和 show 只改打印出来的那一份。空的、不认识的或 `Local` 的 `--tz` 退出码是 3，kind 是 `usage`。

`--start` 和 `--end` 接受 RFC3339、`YYYY-MM-DD HH:MM:SS`、带 `±HH:MM` 的空格形式，以及结尾的 ` UTC`。偏移量或结尾 ` UTC` 就是那个时刻。没有时区的 --start 或 --end，在设置了 --tz 时按 --tz 读取，包括 --tz UTC；没有设置 --tz 时，按运行 binlogviz 的机器的本地时区读取。`--tz UTC` 和 `--tz Etc/UTC` 把这个值解析成同一个时刻。没有时区、又落在夏令时空档或重叠时段的值，退出码是 3：错误会写出该值、时区和原因，并要求补上显式偏移。绝对值大于 14:00 的偏移，或分钟不是 00、30、45 的偏移，同样是这个退出码。

## 可用格式

### `analyze`

| 参数值 | 别名 | 说明 |
|---|---|---|
| `text` | — | 默认，人类可读的终端输出。 |
| `json` | — | 机器可读的 JSON 格式。 |
| `markdown` | `md` | 含表格的 GitHub Flavored Markdown。 |
| `html` | — | 含交互式图表和主题切换器的自包含 HTML 文件。 |
| `csv` | — | 以 RFC 4180 CSV 输出单个报告数据集；由 `--csv-dataset` 选择。 |
| `tsv` | — | 同样的数据集，以制表符分隔。 |

### `compare`

| 参数值 | 说明 |
|---|---|
| `text` | 默认。适合终端阅读的 compare 差异摘要。 |
| `json` | 机器可读的 compare 结果。 |
| `html` | 带交互式图表的自包含可视化 compare 报告。 |

### `trend`

| 参数值 | 说明 |
|---|---|
| `text` | 默认。适合终端阅读的多 snapshot trend 报告。 |
| `json` | 机器可读的 trend 结果，并包含 `pattern_trends`。 |
| `html` | 带图表的自包含 trend 报告，并包含 `Pattern Trends` 分区。 |

### `workflow status`

| 参数值 | 说明 |
|---|---|
| `text` | 默认。面向人的运行时检查摘要，包含步骤 artifact presence 和可选的 resume preview。 |
| `json` | 机器可读的状态对象，包含 `runtime_state`、`resumable`、`resume_error`、`steps` 和可选的 `resume_preview`。 |

### `workflow clean`

| 参数值 | 说明 |
|---|---|
| `text` | 默认。面向人的清理摘要，包含 orphan、deleted 和 skipped 列表。 |
| `json` | 机器可读的清理结果，包含 orphan/deletion 数组和聚合计数。 |

### `workflow export`

| 参数值 | 说明 |
|---|---|
| `text` | 默认。面向人的导出摘要，包含 workflow 名称、输出根目录、archive 路径、纳入计数和可选 warnings。 |
| `json` | 机器可读的导出结果，包含 archive 元数据、纳入计数和 warnings。 |

### `workflow validate`

| 参数值 | 说明 |
|---|---|
| `text` | 默认。面向人的校验摘要或错误输出。 |
| `json` | 机器可读的校验结果，包含 `valid` 及摘要/错误字段。 |

### `workflow describe`

| 参数值 | 说明 |
|---|---|
| `text` | 默认。面向人的静态执行预览，覆盖 analyze、compare 和 trend。 |
| `json` | 由 plan 推导出的机器可读静态描述。 |

## 文本输出

文本模式是默认报告格式。它会渲染固定的六个章节。

### 1. Workload Summary

`Workload Summary` 章节提供分析结果集的顶层汇总：

- 总事务数
- partial 事务数
- unknown 事务数
- 总影响行数
- 总事件数
- 时间范围
- 持续时长

示例标题：

```text
=== Workload Summary ===
```

### 2. Top Tables

`Top Tables` 章节按总影响行数对表进行排序，并包含每张表的操作明细。

文本输出中常见字段包括：

- schema 和表名
- 总行数
- insert 行数
- update 行数
- delete 行数
- 不同事务数量

示例标题：

```text
=== Top Tables ===
```

`binlog_row_metadata=FULL` 且没有主键的表收到了 UPDATE 或 DELETE 行时，Top Tables 后面会有「无主键」一节，按 UPDATE+DELETE 行数排序。没有主键但只有 INSERT 的表在这一节里单独一行；如果没有 UPDATE/DELETE，则只在摘要里一行，不会被编号成延迟风险。如果每张有行变更的表都有主键，摘要只多一行：`每张表都有主键`。如果没有 FULL 元数据，摘要只多一行：`主键是否存在未知（binlog_row_metadata 不是 FULL）`。这一行不是在说某张表没有主键。无主键表上出现 UPDATE 或 DELETE 时，告警里还会有一条 `no_primary_key`。

### 热点行

「热点行」在 Top Transactions 之后、Top Findings 之前。它按主键被 UPDATE 和 DELETE 行镜像碰到的次数排序。INSERT 不计入。没有主键的表不在这一节排名。没有排名、没有无法追踪的表、也没有触及追踪上限时，这一节省略。

每一条包括：

- `schema.table` 和主键值（`id=7`，复合键为 `sku=BOLT, wh=1`）
- 碰到次数
- 碰到该键的不同事务数
- 第一次和最后一次事件时间（UTC）
- 最早和最晚那个事务的 GTID 和 `file:byte`（事务起点，用来打开 `mysqlbinlog` 或 BinlogServer）
- 追踪上限被突破、该键继承了被丢掉键的计数时，标成 `近似`

身份只来自 MySQL 8 `binlog_row_metadata=FULL`（`SIMPLE_PRIMARY_KEY` 或 `PRIMARY_KEY_WITH_PREFIX`）。报告不会拿第 1 列 `@1` 来猜。收到 UPDATE 或 DELETE、但 binlog 里没有主键时，这一节写 `{{表}} 无法追踪热点行：binlog 里没有主键列（binlog_row_metadata 不是 FULL）`。已知主键列但某次行镜像没带上时，说明行镜像里没有主键列。`--top` 限制条数。`--top-rows` 覆盖它。`0` 保留已追踪的全部键。`--sql-context off` 隐藏键值（`主键值已隐藏（--sql-context off）`），次数、时间、GTID 和文件位置仍在。过滤与 Top Tables 相同。

追踪最多保留 8192 个主键。满了之后，新键替换被碰到次数最少的键，次数从被丢掉的计数加一开始。报告会说明达到了上限。继承了被丢掉计数的行标成近似。从未被替换的键保持精确。

```text
=== 热点行 ===
  按主键统计 UPDATE 和 DELETE 行镜像。
  1. shop.counters id=7
     次数=7  事务=6
     最早 2026-10-06 14:00:01 UTC  <gtid> <file>:<pos>
     最晚 2026-10-06 14:00:06 UTC  <gtid> <file>:<pos>
```

Markdown 在 `## 热点行` 下用表格。HTML 使用 `id="hot-rows-table"`。

### 3. Top Threads

`Top Threads` 在任一会话有行变更时按行数排序，否则按事件数、字节、事务数。标题会写出依据，例如 `热点线程（按行数）`。

展示行里没有数据的列会省略。binlog 没有的字段不会写成 `0` 或空的调用方。

- binlog 有 `thread_id` 时写出
- 事件头有 `server_id` 时写出
- binlog 存了调用方时写出 `user@host`
- 会话里的表带了 schema 时写出
- rows、events、bytes、transactions 在该指标非 0 时写出
- 排序指标的占比

`--top` 限制这一节。`--top-threads` 覆盖它。`0` 保留全部会话。JSON 使用同一上限。

### 4. Top Transactions

`Top Transactions` 列出 `--top-transactions` 的排行，和 Markdown、JSON `transactions`、HTML 事务查找是同一批。落在截断后的最大、最长或最宽冠军里的事务仍带这个标签。已提交时长分桶仍写在这一节。

文本输出中常见字段包括：

- 事务 key
- 总行数
- 持续时间
- 事件数
- 该事务实际有的 `server_id`、`thread_id`、GTID、`xid` 或 XA xid，以及 `user@host`；没有则省略
- 当 `--sql-context` 启用时的 SQL 上下文行

示例标题：

```text
=== Top Transactions ===
```

### 5. Top Patterns

`Top Patterns` 章节用于归并重复出现的写入事务形状。

文本输出中常见字段包括：

- 模式标签
- 总行数
- 事务数
- 每事务平均行数
- 可选的代表性 query summary
- 被选中的高信号模式的可选 drilldown 块（why、workload peak minutes、workload transactions）

示例标题：

```text
=== Top Patterns ===
```

### 6. Minute Activity

`Minute Activity` 章节汇总每分钟的写入活动。

文本输出中常见字段包括：

- 分钟时间桶
- 该分钟的总行数
- 该分钟的事务数

示例标题：

```text
=== Minute Activity ===
```

### 副本应用延迟

最忙分钟之后，「副本应用延迟」说明副本当时落后多少。MySQL 8.0 的 GTID 事件（含匿名 GTID）带有 `original_commit_timestamp` 和 `immediate_commit_timestamp`，单位是微秒。延迟是本机提交时间减去原始提交时间。导语说明这假设源库和副本的时钟一致。这句话不是发现，负延迟也不会变成告警。

- 副本：只要有一个计入的事务两个时间戳不同，就打印最大延迟、最近秩 p95、延迟达到峰值的分钟，以及最慢的事务。`--top` 限制列出几条。每一行在有 GTID 时写出 GTID，事务起点 `file:byte`（GTID 事件的 `# at` 字节，和 DDL 时间线同一约定，不是 Query 事件，也不是 `end_log_pos`），两个提交时间、延迟，以及该事务里的表和行数。
- 源库：每一对计入的时间戳都相等时，只有一行 `源库：原始提交时间与本机提交时间相同。`，没有表。
- 没有这些字段的文件（MySQL 5.7、MariaDB，或时间戳为 0）只打印一行 `提交时间戳不可用`，不打印时钟那句。

决定 Top Tables 的表、库、时间、位置、GTID 和 `--dml` 过滤，同样决定哪些事务计入。`--sql-context off` 隐藏语句，保留时间戳和位置。

### 7. Alerts

`Alerts` 章节列出分析逻辑检测到的告警。

示例包括：

- `large_transaction`
- `spike`
- `no_primary_key`

示例标题：

```text
=== Alerts ===
```

### 语句指纹

`语句指纹`是由 `--show-fingerprints` 或 `--details` 开启的可选章节，渲染在「下一步建议」之后。它把归一化的 `Rows_query` 语句形态排名——剥离注释、字面量折叠为 `?`、字面量列表折叠为 `(?)`——覆盖整个已选工作负载，而不只是 `--top-transactions` 列出的事务。每一项显示指纹、归因 `rows`、语句 `stmts` 次数、`share` 行占比、每表计数，以及首次出现语句的有界 `样例`（`--sql-context off` 隐藏样例）。`--top` 限制条数；`--top-fingerprints` 覆盖它；`0` 保留全部已追踪组。binlog 没有携带 `Rows_query` 文本时输出 `未捕获 Rows_query 语句文本;binlog_rows_query_log_events 可能未开启`；有文本但没有语句被保留时输出 `无保留的语句指纹`。被省略的组和追踪上限溢出各自单独一行。追踪最多保留 8192 个不同组。

```text
=== 语句指纹 ===
  1. delete from shop.orders where id in (?)
     rows=40 stmts=1 share=83.3% tables=shop.orders=40
     样例: DELETE FROM shop.orders WHERE id IN (2, 3000000000)
```

## JSON 输出

当结果需要被脚本、自动化流程或下游工具消费时，请使用 `--format json`。

```bash
binlogviz analyze mysql-bin.000123 --format json
```

JSON 报告会以稳定、适合脚本处理的 snake_case 字段名暴露最终分析结果。

### 顶层契约

顶层 JSON 对象始终包含以下字段：

| Field | Type | Required | Notes |
|------|------|----------|------|
| `report_version` | integer | yes | analyze 报告契约版本；当前版本为 `4`；读取端接受 v0-v4 |
| `provenance` | object | no | 确定性的生产者集合和 `mixed_producers`；没有证据时省略 |
| `selection` | object | no | 请求的 selector 与实际 position/GTID 证据；未启用 position 或 GTID selector 时省略 |
| `sql_context` | object | yes | 所选 `mode` 和报告全局源 SQL `available` 标志 |
| `summary` | object | yes | 总体汇总和时间边界 |
| `tables` | array | yes | 按确定性顺序排列的完整表聚合结果；人类可读渲染器可以只显示配置的 Top 表；没有表结果时为空数组 |
| `transactions` | array | yes | Top 事务聚合结果；没有事务结果时为空数组 |
| `transactions_listed` | integer | yes | `transactions` 中实际列出的事务聚合数量 |
| `transactions_omitted` | integer | yes | 从 `transactions` 中省略的分析事务数量；使用 `--top-transactions 0` 时为 `0` |
| `patterns` | array | yes | Top 模式聚合结果；没有模式结果时为空数组 |
| `statement_fingerprints` | array | yes | 有界的 `Rows_query` 语句形态排名；没有捕获到语句文本时为空数组 |
| `statement_fingerprints_listed` | integer | yes | `statement_fingerprints` 中列出的指纹组数 |
| `statement_fingerprints_omitted` | integer | yes | 因 `--top-fingerprints` 限制被省略的组数；不限制时为 `0` |
| `statement_fingerprints_overflow` | integer | no | 达到 8192 组追踪上限后未能分组的发生次数；为 `0` 时省略 |
| `minutes` | array | yes | 每分钟聚合结果；没有分钟桶时为空数组 |
| `alerts` | array | yes | 检测到的告警；没有告警时为空数组 |
| `warnings` | integer | yes | 最终结果中记录的分析警告数量 |
| `pattern_drilldowns` | array | yes | 高信号模式的有界 drilldown 摘要；无模式达到阈值时为空数组 |
| `snapshot` | object | no | 仅在 `analyze` 使用 `--snapshot-name` 时出现 |
| `primary_key_note` | string | no | 至少有一张收到行事件的表没有 FULL 主键元数据时出现。这句话说明主键是否存在未知，因为 `binlog_row_metadata` 不是 FULL。每张有行变更的表都是 `has_pk` 或 `no_pk` 时省略 |
| `replica_apply_delay` | object | no | MySQL 8 提交时间戳上的应用延迟。没有计入的事务同时带上两个时间戳时省略。不会是空对象，也不会用 `0` 代替缺失的时间戳。见下文。 |

### `replica_apply_delay`

只有当至少一个计入的行事务同时带有 `original_commit_timestamp` 和 `immediate_commit_timestamp` 时才出现。计入的事务与 Top Tables 相同：表、库、时间、位置、GTID 和 `--dml` 过滤都生效。只有 DDL 或零行的组不计入。`--sql-context off` 不会删掉这个对象。

| Field | Type | Required | Notes |
|------|------|----------|------|
| `origin` | string | yes | 任一计入延迟不为 0 时是 `replica`，否则是 `source`。这两个英文标记不翻译。 |
| `max_delay_us` | integer | yes | 有符号微秒。每一对都相等时为 `0`。 |
| `p95_delay_us` | integer | yes | 最近秩 95 分位：1 起始的名次 `ceil(0.95*n)`，所以取值是某条真实延迟。按全部计入事务计算，不只是列出的那些。 |
| `peak_minute` | string | no | 最大延迟那条事务的本机提交时间所在的 UTC 分钟。并列时取更早的本机提交时间。RFC3339。该时间为零时省略。 |
| `transactions` | array | no | 仅当 `origin` 为 `replica` 时出现。受 `--top` 限制；显式设置 `--top-transactions` 时改由它限制分析器保留的条数。源库省略，不写成 `[]`。 |

每条 `transactions`：

| Field | Type | Required | Notes |
|------|------|----------|------|
| `gtid` | string | no | 没有 GTID 时省略，匿名 GTID 也省略。 |
| `txn_start_file` | string | no | 传给 analyze 的路径。未知时省略。 |
| `txn_start_pos` | integer | no | GTID 事件的起点字节（`# at`），与 `diagnostics.ddl_events[].txn_start_pos` 同一约定。不是 Query 事件，也不是 `end_log_pos`。 |
| `original_commit_us` | integer | no | 原始提交时间，Unix 纪元以来的微秒。 |
| `immediate_commit_us` | integer | no | 本机提交时间，Unix 纪元以来的微秒。 |
| `original_commit_time` | string | no | RFC3339Nano UTC。 |
| `immediate_commit_time` | string | no | RFC3339Nano UTC。 |
| `delay_us` | integer | yes | 有符号的本机减原始。真实的 `0` 会保留。缺时间戳的事务不会成为一行。 |
| `tables` | object | no | `schema.table` 到通过过滤的行数。空时省略。 |

缺失的字符串和数字会省略，不会写成空字符串，也不会写成 `0`。

### `summary`

`summary` 始终存在，包含：

| Field | Type | Required | Notes |
|------|------|----------|------|
| `total_transactions` | integer | yes | 总分析事务数 |
| `partial_transactions` | integer | yes | 保留证据状态为 `partial_start`、`partial_end` 或 `partial_both` 的事务数 |
| `unknown_transactions` | integer | yes | 无法确定物理边界的事务数 |
| `total_rows` | integer | yes | 逻辑影响行数（UPDATE before/after image 计 1 行） |
| `total_events` | integer | yes | 纳入分析的标准化事件总数 |
| `start_time` | string | yes | RFC3339 时间戳；如果没有可用时间戳则为空字符串 |
| `end_time` | string | yes | RFC3339 时间戳；如果没有可用时间戳则为空字符串 |
| `duration` | string | yes | 最终结果中的持续时间字符串 |

### `provenance`

存在生产者证据时，`provenance` 包含排序且去重的 `server_ids`、`server_versions`、`server_flavors`，以及显式的 `mixed_producers` 布尔值。旧报告缺失的证据保持 unknown，不会伪造成 0 或空身份。

### `selection`

`selection` 将请求的 selector 与实际保留证据分开记录。position 字段为字节偏移；GTID 列表为规范化且已排序的值。

| Field | Type | Required | Notes |
|------|------|----------|------|
| `requested_start_position` | integer | no | 请求的包含式起始 position |
| `requested_stop_position` | integer | no | 请求的不包含式结束 position 或 EOF |
| `effective_start_position` | integer | no | 首个保留的标准化事件边界 |
| `effective_stop_position` | integer | no | 最后一个保留的标准化事件结束边界 |
| `include_gtids` | array | no | 规范化的 MySQL set 或 MariaDB 精确身份 |
| `exclude_gtids` | array | no | 在 include 之后应用的规范化排除项 |
| `resolved_gtid_flavor` | string | no | selector 与输入一致后得到的 `mysql` 或 `mariadb` |
| `matched_gtid_set` | array | no | GTID 选择器匹配到的 GTID，按区间集合输出（report_version 4，#379）。MySQL：每个 server UUID 一项，如 `uuid:1-4:6-3000000`，按 UUID 排序。MariaDB：每段连续序号一项，`domain-server-seq` 或 `domain-server-first..last`，按 domain、server、序号排序。多个 server 写同一个 domain（交替写入），且输入里该 domain 没有重复的序号时，区间按该 domain 的序号合并，再对每个 server 各列一次（`0-7-1..9`、`0-8-1..9`），取条目更少的形式；区间里输入中不存在的身份不是事务。每一项都是合法的 `--include-gtids` 输入，整个列表作为 `--include-gtids` 在同一输入上选中的恰好是匹配到的事务。读到两次的 GTID 只出现一次。只有 GTID 选择器有匹配时才出现 |
| `matched_transactions` | integer | no | GTID 选择器匹配到、且至少有一个事件落在时间和位点窗口内的事务组数，每读到一次计一次：读到两次的 GTID（同一文件传两次，或文件有重叠）计两次，但在 `matched_gtid_set` 里只出现一次。它包含没有保留行的匹配事务组，例如 DDL 事务，或行都在 `--stop-position` 之后的事务组；`summary.total_transactions` 不计这些（它统计有保留行的事务和 XA prepare 事务组），所以两者可能不同。设置了 GTID 选择器时出现；只用位点或时间选择器时没有 |

report_version 3 用 `matched_gtids` 逐个列出匹配到的事务身份，会随匹配数增长。版本 4 去掉这个键；compare 和 trend 仍能读取 v3 报告。区间集合最多 100,000 个条目（区间）；匹配到的 GTID 需要更多时，以退出码 10 `resource` 结束，`details.reason` 为 `matched_gtid_intervals`（附 `limit` 和 `intervals`），不输出报告。请收窄 `--include-gtids`/`--exclude-gtids` 或拆分输入。

### `tables`

`tables` 始终以数组形式存在。每个条目包含：

| Field | Type | Required |
|------|------|----------|
| `schema` | string | yes |
| `table` | string | yes |
| `total_rows` | integer | yes |
| `insert_rows` | integer | yes |
| `update_rows` | integer | yes | 逻辑 UPDATE 行（before/after image 计 1 行） |
| `update_events` | integer | yes | UPDATE 行事件数 |
| `delete_rows` | integer | yes |
| `txn_count` | integer | yes | 对该表写入过 row image 的不同事务数。仅含 DDL 的组不计入。 |
| `key_status` | string | no | 收到行事件的表为 `has_pk`、`no_pk` 或 `unknown`。没有行事件时省略。`has_pk` 表示 FULL 元数据里有 `SIMPLE_PRIMARY_KEY` 或 `PRIMARY_KEY_WITH_PREFIX`。`no_pk` 表示有 FULL 元数据，但这两个字段都没有。`unknown` 表示 `binlog_row_metadata` 不是 FULL，不是在说这张表没有主键。`update_rows` 和 `delete_rows` 是过滤后的计数。`no_pk` 的表只有在 `update_rows` 或 `delete_rows` 大于 0 时才算复制延迟风险 |

`diagnostics.input_format_guess` 为 `ROW` / `STATEMENT` / `MIXED`，信号不足时为空。`diagnostics.ignored_query_dml_events` 统计没有对应 row image 的 Query-DML。`diagnostics.open_explicit_groups` 在出现时，统计输入结束时仍没有 `COMMIT` 或 plain `ROLLBACK` 的 `BEGIN` 组；计数为 0 时省略。这种未关闭的 `BEGIN` 如果后面又来了 GTID，不会产出报告：analyze 以 exit 1 失败，错误是「显式 BEGIN 未关闭」。

`--diagnostics`（默认 `full`）只改变这个 JSON 对象。`summary` 保留 `file_coverage`、`counted_event_bytes`、`open_dml_groups`、`duration_buckets`、`input_format_guess`，以及 ignored-query、未关闭组和 unmapped 计数。`summary` 里的 `ddl_events` 是时间线顺序上的前 `--top` 条（默认 10），`ddl_events_omitted` 是其余条数，全部放下时也写出 0。`full` 仍然列出每一条 DDL，并且不写 `ddl_events_omitted`。`summary` 把 `largest_transactions`、`longest_transactions`、`widest_transactions`、`file_segments`、`hot_intervals`、`findings` 写成空数组（`largest_byte_transactions` 为空时仍然省略）。`off` 省略 `diagnostics` 键。文本、Markdown 和 HTML 忽略此标志。`full`、`summary` 和 `off` 的 `report_version` 相同（#379 起为 4；加入 `summary` 时仍是 3，因为默认和 `full` 文档不变，`ddl_events_omitted` 只加在可选的 `summary` 文档上，ADR 0010）。把 v0.28.0 的 `summary` `ddl_events` 当成完整时间线的调用方会遇到不兼容变更。`summary` 或 `off` 的文档在顶层带 `diagnostics_mode`（`"summary"` 或 `"off"`），完整文档没有这个键。此标志只裁剪 stdout：`--snapshot-name` 保存的快照和所有 workflow 产物都保留完整 diagnostics。`compare`、`trend`、`snapshot save` 和 workflow resume 遇到 `diagnostics_mode` 是字符串且不是 `full` 时以 exit 9 拒绝（`unsupported`，`details.reason: reduced_diagnostics`，`details.diagnostics_mode`），不会把省略的列表当成空列表比较。JSON `null` 和缺少 `diagnostics_mode` 一样，视为没有该字段：compare 照常运行，`snapshot save` 会写入文件。非字符串（例如 `1`）是损坏文档（exit 5），不是 `reduced_diagnostics`。显式传入 `--diagnostics ""` 是用法错误（exit 3）。省略该标志，以及库的零值，仍是 `full`。`snapshot list` 仍以 exit 0 结束，并标出裁剪过的文件（文本列 `DIAGNOSTICS`，JSON 里该条的 `diagnostics_mode`，`schema_version` 仍是 1），而不是等到 show、compare 或 trend 才失败。见 ADR 0011 §3。

`diagnostics.counted_event_bytes` 是 schema/table 过滤后保留的 ROW/DDL 事件字节总数。`diagnostics.file_coverage.selected[].size` 是输入文件的物理大小；人类可读报告中缺少大小元数据时显示为不可用，而不是 0。`diagnostics.ddl_events` 是 DDL 发生时间线（时间、操作、对象、语句、位置，以及 binlog 里有 GTID 时、持有这条 DDL 的事务的 GTID），不是 MDL 或锁等待时长。`diagnostics.open_dml_groups` 在出现时，列出写过行镜像、并且到输入结束仍没有 `COMMIT` 或 plain `ROLLBACK` 的显式 `BEGIN` 组。每条的 `note` 说明该组在本文件/窗口内仍未提交，不是锁冲突证明。`diagnostics.duration_buckets` 统计已提交事务落在 `<1s`、`1s-10s`、`10s-30s`、`>=30s` 的数量。`diagnostics.largest_byte_transactions` 按 binlog 字节排列已提交事务。这三项为空时省略。

默认文本会打印 DDL 时间线、未提交的开放 DML、最多三条最长已提交事务、时长分桶、字节贡献，以及选中多个文件时的单文件大小和时间跨度。`--large-trx-duration` 仍对超过该时长的已提交事务告警，并对超过同一阈值的开放 DML 给出 warning。开放 `BEGIN` 之后又来了 GTID 仍是 exit 1、stdout 为空；如果该组写过行，Error 行会带上时长、行数、表和位置。只有 DDL 的输入 exit 0。只有 ADMIN 的输入仍是 exit 2。`FLUSH TABLES WITH READ LOCK` 仍是未分类 QUERY（exit 1），不会进入 admin/lock 列表。

### `transactions`

`transactions` 始终以数组形式存在。每个条目始终包含：

| Field | Type | Required | Notes |
|------|------|----------|------|
| `txn_key` | string | yes | 报告中使用的事务标识 |
| `xa_xid` | string | no | MariaDB XA 标识；非 XA 事务省略 |
| `server_id` | integer | no | 事件头中的生产者 ID；不可用时省略 |
| `server_version` | string | no | FormatDescription server version；不可用时省略 |
| `server_flavor` | string | no | 根据 server version 推导的 `mysql` 或 `mariadb` |
| `gtid` | string | no | 事务唯一的 MySQL/MariaDB GTID；不可用时省略 |
| `thread_id` | integer | no | QueryEvent thread ID；不可用时省略 |
| `xid` | string | no | 以十进制字符串序列化的提交 XID |
| `actor` | object | no | binlog 中存在时的 best-effort `user` / `host` 调用者证据 |
| `start_time` | string | yes | RFC3339 时间戳；未设置时为空字符串 |
| `end_time` | string | yes | RFC3339 时间戳；未设置时为空字符串 |
| `duration` | string | yes | 持续时间字符串 |
| `total_rows` | integer | yes | 事务触及的总行数 |
| `event_count` | integer | yes | 事务中的事件数量 |
| `binlog_bytes` | integer | yes | 窗口内保留事务证据覆盖的字节数 |
| `binlog_file_start` | string | no | 窗口内保留证据所在的首个文件 |
| `binlog_file_end` | string | no | 窗口内保留证据所在的最后文件 |
| `pos_start` | integer | no | 窗口内保留证据的起始位置 |
| `pos_end` | integer | no | 窗口内保留证据的结束位置 |
| `completeness` | string | yes | `complete`、`partial_start`、`partial_end`、`partial_both` 或 `unknown` |
| `replay_available` | boolean | yes | 是否可以生成可信的完整事务回放命令。输入来自 stdin 时为 false，即使跨度本身完整 |
| `replay_note` | string | no | stdin 且跨度本来可回放时出现。写出起止位置，并说明没有文件路径 |
| `replay_scope` | string | no | 可回放时为 `full_transaction` |
| `mysqlbinlog_cmd` | string | no | 标记为完整事务的回放命令；没有可信完整跨度时省略 |
| `tables` | object | no | JSON 对象，key 是表名，value 是整数计数；为空时省略 |
| `operations` | object | no | JSON 对象，key 是操作名（`INSERT`、`UPDATE`、`DELETE` 或 `LOAD_DATA`），value 是受影响行数；为空时省略 |
| `query_summary` | string | no | 当 SQL 上下文模式抑制该字段，或没有摘要时省略 |
| `query_sql` | string | no | 仅在 `--sql-context full` 且存在受限 SQL 上下文时出现。存储被截断时追加 `… [truncated: <shown> of <original> bytes]` |
| `query_truncated` | boolean | no | 没有 query 上下文时省略。出现时，只有存储 SQL 碰到 4096 字节上限才为 true。更短语句的 160 字符摘要仍是 false |
| `query_original_bytes` | integer | no | 没有 query 上下文时省略；出现时表示原始 SQL 的字节长度 |
| `rows` | array | no | 仅在 `--show-rows` 且 `--sql-context` 不是 `off` 时出现。每个元素是一行逻辑变更：`op`（`INSERT`、`UPDATE`、`DELETE`）、`columns`、`names`（`full` 或 `positional`）、`before`、`after`、`changed`（UPDATE 中前后不同的列名）。单元格是字符串，SQL NULL 为 JSON `null`。每个事务最多 32 条 |
| `rows_omitted` | integer | no | 超过该上限后未保存的逻辑行数 |

事务行数、操作数、事件数和保留位置继续采用 inclusive 的逐事件窗口语义。相邻解析事件只能帮助确定物理边界，不能增加窗口外 workload。`partial_start` 表示物理起点在所选窗口外，`partial_end` 表示物理终点在窗口外，`partial_both` 表示两端均在窗口外。缺失或旧版边界元数据一律为 `unknown`，不会推断为完整。`transactions` 先列出完整事务排行榜，再按 `txn-N` 的自然数字顺序用有界的 partial/unknown 证据填充剩余位置；后者不参与完整事务排名，也不会进入大小直方图、模式或普通大事务告警。

#### SQL 上下文模式行为

`transactions` 中的 query 字段取决于 `--sql-context`：

- `off`：所有格式都省略查询文本和 DDL 语句文本。操作、对象和位置仍保留。`--show-rows` 的单元格也不打印；报告写明 `row values omitted because --sql-context is off`。热点行同样隐藏主键值，次数、时间、GTID 和文件位置仍在
- `summary`：包含经空白归一化、SQL 正文最多 160 个字符的 `query_summary`；被截断时追加 `… [truncated: <shown> of <original> bytes]`。存在上下文时才包含截断元数据。`query_truncated` 只有碰到 4096 字节存储上限才为 true
- `full`：存在上下文时额外包含 UTF-8 安全、最多 4096 字节的 `query_sql` 及原始字节数。被截断时追加同一标记。DDL 时间线语句同样以 4096 字节为上限，被截断时带同一标记

`diagnostics.ddl_events[].statement` 遵守同一模式。`off` 省略它。口令材料（含 MariaDB `IDENTIFIED VIA`/`WITH` … `USING`/`AS` 和 `OR` 插件链）在这之前已经是 `<secret>`。`gtid`、位置和停止提示不是语句文本，任何模式都保留。

每条 `diagnostics.ddl_events` 额外有这些字段：

| 字段 | 类型 | 必填 | 说明 |
|------|------|----------|------|
| `gtid` | string | 否 | 持有这条 DDL 的事务。MySQL 为 `uuid:seq`，MariaDB 为 `domain-server-seq`。binlog 没有 GTID（`GTID_MODE=OFF` 或匿名 GTID）时省略。不会输出空字符串 |
| `gtid_note` | string | 否 | 仅在省略 `gtid` 时出现，说明 GTID 不可用 |
| `txn_start_file` | string | 否 | 该事务起点所在文件 |
| `txn_start_pos` | integer | 否 | 该事务起点的字节偏移。有 GTID 事件时这是该事件的起点，不是 Query 事件，也不是 `end_log_pos` |
| `server_id` | integer | 否 | DDL 事件头里的 server id；binlog 没有记录时省略 |
| `thread_id` | integer | 否 | Query 的线程 id；binlog 没有记录时省略 |
| `actor` | object | 否 | DDL 事件记录了调用方时的 `user` / `host` |
| `mysqlbinlog_stop` | string | 否 | `mysqlbinlog --stop-position=<txn_start_pos> <txn_start_file>`（服务器版本是 MariaDB 时为 `mariadb-binlog`）。回放更早的事件，不包含这条 DDL。stdin 没有文件路径时省略 |
| `stop_gtid` | string | 否 | 与 `gtid` 相同。交给 BinlogServer 的 `stop_gtid`。省略 `gtid` 时一并省略 |
| `stop_note` | string | 否 | 与 `mysqlbinlog_stop` 或 `stop_gtid` 一起出现，说明该停止点回放更早的事件且不包含这条 DDL |

`position_start` / `position_end` 仍是包含语句的 Query 事件。文本、Markdown 和 HTML 在时间线条目旁打印同一 GTID、事务起点、`mysqlbinlog --stop-position` 和 `BinlogServer stop_gtid`。这里不生成回滚 SQL，也不表示恢复已经完成。`SET PASSWORD` 会出现在时间线上。`CREATE`/`ALTER`/`DROP` VIEW、TRIGGER、PROCEDURE、FUNCTION、EVENT 会留在时间线上，对象类型为 `view`、`trigger`、`routine` 或 `event`。`CREATE TRIGGER` 和 `DROP TRIGGER` 都使用触发器名字。无法识别的 DDL 仍以操作 `DDL`、对象 `ddl` 出现。`--include-table` 可以匹配这些名字；匹配到时即使没有行变更也退出 0。

`sql_context.available` 表示整份报告中是否观察到源 SQL，即使该 SQL 不在 Top 事务中。因而 `full` 与 `available=false` 可以同时出现。provenance 不受该模式影响，任何模式都不会序列化 row-image 值。

命名 snapshot 会保存完整的 report-v3 payload。snapshot/compare 读取端继续接受 v0-v2，并且不会伪造缺失身份。

### `hot_rows`

`hot_rows` 始终是数组，对应热点行，并用与文本相同的 `--top` 或 `--top-rows` 截断。`hot_rows_listed` 和 `hot_rows_omitted` 记录这个上限。analyze 追踪了键时 `hot_row_track_limit` 为 `8192`。新键替换了被碰到次数最少的键时，`hot_rows_overflow` 为 true，`hot_rows_note` 重复上限说明。`hot_row_unavailable` 列出收到了 UPDATE 或 DELETE、但 binlog 里没有主键的表。每条有 `schema`、`table`、`reason`（`metadata` 或 `values`）和 `message`。

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `schema` | string | 是 | schema 名 |
| `table` | string | 是 | 表名 |
| `primary_key` | string | 否 | `id=7` 或 `sku=BOLT, wh=1`。`--sql-context off` 时省略 |
| `key_hidden` | boolean | 否 | `--sql-context off` 隐藏了键值时为 true。次数仍在 |
| `touches` | integer | 是 | 带这个键的 UPDATE 和 DELETE 行镜像数。`approximate` 为 true 时才不是精确值 |
| `transactions` | integer | 是 | 碰到该键的不同事务数。从未被替换的键是精确的 |
| `first_time` | string | 否 | 第一次碰到的 RFC3339 UTC 时间 |
| `last_time` | string | 否 | 最后一次碰到的 RFC3339 UTC 时间 |
| `first_gtid` | string | 否 | 最早碰到该键的事务 GTID |
| `first_file` | string | 否 | 该事务起点所在文件的 basename |
| `first_pos` | integer | 否 | 该事务起点的字节偏移 |
| `last_gtid` | string | 否 | 最晚碰到该键的事务 GTID |
| `last_file` | string | 否 | 该事务起点所在文件的 basename |
| `last_pos` | integer | 否 | 该事务起点的字节偏移 |
| `approximate` | boolean | 否 | 触及 8192 上限后，该键继承了被丢掉键的次数时为 true |

### `threads`

`threads` 始终是数组，对应 Top Threads，并用与文本相同的 `--top` 或 `--top-threads` 截断。`threads_listed` 和 `threads_omitted` 记录这个上限。`threads_ranked_by` 为 `rows`、`events`、`bytes` 或 `transactions`；没有任何会话带 thread id 或调用方时省略。

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `thread_id` | integer | 否 | binlog 没有 thread id 时省略 |
| `server_id` | integer | 否 | 不可用时省略 |
| `actor` | object | 否 | binlog 存了调用方时的 `user` / `host` |
| `schema` | string | 否 | 按行数占优的 schema |
| `schemas` | array | 否 | 会话碰到多个 schema 时出现 |
| `rows` | integer | 是 | 影响行数 |
| `events` | integer | 是 | 规范化事件数 |
| `transactions` | integer | 是 | 该会话的事务数 |
| `binlog_bytes` | integer | 否 | 为 0 时省略 |
| `share` | number | 是 | 排序指标的占比 |
| `share_of_rows` | number | 是 | 已排名会话行数中的占比 |

### `patterns`

`patterns` 始终以数组形式存在。每个条目包含：

| Field | Type | Required | Notes |
|------|------|----------|------|
| `pattern_key` | string | yes | 工作负载模式的确定性结构标识 |
| `label` | string | yes | 面向人的模式描述 |
| `total_rows` | integer | yes | 该模式下事务贡献的总行数 |
| `txn_count` | integer | yes | 被归入该模式的事务数 |
| `event_count` | integer | yes | 被归入该模式的事件数 |
| `share_of_rows` | number | yes | 该模式占总分析行数的比例 |
| `share_of_txns` | number | yes | 该模式占总分析事务数的比例 |
| `avg_rows_per_txn` | number | yes | 该模式内每事务平均行数 |
| `tables` | object | yes | 该模式的表到行数聚合映射 |
| `operations` | object | yes | 该模式的操作到行数聚合映射 |
| `sample_query_summary` | string | no | 有可用 query summary 时的代表性摘要 |

### `statement_fingerprints`

`statement_fingerprints` 始终以数组形式存在。它是「语句指纹」章节背后的 `Rows_query` 语句形态排名，与文本章节一样受 `--top` 或 `--top-fingerprints` 限制；`statement_fingerprints_listed` 和 `statement_fingerprints_omitted` 统计这个限制。达到 8192 组追踪上限、有发生次数被丢弃时写出 `statement_fingerprints_overflow`。聚合覆盖整个已选工作负载——包括没有列入 `transactions` 的事务——并遵循同样的过滤条件。binlog 没有携带 `Rows_query` 文本时数组为空；`sql_context.available` 用来区分这种情况和空工作负载。条目按行数、语句次数、指纹文本排序。

| Field | Type | Required | Notes |
|------|------|----------|------|
| `fingerprint` | string | yes | 归一化语句形态；字面量为 `?`，字面量列表为 `(?)` |
| `sample` | string | no | 该组首次存储的语句文本；`--sql-context off` 时省略 |
| `statements` | integer | yes | 产生该形态的语句发生次数 |
| `rows` | integer | yes | 归因到这些发生的行镜像数 |
| `share_of_rows` | number | yes | 该形态占总分析行数的比例 |
| `tables` | object | yes | 该形态的每表行数映射 |

### `pattern_drilldowns`

`pattern_drilldowns` 始终以数组形式存在。它包含高信号模式的有界 drilldown 摘要。当没有模式达到选择阈值时为空数组。

每个条目包含：

| Field | Type | Required | Notes |
|------|------|----------|------|
| `pattern_key` | string | yes | 链回父 pattern 条目的标识 |
| `label` | string | yes | 面向人的模式描述 |
| `why_selected` | string | yes | 简短说明哪些信号触发了选择 |
| `share_of_rows` | number | yes | 该模式占总行数的比例 |
| `share_of_txns` | number | yes | 该模式占总事务数的比例 |
| `avg_rows_per_txn` | number | yes | 该模式内每事务平均行数 |
| `signal_flags` | object | yes | 表示 dominance 和/或 anomaly 的信号标志 |
| `busiest_minutes` | array | yes | 最多 2 个工作负载峰值分钟摘要（窗口级上下文，不保证完全属于该模式本身） |
| `representative_transactions` | array | yes | 最多 2 个工作负载事务摘要（窗口级上下文，不保证完全属于该模式本身） |

#### `signal_flags`

| Field | Type | Required | Notes |
|------|------|----------|------|
| `dominance` | boolean | yes | 该模式是否主导工作负载体量 |
| `anomaly` | boolean | yes | 该模式是否与尖峰对齐或异常集中 |

#### `busiest_minutes` 条目

| Field | Type | Required | Notes |
|------|------|----------|------|
| `minute` | string | yes | RFC3339 时间戳 |
| `total_rows` | integer | yes | 该分钟的行数 |
| `txn_count` | integer | yes | 该分钟的事务数 |

#### `representative_transactions` 条目

| Field | Type | Required | Notes |
|------|------|----------|------|
| `txn_key` | string | yes | 事务标识 |
| `total_rows` | integer | yes | 事务中的总行数 |
| `duration` | string | yes | Go duration 字符串 |
| `query_summary` | string | no | 可选的 query summary |

### `minutes`

`minutes` 始终以数组形式存在。每个条目包含：

| Field | Type | Required | Notes |
|------|------|----------|------|
| `minute` | string | yes | RFC3339 的分钟时间桶时间戳 |
| `total_rows` | integer | yes | 该分钟桶中的总行数 |
| `txn_count` | integer | yes | 该分钟内写入过 row image 的不同事务数。DDL 不计入。 |
| `table_rows` | object | no | JSON 对象，key 是表标识，value 是整数行数；为空时省略 |

### `alerts`

`alerts` 始终以数组形式存在。每个条目包含：

| Field | Type | Required | Notes |
|------|------|----------|------|
| `type` | string | yes | `large_transaction`、`open_dml_group`、`spike`、`input_format`、`partial_transaction`、`unknown_transaction` 或 `no_primary_key` |
| `severity` | string | yes | 当前告警严重级别字符串 |
| `message` | string | yes | 面向人的告警消息。它仍然写内部 `txn_key`，不写 GTID。 |
| `txn_key` | string | no | 事务级告警时出现 |
| `gtid` | string | no | 该事务有 GTID 时出现（MySQL `uuid:seq` 或 MariaDB `domain-server-seq`）。与 `transactions[].gtid` 是同一个字符串。匿名事务，以及不针对单个事务的告警（`spike`、`input_format`、`no_primary_key`）省略。可直接传给 `flashback --include-gtids`。 |
| `minute` | string | no | 分钟级告警时出现；出现时为 RFC3339 |
| `details` | object | no | 包含结构化告警细节的 JSON 对象；没有结构化细节时省略 |

`partial_transaction` 和 `unknown_transaction` 的 details 包含 `completeness`、逐事件窗口内的 `retained_span` 以及 `replay_available`。仅有 XID 的证据不能回放；跨文件端点也不能回放，因为 报告（v3 及以后）不携带完整且有序的中间文件列表。

### `warnings`

`warnings` 始终以整数计数形式存在。

它表示最终结果对象中累计的分析警告数量。这个值属于 `stdout` 上机器可读报告的一部分；它不是进度行数，也不是 `stderr` 消息数量。非零值表示分析已完成，但结果中记录了警告条件；并不表示 JSON 输出本身有问题。

当前实现中，当事务的 query context 因为达到 SQL 存储上限而被截断时，会增加这个计数。它不是数组。MIXED 少计通过 `alerts`（`type=input_format`）和 `diagnostics.ignored_query_dml_events` 表达，不把这个整数改成格式警告。

### `snapshot`

除非 `analyze` 使用 `--snapshot-name`，否则 `snapshot` 会被省略。出现时它是顶层平铺对象，而不是再包一层 metadata。

| Field | Type | Required | Notes |
|------|------|----------|------|
| `name` | string | yes | 由 `--snapshot-name` 选择的快照文件名 stem |
| `label` | string | yes | 当前实现中与 `name` 相同 |
| `created_at` | string | yes | 渲染报告时生成的 RFC3339 时间戳 |
| `binlogviz_version` | string | yes | 二进制内嵌的版本字符串 |
| `input_mode` | string | yes | 位置参数文件模式为 `files`，`--from-dir`/`--prefix` 模式为 `discovery` |
| `input` | object | yes | 快照输入详情 |
| `window` | object | yes | 快照时间窗口详情 |
| `filters` | object | yes | 快照 schema/table 过滤条件 |

#### `snapshot.input`

| Field | Type | Required | Notes |
|------|------|----------|------|
| `files` | array | yes | 本次 analyze 使用的有序输入文件路径 |
| `from_dir` | string | yes | discovery 目录；位置参数文件模式下为空字符串 |
| `prefix` | string | yes | discovery 前缀；位置参数文件模式下为空字符串 |

#### `snapshot.window`

| Field | Type | Required | Notes |
|------|------|----------|------|
| `start_time` | string | yes | RFC3339 时间戳；未设置 `--start` 时为空字符串 |
| `end_time` | string | yes | RFC3339 时间戳；未设置 `--end` 时为空字符串 |

#### `snapshot.filters`

| Field | Type | Required | Notes |
|------|------|----------|------|
| `include_schema` | array | yes | 包含的 schema；没有时为空数组 |
| `exclude_schema` | array | yes | 排除的 schema；没有时为空数组 |
| `include_table` | array | yes | 包含的表；没有时为空数组 |
| `exclude_table` | array | yes | 排除的表；没有时为空数组 |
| `dml` | array | no | `--dml` 的类型，顺序为 `INSERT`、`UPDATE`、`DELETE`。未设置该标志时省略。报告的 `scope` 也有这个字段 |

## Markdown 输出

Markdown 模式输出 GitHub Flavored Markdown 工单报告，包含工作负载摘要（包括 partial/unknown 计数和输入格式上下文）、热点表、热点事务（包括事务键、完整性、回放可用性、字节数、文件跨度和可信回放命令）、分钟级活动、DDL 时间线和发现。证据表会转义管道符；缺失跨度显示为 `N/A`，回放命令使用围栏代码块。

```bash
binlogviz analyze mysql-bin.000123 --format markdown > report.md
```

输出内容可直接传入任意 Markdown 渲染器，或粘贴到 GitHub Issue、PR 评论或 Wiki 页面。

## HTML 输出

HTML 模式输出自包含的单文件报告，所有样式、图表库（ECharts）和数据均内联嵌入，无需外部依赖或网络连接。

```bash
# 重定向：HTML 文档写入 stdout
binlogviz analyze mysql-bin.000123 --format html > report.html

# 交互式 TTY：写入 cwd 下的 mysql-bin.000123.html
binlogviz analyze mysql-bin.000123 --format html

# 指定输出路径
binlogviz analyze mysql-bin.000123 --format html --output report.html

# 强制 stdout
binlogviz analyze mysql-bin.000123 --format html --output -
```

报告包含：

- 摘要统计卡片（事务数、行数、事件数、时间范围）
- 交互式折线图：每分钟行数和事务数
- 交互式条形图：行数最多的热点表
- 交互式环形图：INSERT / UPDATE / DELETE 操作分布
- 热点表详情表格
- 被选中的高信号模式的 Pattern Drilldowns 分区（可折叠卡片，包含信号标志和指标帮助）

## CSV 和 TSV 输出

`--format csv` 和 `--format tsv` 把单个报告数据集写成一张扁平的分隔表输出到 stdout：一行表头加每元素一行记录，使用 Go `encoding/csv` 的 RFC 4180 引用规则（TSV 用制表符分隔）。输出是确定性的——行序与 JSON 报告的排名一致。空数据集也会写出表头，让消费方保留一份结构。这两种格式不支持 `--output`（用法错误，exit 3）；请重定向 stdout。

```bash
# 表聚合（默认数据集）
binlogviz analyze mysql-bin.000123 --format csv > tables.csv

# 事务排名，TSV
binlogviz analyze mysql-bin.000123 --format tsv --csv-dataset transactions > txns.tsv

# 来自 Rows_query 文本的语句指纹
binlogviz analyze mysql-bin.000123 --format csv --csv-dataset fingerprints > fps.csv
```

`--csv-dataset` 从四个数据集中选一个。在其他 `--format` 值下传它是用法错误（exit 3），未知或空的数据集名同样如此。

| 数据集 | 列 |
|---------|---------|
| `tables`（默认） | `schema`, `table`, `total_rows`, `insert_rows`, `update_rows`, `update_events`, `delete_rows`, `txn_count`, `event_count`, `binlog_bytes`, `ddl_count`, `key_status` |
| `transactions` | `txn_key`, `gtid`, `xa_xid`, `thread_id`, `xid`, `start_time`, `end_time`, `duration_ms`, `total_rows`, `event_count`, `binlog_bytes`, `completeness`, `binlog_file_start`, `binlog_file_end`, `pos_start`, `pos_end`, `tables`, `operations`, `query_summary` |
| `patterns` | `pattern_key`, `label`, `total_rows`, `txn_count`, `event_count`, `share_of_rows`, `share_of_txns`, `avg_rows_per_txn`, `tables`, `operations`, `sample_query_summary` |
| `fingerprints` | `fingerprint`, `statements`, `rows`, `share_of_rows`, `tables`, `sample` |

`tables`、`operations` 等映射单元格序列化为 `name=count` 对，用 `;` 连接且按名字排序。`transactions` 数据集列出的是与 JSON 报告相同的有界 top-N `transactions` 排名；`fingerprints` 数据集列出 `--top-fingerprints` 的有界排名（聚合本身始终覆盖整个已选工作负载）。`--sql-context off` 时 `query_summary`、`sample_query_summary`、`sample` 为空——计数仍在。

## Trend 输出

`binlogviz trend` 会为两个或更多 snapshot 生成按时间顺序排列的报告，沿用和其他命令相同的 stdout / stderr 分离规则，并支持 `text`、`json` 和 `html`。

文本输出会包含新的 `Top Pattern Trends` 章节和 `Key Findings` 章节（当存在趋势发现时），随后是 `Recommended Next Checks` 章节（当存在建议时）。JSON 输出会始终包含顶层 `pattern_trends` 数组（其中每个模式都带有 rows 和 share 序列）、`trend_summary` 数组（最多 5 条确定性发现对象）、`recommendations` 数组（基于发现的运维后续建议）以及 `pattern_drilldowns` 数组（高信号跨窗口模式份额变化的边界钻取摘要，无符合条件时为空数组）。趋势建议类别：`track_rising_pattern`、`confirm_declining_pattern`、`review_growing_table`、`watch_workload_concentration` 或 `capture_followup_snapshot`。每条发现可能包含 `evidence_refs`，将其链接回相关报告章节（`pattern_trends`、`table_trends`、`ordered_points`）。HTML 输出会包含交互式 `Pattern Trends` 分区（默认展示 `share of rows`，可切换到绝对 `rows`）、`Key Findings` 分区（当存在发现时，带有可点击的证据引用链接）、`Recommended Next Checks` 分区（带有优先级标签和证据链接），以及当检测到高信号模式时在趋势图表下方显示的钻取详情分区。

#### `pattern_drilldowns` 条目（trend）

每个条目包含：

| Field | Type | Required | Notes |
|------|------|----------|------|
| `pattern_key` | string | yes | 写入模式标识符 |
| `label` | string | yes | 人类可读的模式标签 |
| `why_selected` | string | yes | 一句话解释为何选中此模式进行钻取 |
| `start_share` | float | yes | 第一个快照中的行份额 |
| `end_share` | float | yes | 最后一个快照中的行份额 |
| `share_delta` | float | yes | 序列间的份额差异 |
| `start_rows` | integer | yes | 第一个快照中的行数 |
| `end_rows` | integer | yes | 最后一个快照中的行数 |
| `rows_delta` | integer | yes | 序列间的行数差异 |
| `signal_flags` | object | yes | 检测信号：`dominant_share_shift`、`steady_rise`、`steady_fall`、`concentrated_jump` |
| `key_points` | array | yes | 最多 2 条关键点摘要；每条包含 `label` 和 `summary` 字符串 |

最多返回 2 条钻取，按主导度评分排序。

## Compare JSON 输出

当 compare 结果需要交给脚本或其他工具消费时，请使用 `binlogviz compare --format json`。

```bash
binlogviz compare --current-snapshot current --baseline-snapshot baseline --format json
```

compare JSON 契约始终包含：

| Field | Type | Required | Notes |
|------|------|----------|------|
| `summary` | object | yes | 行数、事务数和 warning 的差异 |
| `key_findings` | array | yes | 确定性发现摘要，最多 5 条；信号不足时为空数组 |
| `recommendations` | array | yes | 基于关键发现的运维后续建议；无建议时为空数组 |
| `table_changes` | array | yes | 按表统计的行数差异 |
| `pattern_changes` | array | yes | 按 `pattern_key` 匹配、按绝对行数变化排序的写入模式变化 |
| `operation_mix` | array | yes | INSERT / UPDATE / DELETE 差异 |
| `alert_changes` | object | yes | 新增和移除的告警 |
| `current_label` | string | yes | 有快照元数据时使用快照感知标签，否则为 `current` |
| `baseline_label` | string | yes | 有快照元数据时使用快照感知标签，否则为 `baseline` |
| `pattern_drilldowns` | array | yes | 高信号跨窗口模式变化的边界钻取摘要；无符合条件时为空数组 |
| `current_snapshot` | object | no | 当前输入报告包含 analyze 快照元数据时出现 |
| `baseline_snapshot` | object | no | 基线输入报告包含 analyze 快照元数据时出现 |

`current_snapshot` 和 `baseline_snapshot` 复用上方 analyze `snapshot` 对象的字段契约。

#### `pattern_drilldowns` 条目（compare）

每个条目包含：

| Field | Type | Required | Notes |
|------|------|----------|------|
| `pattern_key` | string | yes | 写入模式标识符 |
| `label` | string | yes | 人类可读的模式标签 |
| `why_selected` | string | yes | 一句话解释为何选中此模式进行钻取 |
| `baseline_rows` | integer | yes | 基线窗口的行数 |
| `current_rows` | integer | yes | 当前窗口的行数 |
| `delta_rows` | integer | yes | 窗口间的行数差异 |
| `baseline_txns` | integer | yes | 基线窗口的事务数 |
| `current_txns` | integer | yes | 当前窗口的事务数 |
| `delta_txns` | integer | yes | 窗口间的事务数差异 |
| `signal_flags` | object | yes | 检测信号：`dominant_delta`、`new_pattern`、`disappeared`、`txn_rows_diverged` |
| `key_points` | array | yes | 最多 2 条关键点摘要；每条包含 `label` 和 `summary` 字符串 |

最多返回 2 条钻取，按主导度评分排序。

`current_snapshot` 和 `baseline_snapshot` 复用上文 analyze `snapshot` 的同一字段契约。

#### `key_findings` 条目

`key_findings` 数组中的每个条目包含：

| Field | Type | Required | Notes |
|------|------|----------|------|
| `kind` | string | yes | 发现类别：`volume_change`、`pattern_driver`、`table_driver`、`operation_mix_drift` 或 `new_pattern` |
| `title` | string | yes | 简短的人类可读标题 |
| `summary` | string | yes | 单句基于证据的摘要 |
| `evidence` | object | yes | 包含支撑指标的结构化键值映射 |
| `evidence_refs` | array | no | 指向报告章节的可追溯链接；为空时省略 |

每个 `evidence_refs` 条目包含：

| Field | Type | Required | Notes |
|------|------|----------|------|
| `section` | string | yes | 发现所链接的报告章节（如 `table_changes`、`pattern_changes`、`operation_mix`） |
| `key` | string | no | 章节内的项目键（如 `orders.refunds`）；章节级引用时省略 |
| `label` | string | yes | 链接项目的人类可读标签 |
| `anchor` | string | yes | 用于页面内导航的 HTML 锚点 ID |

### 与旧文件模式的兼容性

快照工作流并不会替换原来的 compare 路径。`compare` 仍然支持两个显式 analyze JSON 文件：

```bash
binlogviz compare current.json baseline.json --format json
```

兼容性规则：

- 文件模式仍然完全受支持
- 如果输入报告本身已经带有顶层 `snapshot` 元数据，compare 会把它再次暴露为 `current_snapshot` 和 `baseline_snapshot`
- 较早生成、没有 snapshot 元数据的 analyze JSON 也可以继续成功对比
- 当缺少 snapshot 元数据时，compare 会回退到 `current` 和 `baseline` 标签，并省略 `current_snapshot` / `baseline_snapshot`

## Snapshot 命令输出

`snapshot` 子命令使用以下输出契约：

- `snapshot save` 不向 `stdout` 写入载荷，而是在 `stderr` 打印 `Saved snapshot "<name>" to <path>`
- `snapshot list --format text` 以面向人的表格输出 `name`、`label`、`created_at`、`input_mode`、`window` 和 `diagnostics`。diagnostics 单元格是 `-`，除非该文件的 `diagnostics_mode` 是不等于 `full` 的字符串（把 `--diagnostics summary` 或 `off` 的文档拷进快照目录）。list 仍然以 exit 0 结束
- `snapshot list --format json` 输出包含 `snapshot_dir` 和 `snapshots` 的机器可读对象。`schema_version` 仍是 1。裁剪过的条目带 `diagnostics_mode`，完整快照省略该字段
- `snapshot show --format text` 把元数据和摘要块输出到 `stdout`
- `snapshot show --format json` 输出一个机器可读对象，并把规范化 descriptor 放在 `snapshot` 字段下
- `snapshot rename` 和 `snapshot delete` 不会向 `stdout` 输出报告；成功时会把确认信息打印到 `stderr`

### 主题

HTML 报告 header 右侧提供主题切换器（五个彩色圆点）。可用主题：**Nebula**（默认，深色靛紫/青色）、**Forest**（深色翠绿/琥珀）、**Navy**（深色天蓝/金色）、**Ember**（深色橙/玫红）、**Light**（浅色）。主题选择保存在 `localStorage`，下次打开自动恢复。

## stderr 隔离

BinlogViz 会把最终报告输出保留在 `stdout`。

对于 `analyze`，`stderr` 会承载进度、discovery 解析结果、最终组装状态和错误。对于 `compare`，当前实现不会输出 analyze 风格的进度信息；compare 报告写到 `stdout`，命令失败时通过 CLI 错误链路写到 `stderr`。

### `analyze` 会写到 `stderr` 的内容

`analyze` 会把以下内容写到 `stderr`：

- 解析进度输出
- discovery 模式下的 `Resolved binlog files:` 列表
- `Finalizing analysis...`
- `--format html` 写入文件时的 `HTML report saved to …`
- `--start`/`--end` 匹配到 0 个事件时的 `Error: window matched 0 events`（exit 2，stdout 为空）
- 生效的 schema/对象过滤既没有行活动、也没有 view、event、routine 或 trigger DDL 时的 `Error: schema/table filter matched no events`（exit 2，stdout 为空）。匹配到 view、event、function、procedure 或 trigger 时退出 0，并打印这条 DDL。
- 设置了 `--dml` 但没有计入这些类型的行镜像时：`Error: dml filter matched no events`（exit 2，stdout 为空）。两种过滤都没有匹配时，先报这条，再考虑 schema/table 那条。
- 仅 Format Description（或再加 rotate）的完整 binlog：`Error: binlog has no analyzable events`（exit 2，stdout 为空）
- 文件在事件中间截断，或最后一个完整事件之后还有剩余字节：`Error: binlog is truncated or corrupt: …`（exit 1，stdout 为空）
- 命令错误（未另行说明时为 exit 1）

进度条会停在当前 stderr 行上，直到解析结束。随后的 `Error:` 会先清掉这一行，不会接在进度条后面。

### 为什么这很重要

这种行为让你可以安全地：

- 把文本输出重定向到文件
- 把 JSON 输出重定向到另一个工具
- 查看 analyze 的 discovery 结果和进度，而不会污染报告流

## Compare 输出

当你已经有两份由 `binlogviz analyze --format json` 生成的 JSON 报告，并且想看当前窗口相对基线窗口发生了什么变化时，使用 `binlogviz compare`。

```bash
binlogviz compare current.json baseline.json
binlogviz compare current.json baseline.json --format json > compare.json
binlogviz compare current.json baseline.json --format html > compare.html
```

compare 命令只接受两份 BinlogViz analyze JSON 报告：

- `current.json`：要检查的当前窗口
- `baseline.json`：用于对比的基线窗口

### Compare 文本输出

文本模式会渲染固定结构的 compare 报告，适合终端快速阅读。内容包括：

- `Current Label` 和 `Baseline Label`；如果存在 snapshot 元数据则优先使用 snapshot-aware label，否则回退到 `current` 和 `baseline`
- 当存在 snapshot 元数据时，还会带出请求时间窗口、input mode、来源摘要和过滤条件
- 带有明确基线/当前标签的输入文件大小和已计数事件字节行
- 行数、事务数、warnings 的顶层 delta
- 按绝对行数变化排序的热点表变化
- 位于 warnings 和 `Top Table Changes` 之间的 `Key Findings`（当发现存在时），带有 `evidence:` 标签将发现链接到相关报告章节
- 位于 `Key Findings` 之后的 `Recommended Next Checks`（当建议存在时），带有优先级标签和 `evidence:` 文本标签
- 位于 `Top Table Changes` 和 `Operation Mix` 之间的 `Top Pattern Changes`
- `INSERT` / `UPDATE` / `DELETE` 的操作类型变化
- 告警新增和移除情况

适合 DBA 快速判断当前窗口是否比基线更重、更偏向某种操作，或者是否出现了新的告警。

### Compare JSON 输出

当另一个工具需要结构化 compare 数据时，使用 `--format json`。

```bash
binlogviz compare current.json baseline.json --format json
```

JSON 输出会以稳定的 snake_case 结构序列化 compare 结果。

#### Compare 顶层契约

| Field | Type | Required | Notes |
|------|------|----------|------|
| `summary` | object | yes | current / baseline 汇总值和 delta |
| `key_findings` | array | yes | 确定性发现摘要，最多 5 条；信号不足时为空数组 |
| `recommendations` | array | yes | 基于关键发现的运维后续建议；无建议时为空数组 |
| `table_changes` | array | yes | 按绝对变化排序的表级行数差异 |
| `pattern_changes` | array | yes | 按 `pattern_key` 匹配、按绝对行数变化排序的写入模式变化 |
| `operation_mix` | array | yes | `insert`、`update`、`delete` 的操作差异 |
| `alert_changes` | object | yes | 新增和移除的告警 |
| `current_label` | string | yes | 如果 current 输入带 snapshot 元数据则输出 snapshot-aware label，否则为 `current` |
| `baseline_label` | string | yes | 如果 baseline 输入带 snapshot 元数据则输出 snapshot-aware label，否则为 `baseline` |

从用户视角看，JSON 输出回答的仍然是文本报告中的同一批运维问题，只是更适合进入脚本、仪表盘或自动化链路。

#### `recommendations` 条目

`recommendations` 数组中的每个条目包含：

| Field | Type | Required | Notes |
|------|------|----------|------|
| `kind` | string | yes | 建议类别（compare）：`check_pattern_driver`、`check_table_hotspot`、`check_new_write_pattern`、`check_operation_mix_shift`、`check_volume_growth_source` 或 `check_volume_drop_source` |
| `priority` | string | yes | 后续优先级：`high` 或 `medium` |
| `title` | string | yes | 简短的人类可读标题 |
| `summary` | string | yes | 单句可执行建议 |
| `rationale` | string | yes | 生成该建议的原因 |
| `related_finding_kinds` | array | yes | 触发该建议的发现类别 |
| `evidence_refs` | array | no | 指向报告章节的可追溯链接；为空时省略 |

建议使用保守语言（检查、确认、审查、捕获），不会声称根因。优先级表示后续紧迫程度，而非事件严重程度。最多返回 5 条建议，按优先级和类别排序。

### Compare HTML 输出

HTML 模式输出自包含的可视化 compare 报告，不是把文本 diff 简单包在 HTML 里。

```bash
binlogviz compare current.json baseline.json --format html > compare.html
```

报告包含图表化分区：

- baseline 和 current 顶层汇总对比
- 按行数变化排序的热点表变化
- 按行数变化排序的模式变化
- 操作类型分布对比
- 告警新增 / 移除可视化

如果输入带 snapshot 元数据，HTML header 还会带出 label、请求时间窗口、input mode、来源摘要和过滤条件等 compare 上下文。

页面同时包含 compare summary 卡片和明细表 / 明细列表，方便操作者在图表视图和具体表、具体告警之间快速切换。

### Compare 在 `stderr` 上的错误行为

`compare` 当前不会输出 analyze 风格的进度信息。它会把最终 compare 报告写到 `stdout`；如果命令失败，CLI 会通过 `stderr` 输出错误。

## Workflow Status 输出

`binlogviz workflow status` 用于以只读方式报告一个已有 workflow root 的运行时状态。

### 文本输出

文本模式会把面向人的检查摘要写到 `stdout`。

顶层区块包含：

- `Workflow Status`
- `Output Root`
- `Manifest Version`
- `Mode`
- `Attempt`
- `Status`
- `Runtime State`
- `Resumable`
- 当 `resume_error` 非空时出现可选的 `Reason`

随后会渲染：

- `Steps` 分区，逐条列出 manifest 中记录的步骤
- 每个步骤的 `status`
- 可选的每步骤 `execution`
- 每个步骤的 artifact presence，展示已记录的相对路径，并把缺失文件标记为 missing
- 只有在已持久化数组非空时才渲染的 `Workflow Recommendations`、`Workflow Findings` 和 `Workflow Summary Warnings` 分区
- 可选的 `Resume Preview` 分区，给出 dry-run 的 `reuse` / `rerun` 决策及其原因

代表性含义：

- `Runtime State: complete` 表示 manifest 有 `resolved_input_files`、至少有一个成功 step、所有已记录 artifact 都存在，且在保存的 plan 仍可加载时 resume 所需的可复用 snapshot 也仍然存在
- `Runtime State: incomplete` 表示没有成功 step、`resolved_input_files` 为空、至少有一个已记录 artifact 缺失，或者某个成功的 analyze 步骤所需的可复用 snapshot 已缺失
- `Resumable: yes` 表示该 root 通过了 resume 校验
- `Resumable: no` 且带 `Reason:` 表示该 root 仍可检查，但当前不能 resume

### JSON 输出

JSON 模式会把单个机器可读对象写到 `stdout`。

顶层字段：

| Field | Type | Required | Notes |
|------|------|----------|------|
| `workflow_name` | string | yes | manifest 中记录的 workflow 名称 |
| `output_dir` | string | yes | 本次检查的输出根目录 |
| `manifest_version` | integer | yes | Manifest 契约版本 |
| `mode` | string | yes | `run` 或 `resume` |
| `attempt` | integer | yes | manifest 中记录的 attempt 计数 |
| `status` | string | yes | manifest 顶层状态，例如 `success` 或 `failed` |
| `runtime_state` | string | yes | 基于当前运行时检查得到的 `complete` 或 `incomplete` |
| `resumable` | boolean | yes | 当前是否允许 resume |
| `resume_error` | string | yes | 可 resume 时为空；否则为解释性错误字符串 |
| `workflow_summary` | object | yes | 已持久化的 manifest summary；以规范化数组形式直接透传，不会重建 |
| `steps` | array | yes | 每步骤的运行时检查记录 |
| `resume_preview` | array | no | dry-run resume 决策；当无法基于 plan 构建 preview 时省略 |

每个 `steps` 条目包含：

| Field | Type | Required | Notes |
|------|------|----------|------|
| `kind` | string | yes | 步骤类型，例如 `analyze`、`compare` 或 `trend` |
| `name` | string | yes | manifest 中记录的步骤名 |
| `status` | string | yes | manifest 中记录的步骤状态 |
| `execution` | string | no | 存在时为已记录的执行标签 |
| `artifacts` | array | no | 当前 artifact presence 记录 |

每个 `artifacts` 条目包含：

| Field | Type | Required | Notes |
|------|------|----------|------|
| `path` | string | yes | 相对于 `<output_dir>` 的 artifact 路径 |
| `exists` | boolean | yes | 该 artifact 文件当前是否存在 |

每个 `resume_preview` 条目包含：

| Field | Type | Required | Notes |
|------|------|----------|------|
| `kind` | string | yes | 计划中的步骤类型 |
| `name` | string | yes | 计划中的步骤名 |
| `action` | string | yes | `reuse` 或 `rerun` |
| `reason` | string | yes | 该 dry-run 决策的解释 |

Legacy manifest 在 text 和 JSON 模式下都仍然可被检查。此时命令依然会渲染状态，但会返回 `resumable: false` 和非空的 `resume_error`。

### status 中的信任边界失败

当 manifest 中记录的 plan 路径解析到 workflow root 之外或通过符号链接逃逸时，`workflow status` 会将该 plan 视为不可信。命令仍然成功，但：

- `resumable` 被设为 `false`
- `resume_error` 包含信任边界说明字符串
- `resume_preview` 被省略
- 其余字段（steps、workflow_summary、runtime_state）正常报告

信任检查由 `ValidateWorkflowPlanPath(outputDir, planPath)` 在 plan 文件打开前执行。

## Workflow Clean 输出

`binlogviz workflow clean` 用于报告当前 manifest 已不再引用的 workflow 生成文件，并可选报告孤儿 snapshot JSON 文件。

### 文本输出

文本模式会把清理摘要写到 `stdout`，包含这些区块：

- workflow 名称和输出根目录
- 清理模式：`dry-run` 或 `apply`
- 是否包含 snapshots
- artifact orphans、snapshot orphans、deleted、skipped 的聚合计数
- `Orphaned Artifacts`
- `Orphaned Snapshots`
- `Deleted`
- `Skipped`

代表性行为：

- dry-run 模式下，`Deleted` 保持为空
- 不加 `--include-snapshots` 时，`Orphaned Snapshots` 会显示 `none`
- apply 模式下，删除成功的路径会出现在 `Deleted`
- 删除失败的路径会出现在 `Skipped`

### JSON 输出

JSON 模式会向 `stdout` 写出单个机器可读对象。

顶层字段：

| Field | Type | Required | Notes |
|------|------|----------|------|
| `workflow_name` | string | yes | manifest 中记录的 workflow 名称 |
| `output_dir` | string | yes | 本次检查的输出根目录 |
| `mode` | string | yes | `dry-run` 或 `apply` |
| `include_snapshots` | boolean | yes | 是否启用了 snapshot cleanup |
| `artifact_orphans` | array | yes | `analyze/`、`compare/`、`trend/` 下的相对 artifact 路径 |
| `snapshot_orphans` | array | yes | `manifest.snapshot_dir` 下的 snapshot 文件名 |
| `deleted` | array | yes | 删除成功的候选路径或 snapshot 文件名 |
| `skipped` | array | yes | 无法删除的候选路径或 snapshot 文件名 |
| `counts` | object | yes | cleanup 聚合计数 |

`counts` 对象包含：

| Field | Type | Required | Notes |
|------|------|----------|------|
| `artifact_orphans` | integer | yes | 孤儿 workflow artifact 数量 |
| `snapshot_orphans` | integer | yes | 孤儿 snapshot 数量 |
| `deleted` | integer | yes | 删除成功数量 |
| `skipped` | integer | yes | 跳过删除数量 |

### 失败行为

- `manifest.json` 缺失或不可读时，在渲染前直接失败
- workflow artifact 目录不可读时，在渲染前直接失败
- snapshot 目录缺失时返回零个 snapshot 候选，而不是错误
- apply 模式下出现 skipped 删除时，仍会先写出完整结果，再返回非零命令错误

## Workflow Export 输出

`binlogviz workflow export` 用于报告把一个已有 workflow root 打包成确定性 zip archive 的结果。

### 文本输出

文本模式会向 `stdout` 写出紧凑的运维摘要：

- `Workflow Export`
- `Workflow: <workflow_name>`
- `Output Root: <output_dir>`
- `Archive: <archive_path>`
- `Format: zip`
- `Included Files: <count>`
- `Included Snapshots: <count>`
- 可选的 `Warnings` 分区，其中每条 warning 都以 `- ` 开头

代表性行为：

- 只有在 archive 创建成功后才会写出该摘要
- `Format` 表示 archive 格式（`zip`），而不是 CLI 渲染模式
- `Included Files` 统计所有被纳入归档的 entry，包括 `manifest.json`、存在时的 `index.html`、manifest 声明的 artifacts、通过校验的 `plan.yaml` 以及可选 snapshots
- `Included Snapshots` 只统计归档中位于 `snapshots/` 下的 entry
- 缺失的可选输入（如 `index.html`、manifest artifact、被引用的 snapshot，或解析后落在 workflow root 之外、已不再匹配 `manifest.plan_sha256` / 已无法解析为记录 workflow 元数据的 `plan.yaml`）会进入 `Warnings`
- 当 `--include-snapshots` 开启且 `manifest.snapshot_dir` 为空时，结果会记录 warning，而不会回退去读取当前工作目录

### JSON 输出

JSON 模式会向 `stdout` 写出单个机器可读对象。

顶层字段：

| Field | Type | Required | Notes |
|------|------|----------|------|
| `workflow_name` | string | yes | 从 `manifest.json` 读取的 workflow 名称 |
| `output_dir` | string | yes | 传给命令的 workflow root |
| `archive_path` | string | yes | 最终 archive 输出路径 |
| `format` | string | yes | 当前实现中固定为 `zip` |
| `included_files` | integer | yes | 经过确定性去重后的归档 entry 数量 |
| `included_snapshots` | integer | yes | 位于 `snapshots/` 下的归档 entry 数量 |
| `warnings` | array<string> | yes | 导出过程中收集到的 warning；没有时为空数组 |

### 通道行为

- 所有 export 结果载荷都写到 `stdout`
- 命令不会向 `stderr` 输出进度行
- 命令失败时继续沿用 CLI 默认错误链路

## Workflow Validate 输出

`binlogviz workflow validate` 用于在真正执行前报告一份 plan 在静态层面是否有效。
它会拒绝重复 compare / trend 作业名，以及单个 compare / trend 作业内重复的 format 条目。

### 文本输出

文本模式会向 `stdout` 写出以下两类结构之一：

- 成功：`Workflow plan valid`，随后输出 workflow 名称、window 数量、compare 作业数量、trend 作业数量和 output root
- 失败：`Workflow plan invalid`，随后输出校验错误消息

### JSON 输出

成功输出包含：

| Field | Type | Required | Notes |
|------|------|----------|------|
| `valid` | boolean | yes | 合法 plan 时为 `true` |
| `workflow_name` | string | yes | plan 中的 workflow 名称 |
| `windows` | integer | yes | analyze window 数量 |
| `compare_jobs` | integer | yes | compare 作业数量 |
| `trend_jobs` | integer | yes | trend 作业数量 |
| `output_dir` | string | yes | plan 中声明的输出根目录 |

失败输出包含：

| Field | Type | Required | Notes |
|------|------|----------|------|
| `valid` | boolean | yes | 始终为 `false` |
| `error` | string | yes | 校验或文件读取错误消息 |

## Workflow Describe 输出

`binlogviz workflow describe` 用于只基于 plan 数据报告一份合法 plan 将如何执行。

### 文本输出

文本模式会按顺序向 `stdout` 写出以下分区：

1. workflow 头部，包含 workflow 名称、output root 和 snapshot-save 设置
2. `Analyze Windows`，列出每个命名 window、RFC3339 的 start/end、计划中的 analyze artifact 路径，以及可选的 snapshot 名称
3. `Compare Jobs`，列出每个作业名、声明的依赖关系，以及计划中的 compare artifact 路径
4. `Trend Jobs`，列出每个作业名、声明的 snapshot 依赖，以及计划中的 trend artifact 路径

### JSON 输出

JSON 描述包含：

| Field | Type | Required | Notes |
|------|------|----------|------|
| `workflow_name` | string | yes | plan 中的 workflow 名称 |
| `output_dir` | string | yes | plan 中的输出根目录 |
| `snapshot_save` | boolean | yes | analyze windows 是否计划生成命名快照 |
| `windows` | array | yes | 按顺序排列的 analyze window 描述 |
| `compare` | array | yes | 按顺序排列的 compare 作业描述 |
| `trend` | array | yes | 按顺序排列的 trend 作业描述 |

每个 `windows` 条目包含 `name`、`start`、`end`、`artifacts` 和可选的 `snapshot_name`。
每个 `compare` 条目包含 `name`、`current`、`baseline` 和 `artifacts`。
每个 `trend` 条目包含 `name`、`snapshots` 和 `artifacts`。

plan 文件打不开或加载失败时，JSON stdout 与 `workflow validate` 的失败对象相同：

| Field | Type | Required | Notes |
|------|------|----------|------|
| `valid` | boolean | yes | 始终为 `false` |
| `error` | string | yes | 解码或读文件的错误消息 |

发布的 schema 是上面的成功对象和这个失败对象的 `oneOf`。成功文档仍然要求 `workflow_name`、`output_dir` 和 `snapshot_save`。

## Workflow Manifest

`workflow run` 和 `workflow resume` 会把 `manifest.json` 写入 `<output_dir>/manifest.json`。Manifest v2 增加了支持 resume 工作流的字段。

### Manifest v2 字段

| Field | Type | Required | Notes |
|------|------|----------|------|
| `manifest_version` | integer | yes | Manifest 契约版本；当前版本为 `2` |
| `status` | string | yes | `success` 或 `failed` |
| `mode` | string | yes | `run` 表示全新执行，`resume` 表示恢复执行 |
| `attempt` | integer | yes | 执行尝试编号；`run` 从 `1` 开始，每次 `resume` 递增 |
| `plan_sha256` | string | yes | 首次运行时 plan 文件的 SHA-256 哈希 |
| `resolved_input_files` | array<string> | yes | discovery 阶段解析出的输入文件路径列表 |
| `snapshot_dir` | string | yes | 执行期间使用的快照目录 |
| `workflow_summary` | object | yes | 基于成功 compare/trend JSON artifact 重建的 workflow 级汇总 |
| `steps` | array | yes | 每个步骤的状态记录 |
| `error` | string | no | 当 `status` 为 `failed` 时出现，包含失败消息 |

### `workflow_summary`

`workflow_summary` 始终会被规范化成如下 shape：

| Field | Type | Required | Notes |
|------|------|----------|------|
| `findings` | array | yes | 来源于 compare `key_findings` 和 trend `trend_summary` 的 workflow 级发现 |
| `recommendations` | array | yes | 来源于 compare/trend `recommendations` 的 workflow 级建议 |
| `warnings` | array<string> | yes | best-effort 聚合 warnings |

行为说明：

- 只有成功的 `compare` 和 `trend` 步骤才会贡献 summary 项
- summary 提取只读取 JSON artifact
- 缺少必需顶层数组时会追加 warning；字段存在但为空数组属于有效低信号输入，不会产生 warning
- summary warnings 会记录缺失、不可读或无效的 summary 来源
- summary warnings 不会改变 workflow 成功/失败语义
- workflow summary 的源报告链接优先使用 HTML；只有 HTML 源报告的 evidence link 才会追加 `#anchor`，JSON 回退链接不会带锚点
- 只有在对应数组非空时，`index.html` 才会渲染 `Workflow Recommendations`、`Workflow Findings` 和 `Workflow Summary Warnings`

### 每步骤字段

`steps` 数组中的每个条目包含：

| Field | Type | Required | Notes |
|------|------|----------|------|
| `kind` | string | yes | 步骤类型：`analyze`、`compare` 或 `trend` |
| `name` | string | yes | plan 中的步骤名 |
| `status` | string | yes | `success` 或 `failed` |
| `execution` | string | no | `executed`（步骤被执行）或 `reused`（步骤从上次运行中沿用） |
| `artifacts` | array<string> | no | 相对于 `<output_dir>` 的计划产物路径列表 |
| `snapshot_name` | string | no | analyze 步骤开启 snapshot 保存时出现 |
| `error` | string | no | 步骤失败时出现 |

### Resume 与 Manifest 的交互

- `workflow resume` 会读取已有的 `manifest.json` 来判断哪些步骤已成功、可以复用
- Resume 会拒绝旧版 pre-v2 manifest（缺少 `manifest_version` 字段）
- Resume 会拒绝在 plan 文件哈希与 manifest 中的 `plan_sha256` 不匹配时继续执行
- Resume 会拒绝在 plan 路径解析到 workflow root 之外或通过符号链接逃逸时继续执行（信任边界硬拒绝，由 `ValidateWorkflowPlanPath(outputDir, planPath)` 在文件打开前强制执行）
- `ValidateResumableManifest` 现在接受四个参数 `(m Manifest, outputDir string, planPath string, planSHA256 string)`，以在 resume 校验期间强制执行信任边界
- 更新后的 manifest 会保留原始运行的所有字段，并更新 `mode`、`attempt`、`steps` 和 `status`

### index.html 更新

当 manifest 包含 `mode: resume` 时，`index.html` 会展示：

- resume mode 标签
- 当前 attempt 编号
- 每个步骤的执行标签（`executed` 或 `reused`），与步骤状态并列展示

## 示例

### 保存文本输出，同时在终端保留状态信息

```bash
binlogviz analyze mysql-bin.000123 > report.txt
```

### 保存 JSON 输出供下游处理

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. --format json > report.json
```

### 生成 Markdown 报告

```bash
binlogviz analyze mysql-bin.000123 --format markdown > report.md
```

### 生成自包含 HTML 报告

```bash
binlogviz analyze mysql-bin.000123 --format html
```

TTY 下写入推导出的 cwd 文件（例如 `mysql-bin.000123.html`）。stdout 被重定向时 HTML 文档写入 stdout。使用 `--output report.html` 指定路径，或 `--output -` 强制 stdout。文件完全自包含——所有图表和样式均已内联嵌入，无需互联网连接，在任意浏览器中打开即可。

### 分别捕获两个输出通道

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. \
  > report.txt \
  2> analyze.stderr.log
```
