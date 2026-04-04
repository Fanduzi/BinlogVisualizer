# 输出格式参考

本文档说明 `binlogviz analyze` 和 `binlogviz compare` 会向 `stdout` 和 `stderr` 分别写入什么内容。

如果你想先看最短运维路径，请先阅读[快速开始](../recipe/quickstart.zh-CN.md)或[分析本地 Binlog](../recipe/analyze-local-binlogs.zh-CN.md)。

## 输出通道契约

BinlogViz 会把不同用途的输出写到不同通道：

- `analyze`：`stdout` 承载最终分析报告；`stderr` 承载进度、discovery 解析出的文件列表、最终组装状态、快照保存确认以及运行时错误。
- `compare`：`stdout` 承载最终 compare 报告；命令失败时由 CLI 通过 `stderr` 输出错误。

这种分离很重要，因为它能让报告输出保持适合重定向和自动化处理。

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. --format json > analyze.json
```

在上面的例子中，JSON 报告会写入 `analyze.json`，而进度和 discovery 状态仍然通过 `stderr` 显示在终端上。

如果再加上 `--snapshot-name`，JSON 载荷仍然写到 `stdout`，同时 BinlogViz 会在 `stderr` 上打印类似 `Saved snapshot "incident_current" to /home/user/.binlogviz/snapshots/incident_current.json` 的保存确认。

## 可用格式

### `analyze`

| 参数值 | 别名 | 说明 |
|---|---|---|
| `text` | — | 默认，人类可读的终端输出。 |
| `json` | — | 机器可读的 JSON 格式。 |
| `markdown` | `md` | 含表格的 GitHub Flavored Markdown。 |
| `html` | — | 含交互式图表和主题切换器的自包含 HTML 文件。 |

### `compare`

| 参数值 | 说明 |
|---|---|
| `text` | 默认。适合终端阅读的 compare 差异摘要。 |
| `json` | 机器可读的 compare 结果。 |
| `html` | 带交互式图表的自包含可视化 compare 报告。 |

## 文本输出

文本模式是默认报告格式。它会渲染固定的五个章节。

### 1. Workload Summary

`Workload Summary` 章节提供分析结果集的顶层汇总：

- 总事务数
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

### 3. Top Transactions

`Top Transactions` 章节按总行数对事务进行排序，并展示事务大小和持续时间。

文本输出中常见字段包括：

- 事务 key
- 总行数
- 持续时间
- 事件数
- 当 `--sql-context` 启用时的 SQL 上下文行

示例标题：

```text
=== Top Transactions ===
```

### 4. Minute Activity

`Minute Activity` 章节汇总每分钟的写入活动。

文本输出中常见字段包括：

- 分钟时间桶
- 该分钟的总行数
- 该分钟的事务数

示例标题：

```text
=== Minute Activity ===
```

### 5. Alerts

`Alerts` 章节列出分析逻辑检测到的告警。

示例包括：

- `large_transaction`
- `spike`

示例标题：

```text
=== Alerts ===
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
| `summary` | object | yes | 总体汇总和时间边界 |
| `tables` | array | yes | Top 表聚合结果；没有表结果时为空数组 |
| `transactions` | array | yes | Top 事务聚合结果；没有事务结果时为空数组 |
| `minutes` | array | yes | 每分钟聚合结果；没有分钟桶时为空数组 |
| `alerts` | array | yes | 检测到的告警；没有告警时为空数组 |
| `warnings` | integer | yes | 最终结果中记录的分析警告数量 |
| `snapshot` | object | no | 仅在 `analyze` 使用 `--snapshot-name` 时出现 |

### `summary`

`summary` 始终存在，包含：

| Field | Type | Required | Notes |
|------|------|----------|------|
| `total_transactions` | integer | yes | 总分析事务数 |
| `total_rows` | integer | yes | 总影响行数 |
| `total_events` | integer | yes | 纳入分析的标准化事件总数 |
| `start_time` | string | yes | RFC3339 时间戳；如果没有可用时间戳则为空字符串 |
| `end_time` | string | yes | RFC3339 时间戳；如果没有可用时间戳则为空字符串 |
| `duration` | string | yes | 最终结果中的持续时间字符串 |

### `tables`

`tables` 始终以数组形式存在。每个条目包含：

| Field | Type | Required |
|------|------|----------|
| `schema` | string | yes |
| `table` | string | yes |
| `total_rows` | integer | yes |
| `insert_rows` | integer | yes |
| `update_rows` | integer | yes |
| `delete_rows` | integer | yes |
| `txn_count` | integer | yes |

### `transactions`

`transactions` 始终以数组形式存在。每个条目始终包含：

| Field | Type | Required | Notes |
|------|------|----------|------|
| `txn_key` | string | yes | 报告中使用的事务标识 |
| `start_time` | string | yes | RFC3339 时间戳；未设置时为空字符串 |
| `end_time` | string | yes | RFC3339 时间戳；未设置时为空字符串 |
| `duration` | string | yes | 持续时间字符串 |
| `total_rows` | integer | yes | 事务触及的总行数 |
| `event_count` | integer | yes | 事务中的事件数量 |
| `tables` | object | no | JSON 对象，key 是表名，value 是整数计数；为空时省略 |
| `operations` | object | no | JSON 对象，key 是操作名，value 是整数计数；为空时省略 |
| `query_summary` | string | no | 当 SQL 上下文模式抑制该字段，或没有摘要时省略 |
| `query_sql` | string | no | 仅在 `--sql-context full` 且存在受限 SQL 上下文时出现 |
| `query_truncated` | boolean | no | 没有 query 上下文时省略；出现时表示存储的 SQL 是否被截断 |
| `query_original_bytes` | integer | no | 没有 query 上下文时省略；出现时表示原始 SQL 的字节长度 |

#### SQL 上下文模式行为

`transactions` 中的 query 字段取决于 `--sql-context`：

- `off`：省略所有 query 相关字段
- `summary`：包含 `query_summary`；只有在存在 query 上下文时才包含 `query_truncated` 和 `query_original_bytes`
- `full`：包含 `query_summary`；在存在 query 上下文时包含 `query_sql`、`query_truncated` 和 `query_original_bytes`

### `minutes`

`minutes` 始终以数组形式存在。每个条目包含：

| Field | Type | Required | Notes |
|------|------|----------|------|
| `minute` | string | yes | RFC3339 的分钟时间桶时间戳 |
| `total_rows` | integer | yes | 该分钟桶中的总行数 |
| `txn_count` | integer | yes | 该分钟桶中的事务数 |
| `table_rows` | object | no | JSON 对象，key 是表标识，value 是整数行数；为空时省略 |

### `alerts`

`alerts` 始终以数组形式存在。每个条目包含：

| Field | Type | Required | Notes |
|------|------|----------|------|
| `type` | string | yes | 告警类型，例如 `large_transaction` 或 `spike` |
| `severity` | string | yes | 当前告警严重级别字符串 |
| `message` | string | yes | 面向人的告警消息 |
| `txn_key` | string | no | 事务级告警时出现 |
| `minute` | string | no | 分钟级告警时出现；出现时为 RFC3339 |
| `details` | object | no | 包含结构化告警细节的 JSON 对象；没有结构化细节时省略 |

### `warnings`

`warnings` 始终以整数计数形式存在。

它表示最终结果对象中累计的分析警告数量。这个值属于 `stdout` 上机器可读报告的一部分；它不是进度行数，也不是 `stderr` 消息数量。非零值表示分析已完成，但结果中记录了警告条件；并不表示 JSON 输出本身有问题。

当前实现中，当事务的 query context 因为达到 SQL 存储上限而被截断时，会增加这个计数。

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

## Markdown 输出

Markdown 模式输出 GitHub Flavored Markdown 报告，包含五个章节：工作负载摘要、热点表、热点事务、分钟级活动和告警，所有章节使用管道表格。

```bash
binlogviz analyze mysql-bin.000123 --format markdown > report.md
```

输出内容可直接传入任意 Markdown 渲染器，或粘贴到 GitHub Issue、PR 评论或 Wiki 页面。

## HTML 输出

HTML 模式输出自包含的单文件报告，所有样式、图表库（ECharts）和数据均内联嵌入，无需外部依赖或网络连接。

```bash
binlogviz analyze mysql-bin.000123 --format html > report.html
```

报告包含：

- 摘要统计卡片（事务数、行数、事件数、时间范围）
- 交互式折线图：每分钟行数和事务数
- 交互式条形图：行数最多的热点表
- 交互式环形图：INSERT / UPDATE / DELETE 操作分布
- 热点表详情表格

## Compare JSON 输出

当 compare 结果需要交给脚本或其他工具消费时，请使用 `binlogviz compare --format json`。

```bash
binlogviz compare --current-snapshot current --baseline-snapshot baseline --format json
```

compare JSON 契约始终包含：

| Field | Type | Required | Notes |
|------|------|----------|------|
| `summary` | object | yes | 行数、事务数和 warning 的差异 |
| `table_changes` | array | yes | 按表统计的行数差异 |
| `operation_mix` | array | yes | INSERT / UPDATE / DELETE 差异 |
| `alert_changes` | object | yes | 新增和移除的告警 |
| `current_label` | string | yes | 有快照元数据时使用快照感知标签，否则为 `current` |
| `baseline_label` | string | yes | 有快照元数据时使用快照感知标签，否则为 `baseline` |
| `current_snapshot` | object | no | 当前输入报告包含 analyze 快照元数据时出现 |
| `baseline_snapshot` | object | no | 基线输入报告包含 analyze 快照元数据时出现 |

`current_snapshot` 和 `baseline_snapshot` 复用上文 analyze `snapshot` 的同一字段契约。

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
- `snapshot list --format text` 以面向人的表格输出 `name`、`label`、`created_at`、`input_mode` 和 `window`
- `snapshot list --format json` 输出包含 `snapshot_dir` 和 `snapshots` 的机器可读对象
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
- 命令错误

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
- 行数、事务数、warnings 的顶层 delta
- 按绝对行数变化排序的热点表变化
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
| `table_changes` | array | yes | 按绝对变化排序的表级行数差异 |
| `operation_mix` | array | yes | `insert`、`update`、`delete` 的操作差异 |
| `alert_changes` | object | yes | 新增和移除的告警 |
| `current_label` | string | yes | 如果 current 输入带 snapshot 元数据则输出 snapshot-aware label，否则为 `current` |
| `baseline_label` | string | yes | 如果 baseline 输入带 snapshot 元数据则输出 snapshot-aware label，否则为 `baseline` |

从用户视角看，JSON 输出回答的仍然是文本报告中的同一批运维问题，只是更适合进入脚本、仪表盘或自动化链路。

### Compare HTML 输出

HTML 模式输出自包含的可视化 compare 报告，不是把文本 diff 简单包在 HTML 里。

```bash
binlogviz compare current.json baseline.json --format html > compare.html
```

报告包含图表化分区：

- baseline 和 current 顶层汇总对比
- 按行数变化排序的热点表变化
- 操作类型分布对比
- 告警新增 / 移除可视化

如果输入带 snapshot 元数据，HTML header 还会带出 label、请求时间窗口、input mode、来源摘要和过滤条件等 compare 上下文。

页面同时包含 compare summary 卡片和明细表 / 明细列表，方便操作者在图表视图和具体表、具体告警之间快速切换。

### Compare 在 `stderr` 上的错误行为

`compare` 当前不会输出 analyze 风格的进度信息。它会把最终 compare 报告写到 `stdout`；如果命令失败，CLI 会通过 `stderr` 输出错误。

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
binlogviz analyze mysql-bin.000123 --format html > report.html
```

HTML 文件完全自包含——所有图表和样式均已内联嵌入，无需互联网连接，在任意浏览器中打开即可。

### 分别捕获两个输出通道

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. \
  > report.txt \
  2> analyze.stderr.log
```
