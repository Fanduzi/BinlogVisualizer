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
- `workflow describe`：`stdout` 输出文本或 JSON 的静态执行预览，包括步骤顺序和 artifact 路径。命令只读取 `plan.yaml`，不会渲染 HTML，也不会检查任何运行时产物。失败时还会继续返回命令错误，因此默认 CLI 执行下可能在 `stdout` 载荷之后再向 `stderr` 输出一行错误信息。

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

### 4. Top Patterns

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

### 5. Minute Activity

`Minute Activity` 章节汇总每分钟的写入活动。

文本输出中常见字段包括：

- 分钟时间桶
- 该分钟的总行数
- 该分钟的事务数

示例标题：

```text
=== Minute Activity ===
```

### 6. Alerts

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
| `report_version` | integer | yes | analyze 报告契约版本；当前版本为 `2` |
| `summary` | object | yes | 总体汇总和时间边界 |
| `tables` | array | yes | Top 表聚合结果；没有表结果时为空数组 |
| `transactions` | array | yes | Top 事务聚合结果；没有事务结果时为空数组 |
| `patterns` | array | yes | Top 模式聚合结果；没有模式结果时为空数组 |
| `minutes` | array | yes | 每分钟聚合结果；没有分钟桶时为空数组 |
| `alerts` | array | yes | 检测到的告警；没有告警时为空数组 |
| `warnings` | integer | yes | 最终结果中记录的分析警告数量 |
| `pattern_drilldowns` | array | yes | 高信号模式的有界 drilldown 摘要；无模式达到阈值时为空数组 |
| `snapshot` | object | no | 仅在 `analyze` 使用 `--snapshot-name` 时出现 |

### `summary`

`summary` 始终存在，包含：

| Field | Type | Required | Notes |
|------|------|----------|------|
| `total_transactions` | integer | yes | 总分析事务数 |
| `total_rows` | integer | yes | 逻辑影响行数（UPDATE before/after image 计 1 行） |
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
| `update_rows` | integer | yes | 逻辑 UPDATE 行（before/after image 计 1 行） |
| `update_events` | integer | yes | UPDATE 行事件数 |
| `delete_rows` | integer | yes |
| `txn_count` | integer | yes |

`diagnostics.input_format_guess` 为 `ROW` / `STATEMENT` / `MIXED`，信号不足时为空。`diagnostics.ignored_query_dml_events` 统计没有对应 row image 的 Query-DML。

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
| `txn_count` | integer | yes | 该分钟桶中的事务数 |
| `table_rows` | object | no | JSON 对象，key 是表标识，value 是整数行数；为空时省略 |

### `alerts`

`alerts` 始终以数组形式存在。每个条目包含：

| Field | Type | Required | Notes |
|------|------|----------|------|
| `type` | string | yes | 告警类型，例如 `large_transaction`、`spike` 或 `input_format` |
| `severity` | string | yes | 当前告警严重级别字符串 |
| `message` | string | yes | 面向人的告警消息 |
| `txn_key` | string | no | 事务级告警时出现 |
| `minute` | string | no | 分钟级告警时出现；出现时为 RFC3339 |
| `details` | object | no | 包含结构化告警细节的 JSON 对象；没有结构化细节时省略 |

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

## Markdown 输出

Markdown 模式输出 GitHub Flavored Markdown 报告，包含五个章节：工作负载摘要、热点表、热点事务、分钟级活动和告警，所有章节使用管道表格。

```bash
binlogviz analyze mysql-bin.000123 --format markdown > report.md
```

输出内容可直接传入任意 Markdown 渲染器，或粘贴到 GitHub Issue、PR 评论或 Wiki 页面。

## HTML 输出

HTML 模式输出自包含的单文件报告，所有样式、图表库（ECharts）和数据均内联嵌入，无需外部依赖或网络连接。

```bash
# 默认：写入 mysql-bin.000123.html
binlogviz analyze mysql-bin.000123 --format html

# 指定输出路径
binlogviz analyze mysql-bin.000123 --format html --output report.html

# 输出到 stdout（用于管道）
binlogviz analyze mysql-bin.000123 --format html --output -
```

报告包含：

- 摘要统计卡片（事务数、行数、事件数、时间范围）
- 交互式折线图：每分钟行数和事务数
- 交互式条形图：行数最多的热点表
- 交互式环形图：INSERT / UPDATE / DELETE 操作分布
- 热点表详情表格
- 被选中的高信号模式的 Pattern Drilldowns 分区（可折叠卡片，包含信号标志和指标帮助）

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
- `--format html` 写入文件时的 `HTML report saved to …`
- `--start`/`--end` 匹配到 0 个事件时的 `window matched 0 events`
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

HTML 文件默认写入当前目录（例如 `mysql-bin.000123.html`）。使用 `--output report.html` 指定路径，或 `--output -` 输出到 stdout。文件完全自包含——所有图表和样式均已内联嵌入，无需互联网连接，在任意浏览器中打开即可。

### 分别捕获两个输出通道

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. \
  > report.txt \
  2> analyze.stderr.log
```
