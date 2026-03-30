# 输出格式参考

本文档说明 `binlogviz analyze` 会向 `stdout` 和 `stderr` 分别写入什么内容。

如果你想先看最短运维路径，请先阅读[快速开始](../recipe/quickstart.zh-CN.md)或[分析本地 Binlog](../recipe/analyze-local-binlogs.zh-CN.md)。

## 输出通道契约

BinlogViz 会把不同用途的输出写到不同通道：

- `stdout` 承载最终分析报告。
- `stderr` 承载进度、discovery 解析出的文件列表、最终组装状态以及运行时错误。

这种分离很重要，因为它能让报告输出保持适合重定向和自动化处理。

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. --format json > analyze.json
```

在上面的例子中，JSON 报告会写入 `analyze.json`，而进度和 discovery 状态仍然通过 `stderr` 显示在终端上。

## 可用格式

| 参数值 | 别名 | 说明 |
|---|---|---|
| `text` | — | 默认，人类可读的终端输出。 |
| `json` | `--json` | 机器可读的 JSON 格式。 |
| `markdown` | `md` | 含表格的 GitHub Flavored Markdown。 |
| `html` | — | 含交互式图表和主题切换器的自包含 HTML 文件。 |

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

当结果需要被脚本、自动化流程或下游工具消费时，请使用 `--json`。

```bash
binlogviz analyze mysql-bin.000123 --json
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

## Markdown 输出

Markdown 模式输出 GitHub Flavored Markdown 报告，包含五个章节：工作负载摘要、热点表、热点事务、分钟级活动和告警，所有章节使用管道表格。

```bash
binlogviz analyze mysql-bin.000123 --format markdown > report.md
```

输出内容可直接粘贴到 GitHub Issue、PR 评论或 Wiki 页面。

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
- 带严重等级徽标的告警列表

### 主题

HTML 报告 header 右侧提供主题切换器（五个彩色圆点）。可用主题：**Nebula**（默认，深色靛紫/青色）、**Forest**（深色翠绿/琥珀）、**Navy**（深色天蓝/金色）、**Ember**（深色橙/玫红）、**Light**（浅色）。主题选择保存在 `localStorage`，下次打开自动恢复。

## stderr 隔离

BinlogViz 会把面向操作者的运行时输出隔离在 `stdout` 之外。

### 哪些内容会写到 `stderr`

命令会把以下内容写到 `stderr`：

- 解析进度输出
- discovery 模式下的 `Resolved binlog files:` 列表
- `Finalizing analysis...`
- 命令错误

### 为什么这很重要

这种行为让你可以安全地：

- 把文本输出重定向到文件
- 把 JSON 输出重定向到另一个工具
- 查看 discovery 结果和进度，而不会污染报告流

## 示例

### 保存文本输出，同时在终端保留状态信息

```bash
binlogviz analyze mysql-bin.000123 > report.txt
```

### 保存 JSON 输出供下游处理

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. --json > report.json
```

### 分别捕获两个输出通道

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. \
  > report.txt \
  2> analyze.stderr.log
```
