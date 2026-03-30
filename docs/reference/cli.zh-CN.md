# CLI 参考

本文档定义 `binlogviz analyze` 的用户可见契约。

如果你想先走最短运维路径，而不是直接看完整契约，请先阅读[快速开始](../recipe/quickstart.zh-CN.md)或[分析本地 Binlog](../recipe/analyze-local-binlogs.zh-CN.md)。

## 命令语法

```bash
binlogviz analyze <binlog files...>
binlogviz analyze --from-dir DIR --prefix PREFIX
```

每次调用 `analyze` 只能使用一种输入模式：

- **位置参数文件模式**：通过位置参数传入一个或多个本地 binlog 文件路径。
- **discovery 模式**：同时提供 `--from-dir` 和 `--prefix`，由 BinlogViz 从目录中解析匹配文件。

## 输入规则

### 位置参数文件模式

当你已经知道要分析哪些精确文件时，使用位置参数即可。

```bash
binlogviz analyze mysql-bin.000123
binlogviz analyze mysql-bin.000123 mysql-bin.000124
```

### Discovery 模式

当你希望 BinlogViz 帮你解析一个有序文件集合时，使用 discovery 模式。

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin.
```

### 位置参数与 discovery 的行为关系

- 位置参数文件与 discovery flags 互斥。
- `--from-dir` 和 `--prefix` 必须同时提供。
- 如果既没有位置参数文件，也没有完整的 discovery 参数对，命令会失败。

关于精确的 discovery 匹配、排序、已解析文件报告以及非法组合契约，请参见[输入发现参考](input-discovery.zh-CN.md)。

## Flags

| Flag | 默认值 | 说明 |
|------|--------|------|
| `--start` | none | 开始时间，包含边界，RFC3339 格式。 |
| `--end` | none | 结束时间，包含边界，RFC3339 格式。 |
| `--from-dir` | none | 从该目录自动发现 binlog 文件。必须与 `--prefix` 一起使用。 |
| `--prefix` | none | 配合 `--from-dir` 使用的文件名前缀。必须与 `--from-dir` 一起使用。 |
| `--format` | `text` | 报告输出格式：`text`、`json`、`markdown`（别名 `md`）或 `html`。 |
| `--json` | `false` | `--format json` 的简写，已废弃，建议改用 `--format`。 |
| `--sql-context` | `summary` | SQL 上下文展示模式：`summary`、`off` 或 `full`。 |
| `--top-tables` | `10` | 报告中包含的 Top 表数量。 |
| `--top-transactions` | `10` | 报告中包含的 Top 事务数量。 |
| `--detect-spikes` | `false` | 启用写入尖峰检测。 |
| `--large-trx-rows` | `1000` | 大事务告警的行数阈值。 |
| `--large-trx-duration` | `30s` | 大事务告警的持续时间阈值。 |
| `--top-minutes` | `60` | 报告中包含的最活跃分钟数。 |
| `--spike-window` | `5` | 尖峰检测的滑动基线窗口（分钟）。 |
| `--spike-factor` | `3.0` | 触发尖峰告警的基线倍数。 |
| `--spike-min-rows` | `100` | 纳入尖峰检测候选的最小行数。 |
| `--include-schema` | none | 仅分析指定 schema（逗号分隔，其余均排除）。 |
| `--exclude-schema` | none | 跳过指定 schema（逗号分隔）。 |
| `--include-table` | none | 仅分析指定表（逗号分隔，其余均排除）。 |
| `--exclude-table` | none | 跳过指定表（逗号分隔）。 |

## 时间与校验行为

### 时间过滤

`--start` 和 `--end` 使用 RFC3339 时间戳。

```bash
binlogviz analyze mysql-bin.000123 \
  --start "2026-03-15T10:00:00Z" \
  --end "2026-03-15T10:30:00Z"
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

- 位置参数文件与 `--from-dir` 或 `--prefix` 同时出现
- 只提供 `--from-dir`，不提供 `--prefix`
- 只提供 `--prefix`，不提供 `--from-dir`
- 既没有位置参数文件，也没有完整的 discovery 参数对

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
- 设置 `--format json` 或 `--json` 时输出 JSON 报告
- 设置 `--format markdown`（或 `--format md`）时输出 Markdown 报告
- 设置 `--format html` 时输出 HTML 报告

这种行为便于安全地做 shell 重定向和脚本处理。

```bash
binlogviz analyze mysql-bin.000123 --format json > report.json
binlogviz analyze mysql-bin.000123 --format markdown > report.md
binlogviz analyze mysql-bin.000123 --format html > report.html
```

### 标准错误（`stderr`）

`stderr` 用于面向操作者的运行时状态信息：

- 解析进度输出
- `Finalizing analysis...`
- discovery 模式下的已解析文件列表
- 命令错误

这样可以保持 `stdout` 在管道和重定向场景下的纯净。

关于精确的输出通道契约和 JSON 字段级行为，请参见[输出格式参考](output-format.zh-CN.md)。

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
