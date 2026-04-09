# CLI 参考

本文档定义 `binlogviz` 根命令、`binlogviz analyze`、`binlogviz compare`、`binlogviz trend`、`binlogviz snapshot`、`binlogviz workflow run` 和 `binlogviz workflow resume` 的用户可见契约。

如果你想先走最短运维路径，而不是直接看完整契约，请先阅读[快速开始](../recipe/quickstart.zh-CN.md)或[分析本地 Binlog](../recipe/analyze-local-binlogs.zh-CN.md)。

## 命令语法

```bash
binlogviz --version
binlogviz --lang zh-CN analyze <binlog files...>
binlogviz analyze <binlog files...>
binlogviz analyze --from-dir DIR --prefix PREFIX
binlogviz analyze --from-dir DIR --prefix PREFIX --format json --snapshot-name NAME
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
```

## 全局参数

这些参数定义在根命令上，会在子命令执行前生效：

| Flag | 默认值 | 说明 |
|------|--------|------|
| `--lang` | 按环境变量检测 | 运行时输出语言，例如 `en` 或 `zh-CN`。 |
| `--version`, `-v` | `false` | 仅输出版本号并退出。 |

## `analyze` 命令语法

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
| `--snapshot-name` | none | 把本次 JSON analyze 输出保存成 `<name>.json`。要求同时使用 `--format json`。 |
| `--snapshot-dir` | home-based default | 保存快照时使用的目录。默认值：`~/.binlogviz/snapshots`。 |
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
- 趋势点永远按有效窗口开始时间升序排序
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

- text 模式（默认）：输出面向人的表格，包含 `name`、`label`、`created_at`、`input_mode` 和 `window`
- JSON 模式：输出包含 `snapshot_dir` 和 `snapshots` 的机器可读对象

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

`workflow run` 执行一份声明式 YAML plan，描述一个或多个分析窗口、可选的 compare 作业和可选的 trend 作业。它会产生一个确定性的 artifact 目录树以及一份 `manifest.json`。

### Plan 格式

Plan 文件使用 YAML 格式，`version: 1`。根级段落包括：

- `version` — 必填，必须为 `1`
- `workflow` — workflow 名称和输出目录
- `defaults` — 共享输入源、分析选项和快照设置
- `windows` — 一个或多个命名时间窗口
- `compare` — 可选 compare 作业，引用命名窗口
- `trend` — 可选 trend 作业，引用命名窗口

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

## `workflow resume` 命令语法

```bash
binlogviz workflow resume <output_dir>
binlogviz workflow resume <output_dir> --snapshot-dir /tmp/snapshots
binlogviz workflow resume <output_dir> --rerun analyze:week2 --rerun compare:incident_vs_baseline
```

`workflow resume` 从已有的输出目录继续执行之前运行过的 workflow。它会读取已有的 `manifest.json`，复用成功步骤，并重跑失败或缺失的步骤。

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

### 输出目录布局

输出目录布局与 `workflow run` 完全一致。Resume 会覆盖重跑步骤的 artifact，保留复用步骤的 artifact 不变。

### 输出通道

- `stdout` 不使用
- `stderr` 承载进度行和最终的 manifest 路径
- `index.html` 写入 `<output_dir>/index.html`，包含 resume mode、attempt 编号，以及每个步骤的执行标签（`executed` / `reused`）
