# 快照、对比、趋势与 workflow

本页收录原先写在项目 README 里的快照、compare、trend、workflow 细节和 DBA 工作流示例。参数和 plan schema 见 [CLI 参考](https://github.com/Fanduzi/BinlogVisualizer/blob/main/docs/reference/cli.zh-CN.md)。

## 快照与对比

`snapshot list` 现在会输出面向人的表格，包含 `name`、`label`、`created_at`、`input_mode` 和 `window`。`snapshot list --format json` 和 `snapshot show --format json` 仍然为脚本和外部工具提供稳定的机器可读输出。`snapshot rename` 会在重命名文件的同时保持快照内部 identity 一致，`snapshot delete` 则用于删除单个快照而不影响其余历史。

`compare` 既可以加载已保存的快照，也可以继续加载两份由 `binlogviz analyze --format json` 生成的 JSON 文件。输出格式支持 `text`、`json`、`html`。其中 text 和 HTML 输出会带出 input mode、来源摘要、过滤条件和请求时间窗口。除了 summary 差异、热点表变化、操作类型分布和告警新增/移除之外，compare 现在还会通过一等结果区 `pattern_changes` 直接展示写入模式漂移。`key_findings` 部分会以最多 5 条确定性、基于证据的发现来突出变化的主要驱动因素（体量变化、模式驱动、表趋势、操作漂移、新模式），并通过 `evidence_refs` 把发现链接回支撑它的报告章节。`recommendations` 数组提供基于关键发现的保守、有据可依的后续建议。高信号跨窗口模式变化现在还会在 text、JSON 和 HTML 输出中携带有界 `pattern_drilldowns`。

compare 生成的最终报告写到 `stdout`。如果 compare 命令失败，CLI 会通过 `stderr` 输出错误。

## 趋势

`trend` 会加载已保存的 snapshots，默认按参数顺序（`--order cli`）排列，并输出 `text`、`json` 或 `html` 趋势报告。`--order time` 才按窗口起点升序。新快照优先使用 `snapshot.window.start_time`；较旧的快照则可以回退到 `summary.start_time`，不需要手工重写历史文件。除了总量和热点表变化之外，trend 现在还会输出 `pattern_trends` 用于跨多个窗口查看重复写入模式的演进，以及 `trend_summary` 部分以最多 5 条确定性发现突出上升/下降模式、表趋势、集中度偏移和体量尖峰。这些发现可以包含 `evidence_refs`，指回相关模式、表和 ordered point 证据。`recommendations` 数组提供基于发现的保守、有据可依的后续建议。高信号跨窗口模式趋势现在还会在 text、JSON 和 HTML 输出中携带有界 `pattern_drilldowns`。HTML 报告中的 `Pattern Trends` 分区默认展示 `share of rows`，也可以切换到绝对 `rows`；`text` 和 `json` 则会以终端友好和机器可读的形式暴露同一组模式序列。

## Workflow plan

仓库自带一份可运行的 `incident.yaml`，`from_dir` 指向 `cmd/binlogviz/testdata/sample-binlog` 里的 1500 字节 ROW 样本。在仓库根目录执行下面第一行命令，不需要本机 `/var/lib/mysql`。Release 归档里另有一份 `incident.yaml`，`from_dir` 是 `testdata/sample-binlog`，解压后也可以跑同一条命令：

```bash
binlogviz workflow run incident.yaml
tree artifacts/incident
```

同一份 plan 和样本也可以通过 raw URL 下载：

- plan：https://raw.githubusercontent.com/Fanduzi/BinlogVisualizer/main/incident.yaml
- 样本 binlog：https://raw.githubusercontent.com/Fanduzi/BinlogVisualizer/main/cmd/binlogviz/testdata/sample-binlog/mysql-bin.000001

`workflow run` 执行一份声明式 YAML plan，定义分析窗口、可选 compare 作业和可选 trend 作业。它会产生一个确定性的 artifact 目录，包含 `analyze/`、`compare/`、`trend/`、一份 `manifest.json` 和一个 `index.html` 落地页。`manifest.json` 会始终持久化一个 `workflow_summary` 对象，其中包含 `findings`、`recommendations` 和 `warnings` 三个数组。BinlogViz 只会基于成功 compare/trend 步骤产出的 JSON artifact 以 best-effort 方式重建这份 summary；如果 summary 输入缺失或不可读，只会追加 warnings，不会改变 workflow 或步骤的状态语义。只要 summary 中存在内容，`index.html` 就会渲染 `Workflow Recommendations`、`Workflow Findings` 和 `Workflow Summary Warnings` 分区，并优先链接到 HTML 源报告，必要时回退到 JSON。v1 中 `stdout` 留空，所有状态走 `stderr`。plan schema 和参数请参见 [CLI 参考](https://github.com/Fanduzi/BinlogVisualizer/blob/main/docs/reference/cli.zh-CN.md)。

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
