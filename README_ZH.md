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

## 从这里开始

如果你已经拿到了本地 binlog 文件，下面这些命令就是最快能看到有价值结果的路径：

### 快速检查单个文件

```bash
binlogviz analyze mysql-bin.000123
```

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

### 只看某个 schema 或表

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. \
  --include-schema orders \
  --include-table payments
```

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

`compare` 既可以加载已保存的快照，也可以继续加载两份由 `binlogviz analyze --format json` 生成的 JSON 文件。输出格式支持 `text`、`json`、`html`。其中 text 和 HTML 输出会带出 input mode、来源摘要、过滤条件和请求时间窗口。除了 summary 差异、热点表变化、操作类型分布和告警新增/移除之外，compare 现在还会通过一等结果区 `pattern_changes` 直接展示写入模式漂移。`key_findings` 部分会以最多 5 条确定性、基于证据的发现来突出变化的主要驱动因素（体量变化、模式驱动、表趋势、操作漂移、新模式），并通过 `evidence_refs` 把发现链接回支撑它的报告章节。`recommendations` 数组提供基于关键发现的保守、有据可依的后续建议。

compare 生成的最终报告写到 `stdout`。如果 compare 命令失败，CLI 会通过 `stderr` 输出错误。

### 把多个快照当作一条时间线查看趋势

```bash
binlogviz trend incident_week1 incident_week2 incident_week3 --format text

binlogviz trend --from-snapshots 'incident_week*' \
  --baseline-snapshot baseline_weekly \
  --format html > trend.html
```

`trend` 会加载已保存的 snapshots，按有效窗口开始时间排序，并输出 `text`、`json` 或 `html` 趋势报告。新快照优先使用 `snapshot.window.start_time`；较旧的快照则可以回退到 `summary.start_time`，不需要手工重写历史文件。除了总量和热点表变化之外，trend 现在还会输出 `pattern_trends` 用于跨多个窗口查看重复写入模式的演进，以及 `trend_summary` 部分以最多 5 条确定性发现突出上升/下降模式、表趋势、集中度偏移和体量尖峰。这些发现可以包含 `evidence_refs`，指回相关模式、表和 ordered point 证据。`recommendations` 数组提供基于发现的保守、有据可依的后续建议。HTML 报告中的 `Pattern Trends` 分区默认展示 `share of rows`，也可以切换到绝对 `rows`；`text` 和 `json` 则会以终端友好和机器可读的形式暴露同一组模式序列。

### 用一份 plan 文件跑多步调查

```bash
binlogviz workflow run incident.yaml
tree artifacts/incident-investigation
```

`workflow run` 执行一份声明式 YAML plan，定义分析窗口、可选 compare 作业和可选 trend 作业。它会产生一个确定性的 artifact 目录，包含 `analyze/`、`compare/`、`trend/`、一份 `manifest.json` 和一个 `index.html` 落地页。`manifest.json` 会始终持久化一个 `workflow_summary` 对象，其中包含 `findings`、`recommendations` 和 `warnings` 三个数组。BinlogViz 只会基于成功 compare/trend 步骤产出的 JSON artifact 以 best-effort 方式重建这份 summary；如果 summary 输入缺失或不可读，只会追加 warnings，不会改变 workflow 或步骤的状态语义。只要 summary 中存在内容，`index.html` 就会渲染 `Workflow Recommendations`、`Workflow Findings` 和 `Workflow Summary Warnings` 分区，并优先链接到 HTML 源报告，必要时回退到 JSON。v1 中 `stdout` 留空，所有状态走 `stderr`。plan schema 和参数请参见 [CLI 参考](docs/reference/cli.zh-CN.md)。

如果 workflow 运行中途失败，`workflow resume` 可以从已有的输出目录继续执行：复用成功步骤，重跑失败或缺失的步骤，并支持通过 `--rerun` 选择器强制重跑指定步骤。Resume 会在 plan 文件变更或 manifest 是旧版 pre-v2 产物时拒绝执行。

在真正执行之前，`workflow validate` 会只基于 `plan.yaml` 做静态可运行性检查，`workflow describe` 则会预览该 plan 将产生的 analyze / compare / trend artifact 布局。两个命令都支持 `--format text` 和 `--format json`，且只读取 plan 文件，不会检查 `output_dir`、`manifest.json` 或 `index.html`。

`workflow status` 是针对已有 workflow root 的只读运行时检查命令。它会读取 `manifest.json`，检查 artifact presence，报告 `runtime_state`、`resumable` 和 `resume_error`，在 `--format json` 下直接带出已持久化的 `workflow_summary`，并在保存的 plan 仍可加载时给出 dry `resume_preview`。文本输出只会在这些已持久化数组非空时渲染 `Workflow Recommendations`、`Workflow Findings` 和 `Workflow Summary Warnings`。它不会执行步骤、不会重建 workflow summary，也不会重写任何 workflow 输出。

`workflow clean` 则是这一生命周期中的最终 maintenance 命令。它以当前 manifest 为唯一真相源，默认 dry-run，报告 `analyze/`、`compare/`、`trend/` 下已不再被引用的孤儿生成物，并且只有显式加 `--include-snapshots` 时才会把孤儿 snapshot JSON 纳入候选。`--apply` 会执行 best-effort 删除，但仍然不会触碰 `manifest.json`、`index.html`、plan 文件以及任何超出范围的未知文件。

```bash
binlogviz workflow resume ./artifacts/incident
binlogviz workflow resume ./artifacts/incident --rerun analyze:week2
binlogviz workflow status ./artifacts/incident
binlogviz workflow status ./artifacts/incident --format json
binlogviz workflow clean ./artifacts/incident
binlogviz workflow clean ./artifacts/incident --apply --include-snapshots
binlogviz workflow validate incident.yaml
binlogviz workflow validate incident.yaml --format json
binlogviz workflow describe incident.yaml
binlogviz workflow describe incident.yaml --format json
```

### 生成 Markdown 或 HTML 报告

```bash
# Markdown — 粘贴到 GitHub issue、wiki 或文档
binlogviz analyze mysql-bin.000123 --format markdown > report.md

# HTML — 在任意浏览器打开，无需网络
binlogviz analyze mysql-bin.000123 --format html > report.html
```

HTML 报告包含交互式图表（每分钟行数/事务数、热点表、操作类型分布）和五主题切换器。

## BinlogViz 适合回答什么问题

BinlogViz 重点服务这些 DBA 常见问题：

- **哪些表承受了最重的写入负载？**
- **哪些事务大到值得优先排查？**
- **某个分钟级尖峰是否真实发生过？**
- **指定故障窗口内到底发生了什么变化？**
- **当前窗口和可信基线相比，负载差异到底在哪里？**
- **结果能否安全交给脚本、管道或其他工具？**

## 安装

### macOS 首选：Homebrew Cask

```bash
brew tap Fanduzi/binlogviz
brew install --cask binlogviz
```

这条路径会安装预编译 release artifact，并在安装时移除 macOS quarantine 属性；用户不需要额外安装 DuckDB。

### 通用备选：下载 Release Artifact

从 GitHub Releases 下载与你平台匹配的归档文件，校验 checksum 后再把二进制放到 `PATH` 中。

权威 release artifact 由 GitHub Actions release workflow 产出。macOS 产物在原生 runner 上构建，Linux 产物则在 manylinux2014 用户态中构建，以保持对 CentOS 7 / glibc 2.17 的兼容基线。本地 `goreleaser` 更适合做配置校验和当前宿主机的可选验证，不是主要发布路径。

下面是 `darwin/arm64` 和当前版本 `v0.13.0` 的示例：

```bash
curl -fsSLO https://github.com/Fanduzi/BinlogVisualizer/releases/download/v0.13.0/binlogviz_0.13.0_darwin_arm64.tar.gz
curl -fsSLO https://github.com/Fanduzi/BinlogVisualizer/releases/download/v0.13.0/binlogviz_0.13.0_checksums.txt
shasum -a 256 -c binlogviz_0.13.0_checksums.txt 2>/dev/null | grep "binlogviz_0.13.0_darwin_arm64.tar.gz: OK"
tar -xzf binlogviz_0.13.0_darwin_arm64.tar.gz
install ./binlogviz /usr/local/bin/binlogviz
```

也可以先从同一个 release tag 下载仓库内置安装脚本，再执行它：

```bash
curl -fsSLO https://raw.githubusercontent.com/Fanduzi/BinlogVisualizer/v0.13.0/install.sh
sh ./install.sh --version v0.13.0
```

如果只想预览将要解析出的 artifact，而不实际下载：

```bash
./install.sh --version v0.13.0 --dry-run
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
binlogviz analyze mysql-bin.000123
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
