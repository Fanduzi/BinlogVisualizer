<div align="center">

# BinlogViz

[![Release](https://img.shields.io/github/v/release/Fanduzi/BinlogVisualizer?display_name=tag)](https://github.com/Fanduzi/BinlogVisualizer/releases)
![Platform](https://img.shields.io/badge/platform-darwin%20amd64%20%7C%20darwin%20arm64%20%7C%20linux%20amd64%20%7C%20linux%20arm64-blue)
![Go Version](https://img.shields.io/badge/go-1.24%2B-00ADD8?logo=go)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](#license)

[English](README.md) | 中文 | [发行说明](docs/release-notes-v0.2.1.zh-CN.md)
</div>

BinlogViz 是一个用于分析 MySQL binlog 的 CLI 工具，帮助 DBA 从本地 `ROW` binlog 文件中快速识别热点表、大事务、写入尖峰和整体负载模式。

## 概览

BinlogViz 主要回答这些运维问题：

- **哪些表的写入最重？**
- **是否存在异常大的事务？**
- **某些分钟是否出现了写入尖峰？**
- **指定时间窗口内的整体负载是什么样？**

## 安装

### 首选：下载 Release Artifact

从 GitHub Releases 下载与你平台匹配的归档文件，校验 checksum 后将二进制放到 `PATH` 中。

权威 release artifact 由 GitHub Actions release workflow 在原生 runner 上产出。本地 `goreleaser` 仅用于配置检查和当前宿主机的单目标验证。

下面是 `darwin/arm64` + 当前 Phase 2 版本 `v0.2.1` 的示例：

```bash
curl -fsSLO https://github.com/Fanduzi/BinlogVisualizer/releases/download/v0.2.1/binlogviz_0.2.1_darwin_arm64.tar.gz
curl -fsSLO https://github.com/Fanduzi/BinlogVisualizer/releases/download/v0.2.1/binlogviz_0.2.1_checksums.txt
shasum -a 256 -c binlogviz_0.2.1_checksums.txt 2>/dev/null | grep "binlogviz_0.2.1_darwin_arm64.tar.gz: OK"
tar -xzf binlogviz_0.2.1_darwin_arm64.tar.gz
install ./binlogviz /usr/local/bin/binlogviz
```

也可以直接使用仓库内置的安装脚本：

```bash
curl -fsSL https://raw.githubusercontent.com/Fanduzi/BinlogVisualizer/main/install.sh | sh -s -- --version v0.2.1
```

仅预览解析到的 artifact，而不实际下载：

```bash
./install.sh --version v0.2.1 --dry-run
```

### 备选：从源码构建

```bash
git clone <repository-url>
cd BinlogVisualizer

go build -o binlogviz .
go install .
go run . analyze <binlog files...>
```

## 用法

### 基础分析

```bash
# 分析单个 binlog 文件
binlogviz analyze mysql-bin.000123

# 分析多个文件
binlogviz analyze mysql-bin.000123 mysql-bin.000124

# 用 shell 展开分析一组文件
binlogviz analyze mysql-bin.*
```

### 时间窗口过滤

```bash
binlogviz analyze mysql-bin.* \
  --start "2026-03-15T10:00:00Z" \
  --end "2026-03-15T10:30:00Z"
```

### 输出选项

```bash
# JSON 输出，适合脚本处理
binlogviz analyze mysql-bin.* --json

# 调整 Top 项数量
binlogviz analyze mysql-bin.* --top-tables 20 --top-transactions 20
```

### 告警检测

```bash
# 开启写入尖峰检测
binlogviz analyze mysql-bin.* --detect-spikes

# 自定义大事务阈值
binlogviz analyze mysql-bin.* \
  --large-trx-rows 5000 \
  --large-trx-duration 60s
```

## CLI Flags

| Flag | 默认值 | 说明 |
|------|--------|------|
| `--start` | 无 | 开始时间，包含边界，RFC3339 格式 |
| `--end` | 无 | 结束时间，包含边界，RFC3339 格式 |
| `--json` | false | 使用 JSON 输出 |
| `--sql-context` | summary | SQL 上下文展示模式：`summary`、`off`、`full` |
| `--top-tables` | 10 | 输出的 Top 表数量 |
| `--top-transactions` | 10 | 输出的 Top 事务数量 |
| `--detect-spikes` | false | 开启写入尖峰检测 |
| `--large-trx-rows` | 1000 | 大事务行数阈值 |
| `--large-trx-duration` | 30s | 大事务耗时阈值 |

## 报告部分

输出包含 5 个 section：

### 1. Workload Summary

分析时间窗口内的整体统计：

- 总事务数
- 总影响行数
- 总事件数
- 时间范围和持续时长

### 2. Top Tables

按总影响行数排序的表，展示：

- schema 和表名
- 总行数
- 各操作类型明细（INSERT/UPDATE/DELETE）
- 涉及该表的 distinct transaction 数量

### 3. Top Transactions

按总行数排序的最大事务，展示：

- 事务标识
- 行数和持续时间
- 事件数

### 4. Minute Activity

按分钟拆分的写入活动：

- 每分钟写入行数
- 每分钟事务数

### 5. Alerts

识别出的异常包括：

- **Large Transaction**：超过行数或持续时间阈值的事务
- **Write Spike**：某分钟写入异常高，且开启了 `--detect-spikes`

## 示例

示例输出见：

- [Text Output](docs/examples/analyze-output.txt)
- [JSON Output](docs/examples/analyze-output.json)

## 环境要求

- MySQL `ROW` format binlog 文件
- Go 1.24+（构建时）

## 大文件处理

BinlogViz 当前使用 streaming command path + DuckDB finalize-time result assembly：

- **Parser**：通过 callback 流式读取原始 binlog 事件
- **Command Layer**：立即 normalize 并转发给 `analyzer.Consume`
- **Analyzer**：在内存中只保留 bounded live state
- **DuckDB Temp Store**：在 `Finalize()` 阶段持久化高基数结果
- **Renderer**：输出最终报告

### 性能预期

在 Apple M4 Pro 上的 benchmark：

| 输入规模 | Time/op | Memory/op | Allocs/op |
|---------|---------|-----------|-----------|
| 1 event | ~1μs | 2.5 KB | 32 |
| 100 events | ~40μs | 55 KB | 756 |
| 1000 events | ~492μs | 665 KB | 7.1K |
| 100 tables | ~41μs | 55 KB | 756 |
| 10 transactions | ~245 ns | 469 B | 12 |

### 大文件建议

对较大的 binlog 文件：

1. 优先按顺序直接分析 binlog 范围，command path 本身已经是 streaming。
2. 预留足够的磁盘空间，用于分析过程中的临时 DuckDB 结果存储。

## 限制

- **仅支持 ROW binlog**：MVP 不支持 STATEMENT 和 MIXED
- **仅支持本地文件**：不能直接连接 MySQL 服务端
- **不是实时流式消费**：分析对象是静态文件
- **仅支持 bounded SQL context**：当输入 binlog 包含 `Rows_query_log_event` 时，可通过 `--sql-context summary|full` 展示受限长度 SQL 上下文，但不支持 SQL replay 或完整 statement reconstruction
- **不展示 row values**：关注的是操作模式，不是数据内容

## 非目标

BinlogViz 目前刻意不做这些事情：

- 复制故障排查器
- SQL 重放器
- 实时监控系统
- Prometheus exporter
- Web dashboard
- AI 异常检测器

## 架构

BinlogViz 使用单次流式分析管线：

```text
binlog files → parser → normalizer → analyzer → renderer → output
```

组件职责：

- **Parser**：封装 `go-mysql-org/go-mysql/replication` 做 binlog 解析
- **Normalizer**：把 parser 事件转换为稳定的内部格式
- **Analyzer**：重建事务、聚合表/分钟指标、检测告警
- **Renderer**：输出 text 或 JSON

## License

MIT
