<div align="center">

# BinlogViz

[![Release](https://img.shields.io/github/v/release/Fanduzi/BinlogVisualizer?display_name=tag)](https://github.com/Fanduzi/BinlogVisualizer/releases)
![Platform](https://img.shields.io/badge/platform-darwin%20amd64%20%7C%20darwin%20arm64%20%7C%20linux%20amd64%20%7C%20linux%20arm64-blue)
![Go Version](https://img.shields.io/badge/go-1.26.1-00ADD8?logo=go)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](#license)

[English](README.md) | 中文 | [变更记录](CHANGELOG.md) | [安全策略](SECURITY.md) | [发行说明](docs/releases/)
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

下面是 `darwin/arm64` + 当前版本 `v0.3.0` 的示例：

```bash
curl -fsSLO https://github.com/Fanduzi/BinlogVisualizer/releases/download/v0.3.0/binlogviz_0.3.0_darwin_arm64.tar.gz
curl -fsSLO https://github.com/Fanduzi/BinlogVisualizer/releases/download/v0.3.0/binlogviz_0.3.0_checksums.txt
shasum -a 256 -c binlogviz_0.3.0_checksums.txt 2>/dev/null | grep "binlogviz_0.3.0_darwin_arm64.tar.gz: OK"
tar -xzf binlogviz_0.3.0_darwin_arm64.tar.gz
install ./binlogviz /usr/local/bin/binlogviz
```

也可以直接使用仓库内置的安装脚本：

```bash
curl -fsSL https://raw.githubusercontent.com/Fanduzi/BinlogVisualizer/main/install.sh | sh -s -- --version v0.3.0
```

仅预览解析到的 artifact，而不实际下载：

```bash
./install.sh --version v0.3.0 --dry-run
```

### 备选：从源码构建

```bash
git clone https://github.com/Fanduzi/BinlogVisualizer.git
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

# 从目录和前缀自动发现有序 binlog 范围
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin.

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

## 文档导航

BinlogViz 现在按读者意图拆分产品文档，这样 README 可以专注于安装方式和第一次成功运行，而更稳定的契约与设计说明则放到 `docs/` 下。

### 概念文档

- [产品架构](docs/concept/architecture.zh-CN.md)
- [DuckDB 临时存储](docs/concept/duckdb-temp-store.zh-CN.md)
- [分析模型](docs/concept/analysis-model.zh-CN.md)
- [限制与边界](docs/concept/limitations.zh-CN.md)

### 操作指南

- [快速开始](docs/recipe/quickstart.zh-CN.md)
- [分析本地 Binlog](docs/recipe/analyze-local-binlogs.zh-CN.md)
- [常见错误排查](docs/recipe/troubleshoot-common-errors.zh-CN.md)

### 参考文档

- [CLI 参考](docs/reference/cli.zh-CN.md)
- [输入发现参考](docs/reference/input-discovery.zh-CN.md)
- [输出格式参考](docs/reference/output-format.zh-CN.md)

### 其他资源

- [示例输出](docs/examples/)
- [发行说明](docs/releases/)

## 环境要求

- MySQL `ROW` format binlog 文件
- Go 1.26.1+（构建时）

## License

MIT
