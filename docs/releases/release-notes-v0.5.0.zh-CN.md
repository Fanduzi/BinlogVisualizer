# BinlogViz v0.5.0 发布说明

发布日期：2026-03-28

## 概述

v0.5.0 将此前隐藏的分析器参数暴露为 CLI 标志，并支持在分析阶段对 schema 和表进行过滤。

## 新功能

### 暴露 CLI 标志（Feature A）

以下分析器参数此前仅在代码中硬编码，现已通过 CLI 标志完全开放配置：

| 标志 | 默认值 | 说明 |
|------|--------|------|
| `--top-minutes` | `60` | 报告中显示的最活跃分钟数 |
| `--spike-window` | `5` | 尖峰检测的滑动基线窗口（分钟） |
| `--spike-factor` | `3.0` | 超过基线多少倍触发尖峰告警 |
| `--spike-min-rows` | `100` | 纳入尖峰检测的分钟最小行数 |

示例：

```bash
binlogviz analyze mysql-bin.000001 \
  --detect-spikes \
  --spike-window 10 \
  --spike-factor 5.0 \
  --top-minutes 30
```

### Schema/表过滤（Feature B）

支持在分析入口阶段过滤 schema 和表，而非仅在输出层过滤。当只关注特定 schema 或表时，可降低噪音、提升性能。

| 标志 | 说明 |
|------|------|
| `--include-schema` | 仅分析指定 schema（逗号分隔） |
| `--exclude-schema` | 跳过指定 schema（逗号分隔） |
| `--include-table` | 仅分析指定表（逗号分隔） |
| `--exclude-table` | 跳过指定表（逗号分隔） |

排除规则优先于包含规则。表名不含 schema 前缀。

示例：

```bash
# 仅分析 orders schema，跳过 audit 表
binlogviz analyze mysql-bin.000001 \
  --include-schema orders \
  --exclude-table audit_log

# 跳过内部 schema
binlogviz analyze mysql-bin.000001 \
  --exclude-schema mysql,sys,information_schema,performance_schema
```

## Bug 修复

无。

## 破坏性变更

无。所有新标志均有默认值，保持向后兼容。
