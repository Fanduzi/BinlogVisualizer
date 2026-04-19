# BinlogViz v0.19.0 发布说明

发布日期：2026-04-19

## 范围

`v0.19.0` 将 DuckDB 明细存储改为可选，并将默认 analyze 行为改为不使用它。

默认 `binlogviz analyze` 现在走有界流式报告路径，不再创建 DuckDB 明细存储。这去除了默认情况下的临时存储创建、写入和清理开销，同时保留 `--detail-store duckdb` 作为实验性/调试/兼容后端。

## 主要内容

- 默认 `--detail-store` 从 `duckdb` 改为 `none`
- `none` 模式不创建 DuckDB 数据库，不调用 `ResolveTransactionQuerySQL`，不写入磁盘
- `--detail-store duckdb` 保留为实验性/调试/兼容后端
- `none` 与 `duckdb` 模式的 JSON 输出完全一致：10 个顶层报告字段全部匹配（summary、tables、transactions、patterns、minutes、timeseries、diagnostics、alerts、warnings、pattern_drilldowns）

## 性能说明

在本地 988 MB MySQL 8.0 ROW binlog 样本（`mysql-bin.000009`，1,036,542,903 字节）上，构建后二进制的 A/B 结果：

| 指标 | `--detail-store none` | `--detail-store duckdb` |
|------|-----------------------|-------------------------|
| 总耗时 | 11.3–13.8s | 13.7–14.1s |
| 峰值 RSS | 199–203 MB | 320–323 MB |
| JSON 大小 | 59 KB | 59 KB |
| 事务总数 | 588,693 | 588,693 |
| 行总数 | 3,571,620 | 3,571,620 |
| 事件总数 | 5,887,186 | 5,887,186 |

默认 `none` 模式降低峰值 RSS 约 38%（约 120 MB）。总耗时差异不大，瓶颈仍在 parser 和流式聚合路径。

这些数字会受工作负载和机器影响。它们用于回归基线，而不是通用吞吐承诺。

## 兼容性说明

- 默认行为变更：`binlogviz analyze` 默认不再创建 DuckDB 临时存储
- 如需恢复旧行为，请传入 `--detail-store duckdb`
- 没有 JSON schema 变化
- 现有 snapshot 和工作流保持兼容

## 破坏性变更

- 默认 `--detail-store` 从 `duckdb` 改为 `none`；依赖 DuckDB 明细存储的工作流应显式传入 `--detail-store duckdb`
