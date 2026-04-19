# BinlogViz v0.18.1 发布说明

发布日期：2026-04-19

## 范围

`v0.18.1` 是面向 v0.18 DBA 报告工作流的稳定性与真实性能验证版本。

这个版本保持 v0.18 的 JSON 与报告契约不变，同时用 sysbench 生成的真实 988 MB MySQL 8.0 ROW binlog 校验 analyze 热路径。重点是有界内存行为、可复现 benchmark 门禁，以及更准确的发布表面文档。

## 主要内容

- 新增由 `BINLOGVIZ_REAL_BINLOG` 环境变量控制的外部真实 binlog benchmark，可用本地大样本验证 analyze 路径，而不把二进制日志提交进仓库
- 新增 parser 层真实 fixture benchmark，拆分 parse-only、parse+normalize、parse+progress 和 end-to-end 层
- 降低 finalize 阶段分配热点，避免 representative transactions 的全量切片复制，并且只索引 alert 引用的事务
- 通过预分配 transaction row 读取和惰性 `Tables` / `Operations` map 初始化，降低 store transaction scan 内存
- 在 batch-size benchmark 验证后，把 DuckDB transaction batch flush 阈值从 5,000 行调整到 10,000 行
- 使用真实 988 MB ROW binlog 冒烟样本验证 JSON、HTML 和默认 text analyze 输出
- 删除未使用的 analyzer diagnostics helper

## 性能说明

在本地 988 MB MySQL ROW binlog 冒烟样本上，外部 benchmark 稳定在大约：

- analyze benchmark：约 13.5 秒
- 堆分配：约 7.95 GB/op
- 分配次数：约 151.7M allocs/op
- CLI 峰值 RSS：约 1.9 GB

这些数字会受工作负载和机器影响。它们用于回归基线，而不是通用吞吐承诺。

## 兼容性说明

- 没有 JSON schema 变化
- 没有 CLI flag 变化
- 没有报告契约变化
- 现有 `binlogviz analyze`、`compare`、`trend` 和 snapshot 工作流保持兼容
- 外部真实 binlog benchmark 只有在设置 `BINLOGVIZ_REAL_BINLOG` 时才会运行，否则会跳过

## 破坏性变更

无。
