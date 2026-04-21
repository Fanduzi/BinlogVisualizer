# BinlogViz v0.20.2 发布说明

发布日期：2026-04-21

## Bug 修复

- **Probe 回归导致全部文件被排除**：v0.20.1 引入的 fallback 在大文件 offset 探测失败时将 `LastEventAt` 设为 `FirstEventAt`，导致 planner 将每个文件视为单个时间点，使 `--from-dir --start --end` 对任何时间窗口都解析到 0 个文件。现已移除该 fallback，`LastEventAt` 保持为零，planner 会在结束时间未知时保守地包含文件。
- **静默生成空结果**：当发现阶段解析到 0 个 binlog 文件时（例如没有文件与请求的时间窗口重叠），流水线此前会继续解析空输入、完成空分析并静默保存空 snapshot。现在 CLI 会立即报错并给出明确提示，不再产生误导性的空输出。

## 破坏性变更

无。
