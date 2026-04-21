# BinlogViz v0.20.3 发布说明

发布日期：2026-04-22

## 变更

- **用顺序推断替换基于 offset 的末尾时间戳探测**：之前的 `probeLastTimestamp` 函数从每个 binlog 文件末尾 256KB 处解析以获取 `LastEventAt`。在大文件上，偏移量落在事件中间会导致解析错误，`LastEventAt` 留零——在最需要过滤的场景下反而无效。新方法只探测每个文件的 `FirstEventAt`（从 offset 0 开始，可靠），然后从 `FirstEventAt[N+1]` 推断 `LastEventAt[N]`。由于 binlog 文件按序写入且时间戳单调递增，这个推断始终可靠。序列中最后一个文件的 `LastEventAt` 保持为零（planner 保守包含）。全量解析的 fallback 路径不变，仍然设置精确的 `LastEventAt`。

## Bug 修复

无。

## 破坏性变更

无。
