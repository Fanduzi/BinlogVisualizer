# BinlogViz v0.9.1 发布说明

发布日期：2026-04-04

## 范围

`v0.9.1` 是建立在 `v0.9.0` 之上的一次工作流加固发布。这个补丁版本重点解决已经公开宣传的 `analyze -> snapshot -> compare -> trend` 主线在真实使用中的可信度问题，尤其面向直接从 `analyze` 保存 snapshot、并把新旧报告放在同一历史库中管理的团队。

## 主要内容

- 修复流式分析器中的真实 `txn_count` 传播问题，基于 fixture 的真实分析链路里，表级和分钟级事务数不再全部塌成 `0`
- `analyze --snapshot-name` 现在会在用户未显式传入 `--start` / `--end` 时补写有效 snapshot window，使 analyze 直接生成的快照可以立刻参与 `trend`
- 对缺少 `snapshot.window` 的旧快照，trend 现在会回退使用 `summary.start_time` 和 `summary.end_time`
- `warnings` 不再固定为 `0`，现在会在 JSON、snapshot 工作流和 text 报告中反映 query context 被截断这类分析退化
- `snapshot list --format text` 现在会输出可读的清单表格，包含名称、标签、创建时间、输入模式和窗口摘要

## 兼容性说明

- 已经包含 `snapshot.window.start_time` 的 `v0.9.0` 快照无需修改即可继续使用
- 对于缺少 `snapshot.window.start_time` 的旧快照，只要 `summary.start_time` 存在，就可以参与 `trend`
- `compare` 和 `snapshot show` 继续接受现有 snapshot JSON 结构；这个版本只强化 fallback 和展示行为
- text 和 JSON 输出结构保持兼容，只是当分析过程中发生有界 query-context 丢失时，`warnings` 现在会呈现真实的非零值

## 破坏性变更

无。
