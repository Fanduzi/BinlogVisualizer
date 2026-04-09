# BinlogViz v0.10.0 发布说明

发布日期：2026-04-09

## 范围

`v0.10.0` 是建立在 `v0.9.1` 之上的一次功能发布。这个版本把工作负载模式提升为一等分析维度，让团队可以直接用“重复出现的写入形状”来解释漂移，而不必只停留在总量、热点表或个别大事务上。

## 主要内容

- `analyze` 新增工作负载模式聚合，在 text 输出中展示 top patterns，并在 JSON 输出中稳定暴露顶层 `patterns` 数组
- `compare` 新增 `pattern_changes`，使 current-versus-baseline 对比可以直接指出究竟是哪类写入模式驱动了变化
- `trend` 新增 `pattern_trends`，覆盖 text 摘要、JSON 序列，以及默认展示 `share of rows`、可切换到绝对 `rows` 的 HTML `Pattern Trends` 分区
- 通过显式 `report_version`、legacy 兼容回归、fixture-backed golden 和 release smoke，进一步加固结果可信度和发布就绪度
- 同步更新面向操作者的文档，使 CLI reference、输出契约、安装示例和 release surface 都能反映新的模式分析工作流

## 兼容性说明

- 对于缺少 pattern 数据的旧 snapshot 和 analyze 报告，`compare` 与 `trend` 会按空集合处理，不会直接失败
- snapshot 工作流现在接受 `report_version` 2，同时仍会明确拒绝未知的未来版本
- 现有 analyze、snapshot、compare 和 trend 命令形态保持不变；这个版本是在原有结果上新增分区，而不是移除旧输出

## 破坏性变更

无。
