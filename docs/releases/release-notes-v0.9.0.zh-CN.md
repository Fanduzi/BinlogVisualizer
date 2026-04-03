# BinlogViz v0.9.0 发布说明

发布日期：2026-04-03

## 范围

`v0.9.0` 是建立在 `v0.8.3` 之上的一次功能发布。这个版本把已保存的 snapshot 提升成多时间点分析工作流，让团队不再只停留在单次分析或两两 compare，而是可以直接观察多个窗口之间的负载漂移。

## 主要内容

- 新增顶层命令 `binlogviz trend`，用于多 snapshot 趋势分析
- trend 同时支持显式 snapshot 列表和 `--from-snapshots <pattern>` 模式匹配，并统一按 `snapshot.window.start_time` 排序
- 新增可选 `--baseline-snapshot`，每个趋势点都可以相对基线窗口输出 delta
- 新增 `text`、`json`、`html` 三种趋势报告，覆盖总体指标走势、操作类型占比变化和热点表变化摘要
- 文档和自动化测试已同步覆盖新的 trend 工作流及输出合同

## 兼容性说明

- 现有 `analyze`、`compare`、`snapshot` 工作流保持可用，合同没有破坏性变化
- trend 要求 snapshot 带有合法的 `snapshot.window.start_time`；无效或不完整快照会直接报错，不会被静默跳过
- baseline snapshot 是可选增强信息，不会被自动并入趋势点集合

## 破坏性变更

无。
