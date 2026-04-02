# BinlogViz v0.8.3 发布说明

发布日期：2026-04-02

## 范围

`v0.8.3` 是建立在 `v0.8.2` 之上的一次 snapshot workflow 加固发布。这个补丁版本重点解决“快照能保存，但还不够适合长期运维使用”的问题，尤其面向需要脚本化消费快照库、并且要持续整理快照历史的团队。

## 主要内容

- 新增 `binlogviz snapshot rename <old> <new>` 和 `binlogviz snapshot delete <name>`，用户可以直接在 CLI 内整理快照历史，而不用手工改文件名或删文件
- 新增 `snapshot list --format json` 和 `snapshot show --format json`，方便脚本和外部工具稳定消费快照目录
- compare 的 text 和 HTML 输出现在会带出 input mode、来源摘要、过滤条件和请求时间窗口
- 集成测试补齐了老 analyze JSON 导入、默认 `~/.binlogviz/snapshots` 行为、重名冲突、非法名字和缺失快照等真实运维场景

## 兼容性说明

- 默认快照目录仍然是 `~/.binlogviz/snapshots`
- 旧的 `binlogviz compare <current.json> <baseline.json>` 文件模式仍然保留
- compare JSON 结构保持向后兼容；新增上下文主要体现在 text 和 HTML 输出
- 仍然支持导入不带 `snapshot` 对象的旧 analyze JSON 报告

## 破坏性变更

无。
