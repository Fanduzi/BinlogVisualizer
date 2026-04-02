# BinlogViz v0.8.0 发布说明

发布日期：2026-04-02

## 范围

`v0.8.0` 在现有 analyze 和 compare 命令之上增加了完整的 snapshot 工作流。这个版本的重点是让用户可以按名字保存分析窗口、之后再查看，并且不用手工管理 JSON 文件路径就能直接做对比。

## 主要内容

- `binlogviz analyze --format json --snapshot-name <name>` 会在继续把 JSON 报告写到 `stdout` 的同时，把同一份载荷保存到默认快照目录
- 默认快照目录为 `~/.binlogviz/snapshots`，analyze、snapshot 子命令和 compare 快照模式都可以通过 `--snapshot-dir` 覆盖
- 新增 `binlogviz snapshot save <report.json> --name <name>`，用于把已有的 analyze JSON 报告导入快照目录
- 新增 `binlogviz snapshot list`，按稳定名字顺序列出已保存快照
- 新增 `binlogviz snapshot show <name>`，输出快照元数据和紧凑结果摘要
- 新增 `binlogviz compare --current-snapshot <name> --baseline-snapshot <name>`，可直接执行快照对快照的对比
- 当输入报告带有 analyze 快照元数据时，compare JSON 会额外包含 `current_snapshot` 和 `baseline_snapshot`

## 输出契约说明

- 使用 `--snapshot-name` 时，analyze JSON 会新增可选的顶层 `snapshot` 对象
- 成功保存快照时，会在 `stderr` 输出 `Saved snapshot "<name>" to <path>`
- 为兼容现有自动化，旧的 compare 文件模式继续保留
- 如果输入报告没有 snapshot 元数据，compare 会回退到 `current` / `baseline` 标签

## 打包说明

- 计划提供的 release artifact 平台：
  - `darwin/amd64`
  - `darwin/arm64`
  - `linux/amd64`
  - `linux/arm64`
- Release 下载同时提供 checksum 文件。
- GitHub Release 仍然是打包二进制的首选安装路径。
- 源码构建继续作为本地环境的 fallback。
