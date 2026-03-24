# BinlogViz v0.2.3 发布说明

## 范围

`v0.2.3` 是一次聚焦 CLI 可用性的 Phase 2 小版本发布。

它不改变分析结果模型，主要用于让较长时间运行的 `analyze` 命令具备可观察的解析进度。进度总量基于按命令顺序传入的 binlog 文件总大小。

## 主要内容

- 为 `binlogviz analyze` 新增单条聚合解析进度条
- 进度总量基于输入 binlog 文件大小总和，并按命令输入顺序累计
- 将进度条和最终 `Finalizing analysis...` 状态固定输出到 `stderr`，保证 `stdout` 上的文本/JSON 报告保持干净
- 保持当前串行多文件解析模型，同时正确处理重复输入路径
- 补充回归测试，覆盖 parser 进度上报、重复路径聚合，以及 `stdout`/`stderr` 输出隔离

## 使用说明

- 进度条反映的是解析阶段的文件偏移推进，而不是最终 DuckDB 汇总阶段的完成度
- 解析完成后，命令会先在 `stderr` 打印 `Finalizing analysis...`，随后输出最终报告
- JSON 输出继续保持可机读，因为进度信息不会写入 `stdout`

## 打包说明

- Release artifact 继续覆盖：
  - `darwin/amd64`
  - `darwin/arm64`
  - `linux/amd64`
  - `linux/arm64`
- Release 下载继续附带 checksum 文件。
- 推荐安装方式仍然是 GitHub Release artifact；源码构建仍作为 fallback。
