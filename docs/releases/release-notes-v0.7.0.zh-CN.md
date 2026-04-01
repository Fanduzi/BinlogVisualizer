# BinlogViz v0.7.0 发布说明

发布日期：2026-04-02

## 范围

`v0.7.0` 在现有本地 binlog 分析 CLI 的基础上引入了可视化 compare 工作流。这个版本的核心是比较两份由 `binlogviz analyze --format json` 生成的报告，并以 text、JSON 和图表化 HTML 形式展示差异，便于 DBA 场景下的工作负载回顾。

## 主要内容

- 新增 `binlogviz compare <current.json> <baseline.json>` 命令
- 新增 BinlogViz JSON compare 输入加载与校验
- 新增 summary、table、operation、alert 维度的 compare 结果模型
- 新增面向终端和自动化场景的 text / JSON compare renderer
- 新增基于内嵌 ECharts 的图表化 HTML compare 报告
- 新增最小 `analyze -> compare` 集成覆盖，并稳定了 locale-sensitive 命令断言
- 完成 coverage uplift，使 Go 总覆盖率超过 80% 质量线

## Compare 输出形式

- `--format text`
  - 面向终端的精简 compare 摘要
- `--format json`
  - 面向下游工具的机器可读 compare 结构
- `--format html`
  - 基于图表的可视化 compare 报告，覆盖 rows、transactions、warnings、top table deltas、operation mix 和 alerts

## 打包说明

- 计划提供的 release artifact 平台：
  - `darwin/amd64`
  - `darwin/arm64`
  - `linux/amd64`
  - `linux/arm64`
- Release 下载同时提供 checksum 文件。
- GitHub Release 仍然是打包二进制的首选安装路径。
- 源码构建继续作为本地环境的 fallback。
