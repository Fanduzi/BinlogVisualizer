# BinlogViz v0.2.0 发布说明

## 范围

`v0.2.0` 是 Phase 2 的首个发布版本，包含已经完成的 streaming analysis 主路径、DuckDB 临时结果存储、SQL context 展示控制，以及相应的 benchmark / fixture 覆盖。

## 主要内容

- 真正的 streaming command path：
  - `parser -> normalize -> analyzer.Consume -> analyzer.Finalize`
- 由 command 层管理生命周期的 DuckDB temp store
- `--sql-context summary|off|full`
- 扩展后的 command-path benchmark 与 fixture coverage

## 打包说明

- 计划提供的 release artifact 平台：
  - `darwin/amd64`
  - `darwin/arm64`
  - `linux/amd64`
  - `linux/arm64`
- Release 下载同时提供 checksum 文件。
- 推荐安装方式是直接使用 GitHub Release artifact；源码构建仍作为 fallback。
