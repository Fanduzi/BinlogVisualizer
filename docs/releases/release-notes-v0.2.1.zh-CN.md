# BinlogViz v0.2.1 发布说明

## 范围

`v0.2.1` 是在修正 `v0.2.0` 首次发布尝试中暴露出的 GitHub Actions release matrix 和 workflow 对齐问题后，首个可正式发布的 Phase 2 版本。

## 主要内容

- 真正的 streaming command path：
  - `parser -> normalize -> analyzer.Consume -> analyzer.Finalize`
- 由 command 层管理生命周期的 DuckDB temp store
- `--sql-context summary|off|full`
- 扩展后的 command-path benchmark 与 fixture coverage
- 已与当前 GitHub hosted runner 标签对齐的 release workflow

## 打包说明

- 计划提供的 release artifact 平台：
  - `darwin/amd64`
  - `darwin/arm64`
  - `linux/amd64`
  - `linux/arm64`
- Release 下载同时提供 checksum 文件。
- 推荐安装方式是直接使用 GitHub Release artifact；源码构建仍作为 fallback。
