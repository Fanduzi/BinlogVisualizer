# BinlogViz v0.2.2 发布说明

## 范围

`v0.2.2` 是建立在首个成功公开发布的 Phase 2 版本之上的一次仓库收尾版本。

它不修改核心分析链路，主要用于让仓库门面、Go 工具链要求和对外发布文档与当前实际发布状态保持一致。

## 主要内容

- 将文档和 `go.mod` 中的 Go 版本要求统一提升到 `1.26.1`
- 新增中文仓库 README
- 新增仓库级 `CHANGELOG.md` 和 `SECURITY.md`
- 更新 README 顶部导航和 release notes 入口，使其指向最新发布版本

## 打包说明

- Release artifact 继续覆盖：
  - `darwin/amd64`
  - `darwin/arm64`
  - `linux/amd64`
  - `linux/arm64`
- Release 下载继续附带 checksum 文件。
- 推荐安装方式仍然是 GitHub Release artifact；源码构建仍作为 fallback。
