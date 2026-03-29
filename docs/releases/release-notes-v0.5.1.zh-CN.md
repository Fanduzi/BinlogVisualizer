# BinlogViz v0.5.1 发布说明

发布日期：2026-03-29

## 概述

v0.5.1 主要改进安装体验和发布分发链路。项目现在会在 release workflow 中生成 Homebrew cask 定义，让 macOS 安装更直接，同时继续把 GitHub Release artifact 作为权威分发来源。

## 新功能

### Homebrew Cask 安装路径

BinlogViz 现在支持基于 tap 的 macOS Homebrew 安装方式：

```bash
brew tap Fanduzi/binlogviz
brew install --cask binlogviz
```

生成出的 cask 会指向带 tag 的 GitHub Release artifact，并在安装过程中移除 macOS quarantine 属性，让首次运行更顺畅。

### Release Workflow 同步 Tap

release workflow 现在会：

- 在原生 runner 上直接构建带版本号的归档
- 基于最终 release artifact 生成各平台 checksum
- 为当前 tag 生成 Homebrew cask 定义
- 把该 cask 同步到 `Fanduzi/homebrew-binlogviz` tap 仓库

这样 Homebrew 安装路径就能和仓库中记录的 release artifact 保持一致。

## Bug 修复

### Release 版本号注入

发布构建流程现在会在 release 构建时直接把 tag 版本注入二进制，避免带 tag 的发布版本里混入 snapshot 风格的版本字符串。

## 破坏性变更

无。
