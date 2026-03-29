# BinlogViz v0.5.2 发布说明

发布日期：2026-03-29

## 概述

v0.5.2 修复了 v0.5.1 中引入的 Homebrew tap 同步路径问题。release workflow 现在会在 clone 和 push tap 仓库之前显式配置 git 认证。

## 新功能

无。

## Bug 修复

### Homebrew Tap 同步认证

release workflow 现在会在同步 tap 之前执行 `gh auth setup-git`。这样 workflow 在更新 `Fanduzi/homebrew-binlogviz` 时，git 操作就能稳定使用 GitHub 凭证完成认证。

## 破坏性变更

无。
