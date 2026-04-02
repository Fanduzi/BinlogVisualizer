# BinlogViz v0.8.1 发布说明

发布日期：2026-04-02

## 范围

`v0.8.1` 是建立在 `v0.8.0` snapshot workflow 之上的一次发布加固补丁。这个版本主要修复对外文档中的旧版本引用，并收紧 Linux release pipeline，使打包产物面向更低的 glibc 兼容基线。

## 主要内容

- landing page 的 hero 文案和命令示例已经更新为 `v0.8.0` 的 snapshot workflow，而不是只停留在旧的 visual compare 叙事
- `README.md` 和 `README_ZH.md` 的安装示例已经从过期的 `v0.6.0` artifact URL 切换到当前 release 版本线
- Linux release 构建现在在 manylinux2014 用户态容器内执行，不再继承 GitHub runner 主机的 glibc 版本
- release workflow 增加了 glibc 符号基线检查；如果 Linux 产物漂移到高于 `glibc 2.17` 的依赖，将会直接失败

## 兼容性说明

- Linux release 打包现在以兼容 CentOS 7 / `glibc 2.17` 为目标，同时覆盖 `linux/amd64` 和 `linux/arm64`
- macOS release 打包仍然使用 GitHub 托管的原生 runner
- 这个补丁版本没有修改 CLI 参数、报告契约或 snapshot 命令

## 破坏性变更

无。
