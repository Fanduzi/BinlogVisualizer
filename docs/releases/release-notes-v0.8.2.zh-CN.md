# BinlogViz v0.8.2 发布说明

发布日期：2026-04-02

## 范围

`v0.8.2` 是建立在 `v0.8.1` 之上的一次 release pipeline 热修复。这个补丁版本只修复 Linux manylinux2014 打包工作流，使 tag 触发的发布任务能够顺利完成。

## 主要内容

- Linux manylinux2014 构建任务现在会在容器内的 `go build` 步骤关闭 Go 的 VCS stamping
- 这个修复直接解决了阻塞 `v0.8.1` release workflow 的 `error obtaining VCS status: exit status 128` 错误
- 本次发布没有修改产物命名、安装 URL、CLI 参数或 snapshot workflow 行为

## 兼容性说明

- Linux release 打包仍以兼容 CentOS 7 / `glibc 2.17` 为目标
- macOS release 打包保持不变
- 这个补丁版本不会修改 BinlogViz CLI 的运行时行为

## 破坏性变更

无。
