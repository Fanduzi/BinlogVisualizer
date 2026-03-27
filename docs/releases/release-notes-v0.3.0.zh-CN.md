# BinlogViz v0.3.0 发布说明

## 范围

`v0.3.0` 引入了更便捷的 binlog 输入发现模式、版本命令，以及完整的文档架构重构。

这是一次向后兼容的功能发布，分析结果模型没有任何破坏性变更。

## 主要内容

### 发现模式

- 新增 `--from-dir` 和 `--prefix` 标志，从目录自动发现并排序 binlog 文件
- 文件按前缀加纯数字后缀匹配，再按数字值排序
- 发现模式与位置参数文件模式互斥
- 解析出的文件列表在分析开始前输出到 `stderr`

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin.
```

### 版本命令

- `binlogviz version` 打印 ASCII logo 和版本号
- `binlogviz --version` 只打印版本号
- 版本号在构建时通过 ldflags 注入

### 文档架构

按读者意图重构为三个文档分区：

- **概念文档** (`docs/concept/`)：BinlogViz 是什么、如何工作
- **操作指南** (`docs/recipe/`)：如何完成具体任务
- **参考文档** (`docs/reference/`)：稳定的 CLI 和输出契约

每个分区都有 EN + zh-CN 双语覆盖。根 README 现在作为导航枢纽。

## 使用说明

- 发现模式必须同时提供 `--from-dir` 和 `--prefix`
- 前缀之后没有纯数字后缀的文件会被排除
- 解析出的文件列表输出到 `stderr`，保证 `stdout` 上的报告干净

## 打包说明

- Release artifact 继续覆盖：
  - `darwin/amd64`
  - `darwin/arm64`
  - `linux/amd64`
  - `linux/arm64`
- Release 下载继续附带 checksum 文件。
- 推荐安装方式仍然是 GitHub Release artifact；源码构建仍作为 fallback。
