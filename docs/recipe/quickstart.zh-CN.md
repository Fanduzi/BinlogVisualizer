# 快速开始

本指南提供从安装到第一次拿到有价值 `binlogviz analyze` 结果的最短路径。

## 1. 安装或验证 BinlogViz

如果你需要带发布版本号的二进制，优先使用 release artifact；如果你是在本地开发或验证，也可以直接从源码构建。

当前安装命令和 artifact 示例，请查看项目 [README](../../README_ZH.md#安装)。

安装完成后，可以这样验证二进制：

```bash
binlogviz --version
binlogviz version
```

- `binlogviz --version` 只输出版本号
- `binlogviz version` 输出 ASCII Logo 加 `binlogviz <version>`

## 2. 先用一个文件验证分析链路

从一个本地 `ROW` binlog 文件开始：

```bash
binlogviz analyze mysql-bin.000123
```

这是最快能确认以下几点的方式：

- 文件可读
- BinlogViz 可以成功解析
- 默认文本报告已经能提供有价值的第一轮信息

默认情况下，最终报告写到 `stdout`，进度和运行时状态保留在 `stderr`。

## 3. 按 binlog 顺序分析整个目录

如果文件位于同一个目录下，并且文件名采用数字后缀，discovery 模式通常是最稳定、最可重复的运维路径：

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin.
```

BinlogViz 会在分析前先解析出最终的有序文件集合，并把解析结果打印到 `stderr`。

关于精确的匹配和排序规则，请参见[输入发现参考](../reference/input-discovery.zh-CN.md)。

## 4. 只看你关心的时间窗口

如果这些文件覆盖的时间范围比你要排查的故障窗口更长，可以用 `--start` 和 `--end` 缩小报告范围：

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. \
  --start "2026-03-15T10:00:00Z" \
  --end "2026-03-15T10:30:00Z"
```

这两个参数都使用 RFC3339 时间格式。

## 5. 安全地输出 JSON

如果结果要交给另一个脚本或工具处理，使用 `--json`：

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. --json > analyze.json
```

这样机器可读报告保留在 `stdout`，而进度和运行时状态继续保留在 `stderr`。

## 6. 对照示例输出

如果你希望在接入自己的流程之前先看看一次成功运行大致长什么样，可以查看仓库自带样例：

- 文本报告：`docs/examples/analyze-output.txt`
- JSON 报告：`docs/examples/analyze-output.json`

## 下一步

当第一次运行已经成功后，继续阅读：

- [分析本地 Binlog](analyze-local-binlogs.zh-CN.md)
- [CLI 参考](../reference/cli.zh-CN.md)
- [输入发现参考](../reference/input-discovery.zh-CN.md)
- [输出格式参考](../reference/output-format.zh-CN.md)
