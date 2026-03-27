# 快速开始

本指南提供从安装到第一次成功运行 `binlogviz analyze` 的最短路径。

## 1. 安装 BinlogViz

先安装 BinlogViz，然后回到本指南完成第一次成功分析。

关于当前的安装方式、release artifact 以及本地构建选项，请查看项目 [README](../../README_ZH.md#安装)。

## 2. 分析一个 Binlog 文件

从一个本地 ROW 格式 binlog 文件开始：

```bash
binlogviz analyze mysql-bin.000123
```

默认情况下，BinlogViz 会把最终文本报告写到 `stdout`，并把进度或运行时状态保留在 `stderr`。

文本报告包含五个章节：

- Workload Summary
- Top Tables
- Top Transactions
- Minute Activity
- Alerts

## 3. 使用 JSON 输出进行分析

当你希望把结果交给脚本或下游工具处理时，使用 `--json`：

```bash
binlogviz analyze mysql-bin.000123 --json
```

如果你想直接把 JSON 保存到文件：

```bash
binlogviz analyze mysql-bin.000123 --json > analyze.json
```

## 4. 自动发现一个有序 Binlog 范围

如果你的 binlog 文件位于同一个目录中，并遵循数字命名模式，可以让 BinlogViz 自动解析有序输入集合：

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin.
```

BinlogViz 会在分析前先解析出有序文件集合，并保持最终报告输出适合终端查看或 shell 重定向。

关于精确的 discovery 规则、排序行为和非法组合，请参见 [输入发现参考](../reference/input-discovery.zh-CN.md)。

## 5. 对照示例输出

你可以查看仓库附带的样例输出，了解一次成功分析通常长什么样：

- 文本报告：`docs/examples/analyze-output.txt`
- JSON 报告：`docs/examples/analyze-output.json`

这些示例适合用于上手、演示和下游集成检查。

## 下一步参考

如果你需要稳定的命令与输出契约，请继续阅读：

- [CLI 参考](../reference/cli.zh-CN.md)
- [输入发现参考](../reference/input-discovery.zh-CN.md)
- [输出格式参考](../reference/output-format.zh-CN.md)
