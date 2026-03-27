# 分析本地 Binlog

本文展示如何使用 `binlogviz analyze` 对本地 binlog 文件执行实际可落地的运维分析流程。

## 分析单个文件

当你想以最快路径验证格式、输出形状以及工具基本行为时，先从单个文件开始。

```bash
binlogviz analyze mysql-bin.000123
```

这是最适合的第一次运行方式，用于确认：

- 文件确实存在于本地
- 文件能够被成功解析
- 默认文本报告已足够支撑初步查看

默认情况下，最终报告写到 `stdout`，而进度信息与运行状态写到 `stderr`。

## 分析多个文件

如果你已经明确知道要查看的文件集合及顺序，可以直接使用位置参数传入多个文件。

```bash
binlogviz analyze mysql-bin.000123 mysql-bin.000124 mysql-bin.000125
```

这种模式适用于：

- 你想分析一个手工挑选好的范围
- 你的 shell 或脚本已经解析出了目标文件列表
- 你不希望由发现模式决定输入集合

如果更符合你的运维习惯，也可以让 shell 先展开模式：

```bash
binlogviz analyze mysql-bin.*
```

需要注意，模式展开的规则来自你的 shell，而不是 BinlogViz 自己实现的。

## 使用 discovery 模式

如果你的文件都放在同一目录下，并且遵循稳定的数字后缀命名规则，可以使用 discovery 模式。

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin.
```

discovery 模式成功时，BinlogViz 会：

1. 扫描该目录下的直接子项
2. 仅保留前缀之后为纯数字后缀的文件
3. 按数字后缀排序
4. 将最终解析出的有序文件列表输出到 `stderr`
5. 按该顺序执行分析

这种方式适合你希望用一条稳定、可重复执行的命令完成运维分析，而不想手工列出每个文件时使用。

如果你想了解精确的匹配与排序契约，请继续阅读[输入发现参考](../reference/input-discovery.zh-CN.md)。

## 按时间窗口过滤

如果你只关心某个 RFC3339 时间范围内的活动，可以使用 `--start` 与 `--end`。

```bash
binlogviz analyze mysql-bin.000123 mysql-bin.000124 \
  --start "2026-03-15T10:00:00Z" \
  --end "2026-03-15T10:30:00Z"
```

适用场景包括：

- 你正在排查一个已知的故障时间窗口
- 输入文件覆盖的时间范围远大于你真正关心的范围
- 你希望报告的总量与排名都严格限定在某个时间段内

请记住，无效时间戳会在分析开始前直接失败，而且 `--end` 不能早于 `--start`。

## 为自动化输出 JSON

如果结果需要被其他工具或脚本消费，使用 `--json`。

```bash
binlogviz analyze mysql-bin.000123 --json > analyze.json
```

这种方式会把机器可读报告保留在 `stdout`，同时继续让进度和运行状态输出到 `stderr`。

它更适合：

- shell 管道
- 自动化检查
- 下游数据装载
- 类 CI 的输出结构校验

## 调整 Top-N 输出

如果默认的 top-10 排名对你的排查不够，可以显式扩大范围。

```bash
binlogviz analyze mysql-bin.000123 mysql-bin.000124 \
  --top-tables 20 \
  --top-transactions 20
```

适用场景：

- 你的负载涉及很多热点表
- 你需要比默认更宽的短名单
- 你希望在最终报告里覆盖更多事务

这些参数调整的是报告展示范围，而不是底层解析范围。

## 启用尖峰检测

当你希望分析器主动指出分钟级别的异常高负载活动时，可以开启尖峰检测。

```bash
binlogviz analyze mysql-bin.000123 mysql-bin.000124 \
  --detect-spikes
```

如果你的环境需要不同的告警灵敏度，也可以叠加自定义大事务阈值。大事务告警与尖峰检测彼此独立，所以即使不打开 `--detect-spikes`，你仍然可以单独调整这些阈值。

```bash
binlogviz analyze mysql-bin.000123 mysql-bin.000124 \
  --detect-spikes \
  --large-trx-rows 5000 \
  --large-trx-duration 60s
```

这种方式适合你关注的是运维异常，而不仅仅是负载排名。

## 选择 SQL 上下文模式

使用 `--sql-context` 控制事务查询上下文在报告中的呈现方式。

```bash
binlogviz analyze mysql-bin.000123 --sql-context off
binlogviz analyze mysql-bin.000123 --sql-context summary
binlogviz analyze mysql-bin.000123 --sql-context full
```

模式说明：

- `off`：完全隐藏所有查询相关字段
- `summary`：保留有界摘要，便于运维快速建立上下文
- `full`：在可用时包含有界存储后的 SQL 文本

这个设置改变的是展示方式，而不是底层负载指标本身。

## 有意识地重定向输出通道

如果你想干净地保存最终报告，并把运行日志分开记录，可以分别重定向两个通道。

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. \
  > report.txt \
  2> analyze.stderr.log
```

适用场景：

- 你要归档最终报告
- 你希望把发现模式输出和进度日志单独保存
- 你要把报告喂给另一个进程，同时不混入状态噪音

## 后续参考

当你已经有一条可工作的运维命令后，可以继续阅读这些文档：

- [CLI 参考](../reference/cli.zh-CN.md)
- [输入发现参考](../reference/input-discovery.zh-CN.md)
- [输出格式参考](../reference/output-format.zh-CN.md)
- [分析模型](../concept/analysis-model.zh-CN.md)
