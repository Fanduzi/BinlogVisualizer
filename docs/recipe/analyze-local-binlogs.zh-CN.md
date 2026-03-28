# 分析本地 Binlog

本文聚焦实际 DBA 工作流，说明如何使用 `binlogviz analyze` 对本地 binlog 文件做可落地的分析。

## 先从一个文件开始

如果你希望用最快路径建立信心，先从单个文件开始：

```bash
binlogviz analyze mysql-bin.000123
```

这是最快能确认以下几点的方式：

- 文件确实存在于本地
- 文件能够被成功解析
- 默认文本报告已经足够支撑第一轮排查

默认情况下，最终报告写到 `stdout`，而进度、discovery 解析出的文件列表、最终组装状态以及错误写到 `stderr`。

## 目录分析时优先用 discovery 模式

如果文件位于同一个目录中，并且采用数字后缀命名，discovery 模式通常是最稳妥的运维路径：

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin.
```

成功时，BinlogViz 会：

1. 扫描目录下的直接子项
2. 仅保留前缀之后为纯数字后缀的文件
3. 按数字后缀排序
4. 将最终解析出的有序文件列表输出到 `stderr`
5. 按该顺序执行分析

如果你已经明确知道要分析哪些文件以及顺序，也可以直接使用位置参数：

```bash
binlogviz analyze mysql-bin.000123 mysql-bin.000124 mysql-bin.000125
```

如果更符合你的工作流，也可以让 shell 先展开模式：

```bash
binlogviz analyze mysql-bin.*
```

需要注意，模式展开规则来自你的 shell，而不是 BinlogViz 本身。

## 聚焦已知故障时间窗口

当文件集合覆盖的时间范围比你真正关心的问题更长时，可以使用 `--start` 和 `--end` 缩小范围：

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. \
  --start "2026-03-15T10:00:00Z" \
  --end "2026-03-15T10:30:00Z"
```

适用场景包括：

- 已知故障窗口排查
- 目录中的历史数据远多于你当前想看的范围
- 需要把排名和汇总严格限制在某个时间片内

无效时间戳会在分析开始前直接失败，而且 `--end` 不能早于 `--start`。

## 用 schema / 表过滤减少噪音

当你需要聚焦某个服务、某个 schema 或一组热点表时，使用 schema 和表过滤会更高效。

### 只分析一个 schema

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. \
  --include-schema orders
```

### 排除内部 schema

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. \
  --exclude-schema mysql,sys,information_schema,performance_schema
```

### 聚焦特定表

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. \
  --include-schema orders \
  --include-table payments,refunds
```

这些过滤发生在分析阶段，而不是仅仅在最终渲染阶段生效，因此既能减少噪音，也能更有效地收紧负载范围。

## 有意识地选择文本或 JSON 输出

### 默认文本输出，适合人工查看

```bash
binlogviz analyze mysql-bin.000123
```

这是直接在终端中查看报告时最合适的默认方式。

### JSON 输出，适合脚本和管道

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. --json > analyze.json
```

这种方式更适合：

- shell 管道
- 自动化校验
- 下游数据装载
- CI 或运维工具中的结果比对

## 调整报告宽度

如果默认的 top-10 对你的负载来说不够宽，可以显式扩大输出范围：

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. \
  --top-tables 20 \
  --top-transactions 20 \
  --top-minutes 30
```

这些参数调整的是报告宽度，而不是底层解析范围。

## 关注异常时打开告警检测

当你希望 BinlogViz 主动指出异常负载时，可以开启尖峰检测，并在需要时配合调整大事务阈值：

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. \
  --detect-spikes \
  --large-trx-rows 5000 \
  --large-trx-duration 60s
```

这更适合异常排查，而不仅仅是查看排名。

## 控制 SQL 上下文暴露程度

使用 `--sql-context` 控制事务查询上下文在报告中的展示方式：

```bash
binlogviz analyze mysql-bin.000123 --sql-context off
binlogviz analyze mysql-bin.000123 --sql-context summary
binlogviz analyze mysql-bin.000123 --sql-context full
```

模式说明：

- `off`：省略查询相关字段
- `summary`：保留有界摘要，便于运维快速建立上下文
- `full`：在可用时包含有界存储后的 SQL 文本

这个设置改变的是展示方式，不改变底层负载指标。

## 有意识地重定向输出通道

当你希望归档报告并单独保留运行日志时，可以分别重定向两个输出通道：

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. \
  > report.txt \
  2> analyze.stderr.log
```

这样既能保持报告流纯净，也能保留 discovery 列表、进度、最终组装状态和错误信息。

## 后续参考

当你已经拿到一条稳定可工作的运维命令后，可以继续阅读：

- [CLI 参考](../reference/cli.zh-CN.md)
- [输入发现参考](../reference/input-discovery.zh-CN.md)
- [输出格式参考](../reference/output-format.zh-CN.md)
- [分析模型](../concept/analysis-model.zh-CN.md)
