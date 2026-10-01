# 常见错误排查

本文说明 `binlogviz analyze` 的常见失败场景，以及运维人员通常应采取的排查与修复动作。

## `file not found`

代表性错误：

```text
file not found: mysql-bin.000123
```

含义：

- 你传入的某个位置参数文件路径不存在
- shell 展开出了你没有预期到的路径
- 文件可能已经被轮转、移动，或者路径输入有误

检查点：

- 确认文件在磁盘上确实存在
- 确认当前工作目录或绝对路径是否符合预期
- 确认 shell 展开后得到的是你真正想分析的文件

典型修复：

```bash
binlogviz analyze /var/lib/mysql/mysql-bin.000123
```

如果你的当前目录并不是 binlog 所在目录，优先使用绝对路径。

## `no matching binlog files found`

代表性错误：

```text
no matching binlog files found under /var/lib/mysql with prefix "mysql-bin."
```

含义：

- discovery 模式成功扫描了目录
- 但没有任何目录项满足“前缀 + 纯数字后缀”的契约

常见原因：

- 目录写错了
- 前缀写错了
- 目录里只有 index 文件
- 文件后缀不是纯数字
- 目标文件实际上是符号链接或目录，而不是普通文件

检查点：

- 确认目录是否正确
- 确认前缀是否与真实文件名完全一致
- 确认匹配文件在前缀之后只包含数字

典型修复：

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin.
```

如果你的命名不满足 discovery 模式契约，请改用位置参数模式显式传入文件。

## `--from-dir and --prefix must be provided together`

代表性错误：

```text
--from-dir and --prefix must be provided together
```

含义：

- 你只提供了 discovery 模式所需的一部分参数

典型修复：

在同一条命令里同时提供两个参数：

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin.
```

如果你已经知道具体文件列表，也可以去掉发现模式参数，改用位置参数。

## `cannot combine positional binlog files with --from-dir/--prefix`

代表性错误：

```text
cannot combine positional binlog files with --from-dir/--prefix
```

含义：

- 同一条命令同时给了两种输入模式
- BinlogViz 每次只允许一种输入方式：位置参数文件，或者发现模式

典型修复：

只保留其中一种形式：

```bash
binlogviz analyze mysql-bin.000123 mysql-bin.000124
```

或者：

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin.
```

## `invalid start time format` 或 `invalid end time format`

代表性错误：

```text
invalid start time format: ... (use RFC3339 or YYYY-MM-DD HH:MM:SS)
invalid end time format: ... (use RFC3339 or YYYY-MM-DD HH:MM:SS)
```

含义：

- `--start` 或 `--end` 传入的值既不是合法 RFC3339，也不是 `YYYY-MM-DD HH:MM:SS`

典型修复：

跨机器请使用带显式偏移的 RFC3339：

```bash
binlogviz analyze mysql-bin.000123 \
  --start "2026-09-12T00:00:00+08:00" \
  --end "2026-09-14T15:00:00+08:00"
```

也接受 `YYYY-MM-DD HH:MM:SS`，并按运行 `binlogviz` 的机器本地时区解释。不要默认当成北京时间，除非该机器时区就是 `Asia/Shanghai`。

## `end time must be after start time`

代表性错误：

```text
end time must be after start time
```

含义：

- 两个时间边界都成功解析了
- 但结束时间早于开始时间

典型修复：

交换或修正时间戳，确保时间窗口是向前推进的。

## `invalid --sql-context`

代表性错误：

```text
invalid --sql-context "invalid" (allowed: summary, off, full)
```

含义：

- 传给 `--sql-context` 的值不在支持列表内

典型修复：

只使用支持的取值：

```bash
binlogviz analyze mysql-bin.000123 --sql-context summary
binlogviz analyze mysql-bin.000123 --sql-context off
binlogviz analyze mysql-bin.000123 --sql-context full
```

## 输出通道行为看起来不符合预期

常见现象：

- JSON 已经重定向到文件，但进度文本仍然出现在终端
- 发现模式解析出的文件列表没有出现在报告文件里
- `Finalizing analysis...` 出现在报告之外

含义：

- 这通常是符合预期的通道分离，而不是失败
- BinlogViz 把最终报告写到 `stdout`
- 进度、解析出的文件列表、finalize 状态和命令错误都写到 `stderr`

典型使用方式：

```bash
binlogviz analyze mysql-bin.000123 --format json > report.json
```

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. \
  > report.txt \
  2> analyze.stderr.log
```

如果你希望同时保存报告与运行状态，请显式分别重定向两个通道。

## `显式 BEGIN 未关闭`

这是有意失败。退出码是 1，stdout 为空，stderr 只有一条 `Error:`。

代表性错误：

```text
显式 BEGIN 未关闭
```

含义：

- 事务组里出现了 `BEGIN`
- 没有 `COMMIT`，也没有 plain `ROLLBACK`（`ROLLBACK` 或 `ROLLBACK WORK`，可以带一个结尾分号）
- 下一个不同的 GTID 到达时，这个组仍然开着

Analyze 不会在下一个 GTID 到达时把组关掉。关掉会把缺少的 `COMMIT` 藏起来。这不是 BinlogViz 的 bug。客户端崩溃或会话被杀掉，经常留下这种 bare `BEGIN`。

如果文件在这个 `BEGIN` 里结束，后面没有新的 GTID，analyze 仍可能成功。此时 JSON 在计数不为 0 时带上 `diagnostics.open_explicit_groups`。

## `ROLLBACK TO SAVEPOINT` 不是事务组结束

`ROLLBACK TO SAVEPOINT ...` 仍是 Unclassified QUERY。它不关闭事务组，也不等于 plain `ROLLBACK`。

这条语句出现在未关闭的 `BEGIN` 里，并且下一个 GTID 到来时，analyze 仍然以 exit 1 失败：

```text
显式 BEGIN 未关闭：ROLLBACK TO SAVEPOINT 不是事务组结束
```

同一组里稍后的 `COMMIT` 或 `XID` 仍然会关闭该组。

## Ignored QUERY 不会关闭事务组

`SET timestamp`、`SET NAMES`，以及其他不是 `SET ROLE` / `SET DEFAULT ROLE` 的 `SET`，是 Ignored QUERY。Analyze 丢弃它们、计数，并且从不把它们当作关组边界。这不是缺少 `COMMIT`。

一个 GTID 组里如果只有 Ignored QUERY，下一个 GTID 到来时仍然失败（exit 1）：

```text
Ignored QUERY 不会关闭事务组；这不是缺少 COMMIT
```

这和未关闭的 `BEGIN` 不是同一种失败。同一个 GTID 里、`BEGIN` 之前的 Ignored QUERY 仍然附着在那个 `BEGIN` 上。

## 执行过程中出现解析或分析错误

代表性前缀包括：

```text
parse error: ...
analysis finalize error: ...
create temp DuckDB store: ...
```

通常意味着：

- 解析在完整结果生成前失败了
- 规范化或分析阶段拒绝了某种事件流状态
- finalize 阶段无法组装最终结果
- 命令无法创建或初始化临时 DuckDB 存储

运维侧响应建议：

- 保留完整错误文本
- 确认输入文件是有效的本地 ROW binlog
- 条件允许时，先用更小的一组已知正常文件重试
- 检查机器是否能创建临时文件，以及临时磁盘空间是否可用

如果在小规模、已知正常的 fixture 上仍然稳定复现，这更像是实现问题，而不是命令使用错误。

## 快速排查清单

命令失败时，建议按这个顺序检查：

1. 输入模式：是位置参数模式还是发现模式，且两者不能混用
2. 文件存在性：给定路径是否真的存在于本地
3. 发现契约：目录是否正确、前缀是否精确、后缀是否纯数字
4. 时间格式：是否为合法 RFC3339 或 `YYYY-MM-DD HH:MM:SS`
5. 输出预期：是否把 `stdout` 报告与 `stderr` 状态输出混淆了
6. 运行环境：命令是否能创建临时文件并完成 finalize

## 后续参考

- [CLI 参考](../reference/cli.zh-CN.md)
- [输入发现参考](../reference/input-discovery.zh-CN.md)
- [输出格式参考](../reference/output-format.zh-CN.md)
- [分析本地 Binlog](analyze-local-binlogs.zh-CN.md)
