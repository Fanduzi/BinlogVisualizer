# 输入发现参考

本文档定义了 BinlogViz 在 discovery 模式下如何解析 binlog 文件。

## Discovery 模式契约

通过同时使用这两个 flag 开启 discovery 模式：

```bash
binlogviz analyze --from-dir DIR --prefix PREFIX
```

要求如下：

- `--from-dir` 提供需要扫描的目录。
- `--prefix` 提供匹配文件必须具备的文件名前缀。
- 这两个 flag 必须一起提供。
- discovery 模式不能与位置参数 binlog 文件参数同时使用。

如果你已经明确知道要分析哪些文件，应该改用位置参数文件模式。

## 匹配规则

BinlogViz 只扫描 `DIR` 目录下的直接子项，并对每个条目应用以下规则：

1. 目录会被排除。
2. 符号链接会被排除。
3. 文件名必须以 `PREFIX` 开头。
4. `PREFIX` 之后的后缀不能为空。
5. `PREFIX` 之后的后缀必须是数字；`PREFIX` 和数字之间允许有一个可选的 `.`。

因此 `--prefix mysql-bin` 和 `--prefix mysql-bin.` 都会匹配 `mysql-bin.000008`（`log_bin_basename` 的常见形式）。

discovery 会接受这样的文件名：

- `mysql-bin.000123`
- `mysql-bin.000124`

也会排除这样的文件名：

- `mysql-bin.`，因为后缀为空
- `mysql-bin.index`，因为后缀不是纯数字
- `mysql-bin.000123.tmp`，因为后缀不是纯数字
- `relay-bin.000123`，因为前缀不匹配

## 数字后缀匹配

前缀之后的部分会被解析为一个十进制整数。

例如，当 `--prefix mysql-bin.` 时：

- `mysql-bin.9` 会以 `9` 这个后缀匹配
- `mysql-bin.010` 会以 `010` 这个后缀匹配
- `mysql-bin.000123` 会以 `000123` 这个后缀匹配

正是“仅数字后缀”这一规则，使 discovery 模式适合处理有序的 MySQL 风格 binlog 序列。

## 排序规则

匹配完成后，BinlogViz 会在分析前先对文件排序。

主规则：

- 文件按解析后的数字后缀升序排列。

这样可以避免类似把 `mysql-bin.10` 排到 `mysql-bin.2` 前面的字符串字典序错误。

平手规则：

- 如果两个匹配文件解析出的数字值相同，BinlogViz 会回退到按文件名字典序排序。

最终解析出的路径列表会按照这个顺序传入分析管线。

## 已解析文件报告

当 discovery 成功时，BinlogViz 会在开始解析前把有序文件列表打印到 `stderr`：

```text
Resolved binlog files:
- /var/lib/mysql/mysql-bin.000123
- /var/lib/mysql/mysql-bin.000124
```

这能帮助操作者确认到底会分析哪些文件，同时保持 `stdout` 仍然可用于文本或 JSON 报告输出。

## 非法用法

BinlogViz 会拒绝以下与 discovery 相关的输入组合：

### 混用输入模式

不能把位置参数文件与 discovery flags 混在一起使用。

```bash
binlogviz analyze mysql-bin.000123 --from-dir /var/lib/mysql --prefix mysql-bin.
```

它会以如下错误失败：

```text
cannot combine positional binlog files with --from-dir/--prefix
```

### Discovery 参数不完整

不能只提供两个 discovery flag 中的一个。

```bash
binlogviz analyze --from-dir /var/lib/mysql
binlogviz analyze --prefix mysql-bin.
```

它们都会以如下错误失败：

```text
--from-dir and --prefix must be provided together
```

### 无匹配项

如果目录可读，但没有任何条目满足匹配规则，命令会失败。

代表性错误如下：

```text
no matching binlog files found under /var/lib/mysql with prefix "mysql-bin."
```

## 操作建议

在以下情况使用 discovery 模式：

- binlog 文件集中存放在同一个目录中
- 文件序列遵循稳定的数字命名规则
- 你希望由 BinlogViz 自动确定有序输入列表

在以下情况使用位置参数文件模式：

- 你需要手工挑选一个子集
- 文件并不遵循统一的“前缀 + 数字后缀”契约
- 你希望使用 shell 展开或显式逐文件控制
