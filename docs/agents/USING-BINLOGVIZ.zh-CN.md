# 使用 BinlogViz

这是给操作者和驱动 `binlogviz` 的代理看的用法说明。`docs/agents/` 里的其他文件是过去交付的内部笔记，不是运行这个工具的说明。

发布归档里有本文件、`docs/agents/USING-BINLOGVIZ.md`、`README.md`、`README_ZH.md`、`binlogviz`、`incident.yaml` 和 `testdata/`。下面的命令用的是归档里的路径。在 git 检出里，`testdata/minimal.binlog` 对应 `cmd/binlogviz/testdata/minimal.binlog`。

报告里的时间是 UTC。`--start` 和 `--end` 接受 RFC3339。带偏移量的值就是那个时刻：`2026-10-19T14:05:00+08:00` 等于 `2026-10-19T06:05:00Z`。`YYYY-MM-DD HH:MM:SS` 按运行 `binlogviz` 的机器的本地时区解释。跨机器时请带偏移量。

`flashback` 不连接 MySQL。先审阅脚本，再测试，然后由你自己执行。

## 配方 1：写入尖峰，然后 flashback

先找到繁忙的那一分钟，用 GTID 点名事务，预览撤销，再生成带 guard 的脚本。

```bash
binlogviz analyze mysql-bin.000001 \
  --start 2026-10-19T06:00:00Z --end 2026-10-19T06:10:00Z \
  --detect-spikes

binlogviz analyze mysql-bin.000001 \
  --start 2026-10-19T06:05:00Z --end 2026-10-19T06:06:00Z \
  --format json --diagnostics summary

binlogviz flashback mysql-bin.000001 --include-gtids '<gtid>' --plan
binlogviz flashback mysql-bin.000001 --include-gtids '<gtid>' --conflict guard
```

不加 `--detect-spikes` 就不会做尖峰检测。此时 `alerts` 仍会列出超过 `--large-trx-rows`（默认 1000）的事务。告警针对某一个事务时，读 `alerts[].gtid`。它和 `transactions[].gtid` 是同一个字符串。把这个字符串传给 `--include-gtids`。尖峰告警、输入格式警告和没有主键的警告没有 `gtid`。

`--plan` 只打印 JSON，不打印撤销 SQL。`plan_version` 是 2。退出码 7 表示选中的行后来又被改过（默认 `--conflict refuse`）。`--conflict guard` 按日志里的后镜像匹配，而不是直接拒绝。退出码 6 表示 flashback 不会写出脚本（范围内有 DDL、行镜像不是 FULL，或 `--sql-context off`）。

`testdata/minimal.binlog` 是一份很小的 ROW 样本。它可能没有尖峰，也没有告警。在真实文件上命令是一样的。

## 配方 2：更小的 JSON

```bash
binlogviz analyze testdata/minimal.binlog --format json --diagnostics summary > window.json
binlogviz analyze testdata/minimal.binlog --format json --diagnostics off > tiny.json
binlogviz analyze testdata/minimal.binlog --format json --snapshot-name window --snapshot-dir /tmp/snaps
```

| `--diagnostics` | JSON |
|---|---|
| `full`（默认） | 现在的 `diagnostics` 对象。没有 `diagnostics_mode` 键。 |
| `summary` | 去掉已经在顶层出现的事务排行和其他列表。`ddl_events` 是前 `--top` 条（默认 10）。`ddl_events_omitted` 是其余条数，包括 0。顶层的 `alerts`、`transactions`、`tables` 和 `summary` 都保留。 |
| `off` | 省略 `diagnostics`。 |

`summary` 和 `off` 会把 `"diagnostics_mode"` 设成这个模式。文本、Markdown 和 HTML 忽略此标志。`--snapshot-name` 保存的快照始终是完整 diagnostics；只有 stdout 被裁剪。

不要把 `window.json` 交给 `compare`、`trend` 或 `snapshot save`。这些命令会以退出码 9 结束（`unsupported`，`details.reason` 为 `reduced_diagnostics`）。请使用 `--snapshot-name` 保存的快照。`snapshot list` 以退出码 0 结束，并在 `DIAGNOSTICS` 列、以及 `--format json` 的 `diagnostics_mode` 里写出模式。这样，拷进去的 summary 文件会在 show 或 compare 失败之前就被标出来。

`diagnostics_mode` 的 JSON `null` 和缺少该键相同。非字符串（例如 `1`）是损坏文档（退出码 5）。`--diagnostics ""` 是用法错误（退出码 3）。省略该标志就是 `full`。

## 配方 3：比较两个窗口

```bash
binlogviz analyze mysql-bin.000001 --format json --snapshot-name before \
  --start 2026-10-19T05:00:00Z --end 2026-10-19T06:00:00Z --snapshot-dir /tmp/snaps
binlogviz analyze mysql-bin.000001 --format json --snapshot-name during \
  --start 2026-10-19T06:00:00Z --end 2026-10-19T07:00:00Z --snapshot-dir /tmp/snaps
binlogviz snapshot list --snapshot-dir /tmp/snaps
binlogviz compare --current-snapshot during --baseline-snapshot before --snapshot-dir /tmp/snaps
binlogviz trend before during --snapshot-dir /tmp/snaps
```

`compare --format json` 和 `trend --format json` 都以 `"schema_version": 1` 开头。两边的 analyze 使用同一个 `--workload-id` 时，才会给出因果类发现。不同的 workload id 是 `not_comparable`。数值差值仍然会打印。

`binlogviz workflow run incident.yaml` 用归档里的计划跑 `testdata/sample-binlog`。

## 退出码

kind 字符串不随 `--lang` 变化。`--error-format json` 在 stderr 打印一个 JSON 对象。

| 退出码 | kind | 常见原因 |
|---|---|---|
| 0 | | 统计到了事件，或命令完成 |
| 1 | `error` | 没有更具体分类的解析或其他失败 |
| 2 | `no_data` | binlog 可读，但窗口或过滤条件里没有事件 |
| 3 | `usage` | 标志或参数错误，包括 `--diagnostics ""` 或 `nope` |
| 4 | `input` | 文件或快照不存在 |
| 5 | `corrupt` | 不是 binlog，或 JSON 不符合报告形状，包括非字符串的 `diagnostics_mode` |
| 6 | `refused` | flashback 不会输出 SQL |
| 7 | `conflict` | 选中的行后来又被修改 |
| 8 | `output` | 写不出报告或脚本 |
| 9 | `unsupported` | 没有 ROW 镜像的 STATEMENT/MIXED、未知版本，或裁剪过的 diagnostics 文档 |
| 10 | `resource` | flashback 达到溢出上限（`BINLOGVIZ_SPILL_BYTES`）或进程内存上限（`GOMEMLIMIT`）。不是空结果 |

## JSON 版本

先读版本键。新增键不会抬版本号。删掉、改名或改变一个键的含义才会，CHANGELOG 会把这次抬版本标成不兼容。

| 输出 | 键 | 当前 |
|---|---|---|
| `analyze --format json`、保存的快照 | `report_version` | 3 |
| `flashback --plan` | `plan_version` | 2 |
| `compare`、`trend`、`snapshot list`、`snapshot show` 的 `--format json` | `schema_version` | 1 |
| `workflow validate`、`describe`、`status`、`clean`、`export` 的 `--format json` | `schema_version` | 1 |
| `version --format json` | `schema_version` | 1 |

`binlogviz version --format json` 还会打印 `version`、`commit`、`commit_time`、`modified` 和 `go_version`。

`--diagnostics summary` 和 `off` 的 `report_version` 仍是 3。summary 的 DDL 列表有上限；看 `ddl_events_omitted`。默认的 `full` JSON 没有这个键。
