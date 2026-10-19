# BinlogViz v0.28.0 发布说明

发布日期：2026-10-19

## 概述

v0.28.0 让 agent 可以把 `analyze` 的 JSON 缩小到能放进上下文窗口，并给每条针对单个事务的告警带上 GTID，告警可以直接交给 `flashback --include-gtids`。裁剪过的文档永远不会被当成完整文档比较：快照和 workflow 产物始终保留完整的 diagnostics，所有读取报告的命令都会拒绝裁剪过的文档。设计记录在 ADR 0011（`docs/adr/0011-diagnostics-budget-and-alert-gtid.md`）。没有不兼容变更。

## 新增

- **`analyze --diagnostics full|summary|off`（#282、#307、ADR 0011）**：设置 `analyze --format json` 输出多少 `diagnostics`。默认 `full`，与 v0.27.0 的对象相同。
  - `summary` 保留 `file_coverage`、`counted_event_bytes`、`ddl_events`、`open_dml_groups`、`duration_buckets`、`input_format_guess` 和各项计数，清空四个事务排行、`file_segments`、`hot_intervals` 和 `findings`。这些内容已经在顶层的 `transactions`、`timeseries`、`minutes` 和 `alerts` 里。
  - `off` 省略 `diagnostics`。
  - `summary` 或 `off` 的文档在顶层带 `"diagnostics_mode": "summary"` 或 `"off"`，完整文档没有这个键。
  - 文本、Markdown 和 HTML 忽略此标志。非法取值是用法错误（退出码 3）。
  - stdout 大小（字节）：

    | binlog | full | summary | off |
    |---|---:|---:|---:|
    | 1.07 GB，DDL 较多 | 63042 | 42759 | 22627 |
    | `mysql-8.0.46-busiest-minute.binlog` | 43556 | 21307 | 20603 |
    | `mysql-8.0.46-dml-full.binlog` | 32639 | 16011 | 12368 |

- **`alerts[].gtid`（#282、#307）**：针对单个事务的告警，在该事务有 GTID 时带上 `gtid`，与 `transactions[].gtid` 是同一个字符串（MySQL `uuid:seq` 或 MariaDB `domain-server-seq`）。匿名事务，以及不针对单个事务的告警（spike、输入格式、无主键）没有这个键。`message` 和 `txn_key` 不变。使用 `--detail-store duckdb` 时同样保留告警的 GTID，输出与内存存储一致。
- **快照和 workflow 产物始终保留完整 diagnostics（#307、ADR 0011 §3）**：`--diagnostics` 只限制 stdout 的大小。无论 `--diagnostics` 是什么，`analyze --snapshot-name` 都保存完整文档。workflow 的 analyze 产物和 workflow 快照始终是完整的，workflow 也没有 `--diagnostics`。
- **拒绝裁剪过的报告（#307、ADR 0011 §3）**：`diagnostics_mode` 存在且不是 `full` 的报告，以退出码 9 拒绝（`unsupported`，`details.reason: reduced_diagnostics`，`details.diagnostics_mode`），stdout 为空，不写任何文件。提示信息会建议去掉 `--diagnostics` 重新运行 `analyze`，或者使用 `--snapshot-name` 保存的快照。拒绝范围：
  - `compare` 的报告文件（两侧都适用），以及 `--current-snapshot`/`--baseline-snapshot`；
  - `trend` 的具名快照、`--baseline-snapshot` 和 `--from-snapshots`；
  - `snapshot save <file>` 和 `snapshot show`；
  - `workflow resume` 和 `workflow resume --rerun`，只要它们复用 analyze 产物。

  在此之前，裁剪过的文档会被当成省略的列表为空来比较。对同一个 binlog，这会报告新增了三条 DDL、热点时段变化 48 行，并且以退出码 0 结束。没有 `diagnostics_mode` 的报告（例如 v0.27.0 及更早版本写出的）与以前完全一样地比较。

## 验证

BinlogQA 对 #307 的 PR QA 和对 main `afeadd31` 的发布前 QA 都通过，没有 P0、P1 或 P2，基准是 v0.27.0 tag 的构建和已发布的 v0.27.0 linux_amd64 归档。

- **输出不变**：在 50 个 binlog × en/zh-CN 上，analyze 的文本、Markdown 和 HTML（屏蔽时间戳）在每种 `--diagnostics` 模式下都逐字节一致。默认 JSON 共 100 次运行，84 次逐字节一致，其余 16 次只多了告警的 `gtid` 行。在 1.07 GB 的 binlog 上，完整 JSON 与 v0.27.0 逐字节一致。`flashback --plan`（包括 guard 和 `--conflict-limit 0`）和 flashback SQL 逐字节一致。67 项退出码矩阵逐行一致。
- **告警 gtid**：24/24 条带 gtid 的告警与对应事务的 gtid 相同；事务有 gtid 时，告警都带上了。92 份 `--detail-store duckdb` 文档与内存存储逐字节一致。
- **快照**：`summary`/`off` 快照与同一 binlog 的完整快照比较 16/16 次，DDL、热点时段和最大事务的差值都是 0。所有 workflow 产物和快照中都没有 `diagnostics_mode`。
- **拒绝**：共 528 次运行（12 种文档 × 11 个命令 × en/zh-CN × text/json 错误格式）。360 次拒绝全部以退出码 9 结束，stdout 为空，文件未改动；JSON 错误为 `unsupported`/`reduced_diagnostics`。把裁剪过的产物换进 workflow 后，`--rerun compare` 和 `--rerun trend` 8/8 次以退出码 9 结束。
- **旧报告**：v0.26.1 和 v0.27.0 的报告和快照在 compare、trend、`trend --from-snapshots` 和 `snapshot list|show` 上共 76/76 次运行，与 v0.27.0 一致。
- CI、`go vet ./...` 和 `go test ./...` 通过。

## 兼容性

- 没有不兼容变更。`report_version` 仍为 3，`plan_version` 仍为 2。`diagnostics_mode` 和 `alerts[].gtid` 都是新增字段；拒绝未知字段的调用方会在事务有 GTID 的告警上看到 `gtid`。
- 除非告警对应的事务有 GTID，默认的 `analyze` JSON 与 v0.27.0 逐字节一致。文本、Markdown、HTML、flashback 输出、退出码、快照名称和支持的平台都不变。
- `flashback` 从不连接 MySQL。请先审阅并测试脚本，再带着脚本头，在主库上用一个新会话、`sql_log_bin=1` 执行。

## 已知问题

- **#303（P2）**：Linux 发布版二进制在 `binlogviz version` 中报告的 `commit` 和 `commit_time` 为空，因为发布 workflow 还没有给 Linux 构建传入 commit ldflags。macOS 归档以及从 git checkout 构建的二进制都能报告 commit。#303 修复之前，请用 `binlogviz_0.28.0_checksums.txt` 而不是 commit 来核对 Linux 二进制。
- **#308（P3）**：`--diagnostics` 的后续问题。
  - `"diagnostics_mode": null` 被当作不存在，非字符串取值以退出码 5 结束，两者都没有文档说明。
  - `snapshot list` 不会标出裁剪过的文档。
  - `summary` 保留全部 `ddl_events`，DDL 较多的窗口只能缩小约 32%。
  - `--diagnostics ""` 被当作 `full` 接受。
  - zh-CN 的 `--help` 中各标志说明仍是英文。
- HTML 报告的 “Generated” 时间戳会让跨秒的两次 HTML 运行结果不同（原有问题）。
- #285 遗留的 `install.sh` P3，以及 #255、#256、#257，沿用自 v0.27.0。

更早版本遗留的 flashback 限制见 `docs/concept/limitations.md`。
