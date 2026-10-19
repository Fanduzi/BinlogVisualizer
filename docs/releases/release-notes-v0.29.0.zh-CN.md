# BinlogViz v0.29.0 发布说明

发布日期：2026-10-19

## 概述

v0.29.0 让 `--diagnostics summary` 在 DDL 较多的窗口里真正变小。`snapshot list` 会在读取之前就标出裁剪过的报告，`diagnostics_mode` 的各种异常取值有了明确的处理方式，release 归档里也附带了可直接照做的示例和使用指南，供运维和 agent 使用。设计记录在 ADR 0011（`docs/adr/0011-diagnostics-budget-and-alert-gtid.md`）。默认和 `--diagnostics full` 的输出与 v0.28.0 逐字节一致。只有一处变更会影响 `--diagnostics summary` 的使用方，见“变更”。

## 变更

- **仅对 `--diagnostics summary` 不兼容：DDL 时间线设上限（#308、#311、ADR 0011）。**
  - summary 文档里的 `diagnostics.ddl_events` 现在是按时间线顺序的前 `--top` 条（默认 10）。
  - 新增的 `diagnostics.ddl_events_omitted` 统计其余条数，在 summary 中始终存在，为 0 时也输出。
  - v0.28.0 的 summary 会列出全部 DDL 事件。把 summary 的 `ddl_events` 当作完整时间线的使用方，需要读取 `ddl_events_omitted`，或者改用 `full`。
  - `--top` 同时控制其他排行列表，`--top 0` 表示默认的 10，而不是不限（#312）。
  - 默认和 `--diagnostics full` 的 JSON 与 v0.28.0 逐字节一致：仍然列出全部 DDL 事件，也从不输出 `ddl_events_omitted`。`report_version` 仍是 3：默认文档没有变化，新字段只出现在需要主动开启的 summary 文档里（ADR 0010）。
  - 在一个 DDL 较多的 1.07 GB binlog 上的 stdout 大小：

    | | v0.28.0 | v0.29.0 |
    |---|---:|---:|
    | full | 63042 B | 63042 B（一致） |
    | summary | 42759 B | 32632 B（21 条 DDL 保留 10 条，省略 11 条） |
    | summary `--top 1` | — | 10511 B |
    | summary `--top 20` | — | 57233 B |

## 新增

- **`snapshot list` 标出裁剪过的文件（#308、#311）。** 把 `--diagnostics summary` 或 `off` 的文档复制到快照目录后，列表里会显示它的模式，在 `snapshot show`、compare 或 trend 以退出码 9 拒绝之前就能看到。
  - 文本输出的每次列表都新增一列 `DIAGNOSTICS`，普通快照显示 `-`。
  - `--format json` 只在这一项上增加 `diagnostics_mode`，`schema_version` 仍为 1。
  - list 仍然以退出码 0 结束。用 `--snapshot-name` 保存的快照始终完整，不会被标记。
- **六个命令的 `--help` 带示例（#255、#311）。** 包括 `analyze`、`flashback`、`snapshot`、`compare`、`trend` 和 `workflow`。analyze 和 flashback 的示例按排障流程走：在 UTC 窗口里找出写入峰值，从 `alerts[].gtid` 读出 GTID，然后 `flashback --plan`，再 `--conflict guard`。
- **运维和 agent 使用指南（#255、#311）。** `docs/agents/USING-BINLOGVIZ.md` 和 `docs/agents/USING-BINLOGVIZ.zh-CN.md` 包含三个流程、退出码、JSON 版本号和 diagnostics 模式。release 归档现在除了二进制、示例 binlog 和 `incident.yaml`，还包含这两份指南以及 `README.md`、`README_ZH.md`。

## 修复

- **`diagnostics_mode` 的异常取值有了明确的处理方式，并写入文档（#308、#311）。**
  - JSON `null` 等同于没有这个键，报告正常读取。
  - 字符串 `"full"` 被接受。
  - 其他任何字符串（包括 `""`）都视为裁剪过的报告：退出码 9（`unsupported`，`details.reason: reduced_diagnostics`）。
  - 非字符串的值（例如 `1`）视为损坏的报告：退出码 5。
- **`--diagnostics ""` 是用法错误（#308、#311）。** 显式设置为空值时以退出码 3 结束，提示 `invalid --diagnostics "" (allowed: full, summary, off)`。不传这个标志时仍是 `full`。`FULL`、`Summary`、`" full"` 等其他未知取值和以前一样以退出码 3 结束。
- **`flashback --help` 按 flashback 的含义描述 `--sql-context`（#255、#311）。** `summary`（默认）和 `full` 都不改变撤销 SQL；`off` 会被拒绝，因为脚本内容就是行值。帮助不再沿用 analyze 关于查询文本、DDL 语句和 `--show-patterns` 的说明。

## 验证

BinlogQA 对 #311 的 PR QA 和对 main `4cb6cc24` 的发布前 QA 都通过，没有 P0、P1，也没有新的 P2。基准是 v0.28.0 tag 的构建和已发布的 v0.28.0 linux_amd64 归档。

- **输出不变**：在 50 个文件 × en/zh-CN 的语料上，3308 次比较全部一致，0 个异常。覆盖 analyze 的 json/text/markdown/html（屏蔽时间戳）、`--large-trx-rows 1`、`--detail-store duckdb`，以及 flashback 的 `--plan`、guard、`--conflict-limit 0`、SQL 和冲突路径。400 次运行中默认输出都等于 `--diagnostics full`，所有 full 文档都没有 `ddl_events_omitted`。
- **summary 上限**：92/92 份 summary 文档满足 保留数 = min(10, 总数)、保留数 + 省略数 = 总数，并且保留的列表是 full 时间线的前缀。25 个 `--top` 探测全部通过。
- **拒绝**：show/compare/trend/save 共 528 次运行，其中 400 次非零（360 次退出码 9，40 次退出码 5），stdout 都为空，文件都未改动。把裁剪过的产物换进 workflow 后，run/status/resume/`--rerun` 有 32 次退出码 9、8 次退出码 5，目录未变。v0.26.1、v0.27.0 和 v0.28.0 的报告 72/72 一致。
- **标志和帮助**：28 个非法的 `--diagnostics` 取值都以退出码 3 结束，stdout 为空。31 次 `--help` 示例运行的行为都与说明一致。67 项退出码矩阵逐行一致，只是 `snapshot` 和 `workflow` 的帮助变长了。
- **归档**：`scripts/pack_release_archive.sh` 打包 8 个文件，二进制和所有文档都与仓库一致。
- CI、`go vet ./...` 和 `go test ./...` 通过。

## 兼容性

- 只对 `--diagnostics summary` 的使用方不兼容（见“变更”）。默认和 `full` 的 analyze JSON、文本、Markdown、HTML 以及 flashback 输出都与 v0.28.0 逐字节一致。
- `report_version` 仍为 3，`plan_version` 仍为 2，`snapshot list` 的 `schema_version` 仍为 1。新增的字段是 `diagnostics.ddl_events_omitted`（仅 summary）和裁剪过的 `snapshot list` 条目上的 `diagnostics_mode`。
- 解析 `snapshot list` 文本列的脚本会看到新增的最后一列 `DIAGNOSTICS`。
- `--diagnostics ""` 在 v0.28.0 中被当作 `full` 接受，现在以退出码 3 结束。
- `flashback` 从不连接 MySQL。请先审阅并测试脚本，再带着脚本头，在主库上用一个新会话、`sql_log_bin=1` 执行。

## 已知问题

- **#303（P2）**：Linux 发布版二进制在 `binlogviz version` 中报告的 `commit` 和 `commit_time` 为空，因为发布 workflow 还没有给 Linux 构建传入 commit ldflags。macOS 归档以及从 git checkout 构建的二进制都能报告 commit。#303 修复之前，请用 `binlogviz_0.29.0_checksums.txt` 而不是 commit 来核对 Linux 二进制。
- **#312（P3）**：四个后续问题。
  - 归档里的 `README.md` 和 `README_ZH.md` 中，大多数相对链接指向不在归档里的文件，请使用 GitHub 上的版本。
  - summary 的 DDL 上限与 `--top` 共用，`--top 0` 表示 10，`--diagnostics` 的帮助里也没有提到这个上限。
  - `snapshot list` 把 `""` 标记显示为 `-`，与 ADR 0011 的描述不一致。
  - zh-CN 的 `flashback --help` 仍有部分标志说明是英文。
- `.github/workflows/README.md` 里的维护说明仍列出旧的归档文件清单。修复只涉及文档，需要用有 workflow 权限的 token 提交，不影响 release 构建。
- HTML 报告的 “Generated” 时间戳会让跨秒的两次 HTML 运行结果不同（原有问题）。
- #285 遗留的 `install.sh` P3，以及 #255（第 3、5、7 项）、#256、#257，继续沿用。

更早版本遗留的 flashback 限制见 `docs/concept/limitations.md`。
