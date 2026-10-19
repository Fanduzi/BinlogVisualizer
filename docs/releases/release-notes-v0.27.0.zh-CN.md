# BinlogViz v0.27.0 发布说明

发布日期：2026-10-19

## 概述

v0.27.0 为每一种 JSON 输出加上版本号，并让二进制能说明自己是哪个构建，脚本或 agent 可以据此确认读到的是什么、驱动的是哪个版本。设计记录在 ADR 0010（`docs/adr/0010-every-json-output-is-versioned.md`）。没有不兼容变更：`analyze` 和 `flashback` 的输出与 v0.26.1 逐字节一致。

## 新增

- **所有 JSON 输出都带 `schema_version`（#282、#302、ADR 0010）**：`compare`、`trend`、`snapshot list|show` 以及 `workflow validate|describe|status|clean|export` 加 `--format json` 时，开头都是 `"schema_version": 1`。`workflow run` 写出的 compare 和 trend 产物也带这个字段。`analyze` 仍然使用 `report_version` 3，`flashback --plan` 仍然使用 `plan_version` 2，workflow manifest 仍然使用 `manifest_version`。详见 `docs/reference/output-format.md` 中的 “JSON Versioning”。
- **workflow 拒绝未知版本的产物（#282、#302）**：`workflow status`、`workflow resume` 和 `workflow clean` 遇到当前版本不认识 `schema_version` 的 compare 或 trend 产物时，以退出码 9 结束（`unsupported`，`details.reason: unsupported_artifact_version`）。stdout 为空，不执行、不移动、不删除任何东西。没有这个字段的产物（v0.26.1 及更早版本写出的）仍然可以读取。
- **`binlogviz version` 显示构建信息（#282、#302）**：文本形式输出 git commit 和提交时间（工作区有改动时带 `-dirty`）。`binlogviz version --format json` 输出 `schema_version`、`version`、`commit`、`commit_time`、`modified` 和 `go_version`。commit 来自 Go 构建信息；构建信息里没有时，取 `-X binlogviz/internal/version.Commit`/`CommitTime` ldflags。
- `scripts/release_smoke.sh` 会检查 `version --format json` 带有版本号，并在二进制没有报告预期的 commit 时失败（`BINLOGVIZ_SMOKE_EXPECT_COMMIT=<sha>` 或 `BINLOGVIZ_SMOKE_REQUIRE_COMMIT=1`）。

## 验证

BinlogQA 对 main `5b71d7da` 的发布前 QA 通过，基准是 v0.26.1 tag 的构建和已发布的 v0.26.1 linux_amd64 归档。

- **输出不变**：analyze text/json/markdown、`flashback --plan`（包括 guard 和 `--conflict-limit 0`）、普通 flashback SQL 以及冲突拒绝路径，在 50 个文件 × en/zh-CN 上运行 654 次，与两个基准都是 0 差异。HTML 只有 “Generated” 时间戳不同。67 项退出码矩阵逐行一致。
- **schema_version 覆盖范围**与 ADR 0010 的表格一致，覆盖每个命令。
- **未知版本的产物**：compare 和 trend 的 `schema_version` 为 99、0、`"1"` 和 `null`，覆盖 status、resume、clean（text 和 json、en 和 zh-CN），共 192 次，全部退出码 9、stdout 为空、目录未变。v0.26.1 的 workflow 目录仍可读取。
- **version**：从 checkout 构建的二进制报告 commit 和提交时间；`-buildvcs=false` 的构建报告注入的值；工作区有改动时报告 `modified: true`。
- **workflow 端到端**：在真实 MySQL 8.0.36 binlog 上，validate、run、status、resume、rerun、clean、export 和 restore 全部通过。
- CI 和 `go test ./...` 通过。

## 兼容性

- 没有不兼容变更：没有删除或修改任何字段。拒绝未知 JSON 字段的调用方会看到新增的 `schema_version`。
- `report_version` 仍为 `3`，`plan_version` 仍为 2。退出码、快照、产物名称和支持的平台与 v0.26.1 相同。
- `version --format json` 是新增的；以前它是用法错误（退出码 3）。
- `flashback` 从不连接 MySQL。请先审阅并测试脚本，再带着脚本头，在主库上用一个新会话、`sql_log_bin=1` 执行。

## 已知问题

- **#303（P2）**：Linux 发布版二进制在 `binlogviz version` 中报告的 `commit` 和 `commit_time` 为空，因为发布 workflow 还没有给 Linux 构建传入 commit ldflags。macOS 归档以及从 git checkout 构建的二进制都能正确报告 commit。#303 修复之前，请用 `binlogviz_0.27.0_checksums.txt` 而不是 commit 来核对 Linux 二进制。
- HTML 报告的 “Generated” 时间戳会让跨秒的两次 HTML 运行结果不同（原有问题）。
- #285 遗留的 `install.sh` P3，以及 #255、#256、#257，沿用自 v0.26.1。

更早版本遗留的 flashback 限制见 `docs/concept/limitations.md`。
