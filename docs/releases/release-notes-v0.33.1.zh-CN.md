# BinlogViz v0.33.1 发布说明

发布日期：2026-10-20

## 概述

v0.33.1 是补丁版本，收尾 #312 剩下的四项：`snapshot list` 会显示 `diagnostics_mode` 为空的文件，而不是用 `-` 掩盖；`analyze --help` 写明 summary 的 DDL 上限；README 中指向发布包之外的链接在解压后的 tarball 里也能打开；zh-CN 的 `flashback --help` 翻译补全。没有不兼容变更。analyze 输出、flashback 脚本、退出码、`schema_version` 1、`plan_version` 2 和 `report_version` 3 与 v0.33.0 相同。

## 修复

- **`snapshot list` 显示空的 `diagnostics_mode`（#312）。** `diagnostics_mode` 为 `""` 的快照文件显示为 `(empty)`，并保留 label、created_at 和时间窗口，JSON 中包含 `"diagnostics_mode": ""`。字段缺失或为 null 时仍显示 `-`，JSON 中不输出该键。读取该文件的命令仍以退出码 9（`reduced_diagnostics`）拒绝，与 ADR 0011 §3 一致。
- **`analyze --help` 写明 DDL 上限（#312）。** `--diagnostics summary` 把 `ddl_events` 限制为 `--top` 条，并报告 `ddl_events_omitted`。`--top 0` 表示默认的 10，而不是不限。同一 `--top` 下的 analyze JSON 不变。
- **发布包里的 README 链接可用（#312）。** `README.md` 和 `README_ZH.md` 中指向 tarball 未包含文件的链接改为 `main` 上的 GitHub 绝对地址；指向包内文件的链接仍为相对路径。如果 README 的相对链接不在发布包文件列表中，测试会失败。
- **zh-CN `flashback --help` 补全（#312）。** `--conflict`、`--plan`、`--error-format` 和 `-q` 已翻译。英文帮助不变。

## 验证

BinlogQA 对 #350（`4ea3ed05`）的 PR QA 通过，0 P0、0 P1、0 P2、0 P3。PR 以快进方式合并，main 即 `4ea3ed05`，main 上的 tip QA 也已通过。

- **`snapshot list`**：`""` 标记显示 `(empty)`，JSON 为 `"diagnostics_mode": ""`；缺失、null 和 `full` 显示 `-` 且不输出该键。在 list、show、compare、trend、save 中，reduced 字符串退出码 9，非字符串退出码 5。
- **帮助**：`analyze --help` 只改了文档说明的两行；DDL 上限行为与文档一致（`--top 5` 列出 5 条 DDL，`--top 0` 列出 10 条）。除 zh-CN `flashback` 的几行外，en 和 zh-CN 的其他帮助逐字节一致。
- **README**：39 个指向 `main` 的绝对链接，0 个失效；包内链接为相对路径且都能解析。
- **未变化**：tip QA 语料上的 analyze text、JSON、Markdown、HTML 输出与 v0.33.0 逐字节一致（20/20，HTML 的 `Generated` 时间除外）。flashback 脚本逐字节一致，digest 相同。`verify | mysql` 在 MySQL 8.0.36 和 5.7.44 上均输出 `binlogviz: undo committed, 10 statements`；8.0 上的 rt325（60000 条语句）恢复后 `CHECKSUM TABLE` 一致。
- `go test ./...` 和 `go vet` 通过，CI 全绿。

## 兼容性

- 没有不兼容变更。`report_version` 仍为 3，`plan_version` 仍为 2，`flashback verify --format json` 的 `schema_version` 仍为 1。退出码、快照、产物名称和支持的平台与 v0.33.0 相同。
- v0.33.0 生成的 flashback 脚本可以直接 verify 和执行。v0.32.0 及更早版本的脚本仍需重新生成。

## 已知问题

- **#303（P2）**：Linux 发布包里的二进制在 `binlogviz version` 中 `commit` 和 `commit_time` 为空。修复在 `.github/workflows/release.yml`，需要有 workflow 权限的维护者处理。请用 `binlogviz_0.33.1_checksums.txt` 校验 Linux 二进制。
- `flashback verify` 不接受 `-` 作为 stdin（退出码 4）。
- `internal/guard` 的 `TestVerifyDoesNotBufferTheScript` 在内存压力下的完整 `go test ./...` 中约 11 次失败过 1 次；单独运行和 CI 中都通过，不影响二进制。
- **#270**、**#255**、**#256**、**#257**（P3）延续到下个版本。

flashback 的保证及其限制见 `docs/concept/limitations.md` 和 `docs/adr/0014-flashback-no-partial-commit.md`。
