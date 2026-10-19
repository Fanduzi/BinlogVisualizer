# BinlogViz v0.30.0 发布说明

发布日期：2026-10-19

## 概述

v0.30.0 完成了 ADR 0012 的第 1–4 步：表达式求值器、schema 目录、apply 守卫和 flashback 规划器各自独立成包。在已记录的语料上，analyze 和 flashback 的输出与 v0.29.0 逐字节一致。对用户可见的修复是生成列审阅的内存崩溃（#317），以及守卫在 digest 过短时的 panic（#319）。新增退出码 10（`resource`），这样强制的溢出或内存上限就不会再被报告成退出码 2（`no_data`）。

## 变更

- **内部包拆分（ADR 0012、#257、#315、#316、#320）。**
  - 第 1–2 步（#315）：`internal/mysqlexpr` 和 `internal/schemacatalog`。
  - 第 3 步（#316）：apply 守卫移到 `internal/guard`。
  - 第 4 步（#320）：flashback 规划器移到 `internal/flashback`。
  - 在 tip QA 语料上，analyze 的 JSON、文本、Markdown、HTML，以及 flashback 的 SQL / `--plan`，都与 v0.29.0 逐字节一致。`report_version` 仍为 3，`plan_version` 仍为 2。

## 新增

- **flashback 资源上限使用退出码 10 `resource`（#317、#320、ADR 0012）。**
  - 当 `BINLOGVIZ_SPILL_BYTES` 是正整数、且行溢出加上生成列审阅溢出超过该值时，`details.reason` 为 `spill_limit`。
  - 当一次 GC 之后 `HeapAlloc` 仍高于 `GOMEMLIMIT` 的 90% 时，`details.reason` 为 `memory_limit`。
  - 未设置、非正数或无法解析的 `BINLOGVIZ_SPILL_BYTES` 表示没有字节上限。磁盘写满（ENOSPC）仍是退出码 8（`output`）。
  - 退出码 10 绝不是 2：进程能感知的上限不是空结果。在 `--error-format json` 时，stderr 仍是常规的 JSON 错误。不会写出 plan，也不会写出脚本。
  - `flashback --help` 和 agent 使用指南写明了退出码 10、reason 取值，以及 `BINLOGVIZ_SPILL_BYTES`（#322）。

## 修复

- **生成列审阅改为流式，不再把每一行镜像留在内存里（#317、#320）。** 使用 `--schema-file` 时，审阅以前会把每个前镜像和后镜像都拷进内存。在一个会生成 1.13 GB 撤销脚本的 1.07 GB binlog 上，v0.29.0 大约在 813 MB RSS 时以退出码 2 OOM；v0.30.0 大约 128 MB RSS 就能跑完，并在 MySQL 8.0.36 上正确回滚。以前与 v0.29.0 一致的小脚本仍然逐字节一致。
- **`guard.Seal` / `SealWriter` 在 digest 短于 8 字节时报错，而不再 panic（#319、#320）。**

## 验证

BinlogQA 对 #320（`f3f3387`）的 PR QA，以及对 main `136a937` 的发布前 QA，都通过，没有 P0 或 P1。基准是 main `f66820d`，以及已发布的 v0.29.0 linux_amd64 归档 / tag 构建。

- **输出不变**：语料 1806/1806、生成列语料 1500/1500、67 例矩阵，都与 v0.29.0 逐字节一致。
- **#317**：整文件 1.13 GB 脚本 tip RSS 128 MB（v0.29.0 同范围是退出码 2 OOM）；43 MB 脚本哈希一致；在 MySQL 上回滚后校验和回到事故前。`BINLOGVIZ_SPILL_BYTES=100000000` → 退出码 10，`reason` 为 `spill_limit`，stdout 为空。
- **#319**：0–7 字节的 digest 报错不 panic；8–32 字节接受。
- `go test` / `go vet` / `CGO_ENABLED=0` 构建通过；`version --format json` 正确。CI 绿。

## 兼容性

- 对 analyze 或 flashback 的使用方没有不兼容变更。在 tip QA 语料上，默认的 analyze 和 flashback 输出与 v0.29.0 逐字节一致。
- `report_version` 仍为 3，`plan_version` 仍为 2。退出码 10 是新增的：只认识 0–9 的 agent 应当把未知的非零退出码当作失败，而不是当作 `no_data`。
- `flashback` 从不连接 MySQL。请先审阅并测试脚本，再带着脚本头，在主库上用一个新会话、`sql_log_bin=1` 执行。磁盘写满（退出码 8）留下的半截脚本不要拿去 apply；先看退出码和封印（#321）。

## 已知问题

- **#303（P2）**：Linux 发布版二进制在 `binlogviz version` 中报告的 `commit` 和 `commit_time` 为空，因为发布 workflow 还没有给 Linux 构建传入 commit ldflags。macOS 归档以及从 git checkout 构建的二进制都能报告 commit。#303 修复之前，请用 `binlogviz_0.30.0_checksums.txt` 而不是 commit 来核对 Linux 二进制。
- **#321（P2）**：磁盘写满（退出码 8）截断的 flashback 脚本仍可能部分执行。不要对写到一半的脚本做 apply；先检查退出码和封印。v0.29.0 写出的前缀行为相同。
- **#318（P2，设计）**：flashback 守卫的 token 用来区分用错脚本或连错库，不会检测对语句内容的篡改（改字面量、删掉 undo 行、调换语句顺序）。
- **#312（P3）** 以及 v0.29.0 遗留的其他 P3 继续沿用（归档内 README 相对链接失效、`--top 0` 表示 10、zh-CN 的 flashback 标志帮助仍有英文、`snapshot list` 对 `""` 标记的显示）。
- `.github/workflows/README.md` 里的维护说明仍列出旧的归档文件清单。修复只涉及文档，需要用有 workflow 权限的 token 提交，不影响 release 构建。
- #285 遗留的 `install.sh` P3，以及 #255（第 3、5、7 项）、#256、#257（ADR 0012 的后续步骤），继续沿用。

更早版本遗留的 flashback 限制见 `docs/concept/limitations.md`。
