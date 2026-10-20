# BinlogViz v0.34.0 发布说明

发布日期：2026-10-20

## 概述

v0.34.0 是次版本，完成 ADR 0012 第 5 步：text、Markdown、JSON 和 HTML 现在读取 `internal/report/view` 构建的同一份事务视图，`report.Write` 是唯一的格式分支（#353、#356）。其余都是测试加固：flashback 和 verify 的内存上界测试不再依赖调度，在 `GOMAXPROCS=1` 下也能通过（#358、#361，关闭 #351）；新增 `docs/production-readiness.md`（#359）。没有不兼容变更，也没有用户可见的输出变化。analyze 输出、flashback 脚本和 plan、`--help`、退出码、`schema_version` 1、`plan_version` 2 和 `report_version` 3 与 v0.33.1 相同。

## 变更

- **所有 analyze 格式共用一份事务视图（ADR 0012 第 5 步，#353，属于 #257）。** `view.Build` 是唯一挑选事务的地方。text、Markdown、JSON 和 HTML 读取这份结果，不再各自截取事务列表。`report.Write` 先规范化选项、构建视图，再按格式分派。挑选规则和输出字节都不变。text 和 Markdown 选出的重点事务仍然不同，与 v0.33.1 一致（#352）。`scripts/compare-analyze-formats.sh` 会在两个 commit 上对仓库里的每个 binlog 输出 text、JSON、Markdown、HTML、flashback SQL 和 flashback `--plan`，再逐一比对。
- **格式分支漏了分支时报错，而不是落到默认分支（#356，修复 #354）。** `knownFormats` 是 `ParseFormat` 接受的唯一列表。`report.Write` 的 switch 没有 default 分支，列表里有但没有对应分支的格式会返回 `unhandled format` 且不写任何内容，而不是悄悄输出 text。`TestWriteRendersEveryKnownFormat` 会渲染列表中的每个值。未知的 `--format` 仍在读取输入之前以退出码 3（usage）失败，提示信息不变。

## 修复

- **内存上界测试在单 CPU 下稳定（#358、#361，关闭 #351）。** 只改了测试，发布的二进制不变。
  - `TestGeneratedReviewRSS`（`internal/flashback`）原来断言子进程的 VmHWM 低于固定的 64MB，曾在一个只改文档的 commit 上让 CI 失败（#351）。现在它在生成列复核自己的 goroutine 里、复核结束时读取 `/gc/heap/live:bytes`，Prepare 和写出之后再各读一次，结果不再依赖调度。上限是每行 100 字节（流式约 75），所以保留一半、四分之一或八分之一的行镜像都会失败。复核没有上报时测试失败；`BINLOGVIZ_GENMEM=measure` 会 SKIP，而不是不做断言就通过。它用到的钩子 `internal/flashback/review.go` 中的 `reviewProbe` 只有测试会设置，否则为 nil。
  - `TestVerifyDoesNotBufferTheScript`（`internal/guard`）比较 verify 一个脚本和一个大四倍的脚本时的 live heap 峰值：大脚本多出的峰值超过多出字节数的十分之一就失败，小脚本的峰值上限为 512KiB。部分缓冲、`io.ReadAll`，以及 1MiB、16MiB、64MiB 的读缓冲在 1、2、8 个 CPU 下都会失败。流式读取的峰值约 90KB。
  - `TestRunAnalysisParsesMultipleFilesConcurrentlyAndConsumesInOrder`（`cmd/binlogviz`）改用阻塞而不是 sleep 来证明解析是并行的，并按文件数设置 `GOMAXPROCS`，因为解析池的大小取自它。修改之前它在 `GOMAXPROCS=1` 下会失败（"peak concurrency was 1"）。

## 新增

- **`docs/production-readiness.md`（#359）。** 24 项生产级验收清单（中文），覆盖分析正确性、flashback 安全、MySQL 版本矩阵、agent 使用契约、资源上界、发布完整性和文档。只有 CI 里有自动检查的项才算 MET。只改文档。

## 验证

BinlogQA 对 #353、#356、#358（`ab53d654`，以 `6559ba65` 合入 main）和 #361（`14bd41b3`）的 PR QA 均通过。main `14bd41b3` 上的 tip QA 通过，0 P0、0 P1、0 P2、0 P3。

- **输出不变**：analyze 输出在 2442/2442 次渲染上与 v0.33.1 一致。mf、mi、n316、pc、rt325 语料的 flashback SQL、stderr 和 `--plan` JSON 逐字节一致。MySQL 8.0 上 `verify | mysql` 输出 `binlogviz: undo committed, 4 statements`，撤销后的 `CHECKSUM TABLE` 等于 DML 之前的值。5.7 上 flashback 以退出码 6 拒绝，提示与 v0.33.1 相同。
- **Agent 契约**：全部 21 个命令的 `--help` 在 en 和 zh-CN 下逐字节一致（42/42）。退出码 3、4、5、6、9 及其 JSON 错误信封与 v0.33.1 一致。`version`、`snapshot`、`compare`、`trend` JSON 的键不变。
- **内存测试**：改写后的三个测试在 750 次定向运行中 0 次失败，覆盖 `GOMAXPROCS` 1、2、8，`taskset -c 0`，go1.26.1，`GOGC` 50、200、off，以及内存和 CPU 压力。512KiB 上限在 720 次流式运行中 0 次误报，最高峰值 89,800 字节。
- `go test ./...` 在默认 `GOMAXPROCS` 和 `GOMAXPROCS=1` 下都通过；改动涉及的包在 `-race` 下无数据竞争。CI 全绿。

## 兼容性

- 没有不兼容变更。`report_version` 仍为 3，`plan_version` 仍为 2，`flashback verify --format json` 的 `schema_version` 仍为 1。退出码、快照、产物名称和支持的平台与 v0.33.1 相同。
- v0.33.0 和 v0.33.1 生成的 flashback 脚本可以直接 verify 和执行。v0.32.0 及更早版本的脚本仍需重新生成。

## 已知问题

- **#303（P2）**：Linux 发布包里的二进制在 `binlogviz version` 中 `commit` 和 `commit_time` 为空。修复在 `.github/workflows/release.yml`，需要有 workflow 权限的维护者处理。请用 `binlogviz_0.34.0_checksums.txt` 校验 Linux 二进制。
- **#352**：text 和 Markdown 选出的重点事务不同。`compare` 和 `trend` 的格式分支仍有 default 分支（#357）。
- `flashback verify` 不接受 `-` 作为 stdin（退出码 4）。
- **#270**、**#255**、**#256**、**#257**（P3）延续到下个版本。

flashback 的保证及其限制见 `docs/concept/limitations.md` 和 `docs/adr/0014-flashback-no-partial-commit.md`。
