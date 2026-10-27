# BinlogViz v0.35.2 发布说明

发布日期：2026-10-27

## 概述

v0.35.2 是补丁版本。按 (表, 分钟) 记录的活动序列不再让堆随表数乘分钟数增长（#423，关闭 #389，ADR 0019）。超过 8192 个点后，序列写到一个已 unlink 的临时文件：解析期间写成有序的 run，分层归并，最后再做一次归并。输出逐字节不变：`report_version` 仍为 4，`plan_version` 仍为 2，`flashback verify --format json` 的 `schema_version` 仍为 1。在 200 万个点的文件上，analyze 耗时 24.05s，v0.35.1 为 27.43s；RSS 1372MiB，v0.35.1 为 2260MiB。

## 修复

- **活动序列有上界（#423，关闭 #389）。** `analyze` 和 `flashback` 以前为每个 (表, 分钟) 点保存每张表的每分钟活动（HTML 图表）和每分钟各表的行数（JSON `minutes[].table_rows`、text 和 Markdown 的主导表、HTML 热点区间、尖峰检测、DuckDB 分钟-表行），每个点约 220 字节。500 张表跑 7 天约 1GB live heap。参见 `docs/adr/0019-activity-series-spill.md`。
  - 8192 个点以内序列留在内存，报告走原来的路径。
  - 超过 8192 个点后序列写到已 unlink 的临时文件（`binlogviz-activity-*`）。一个 320KiB 的内存缓冲区累加计数，包括已经写出的分钟再次被访问时的增量，满了就写成一个有序 run。解析期间从不读回这个文件。
  - 同一层满 8 个 run 就归并成下一层的一个 run，每条记录约重写 log8(点数 / 缓冲单元格) 次，临时文件读写量接近线性。最后做一次 k 路归并把剩下的 run 相加，报告按分钟顺序读这个 run。
  - JSON、text、Markdown，以及 HTML 图表、热点区间、compare 和 trend 的输入，与序列在内存里时字节相同。
- **新退出码 11，kind 为 `spill`。** 分析收尾时（默认存储和 `--detail-store duckdb`）或渲染 json、text、markdown、html 时读活动 spill 失败，退出码 11，kind 为 `spill`，stdout 为空。它不是一般失败（退出码 1），也不是写报告失败（退出码 8）。参见 `docs/reference/exit-codes.md`。
- **spill 是可选的。** 每次 flush 扩大文件前，检查临时文件系统的剩余空间是否够这次写入再加 32MiB 预留。写入后会剩不到这么多时，停止 spill，序列其余部分留在内存。spill 文件建不了或写失败时，序列同样留在内存，运行继续。

## 测试与 CI

- `TestWideActivityHeap` 用 24000 个事务比较每分钟 4 张表和每分钟 60 张表，live heap 的差距不得超过 2MiB。#423 之前的 main 上，analyze 差 7.4MB，flashback 差 5.4MB。它在 CI `verify` 里跑。
- `TestSequentialSpillIOScales`（25 万、50 万、100 万个点）和 `TestLaggingReplicaScales`（6000 和 12000 张表）检查临时文件读写量不超过分层归并的上界，耗时接近线性增长。平铺归并的设计在 CI 里让读写量测试变红。
- `TestRevisitedSpillStaysBounded`、`TestAlternatingMinuteSpillStaysBounded` 和 `TestInterleavedChannelsStayResident` 锁住文件大小、峰值和 RSS。`TestSpillBoundFactorsStayTight` 防止上界系数被放宽。`TestReserveIncludesPendingWrite` 和 `TestSpillStopsBeforeFillingTempFS` 覆盖 32MiB 预留。
- spill 读失败在分析、DuckDB 明细读取和每种渲染格式上都有测试（`TestSpillReadErrorWritesNoJSON`、`TestAnalysisSpillReadReportsSpillKind`、`TestDuckDBDetailStoreSpillReadFails`）。
- `docs/production-readiness.md`：E1 写明了按 (表, 分钟) 增长的这一项。24 MET / 0 PARTIAL / 0 NOT MET。

## 验证

BinlogQA 在第六轮（r6，`d328855f`）通过了 #423，main `d328855f` 上的 tip QA 通过，0 个 P0、0 个 P1、0 个 P2（新提一个 P3，#439，v0.35.1 已存在）。`v0.35.1..d328855f` 共 13 个 commit，都来自 #423。本发布 commit 在 `d328855f` 之上只加了这份说明和 CHANGELOG。

- **CI**：`d328855f` 的 PR 运行 40457750182 和 push 运行 40466867793 全绿，包括 vet、`-race` 和 flashback e2e（73 通过）。
- **语料**（680 个文件、4556 个任务、7 种格式），对比官方 v0.35.1：退出码 0 处不同、输出 0 处不同；强制所有序列 spill 的构建也一样。
- **大输入**（200 万个点，8GiB 内存上限）：JSON 24.05s 对 27.43s，RSS 1372MiB 对 2260MiB，哈希相同。临时文件读写量是最终 spill 文件的 14.7 倍（上界 31.2 倍）。在 QA 的 binlog 形态上（多通道交错、8000 张表的延迟从库、50 万到 350 万个点的顺序写入），耗时是 v0.35.1 的 0.93–1.02 倍，RSS 处处低于 v0.35.1。两个版本在 3GiB 上限下都会因这个文件内存不足，和以前一样。
- **失败路径**：spill 读失败退出码 11，stdout 为空，不留临时文件。写失败和 ENOSPC 退回内存，结果与 v0.35.1 相同。SIGINT、SIGTERM、SIGKILL 分别得到 130、143、137，不留临时文件。
- **flashback**：MySQL 8.0.36 上 202 行往返，校验和一致；5.7.44 上与 v0.35.1 一样以退出码 6 结束。8.0.36 和 5.7.44 上的在线 guard 任务 0 个漏放。
- **Schema**：13 个发布的 schema 逐字节相同；1276 份文档用 ajv 和 python-jsonschema 校验通过。`snapshot`、`compare`、`trend`（36 个任务）和 `--help`（26 个节点、19 个失败用例）逐字节相同。
- **测试**：`go test` 通过；analyzer 的 DuckDB 测试在 3GiB 上限下内存不足（环境原因），8GiB 下通过。

## 兼容性

- `report_version` 仍为 4，`plan_version` 仍为 2，`flashback verify --format json` 的 `schema_version` 仍为 1。JSON 没有增删字段，所有输出与 v0.35.1 逐字节相同。
- 新增退出码 11（kind `spill`），表示读活动 spill 失败。以前序列从不落盘，不会出现这种失败。
- 序列较大时，`analyze` 和 `flashback` 可能在临时目录建一个已 unlink 的临时文件。目录里看不到它的名字，进程退出时空间即释放。剩余空间不够这次写入再加 32MiB 时，序列改为留在内存。
- 产物名称和支持的平台不变。

## 已知问题

- **#439（P3，v0.35.1 已存在）**：`docs/examples/analyze-output.json` 和 `.txt` 还是 v4 之前的格式，通不过随附的 analyze schema，而 quickstart 称它们为随附示例。
- **#418**、**#420**、**#421**（P3）延续到下个版本。**#399**、**#400**、**#403**（P3）延续到下个版本。

参见 `docs/adr/0019-activity-series-spill.md`、`docs/reference/exit-codes.md` 和 `docs/production-readiness.md`。
