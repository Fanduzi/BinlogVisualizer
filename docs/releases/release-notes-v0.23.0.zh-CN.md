# BinlogViz v0.23.0 发布说明

发布日期：2026-08-30

## 概述

v0.22.1 能看一眼 ROW，但不能结案。本版本对应 #59 评分表：交付 #42–#58，让值班能框定窗口、保持诚实范围，并把可回放证据交出去，不必再开第二套工具。

## 新功能

- **位点与 GTID 分析窗口（#52）**：`--start-position` / `--stop-position` 是单个显式文件上的半开区间 `[start, stop)`。`--include-gtids` / `--exclude-gtids` 支持 MySQL UUID:interval 集合和 MariaDB `domain-server-seq` 列表。仍可同时给时间条件，谓词取交集。GTID 选择器下的匿名/无键事件会被省略，而不是算进所选集合。
- **事务保留来源与 SQL 上下文（#51）**：binlog 里有的 `user@host`、`thread_id`、GTID、`server_id`、`xid` 会留在报告里。`--sql-context full` 不再是空操作。
- **Markdown 能用来结工单（#55）**：`--format markdown` 保留 `file:pos`、`mysqlbinlog`、DDL 和 `txn_key`，不再丢掉结案所需证据。
- **区分输入体积与计入体积（#48）**：HTML 摘要把文件/窗口体积和实际计入的 ROW image 分开，311 MiB 文件不会只被热分钟子集概括。
- **compare / trend 带上回放证据（#44）**：HTML findings 给出 `file:pos` 和可复制的 `mysqlbinlog` 命令，而不只是热表名。

## Bug 修复

- **相对路径的 workflow 输出可 resume（#42）**：相对解压目录的 `output_dir` 在 `workflow resume` 时仍能找到 `plan.yaml`。
- **对象过滤作用于整份 analyze 报告（#43）**：`--include-table` / `--exclude-schema` 约束 summary、大事务和 diagnostics，而不只是 Top Tables。
- **`compare` 拒绝原始 binlog（#45）**：两个 `.000001` 不再 Usage 两遍外加 `invalid character`。请传命名 snapshot 或 `analyze --format json` 报告。
- **干净报告不再编造可疑位点（#46）**：findings 和 alerts 为空时，Next Actions 不再写 “First suspicious position”。只有 alert/finding 点到真实事务时才打印位置。
- **纯 DDL 不再写成 healthy（#47）**：同一页已有 CREATE TABLE 时间线时，analyze HTML 首屏不再写 No issues / healthy。
- **HTML 事务证据是 top-N 集合（#49）**：均匀大文件上 largest / longest / widest 不再塌成同一张 `txn-1` 卡片。
- **因果 compare/trend 需要可比负载（#50）**：不同 server / schema / 选择器 / 不完整事务证据不能编出因果故事。护栏先打印；原始 delta 仍保留。持久化的位点与 GTID 选择器计入该范围。
- **被窗口切开的事务标为 partial（#53）**：`--start` / `--end` 切到事务中间时，不再看起来完整。
- **`--top` 只影响展示，不改写数据（#54）**：`--top 1` 不再把剩余 Row Share 改成 100%，也不从 JSON 删除其他表。
- **有界事务输出会被披露（#56）**：JSON `transactions` 不再是按字符串排序的前 10 个 key。界限可见，`txn-5` 不会被静默丢掉。
- **MariaDB XA 与 LOAD DATA 保持原语义（#57）**：XA 组不会粘到下一笔 GTID；LOAD DATA 不再显示成普通 INSERT。
- **时间戳统一为 UTC（#58）**：文本和 HTML 打印 `UTC`，不再和 JSON 的 `Z` 对不上。

## 破坏性变更

- **`compare` 不再接受原始 binlog 文件。** 以前直接传两个 binlog 的脚本，需要先 analyze 成 JSON 或 snapshot。
- **可比性为 `unknown` 或 `not_comparable` 时，抑制因果 findings、recommendations 和 drilldowns。** 原始数值 delta 仍会输出。
- **`--top` 不再改写 JSON 的表集合或份额。** 假定 JSON 里只有 N 张表的消费者会看到完整表集合，外加展示上限。
- **GTID 选择器会省略匿名和无键事件**，而不是把它们算进所选窗口。
- **文本/HTML 时间戳带 `UTC` 后缀。**

## 兼容性说明

- Snapshot、compare、trend 的 JSON 顶层形状保持不变。新增字段是加性的：comparability、provenance、输入/计入字节、evidence refs、事务完整性。
- v0.22.1 的退出码不变。
- #59 是本轮工作的评分表，不是单独的运行时契约。
