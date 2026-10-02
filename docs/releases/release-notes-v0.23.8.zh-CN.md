# BinlogViz v0.23.8 发布说明

发布日期：2026-10-02

## 概述

v0.23.8 在默认 analyze 输出中补齐了 DBA 事故排查核心证据：DDL 发生时间线、未提交的 BEGIN+DML 悬挂事务组、已提交事务耗时排行与分布桶、以及输入文件大小与计入事件字节数对比。此外，本版本修复了仅含 DDL 事务的计数问题，支持独立 ROLLBACK 与 XA END 的平稳释放，明确区分了未关组 BEGIN、SAVEPOINT 和仅含 Ignored QUERY 的失败原因，并内置了 verify-binlogviz 技能。

## 新功能

- **DDL 发生时间线（#89）**：默认文本输出呈现 DDL 发生时间线（时间戳、操作类型、影响对象、binlog 位点与语句前缀），快速回答变更漂移问题且不引入虚假的锁等待时长语义。JSON 在 `diagnostics.ddl_events` 暴露相同事件。仅含 DDL 的文件现在返回 exit 0 并展示该时间线；纯 ADMIN 文件仍保持 exit 2。`FLUSH TABLES WITH READ LOCK` 保持为 Unclassified QUERY（exit 1）。
- **未提交 BEGIN+DML 悬挂事务组（#89）**：写入了 ROW image 但未见 `COMMIT` 或 `ROLLBACK` 的显式 `BEGIN` 组，现在直接作为 open DML group 输出，展示耗时、涉及表、影响行数与文件位置跨度。耗时超过 `--large-trx-duration` 的事务会标记预警。若后续遇到不同 GTID，analyze 退出 exit 1 并在 Error 报错中完整保留上述排查证据。
- **已提交事务耗时排行与分布桶（#89）**：默认文本报告在行数排行旁增加耗时最长的已提交事务排行，耗时告警中附带具体执行时间，并展示已提交事务耗时分布桶（`<1s`、`1s-10s`、`10s-30s`、`>=30s`）。只要统计到已提交事务，JSON 即输出 `diagnostics.duration_buckets`。
- **输入文件体积与计入事件字节（#89）**：默认文本输出展示按字节贡献排名的事务与热表，并在多文件分析时输出各文件大小与时间跨度。JSON 增加 `diagnostics.largest_byte_transactions`。
- **项目内置 verify-binlogviz 技能（#81）**：在 `.cursor/skills/verify-binlogviz` 提供面向 DBA 的 CLI 验证体系，支持在真实 ROW fixture 上执行 analyze、snapshot、compare、trend 和 workflow 验证。

## Bug 修复

- **txn_count 正确统计含行图的事务（#82）**：表级和分钟级的 `txn_count` 仅统计包含行变更的事务，不再将仅含 DDL 的组计入事务集合。包含 `CREATE TABLE` 的文件报告的事务数与 summary.total_transactions 严格一致。
- **trend 与 snapshot 报错精简为单行 Error（#82）**：`binlogviz trend` 与 `binlogviz snapshot` 缺少参数或失败时仅输出一行 `Error:`，不再额外刷屏 Usage，与 `analyze` 及 `compare` 体验保持一致。
- **独立 ROLLBACK 正常关组（#83）**：显式事务组内的独立 `ROLLBACK`、`ROLLBACK WORK`（含分号变体）被识别为事务关闭，后续合法 GTID 不再因伪 conflicting GTID 异常中断。
- **XA END 允许在后续 GTID 正常结算释放（#83）**：`XA END` 记录组结束边界，后续不同的 GTID 或文件流结束会正常结算该组，不再触发 conflicting GTID 报错。
- **明确区分未关闭 BEGIN、SAVEPOINT 与 Ignored QUERY 报错（#84、#85、#86）**：
  - 遇到未提交 `BEGIN` 后出现新 GTID 时，报错明确指示 `open BEGIN without close`（exit 1）。
  - `ROLLBACK TO SAVEPOINT` 不会结束事务组，遇冲突 GTID 时 Error 明确提示其不是关组操作。
  - 仅包含 Ignored QUERY 的事务组遇新 GTID 时，明确提示 Ignored QUERY 不会关组且该报错并非遗漏 `COMMIT`。
  - 当文件在显式 `BEGIN` 组内结束且无新 GTID 时，JSON 统计 `diagnostics.open_explicit_groups`。

## 破坏性变更

无。

## 兼容性说明

- 退出码 0 / 1 / 2 严格遵循 ADR-0001 规范：有计数事件 exit 0，致命错误或未分类/未闭合异常 exit 1，无数据（空窗口或仅 ADMIN）exit 2。
- 现有快照、工作流和命令行参数完全向后兼容。
- 构建产物命名与支持的操作系统架构平台与 v0.23.7 完全一致。
