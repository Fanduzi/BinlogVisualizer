# BinlogViz v0.23.16 发布说明

发布日期：2026-10-07

## 概述

v0.23.16 把 `--show-rows` 里的 MySQL `TIMESTAMP` 打成所存时刻的 UTC 墙钟，含小数秒。打印结果与进程时区无关。`DATETIME` 仍是 binlog 里写下的墙钟。本版本修复 #132。

## 新功能

无。

## Bug 修复

- **`--show-rows` 的 TIMESTAMP 按 UTC 打印（#132）**：`TIMESTAMP` 单元格（含小数秒）是所存时刻的 UTC 墙钟。在 `TZ=Asia/Shanghai` 和 `TZ=UTC` 下，MySQL 8.0.46 FULL fixture 都打印 `updated_at='2026-10-06 14:00:01.000000'`。v0.23.15 在 `Asia/Shanghai` 下打印的是 `2026-10-06 22:00:01.000000`。这与 `mysqlbinlog` 的 `@6=1791295201`（UTC 14:00:01）一致。`DATETIME` 单元格不变。列名、无符号整数、每事务 32 行和每值 64 字节的上限，以及 `--dml`，都不变。

## 验证

tip `33a3f50`（#135）的 BinlogQA dogfood：2026-10-07 PASS。`Asia/Shanghai` 和 `UTC` 都显示 `updated_at='2026-10-06 14:00:01.000000'`，与 `mysqlbinlog` `@6=1791295201` 一致。两种时区下 `go test` 都通过，包括 `TestShowRowsTimestampIsUTCInEveryZone`。

## 破坏性变更

无。

## 兼容性说明

- 进程时区不是 UTC 时，`--show-rows` 的 `TIMESTAMP` 文本会变。v0.23.15 在 `Asia/Shanghai` 下打印成 `22:00:01` 的同一时刻，现在打印成 `14:00:01`。`DATETIME` 文本不变。
- `--show-rows` 仍默认关闭。`--dml`、列名、FULL 元数据下的无符号整数，以及 32 行 / 64 字节上限，与 v0.23.15 相同。
- 退出码 0 / 1 / 2 仍按 ADR-0001。
- 快照、工作流、产物命名和支持平台除此之外与 v0.23.15 一致。
- workflow 与 trend 行为不变。

## 已知问题

无。
