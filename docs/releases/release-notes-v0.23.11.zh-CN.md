# BinlogViz v0.23.11 发布说明

发布日期：2026-10-03

## 概述

v0.23.11 把空的 schema/table 过滤和读不了的文件分开。过滤没有匹配时仍是 exit 2，stdout 为空，stderr 为 `Error: schema/table filter matched no events`。文件在事件中间结束时是 exit 1，`Error: binlog is truncated or corrupt`。`--snapshot-name` 配上 json 以外的任何 format 会在写出报告之前失败，并且不写快照文件。失败的命令会先清掉 stderr 上的进度行，再打印 `Error:`。analyze 排行不变。workflow 与 trend 行为不变。

## Bug 修复

- **空过滤与损坏文件分开（#102、#93）**：schema 或 table 过滤没有匹配时仍是 exit 2，stdout 为空，stderr 为 `Error: schema/table filter matched no events`。文件在事件中间结束时是 exit 1，`Error: binlog is truncated or corrupt`。错误的 magic header 仍是 exit 1，信息里有 `not a MySQL binlog`。只有 Format Description 的完整文件仍是 exit 2，`binlog has no analyzable events`。
- **`--snapshot-name` 要求 json（#102、#94）**：`--snapshot-name` 配上 json 以外的任何 format，会在写出报告之前失败：`Error: --snapshot-name requires --format json`，并且不写快照文件。`--format json --snapshot-name` 仍会保存 JSON 快照并以 exit 0 结束。
- **Error 行不再和进度行挤在一起（#102、#95）**：失败的命令会先清掉 stderr 上的进度行，再打印 `Error:`，所以错误不会留在进度文字的同一行。analyze 结果不变。

## 验证

BinlogQA 在 tip `f99dc5b`（#102）上的 dogfood：四项检查 PASS（空 schema/table 过滤、截断文件、错误 magic header、仅 Format Description 的文件）。没有新问题。

## 破坏性变更

以前与空 schema/table 过滤共用 exit 2 的截断或损坏文件，现在是 exit 1。

## 兼容性说明

- 退出码 0 / 1 / 2 仍按 ADR-0001。schema 或 table 过滤没有匹配仍是 exit 2。错误的 magic header 仍是 exit 1。仅 Format Description 的完整文件仍是 exit 2。
- CLI 参数和 analyze 排行语义除此之外与 v0.23.10 一致。`--format json --snapshot-name` 仍保存 JSON 快照并以 exit 0 结束。
- workflow 与 trend 行为不变。
