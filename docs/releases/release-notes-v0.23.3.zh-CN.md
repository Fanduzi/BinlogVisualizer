# BinlogViz v0.23.3 发布说明

发布日期：2026-08-30

## 概述

v0.23.3 恢复 MariaDB XA 事故排查所需的证据。已 PREPARE 的事务会保留 XA 身份；即使 `XA COMMIT` 影响 0 行，也会以自己的 GTID 留在报告中并可被选择器命中。

## Bug 修复

- **XA PREPARE 保留 XA 身份（#63）**：物理 MariaDB `XAPrepareLogEvent` 的 payload 现在会把 `xa_xid` 写入已 PREPARE 的事务。
- **0 行 XA COMMIT 仍作为事务保留**：即使没有行变更，提交 GTID 与 `xa_xid` 也不会从报告消失。
- **GTID 与位点选择器可命中 XA COMMIT**：`--include-gtids` 和覆盖 COMMIT 的位点窗口会返回这笔 0 行事务，不再报无事件。

## 破坏性变更

无。

## 兼容性说明

- CLI 参数、退出码、报告 schema、产物命名和支持平台均与 v0.23.2 保持一致。
- 普通 0 行 GTID 组仍会省略；新增保留仅适用于窗口内存在物理证据的 XA 事务。
