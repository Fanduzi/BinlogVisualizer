# BinlogViz v0.23.10 Release Notes

Release date: 2026-10-02

## Overview

v0.23.10 measures committed transaction duration as the earliest-to-latest non-zero in-window timestamp. On MySQL 8 the leading GTID event is written at commit and shares that stamp with the XID, so a file-order span after `SLEEP` was 0s. `BEGIN` and row events keep the statement-start second, and that wall span is the duration. A checked-in MySQL 8.0.46 fixture also locks the open BEGIN+DML path. Workflow and trend behavior are unchanged.

## Bug Fixes

- **Committed duration uses the server clock (#99, #92)**: Group duration is the earliest-to-latest non-zero timestamp inside the window. MySQL stamps the leading GTID and the XID at commit; `BEGIN` and row images keep the statement start. The admitting fixture is `mysql-8.0.46-committed-duration.binlog`: `SELECT SLEEP(2)` between an `INSERT` and `COMMIT` (the sleep is not logged) lands in the `1s-10s` bucket, and a following autocommit insert stays in `<1s`. `--large-trx-duration 1s` warns with that duration. The default `30s` threshold does not. Nothing in the file is a lock wait.
- **Open BEGIN+DML dialect fixture (#98, #91)**: `mysql-8.0.46-open-begin-dml.binlog` is an explicit `BEGIN` with two `INSERT` row images and no `COMMIT` or `ROLLBACK`, then a later business GTID. Analyze of the full file exits 1, stdout is empty, and the single `Error:` line names `open BEGIN without close`, duration, rows, tables, and the file span, and says the span is not lock-contention proof. A prefix that stops at the later GTID exits 0 and JSON keeps `diagnostics.open_dml_groups`. That open group stays out of the committed duration ranking. The error-line duration is the wall clock from the open group's first event to the later GTID (`4s` in this file). An EOF prefix reports group duration `2s`, because that group's first event and its last row are two seconds apart.

## Verification

BinlogQA dogfood on tip `1c2d527` (merge of #99): PASS for the committed-duration fixture. The open-BEGIN fixture from #98 is also OK on that tip.

## Breaking Changes

None.

## Compatibility

- Exit codes 0, 1, and 2 keep ADR-0001 meaning. A later GTID after an open `BEGIN` that wrote rows is still exit 1. An EOF-open prefix of that shape is still exit 0.
- Reported durations for MySQL 8 GTID groups can be longer than the file-order span v0.23.9 printed when the leading GTID shared the commit stamp.
- CLI flags, snapshots, workflows, artifact names, and supported platforms are unchanged from v0.23.9.
