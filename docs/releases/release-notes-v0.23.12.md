# BinlogViz v0.23.12 Release Notes

Release date: 2026-10-03

## Overview

v0.23.12 names transaction identity on the default text, Markdown, and HTML analyze reports. Each transaction lists `server_id`, `thread_id`, GTID, and `xid` or XA xid when the events have them. `user@host` appears only when the binlog stored an invoker. A field the events do not have stays absent: no `0`, no empty string. JSON already had these fields. `--sql-context full` is unchanged. Workflow and trend behavior are unchanged.

## New Features

- **Transaction identity on text, Markdown, and HTML (#104)**: Default text, Markdown, and HTML analyze reports name, per transaction, `server_id`, `thread_id`, GTID, and `xid` or XA xid when the events have them. The tokens are `server_id=`, `thread_id=`, `gtid=`, `xid=`, and `xa_xid=` for whichever of those the transaction carries. `user@host=` appears only when the binlog stored an invoker. A missing field stays absent (no `0`, no empty string). The MySQL 8.0.46 `FLUSH TABLES` fixture prints `server_id=1`, `thread_id=12`, its GTID, and `xid=16`, and does not print `user@host`. `minimal.binlog` prints `server_id`, `thread_id`, and `xid`, and does not print `gtid=` or `user@host`. JSON already had these fields and still omits an empty one. `--sql-context full` is unchanged.

## Verification

BinlogQA dogfood on tip `72cd660` (#104): PASS on 2026-10-03.

## Breaking Changes

None.

## Compatibility

- Exit codes 0, 1, and 2 keep ADR-0001 meaning.
- JSON transaction identity is unchanged. `--sql-context full` is unchanged.
- CLI flags, snapshots, workflows, artifact names, and supported platforms are otherwise unchanged from v0.23.11.
- Workflow and trend behavior are unchanged.
