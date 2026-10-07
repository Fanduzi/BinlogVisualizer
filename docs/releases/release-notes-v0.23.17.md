# BinlogViz v0.23.17 Release Notes

Release date: 2026-10-07

## Overview

v0.23.17 reports tables that received UPDATE or DELETE row events and have no primary key. On a replica applying ROW events, each of those rows can force a table scan, a common cause of replication lag. MySQL 8 `binlog_row_metadata=FULL` TABLE_MAP metadata marks each table `has_pk`, `no_pk`, or `unknown`. `SIMPLE_PRIMARY_KEY` and `PRIMARY_KEY_WITH_PREFIX` are `has_pk`. FULL metadata with neither field is `no_pk`. Without FULL metadata, presence is `unknown`; the report does not emit `no_pk`. INSERT-only no-PK tables are named and are not ranked as a lag risk. A `no_primary_key` warning is added to Top Findings. There is no new CLI flag. This is #138.

## New Features

- **No-primary-key UPDATE/DELETE is a replica lag finding (#138)**: `binlogviz analyze` ranks tables whose `key_status` is `no_pk` and that received at least one UPDATE or DELETE row, by those counts, in text, Markdown, JSON, and HTML. The section follows Top Tables. Text heading: `=== No Primary Key ===`, led by `UPDATE/DELETE on these tables can force a replica table scan.` On `mysql-8.0.46-no-pk-full.binlog`, `shop.heap` (2 UPDATE, 1 DELETE) ranks ahead of `shop.log` (1 UPDATE, 0 DELETE). `shop.scratch` (2 INSERT) is `INSERT-only, not a lag risk: shop.scratch (2 INSERT)` and is not numbered. `shop.orders` and `shop.prefixed` (`PRIMARY KEY (sku(8))`) are `has_pk` and stay out of the section. Top Findings adds `[warning] shop.heap has no primary key and received 2 UPDATE and 1 DELETE rows; replica apply can scan the table`. JSON field: `tables[].key_status` (`has_pk`, `no_pk`, or `unknown`). One such row is enough. There is no threshold flag.
- **Missing FULL metadata stays unknown (#138)**: On `mysql-8.0.46-no-pk-minimal.binlog` (`binlog_row_metadata=MINIMAL`), every row table is `unknown`. The summary grows by one line, `primary key presence unknown (binlog_row_metadata is not FULL)`. JSON sets `primary_key_note` to that sentence. There is no `No Primary Key` section and no `no_primary_key` alert. When every row table has a primary key (`mysql-8.0.46-dml-full.binlog`), the summary line is `primary key present on every table` and `primary_key_note` is omitted. INSERT-only no-PK tables with no lag ranking use `no primary key, INSERT-only (not a replica lag risk): …`.

## Bug Fixes

None.

## Verification

Tip dogfood on `cca873f` (#138): PASS on 2026-10-07 (BinlogQA).

## Breaking Changes

None.

## Compatibility

- Default text, Markdown, JSON, and HTML grow when the binlog has row events. Every table with a primary key adds `primary key present on every table`. Metadata that is not FULL adds `primary key presence unknown (binlog_row_metadata is not FULL)`. A no-PK table with UPDATE or DELETE rows adds a `No Primary Key` section and a `no_primary_key` warning. JSON adds `tables[].key_status` and, when presence is unknown, `primary_key_note`.
- `--include-table`, `--exclude-table`, schema filters, `--dml`, and time, position, and GTID filters apply to this section the same way they apply to Top Tables. `--dml insert` names INSERT-only no-PK tables and does not rank them as a lag risk.
- There is no new CLI flag. Exit codes 0, 1, and 2 keep ADR-0001 meaning.
- Snapshots, workflows, artifact names, and supported platforms are otherwise unchanged from v0.23.16.
- Workflow, trend, compare, and snapshot behavior are unchanged. This release does not emit rollback or flashback SQL.

## Known issues

None.
