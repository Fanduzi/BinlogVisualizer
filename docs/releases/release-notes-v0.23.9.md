# BinlogViz v0.23.9 Release Notes

Release date: 2026-10-02

## Overview

v0.23.9 admits `CHECK TABLE` and `SET ROLE` as ADMIN, so a maintenance-window analyze that used to die on those statements now exits 0. Admission is from committed MySQL 8.0.46 ROW+GTID dialect fixtures. Exact `FLUSH TABLES` is unchanged. `FLUSH TABLES WITH READ LOCK` stays Unclassified QUERY. Workflow and trend behavior are unchanged.

## Bug Fixes

- **`CHECK TABLE` is ADMIN (#87)**: The two-word prefix `CHECK TABLE` closes a GTID-started non-explicit group whose only work is that statement, including `CHECK TABLE schema.table`. The statement is not DDL and does not appear on the DDL timeline. The admitting fixture is `mysql-8.0.46-check-table.binlog`: a maintenance-only GTID group, then one business INSERT. Analyze of that file exits 0 and reports the business transaction.
- **`SET ROLE` is ADMIN (#87)**: The two-word prefix `SET ROLE` covers `SET ROLE ALL` and `SET ROLE <name>`. It does not match `SET DEFAULT ROLE`, which stays the existing three-word ADMIN phrase. `SET timestamp`, `SET NAMES`, and other `SET` that is not `SET ROLE` or `SET DEFAULT ROLE` stay Ignored QUERY. The admitting fixture is `mysql-8.0.46-set-role.binlog` (`SET ROLE ALL`, then one business INSERT). Analyze of that file exits 0.
- **Exact `FLUSH TABLES` is unchanged**: Membership remains exact equality from the existing MySQL 8.0.46 fixture. `FLUSH TABLES WITH READ LOCK` and `FLUSH TABLES <table>` stay Unclassified QUERY. When that statement is the only work of a GTID-started non-explicit group, analyze still exits 1.

## Verification

BinlogQA dogfood on tip `d43513e` (merge of #96): PASS.

## Breaking Changes

None.

## Compatibility

- Exit codes 0, 1, and 2 keep ADR-0001 meaning. A file that contains only these ADMIN statements and no ROW images is still a no-data result (exit 2). Unclassified QUERY, including `FLUSH TABLES WITH READ LOCK`, is still exit 1.
- CLI flags, snapshots, workflows, artifact names, and supported platforms are unchanged from v0.23.8.
