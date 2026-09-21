# BinlogViz v0.23.7 Release Notes

Release date: 2026-09-21

## Overview

v0.23.7 stops independent management QUERY from being silently dropped. Analyze classifies QUERY as ADMIN, Ignored QUERY, or Unclassified QUERY. An Unclassified QUERY that is the only work of a GTID-started non-explicit group fails with a statement prefix instead of a fake conflicting GTID. Transaction payload wrappers expand to inner ROW images, and exact `FLUSH TABLES` becomes ADMIN from a committed MySQL 8.0.46 dialect fixture.

## Bug Fixes

- **Unclassified QUERY fails analyze**: `CHECK TABLE`, `SET ROLE`, and other unaffiliated independent statements are no longer skipped. If that statement is the only work of a GTID-started non-explicit group (named or anonymous), analyze exits 1 with empty stdout and one `Error:` line that includes a bounded statement prefix. The error is not “conflicting GTID”.
- **Ignored QUERY is counted and never closes**: `SET timestamp`, `SET NAMES`, and other `SET` that is not `SET ROLE` or `SET DEFAULT ROLE` stay session prefix. JSON diagnostics count them on a new optional field, separate from Query-DML and from unmapped events. A next GTID after an Ignored-only group is still a conflicting GTID.
- **After-window Unclassified QUERY does not wipe an in-window report**: clip flags treat an Unclassified QUERY that never intersected the selected window as a boundary observation. An unclassified-only group that intersects the window still fails.
- **Transaction payload inner ROW images are counted**: a compressed wrapper is expanded before classification. After a successful expand the wrapper does not increment unmapped events. Inner images share the wrapper's file-relative span once, so `--start-position` / `--stop-position` see the on-disk range. Partial-update row events count as UPDATE. Anonymous GTID still opens a group with empty identity.
- **Exact `FLUSH TABLES` is ADMIN**: a committed MySQL 8.0.46 ROW+GTID fixture admits that statement as ADMIN. `FLUSH TABLES WITH READ LOCK` and `FLUSH TABLES tbl` stay Unclassified QUERY. The existing four ADMIN statements are unchanged.

## Breaking Changes

None.

## Compatibility

- Exit 0 / 1 / 2 keep ADR-0001 meaning. Unclassified QUERY is exit 1. An ADMIN-only file with no ROW images is still a no-data result (exit 2).
- Snapshots that gain the optional Ignored QUERY count remain loadable.
- CLI flags, artifact names, and supported platforms are unchanged from v0.23.6.
