# BinlogViz v0.23.1 Release Notes

Release date: 2026-08-30

## Overview

v0.23.1 is a patch release for MariaDB operators. It restores analysis of files that contain consecutive DDL GTIDs, which v0.23.0 rejected before producing a report.

## Bug Fixes

- **Consecutive MariaDB DDL GTIDs analyze successfully (#61)**: each GTID-started DDL group now closes at its implicit DDL boundary, so a following legal GTID starts a new group instead of raising `conflicting GTID`.
- **Object filters preserve DDL transaction boundaries**: an excluded DDL still closes its GTID group without entering table, minute, DDL timeline, or transaction aggregates. `--include-*` and `--exclude-*` therefore cannot recreate the same failure.
- **Real in-transaction GTID conflicts still fail**: the provenance integrity guard remains active for explicit transactions; this patch changes only the implicit DDL boundary.

## Breaking Changes

None.

## Compatibility

- CLI flags, exit codes, report schemas, artifact names, and supported platforms are unchanged from v0.23.0.
- Position, time, and GTID selectors continue to intersect as documented.
