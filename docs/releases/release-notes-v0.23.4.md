# BinlogViz v0.23.4 Release Notes

Release date: 2026-09-06

## Overview

v0.23.4 restores analyze on MariaDB init binlogs that carry consecutive GRANT / CREATE USER DDL GTIDs, and fixes table, prefix, and export CLI mismatches that blocked on-call workflows.

## Bug Fixes

- **Consecutive MariaDB GRANT DDL GTIDs analyze successfully (#67)**: `GRANT` and `REVOKE` Query events now close their GTID-started group, so `--from-dir` no longer fails on init files such as `mysql-bin.000001`. Real in-transaction GTID conflicts remain rejected.
- **`--include-table` accepts `SCHEMA.TABLE` (#65)**: `dogfood.orders` matches schema `dogfood` and table `orders` instead of a literal table name that filtered the workload to zero.
- **`--prefix` accepts a complete filename (#69)**: `--prefix mysql-bin.000008` selects that file. A no-match error that looks like a filename hints the shared prefix or a direct `analyze DIR/file` path.
- **`workflow export` accepts `-o` (#68)**: the shorthand matches `analyze --output`.

## Documentation

- Chinese README now includes the SHOW MASTER STATUS / GTID quick-start section already present in English (#66).

## Breaking Changes

None.

## Compatibility

- CLI flags, exit codes, report schemas, artifact names, and supported platforms are unchanged from v0.23.3.
