# BinlogViz v0.23.5 Release Notes

Release date: 2026-09-07

## Overview

v0.23.5 closes MariaDB XA ROLLBACK groups so the next GTID no longer fails analyze, and default analyze no longer needs CGO or DuckDB.

## Bug Fixes

- **XA ROLLBACK closes its GTID group**: a Query `XA ROLLBACK` ends the open transaction group. XA END still continues. The next legal GTID is not treated as a conflict.
- **Zero-row XA stays in the report only with a file location**: retain requires an XA identity, a recorded start file path, and `PositionEnd > PositionStart`. DDL-only groups stay on the DDL timeline.
- **Default analyze builds and runs without CGO**: DuckDB is an optional `--detail-store duckdb` adapter. `NewDuckDBStore` fails with `ErrDuckDBRequiresCGO` when the binary was built without CGO. Workflow analyze uses the same default `none` path as the CLI.
- **Unmapped parser kinds are counted**: events with no canonical kind (ROTATE and similar) increment JSON `unmapped_events` instead of disappearing from diagnostics.

## Improvements

- Parser `EventType` is one canonical kind (`QUERY`, `WRITE_ROWS`, `GTID`, `XA_PREPARE`, …). go-mysql `String()` names no longer leak into analysis.
- Analyze, compare, and trend HTML share the same five-theme CSS tokens.

## Breaking Changes

None.

## Compatibility

- CLI flags, exit codes, report schemas, artifact names, and supported platforms are unchanged from v0.23.4. JSON may include optional `unmapped_events`.
