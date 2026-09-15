# BinlogViz v0.23.6 Release Notes

Release date: 2026-09-15

## Overview

v0.23.6 closes independent management QUERY GTID groups so the next GTID no longer fails analyze, and `--start`/`--end` accept the DBA-common space-separated local time format.

## Bug Fixes

- **Independent management QUERY closes its GTID group**: `ANALYZE TABLE`, `OPTIMIZE TABLE`, `FLUSH PRIVILEGES`, and `SET DEFAULT ROLE` normalize as `ADMIN`, not `DDL`. A GTID-started group with no BEGIN/XA closes at the QUERY end position. The next legal GTID is not treated as a conflict. Unknown QUERY, including other `SET` and `FLUSH` forms, stays skipped. Explicit `BEGIN`/`XA START` followed by a different GTID still fails analyze.
- **`--start`/`--end` accept `YYYY-MM-DD HH:MM:SS`**: values are trimmed first. RFC3339 with explicit offsets such as `Z` or `+08:00` still defines the instant. The space format uses the local timezone of the machine running `binlogviz`. Inclusive endpoints and UTC report display are unchanged.

## Breaking Changes

None.

## Compatibility

- CLI flags, exit codes, report schemas, artifact names, and supported platforms are unchanged from v0.23.5. Zero-row admin groups still do not become report transactions.
