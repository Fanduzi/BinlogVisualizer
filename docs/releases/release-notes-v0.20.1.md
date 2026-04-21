# BinlogViz v0.20.1 Release Notes

Release date: 2026-04-21

## Bug Fixes

- **Probe offset-parse tolerance**: The `analyze` command no longer crashes when probing binlog files where the offset-based timestamp scan lands on a non-event boundary (e.g., corrupted or truncated events). The last timestamp now gracefully falls back to the first event timestamp instead of returning a fatal error.

## Improvements

- **Drill-down chart Y-axis labels**: Table activity and operation distribution charts in HTML reports now display a "Rows/min" Y-axis label for clarity.

## Breaking Changes

None.
