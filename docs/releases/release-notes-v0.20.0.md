# BinlogViz v0.20.0 Release Notes

Release date: 2026-04-20

## Scope

`v0.20.0` speeds up discovery-mode binlog probing and improves text report readability for multi-hour analysis windows.

## Highlights

- **Fast probe**: discovery-mode (`--from-dir`) now uses two-phase timestamp extraction — early termination for the first event, offset-based parse from the file tail for the last event — instead of a full file scan. This reduces the delay before "Resolved binlog files" from seconds to sub-second.
- **Sparkline downsampling**: text report activity sparklines are now capped at 50 bars with averaged buckets. A resolution annotation (e.g. `3 min/bar`) is shown when data is downsampled, keeping the overview readable for windows spanning hours.
- Added `OffsetParser` interface for offset-based binlog parsing.

## Bug Fixes

None.

## Breaking Changes

None.
