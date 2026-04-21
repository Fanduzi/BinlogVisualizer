# BinlogViz v0.20.2 Release Notes

Release date: 2026-04-21

## Bug Fixes

- **Probe regression that excluded all files**: v0.20.1 introduced a fallback that set `LastEventAt = FirstEventAt` when offset-based probing failed on large binlog files. This made the planner treat each file as spanning a single point in time, causing `--from-dir --start --end` to resolve 0 files for any time-windowed analysis. The fallback has been removed; `LastEventAt` now stays zero so the planner includes files conservatively when the end time is unknown.
- **Silent empty-result generation**: When discovery resolved 0 binlog files (e.g., no files overlap the requested window), the pipeline previously proceeded to parse nothing, finalize an empty analysis, and silently save an empty snapshot. The CLI now errors immediately with a clear message instead of producing misleading empty output.

## Breaking Changes

None.
