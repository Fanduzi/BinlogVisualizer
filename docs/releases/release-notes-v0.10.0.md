# BinlogViz v0.10.0 Release Notes

Release date: 2026-04-09

## Scope

`v0.10.0` is a feature release built on top of `v0.9.1`. This version turns workload patterns into a first-class analysis dimension so teams can explain drift in terms of repeated write shapes instead of stopping at totals, hot tables, or one-off large transactions.

## Highlights

- added workload pattern aggregation to `analyze`, including top patterns in text output and a stable `patterns` array in JSON output
- added `pattern_changes` to `compare` so current-versus-baseline review can identify which write patterns actually drove the delta
- added `pattern_trends` to `trend`, including text summaries, JSON series, and an HTML `Pattern Trends` section that defaults to `share of rows` and can switch to absolute `rows`
- strengthened result integrity and release readiness with explicit `report_version`, legacy compatibility coverage, fixture-backed goldens, and release smoke checks
- updated operator-facing docs so CLI references, output contracts, install examples, and release surfaces all describe the new pattern-analysis workflow

## Compatibility Notes

- older snapshots and analyze reports without pattern data remain usable; `compare` and `trend` treat missing patterns as an empty set instead of failing
- snapshot-backed workflows now accept `report_version` 2 while continuing to reject unknown future versions explicitly
- the existing analyze, snapshot, compare, and trend command shapes remain intact; this release adds new result sections without removing prior ones

## Breaking Changes

None.
