# BinlogViz v0.18.0 Release Notes

Release date: 2026-04-16

## Scope

`v0.18.0` turns BinlogViz into a more DBA-oriented incident analysis workflow.

This release adds multi-file time-window discovery, richer analyzer diagnostics, redesigned analyze HTML reports, compare/trend diagnostic deltas, and complete English/Chinese localization across the main HTML surfaces.

## Highlights

- added directory-based analyze discovery with `--from-dir`, `--prefix`, `--start`, and `--end`
- added file probe planning plus `diagnostics.file_coverage` so selected and skipped binlogs are explicit
- added DDL timelines, hot intervals, TPS and event-mix series, binlog throughput segments, and transaction evidence
- analyze HTML now includes larger charts, Top Tables inline drilldown, File Coverage, Binlog Throughput, and Transaction Evidence sections
- compare and trend now consume diagnostics/timeseries to render DBA-facing deltas and trends
- HTML reports now honor `--lang` consistently across analyze, compare, and trend
- reduced streaming allocation pressure in normalize, transaction building, store batching, and minute-bucket draining

## JSON Contract Additions

Analyze reports now include additive top-level diagnostics and timeseries payloads:

```json
{
  "timeseries": {
    "tps_series": [],
    "rows_series": [],
    "events_series": []
  },
  "diagnostics": {
    "file_coverage": {
      "selected": [],
      "skipped": []
    },
    "ddl_events": [],
    "hot_intervals": [],
    "largest_transactions": [],
    "longest_transactions": [],
    "widest_transactions": [],
    "file_segments": []
  }
}
```

Compare and trend consume these new structures while continuing to tolerate legacy reports that omit them.

## Compatibility Notes

- existing `binlogviz analyze <files...>` usage remains supported
- new JSON fields are additive; existing consumers that ignore unknown fields remain compatible
- compare and trend continue handling older snapshots that do not contain `diagnostics` or `timeseries`
- release artifacts remain self-contained and continue shipping as platform tarballs plus a checksum manifest

## Breaking Changes

None.
