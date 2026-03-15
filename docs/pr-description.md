# PR Description

## Summary

Build `binlogviz` MVP as a DBA-focused offline MySQL binlog analyzer for local `ROW` binlog files.

This PR adds:
- binlog parsing and normalization
- transaction reconstruction
- top tables / top transactions / minute activity analysis
- time window filtering
- large transaction alerts
- overall and table-level spike detection
- text and JSON report output
- real binlog fixture E2E coverage
- bounded `Rows_query_log_event` SQL context support

## Why

The goal is not just to parse binlog, but to help answer practical DBA questions:
- what happened in a time window
- which table is writing the most
- whether there are large transactions
- whether there are obvious write spikes
- what the workload pattern looks like

## Scope

Included:
- CLI-first workflow with `binlogviz analyze`
- analysis pipeline: parser -> normalizer -> analyzer -> renderer
- real fixture-based integration coverage
- SQL context capture with bounded storage and safe default output

Not included:
- Web UI
- real-time streaming consumption
- Prometheus exporter
- SQL replay
- complex anomaly detection
- cross-instance aggregation

## Known Limitations

- primarily supports local `ROW` binlog files
- current command path still retains normalized events in memory before analysis
- SQL context depends on `Rows_query_log_event` being present in the source binlog

## Next Priorities After Merge

1. streaming analysis to reduce memory pressure on large binlogs
2. richer SQL context presentation controls
3. broader benchmark and fixture coverage
4. release/tag and packaging polish
