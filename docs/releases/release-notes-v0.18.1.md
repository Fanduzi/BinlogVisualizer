# BinlogViz v0.18.1 Release Notes

Release date: 2026-04-19

## Scope

`v0.18.1` is a stabilization and performance-validation release for the v0.18 DBA report workflow.

This release keeps the v0.18 JSON and report contracts intact while tightening the analyze hot path against a real 988 MB MySQL 8.0 ROW binlog generated with sysbench. The focus is bounded memory behavior, repeatable benchmark gates, and safer release-surface documentation.

## Highlights

- added an external real-binlog benchmark gated by `BINLOGVIZ_REAL_BINLOG`, so large local fixtures can validate the analyze path without committing binary logs
- added parser-layer real-fixture benchmarks that isolate parse-only, parse+normalize, parse+progress, and end-to-end layers
- reduced finalize allocation hotspots by avoiding full-slice copies for representative transactions and indexing only alert-referenced transactions
- reduced store transaction scan memory with pre-sized transaction row reads and lazy `Tables` / `Operations` map hydration
- tuned DuckDB transaction batch flushes from 5,000 to 10,000 rows after batch-size benchmarking
- validated JSON, HTML, and default text analyze outputs against a real 988 MB ROW binlog smoke workload
- removed unused analyzer diagnostics helpers

## Performance Notes

On the local 988 MB MySQL ROW binlog smoke sample, the external benchmark stabilized around:

- analyze benchmark: about 13.5 seconds
- heap allocation: about 7.95 GB/op
- allocation count: about 151.7 million allocations/op
- CLI peak RSS: about 1.9 GB

These numbers are workload- and machine-dependent. They are intended as a regression baseline, not a universal throughput guarantee.

## Compatibility Notes

- no JSON schema changes
- no CLI flag changes
- no report contract changes
- existing `binlogviz analyze`, `compare`, `trend`, and snapshot workflows remain compatible
- the external real-binlog benchmark skips unless `BINLOGVIZ_REAL_BINLOG` is set

## Breaking Changes

None.
