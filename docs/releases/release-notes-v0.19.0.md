# BinlogViz v0.19.0 Release Notes

Release date: 2026-04-19

## Scope

`v0.19.0` makes the DuckDB detail store optional and changes the default analyze behavior to skip it.

The default `binlogviz analyze` now uses the bounded streaming report path without creating a DuckDB detail store. This removes the temp-store creation, write, and cleanup overhead from the default case while keeping `--detail-store duckdb` available as an experimental/debug compatibility backend.

## Highlights

- changed the default `--detail-store` from `duckdb` to `none`
- `none` mode does not create a DuckDB database, does not call `ResolveTransactionQuerySQL`, and does not write to disk
- `--detail-store duckdb` remains available as an experimental/debug compatibility backend
- JSON output is identical between `none` and `duckdb` modes: all 10 top-level report fields match (summary, tables, transactions, patterns, minutes, timeseries, diagnostics, alerts, warnings, pattern_drilldowns)

## Performance Notes

On the local 988 MB MySQL 8.0 ROW binlog sample (`mysql-bin.000009`, 1,036,542,903 bytes), built binary A/B results:

| Metric | `--detail-store none` | `--detail-store duckdb` |
|--------|-----------------------|-------------------------|
| Wall time | 11.3–13.8s | 13.7–14.1s |
| Max RSS | 199–203 MB | 320–323 MB |
| JSON size | 59 KB | 59 KB |
| total_transactions | 588,693 | 588,693 |
| total_rows | 3,571,620 | 3,571,620 |
| total_events | 5,887,186 | 5,887,186 |

Default `none` reduces max RSS by about 38% (~120 MB). Wall time is not materially different; the bottleneck remains in the parser and streaming aggregation path.

These numbers are workload- and machine-dependent. They are intended as a regression baseline, not a universal throughput guarantee.

## Compatibility Notes

- default behavior change: `binlogviz analyze` no longer creates a DuckDB temp store by default
- to restore the previous behavior, pass `--detail-store duckdb`
- no JSON schema changes
- existing snapshots and workflows remain compatible

## Breaking Changes

- default `--detail-store` changed from `duckdb` to `none`; workflows that depended on the DuckDB detail store should pass `--detail-store duckdb` explicitly
