# Detail store

Optional analysis-detail persistence (ADR 0012). `internal/analyzer` writes through its narrow `detailStore` interface and sees `detailstore.Transaction` records; `cmd` injects `*detailstore.DuckDBStore` via `analyzer.NewWithStore` only when `--detail-store duckdb` is set and CGO is enabled. This package is a leaf: it imports `internal/model` and go-duckdb only — never `internal/analyzer`.

## Members

| File | Responsibility |
|------|----------------|
| `types.go` | `Transaction` (the record the analyzer writes, fields mirror the DuckDB schema), `ErrDuckDBRequiresCGO`, the batch flush thresholds, and the row DTOs plus sizing helpers shared by the adapter. |
| `store_duckdb.go` | CGO DuckDB adapter: `NewDuckDBStore`, batch append, and query helpers; the alerts table keeps each alert's GTID. |
| `store_nocgo.go` | `!cgo` stub so `NewDuckDBStore` fails with `ErrDuckDBRequiresCGO` without importing go-duckdb. |
| `store_test.go` | Adapter coverage: schema initialization, batch flushing at row and byte thresholds, bounded SQL hydration, GTID round-trip, and benchmarks. |
