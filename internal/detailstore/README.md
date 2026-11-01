# Detail store

Optional analysis-detail persistence (ADR 0012). `internal/analyzer` writes through its narrow `detailStore` interface and sees `detailstore.Transaction` records; `cmd` injects `*detailstore.DuckDBStore` via `analyzer.NewWithStore` only when `--detail-store duckdb` is set and CGO is enabled. This package is a leaf: it imports `internal/model` and go-duckdb only — never `internal/analyzer`.

## Members

| File | Responsibility |
|------|----------------|
| `types.go` | `Transaction` (the record the analyzer writes, fields mirror the DuckDB schema), `ErrDuckDBRequiresCGO`, the batch flush thresholds, and the row DTOs plus sizing helpers shared by the adapter. |
| `store_duckdb.go` | CGO DuckDB adapter: `NewDuckDBStore`, batch append with flush and close error propagation, and query helpers; the alerts table keeps each alert's GTID. |
| `store_nocgo.go` | `!cgo` stub so `NewDuckDBStore` fails with `ErrDuckDBRequiresCGO` without importing go-duckdb. |
| `store_test.go` | Adapter coverage: schema initialization, batch flushing at row and byte thresholds, bounded SQL hydration, GTID round-trip, and benchmarks. |

## Interfaces

- `NewDuckDBStore(path string, batchRows int) (*DuckDBStore, error)` opens the optional detail store; without CGO it returns `ErrDuckDBRequiresCGO`.
- `Transaction` is the record passed from the analyzer to the store.
- `DuckDBStore` records transactions, minute buckets, and alerts; flushes batches; queries stored details; and supports reset and close.

## Dependencies

- Upstream: `internal/model` and the DuckDB driver when CGO is enabled.
- Downstream: `internal/analyzer` uses the store interface and `cmd/binlogviz` injects the adapter.

## Update Rule

If members, interfaces, or dependencies change, update this file in the same change.
