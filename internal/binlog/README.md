# Binlog Module

Binlog parsing, raw event extraction, normalization, and parse-progress contracts.

## Files

| File | Responsibility |
|------|----------------|
| `types.go` | Defines `RawEvent`, `Parser`, `ParseProgress`, and the optional `ProgressParser` contract used by the command layer. |
| `parser.go` | Wraps `go-mysql-org/go-mysql/replication`, extracts raw binlog events, and emits monotonic per-input progress offsets. |
| `normalize.go` | Converts parser-emitted `RawEvent` values into stable analyzer-facing normalized events. |
| `*_test.go` | Covers parser construction, helper behavior, and normalization semantics. |
| `testdata/*` | Real binlog fixtures used by integration and regression tests. |

## Exports

- `type RawEvent`
- `type Parser`
- `type ParseProgress`
- `type ProgressParser`
- `func NewParser() Parser`
- `func NormalizeRawEvent(RawEvent) (*model.NormalizedEvent, error)`

## Dependencies

- Upstream:
  - `github.com/go-mysql-org/go-mysql/replication` for binlog parsing
  - `internal/model` for normalized-event output types
- Downstream:
  - `cmd/binlogviz` consumes parser/progress contracts and normalization
  - `internal/analyzer` consumes normalized events produced through this module

## Update Rule

If members, interfaces, or dependencies change, update this file in the same change.
