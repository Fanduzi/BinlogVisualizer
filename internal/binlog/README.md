# Binlog Module

Binlog parsing, raw event extraction, normalization, and parse-progress contracts.

## Files

| File | Responsibility |
|------|----------------|
| `types.go` | Defines `RawEvent`, `Parser`, `ParseProgress`, and the optional `ProgressParser` contract used by the command layer. |
| `parser.go` | Wraps `go-mysql-org/go-mysql/replication`, extracts raw binlog events, reuses `TableID` table names in the rows-event hot path, and emits monotonic per-input progress offsets. |
| `normalize.go` | Converts parser-emitted `RawEvent` values into stable analyzer-facing normalized events, including a destination-reuse fast path for streaming callers. Uses first-character dispatch to reduce string comparison overhead in the hot normalization path. |
| `format.go` | Cheap Query-DML vs ROW-image observation used to guess STATEMENT/MIXED/ROW and warn when only row images are counted. |
| `probe.go` | Scans binlog files for reusable file-level metadata such as size and chronological earliest/latest non-zero event timestamps, with internal parser-injectable helpers for reuse in tests and later planning work. |
| `*_test.go` | Covers parser construction, helper behavior, normalization semantics, and real-fixture parser benchmarks isolating parse-only, parse+normalize, and parse+progress layers. |
| `testdata/*` | Real binlog fixtures used by integration and regression tests. |

## Exports

- `type RawEvent`
- `type Parser`
- `type ParseProgress`
- `type ProgressParser`
- `type FileProbe`
- `type FormatObserver`
- `func NewParser() Parser`
- `func NormalizeRawEvent(RawEvent) (*model.NormalizedEvent, error)`
- `func NormalizeRawEventInto(RawEvent, *model.NormalizedEvent) (bool, error)`
- `func ProbeFile(path string) (FileProbe, error)`
- `func ProbeFiles(paths []string) ([]FileProbe, error)`

## Dependencies

- Upstream:
  - `github.com/go-mysql-org/go-mysql/replication` for binlog parsing
  - `internal/model` for normalized-event output types
- Downstream:
  - `cmd/binlogviz` consumes parser/progress contracts and normalization
  - probe helpers rely on parser implementations populating `RawEvent.BinlogPath`
  - `internal/analyzer` consumes normalized events produced through this module

## Update Rule

If members, interfaces, or dependencies change, update this file in the same change.
