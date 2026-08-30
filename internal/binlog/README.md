# Binlog Module

Binlog parsing, raw event extraction, normalization, and parse-progress contracts.

## Files

| File | Responsibility |
|------|----------------|
| `types.go` | Defines `RawEvent` with optional server/version/flavor, GTID, Query thread/actor, transaction/XA XID, SQL, and location evidence plus parser/progress contracts. |
| `parser.go` | Wraps `go-mysql-org/go-mysql/replication`, extracts event-header server ID, propagated FormatDescription version/flavor, MySQL/MariaDB GTID, Query thread and best-effort invoker, decimal XID, physical MariaDB XA PREPARE identity, row annotations, table names, positions, and progress. |
| `normalize.go` | Preserves raw provenance while converting supported events into stable analyzer kinds, including GTID, MariaDB `XAPrepareLogEvent` boundaries, XA queries, Query/AnnotateRows LOAD DATA context, bounded SQL, row intent, and Query DDL. |
| `format.go` | Cheap Query-DML vs ROW-image observation used to guess STATEMENT/MIXED/ROW, capture Format Description server version, and warn when only row images are counted. |
| `probe.go` | Scans binlog files for reusable file-level metadata such as size and chronological earliest/latest non-zero event timestamps, with internal parser-injectable helpers for reuse in tests and later planning work. |
| `*_test.go` | Covers parser construction, helper behavior, normalization semantics, and real-fixture parser benchmarks isolating parse-only, parse+normalize, and parse+progress layers. |
| `testdata/*` | Real binlog fixtures used by integration and regression tests. |

## Exports

- `type RawEvent` — Raw parser event with optional producer/transaction provenance; zero/empty identity is unknown.
- `type Parser`
- `type ParseProgress`
- `type ProgressParser`
- `type FileProbe`
- `type FormatObserver`
- `func NewParser() Parser`
- `func NormalizeRawEvent(RawEvent) (*model.NormalizedEvent, error)` — Preserves available provenance and bounds SQL to 4096 UTF-8-safe bytes.
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
