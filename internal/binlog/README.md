# Binlog Module

Binlog parsing, raw event extraction, normalization, and parse-progress contracts.

## Files

| File | Responsibility |
|------|----------------|
| `types.go` | Defines `RawEvent` with optional server/version/flavor, GTID, MySQL 8 commit timestamps, Query thread/actor, transaction/XA XID, SQL, primary-key presence, and location evidence plus parser/progress contracts. |
| `event_kind.go` | Maps go-mysql `EventType` enums onto one RawEvent kind (`QUERY`, `WRITE_ROWS`, `GTID`, `XA_PREPARE`, …) so `String()` spellings never reach normalize. Partial-update and MariaDB compressed row events map onto the existing ROW-image kinds; anonymous GTID maps to `GTID`. A transaction payload wrapper has no kind. |
| `parser.go` | Wraps `go-mysql-org/go-mysql/replication` through one parse loop (offset and progress are options), expands a decoded transaction payload into inner events before the handler, stamps those inners with the wrapper's file-relative span once (inner uncompressed `LogPos` is not a file offset; `BinlogBytes` is charged once), extracts event-header server ID, propagated FormatDescription version/flavor, MySQL/MariaDB GTID (anonymous identity stays empty) plus MySQL 8 commit timestamps when both are non-zero, Query thread and best-effort invoker, decimal XID, physical MariaDB XA PREPARE identity, row annotations, table names, primary-key presence (`has_pk`, `no_pk`, or `unknown`) from FULL TABLE_MAP metadata, positions, and progress. `TIMESTAMP` row values, including cells inside a compressed transaction payload, are the UTC wall clock of the stored instant, including fractional seconds, and do not follow the process zone. `DATETIME` stays the wall clock stored in the binlog. A full-file parse that leaves unread bytes after the last complete event returns an error instead of a clean EOF. Row-image capture stays off unless `SetCaptureRowImages` is set. Flashback capture stays off unless `SetCaptureFlashback` is set; only that path keeps the binary JSON document beside the decoded row, including JSON inside a transaction payload. |
| `rows.go` | Formats bounded cell values from an already-decoded rows event when capture is on: NULL, signed/unsigned integers (MEDIUMINT unsigned is 24 bits), decimals in full, BIT as an unsigned integer, FLOAT/DOUBLE decimals that cast back to the same bits (`<FLOAT>` / `<DOUBLE>` when the decimal is outside MySQL's range), strings, datetimes, JSON, and hex blobs. Also formats primary-key text from FULL metadata column names. |
| `signedness.go` | Reads the TABLE_MAP SIGNEDNESS bitmap the way MySQL writes it (`has_signedess_information_type`, including YEAR and old DECIMAL). go-mysql v1.14 `UnsignedMap` skips YEAR, so a later numeric column would take its neighbour's bit. |
| `numeric.go` | Turns a decoded integer or BIT into the value MySQL stored. MEDIUMINT is 24 bits inside go-mysql's sign-extended int32, so the unsigned reading is the low 24 bits. BIT(64) with the high bit set is the uint64 of that int64. |
| `flashback.go` | Formats exact SQL literals from the same decoded rows event when flashback capture is on. JSON comes from the binary document. Non-utf8mb4 character columns use a charset introducer and hex bytes. ENUM is the member index and SET is the bitmask. MEDIUMINT unsigned is the low 24 bits. BIT is a `b'...'` literal of the column width. Each row keeps TABLE_MAP column metadata for a schema-file check, and marks ENUM index 0. Signedness comes from `signedness.go`. Refuses missing names, skipped columns, and types that cannot be rendered exactly. |
| `flashback_json.go` | Renders one binary JSON document as SQL that stores the same bytes, or refuses the value. |
| `flashback_walk.go` | Copies JSON documents out of the row image while flashback capture is on, including a transaction-payload inner event whose parser did not keep the outer rows hook. A walk that does not finish leaves the value to be refused. |
| `flashback_charset.go` | Maps MySQL 8.0 collation ids to charset names. An id that is not in the map is refused. |
| `normalize.go` | Classifies canonical RawEvent kinds into analyzer events: Query SQL (BEGIN/COMMIT/plain ROLLBACK/XA/DDL including SET PASSWORD/LOAD DATA, independent ADMIN: ANALYZE TABLE, OPTIMIZE TABLE, FLUSH PRIVILEGES, SET DEFAULT ROLE, exact FLUSH TABLES, CHECK TABLE prefix, SET ROLE prefix, Unclassified QUERY with bounded SQL, and dropped Ignored QUERY / Query-DML), ROW images, GTID, XID, XA PREPARE, table map, and bounded row-annotation SQL. |
| `format.go` | Cheap Query-DML vs ROW-image observation used to guess STATEMENT/MIXED/ROW, count Ignored QUERY, ADMIN QUERY, and unmapped kinds, capture Format Description server version, and warn when only row images are counted. |
| `probe.go` | Scans binlog files for reusable file-level metadata such as size and chronological earliest/latest non-zero event timestamps, with internal parser-injectable helpers for reuse in tests and later planning work. |
| `*_test.go` | Covers canonical event kinds, parser construction, helper behavior, transaction-payload expand, ParseFiles admission that compressed-payload inners share the wrapper file span once, UTC TIMESTAMP cells inside a payload, normalization semantics including ADMIN (exact FLUSH TABLES, CHECK TABLE prefix, SET ROLE prefix, not FLUSH TABLES WITH READ LOCK), Unclassified QUERY emission, Ignored QUERY skip/count, and real-fixture parser benchmarks isolating parse-only, parse+normalize, and parse+progress layers. |
| `testdata/*` | Real binlog fixtures used by integration and regression tests, including the MySQL 8.0.46 ROW+GTID `FLUSH TABLES`, `CHECK TABLE`, and `SET ROLE` dialect fixtures, the MySQL 8.0.46 open BEGIN+DML fixture (first XID removed so a later GTID meets the open group), the MySQL 8.0.46 committed multi-second duration fixture, the MySQL 8.0.46 source and replica apply-delay pair, the MySQL 8.0.46 hot-row FULL and MINIMAL pair, the MySQL 8.0.46 flashback FULL and MINIMAL-image pair, and the MySQL 8.0.36 compressed transaction-payload fixture, each with a regen script. |

## Exports

- `type RawEvent` — Raw parser event with optional producer/transaction provenance; zero/empty identity is unknown. `EventType` is a canonical kind: `QUERY`, `WRITE_ROWS`, `UPDATE_ROWS`, `DELETE_ROWS`, `ROWS_QUERY`, `GTID`, `XID`, `XA_PREPARE`, `TABLE_MAP`, `FORMAT_DESCRIPTION` (empty means unmapped). A successfully expanded transaction payload is omitted; only its inner events are emitted, each with the wrapper's file-relative `[PositionStart, PositionEnd)` and with `BinlogBytes` charged once.
- `type Parser`
- `type ParseProgress`
- `type ProgressParser`
- `type FileProbe`
- `type FormatObserver`
- `func NewParser() Parser`
- `func NormalizeRawEvent(RawEvent) (*model.NormalizedEvent, error)` — Preserves available provenance and bounds SQL to 4096 UTF-8-safe bytes. Independent management QUERY becomes `ADMIN`, not `DDL`. Other non-Ignored QUERY becomes `UNCLASSIFIED_QUERY`.
- `func NormalizeRawEventInto(RawEvent, *model.NormalizedEvent) (bool, error)`
- `func IsIgnoredQuery(query string) bool` — Session-prefix `SET` that is not `SET ROLE`, `SET DEFAULT ROLE`, or `SET PASSWORD`.
- `func IsIgnoredQueryEvent(RawEvent) bool` — QUERY event whose SQL is Ignored QUERY. It is still dropped and does not close a group.
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
