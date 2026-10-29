# Remote Module

Replication-protocol client for remote binlog input (ADR 0021): a pseudo-replica `COM_BINLOG_DUMP`/`COM_BINLOG_DUMP_GTID` session over `go-mysql`'s `BinlogSyncer`, bounded to the `SHOW BINARY LOGS` manifest taken at connect time.

## Files

| File | Responsibility |
|------|----------------|
| `remote.go` | Defines `Config` (host/port/user/password, server-id, SSL mode, file:pos or GTID start) and `Source` (the live dump). `Open` connects, runs the manifest probe inside go-mysql's `Option` hook — post-auth, pre-dump, on the dump connection itself — validates the start point against the probed manifest, and starts the dump in raw mode with retries disabled and logging silenced. `Source.Next` returns one whole event frame per call (the same bytes a binlog file stores), the exact `io.EOF` sentinel only on a clean close, and a `*remote.Error` for anything else. `ssl-mode preferred` retries plain only when the server reports it cannot do TLS at all — before credentials are sent; no other failure falls back. `NormalizeHost` splits a `host:port` form. `randomServerID` draws the pseudo-replica id cryptographically; a collision is the server's error to report. |
| `manifest.go` | `SHOW BINARY LOGS` probe: the ordered `(name, size)` manifest whose last row is the bounded-read end; needs `REPLICATION CLIENT`. `validateStart` fails `--start-file` absent from the manifest (`start_file`) and `--start-pos` past its probed size (`start_pos`) before the dump starts. An empty start file means the server's first binlog, as an empty `MASTER_LOG_FILE` does for a real replica. |
| `gtid.go` | `ParseStartGTID`: tries the MySQL `uuid:intervals` form, then the MariaDB `domain-server-seq` form; the format that parses selects the protocol flavor (`mysql` → `COM_BINLOG_DUMP_GTID`, `mariadb` → `SET @slave_connect_state` + dump). The value means "the set already consumed"; a bare `uuid:N` expands to `uuid:1-N`. |
| `errors.go` | `Error{Reason}` taxonomy and `classifyOpenError`: `connect`, `tls`, `auth`, `manifest`, `privilege`, `start_file`, `start_pos`, `stream`. Command layer maps them to `clierr` kinds and `details.reason`. |
| `*_test.go` | Flag-value parsers, manifest/start validation, error classification, and the `BinlogStreamer` seam (hand-fed frames, clean close, stream EOF vs wrapped EOF). No test needs a server. |

## Exports

- `type Config` — one remote input request; `Password` is never printed (go-mysql clears it before logging config; the syncer logger is discarded anyway).
- `func Open(ctx, Config) (*Source, error)` — probe, validate, dump.
- `type Source` — `Next(ctx) ([]byte, error)`, `Manifest() []LogFile`, `ServerVersion() string`, `Close()`. Satisfies `binlog.FrameSource`.
- `type LogFile` — one manifest row.
- `type StartPos` — `File` (empty = first binlog), `Pos`.
- `type SSLMode` — `disabled`, `preferred`, `required` (`ParseSSLMode` validates).
- `func ParseStartGTID(string) (mysql.GTIDSet, flavor string, error)`
- `func NormalizeHost(host string, port uint16, portSet bool) (string, uint16, error)`
- `type Error` / `type Reason` — typed failures for the command layer.

## Dependencies

- Upstream:
  - `github.com/go-mysql-org/go-mysql` (`client`, `mysql`, `replication`) — protocol client; no new runtime dependency.
  - Standard library only otherwise; `clierr` stays in `cmd`.
- Downstream:
  - `cmd/binlogviz/analyze_remote.go` builds `Config`, maps `Error` to kinds.
  - `internal/binlog` consumes `Source` through the `FrameSource` seam.

## Update Rule

If members, interfaces, or dependencies change, update this file in the same change.
