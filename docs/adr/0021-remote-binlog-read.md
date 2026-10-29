# ADR 0021: Remote binlog read for analyze

Status: accepted (2026-10-29). Roadmap #256 top item: the `mysqlbinlog -R` parity gap.

## Context

`analyze` reads binlog bytes from three places today: positional file paths, stdin `-`, and `--from-dir` discovery (`--prefix` or `--index-file`). All three end as files the `internal/binlog` parser reads by seeking. Operators who cannot copy the files — managed databases, hosts they may not stage on, space or permission limits — have to run `mysqlbinlog -R` first and feed the output back. That output is text, not a binlog, so the round trip loses the byte-level checks `event_reader.go` applies and costs a decode/re-encode step.

A replication stream is the same event byte stream a file holds. A pseudo-replica client (`COM_BINLOG_DUMP`, `COM_BINLOG_DUMP_GTID`) can feed the existing `RawEvent` pipeline with no parser change. The repo already depends on `github.com/go-mysql-org/go-mysql` for `replication.BinlogParser`; the same module ships `replication.BinlogSyncer`, a pure-Go sync client, so the protocol client needs no new runtime dependency and `CGO_ENABLED=0` still builds.

Constraints that shaped the decisions:

- `BinlogSyncer` in raw mode still hands every event's full bytes (`BinlogEvent.RawData`) to the consumer, so the file path's integrity checks (CRC32 with the in-use flag masked, allocation bounds, transaction-payload streaming) can gate network bytes exactly as they gate file bytes.
- In raw mode the syncer decodes only the Format Description and Rotate events it needs for its own bookkeeping; every other event reaches us undecoded.
- `BINLOG_DUMP_NON_BLOCK` does not give a usable end signal here: the server sends an EOF packet at end of binlog and go-mysql logs and swallows it, then blocks on the next read until the server closes the connection. A mid-stream network drop looks identical, so mapping that error to "clean end" would silently truncate an analysis.
- `SHOW BINARY LOGS` needs `REPLICATION CLIENT` and the dump needs `REPLICATION SLAVE`. Both are normal grants on a replication account.
- The `Option func(*client.Conn)` hook runs between connect/auth and the dump command, so the manifest probe rides on the dump connection with no second connect.
- MariaDB speaks `COM_BINLOG_DUMP` for file:pos but GTID start differs: MySQL encodes the executed set in `COM_BINLOG_DUMP_GTID`; MariaDB wants `SET @slave_connect_state` then a plain dump. go-mysql picks between them from `cfg.Flavor`, which must be known before connecting.

## Decision

### Input mode and flag surface

Remote is a fourth `analyze` input mode, engaged by `--host`. New flags:

| Flag | Default | Meaning |
|---|---|---|
| `--host` | none | Server host or `host:port` target; its presence selects remote mode |
| `--port` | `3306` | Server port, unless `--host` already carried one |
| `--user` | OS login name (mysql client convention) | Account used to connect |
| `--password` | none | Account password. `BINLOGVIZ_MYSQL_PASSWORD` fills it when the flag is absent; the flag wins when both are set (explicit beats ambient). Never logged or printed |
| `--server-id` | `0` = random | The pseudo-replica `server_id`; when unset, a crypto-random value in `[1, 2^32-1]` |
| `--start-file` | none | Binlog file name on the server to begin dumping from |
| `--start-pos` | `4` | Byte position in `--start-file`; requires `--start-file` |
| `--start-gtid` | none | Executed GTID set to resume after; mutually exclusive with `--start-file`/`--start-pos` |
| `--ssl-mode` | `preferred` | `disabled`, `preferred`, or `required` (MySQL vocabulary) |

No `--stop-at-end` or `--follow` flag exists: a flag whose only legal value is its default is noise, and bounded-to-end is the only mode analyze has. `--follow` arrives with its own flag when a consumer (flashback planning, continuous watch) needs it — it changes termination, reconnect, and memory semantics, none of which are free. `--stop-position` is not borrowed for remote: it is an exact-boundary file selector and remote positions are server coordinates the operator supplies through `--start-pos`.

### Termination: bounded read to the connect-time end

Remote analyze reads up to the end of binlog **as it stood when the connection opened**, then stops:

1. Inside the `Option` hook (post-auth, pre-dump) the client runs `SHOW BINARY LOGS` once. The result is the manifest: the ordered `(name, size)` list; the last row is the boundary `(endFile, endSize)`.
2. The dump is issued in normal blocking mode. Events are emitted until one of three stop conditions, checked before an event reaches the handler:
   - the event sits in `endFile` and its `end_log_pos` exceeds `endSize` — data written after the probe; stop without emitting it;
   - a ROTATE event names a file beyond `endFile` (or absent from the manifest and sorting after it) — the server rotated since the probe; stop;
   - the stream errors or closes.
3. `--start-file` must be present in the manifest and `--start-pos` must not exceed its probed size — both checked before the dump starts, so a purged file or a stale position fails as `input`/`usage` instead of a server error the operator has to decode. With no start selector, the dump begins at the first manifest file, position 4 — the whole retained history; the flag docs say so.

This trades one cheap query for a deterministic boundary: no dependence on EOF-packet timing, no confusing a network drop with end-of-binlog, and events written during the read are simply excluded rather than racing the report.

### GTID vs file:pos start, and MariaDB

`--start-file`/`--start-pos` issues `COM_BINLOG_DUMP`. `--start-gtid` parses the value with `mysql.ParseMysqlGTIDSet`; on failure `ParseMariadbGTIDSet` is tried. The format that parses selects `Flavor` (`mysql` → `COM_BINLOG_DUMP_GTID`, `mariadb` → `@slave_connect_state` + `COM_BINLOG_DUMP`) and an unparseable value is a `usage` error. The value means "the set already consumed": the stream begins at the first transaction after it. A bare `uuid:N` — one SID, one point — is expanded to `uuid:1-N`, matching the operator meaning "start after GTID N"; anything else is sent literally.

No `--flavor` flag exists: for file:pos the dump command is flavor-identical (go-mysql's `checkFlavor` warning is not user-facing here and the MariaDB event classes are decoded by event type either way), and for GTID the set's own format is unambiguous.

### Server-id and reconnect policy

`--server-id` accepts the full nonzero `uint32` range. Unset, it is derived crypto-randomly in `[1, 2^32-1]` per run. We do not probe for collisions: a collision is the server's error to report (`ER_SLAVE_*` class) and it surfaces as an `input` failure naming the cause.

`DisableRetrySync` is set: a bounded one-shot read fails loudly on a broken connection rather than silently resuming at an approximate position. Reconnect/resume is a later feature with its own correctness design (re-dump from a tracked position), not a config toggle here.

### Auth, TLS, secrets

Authentication is delegated to go-mysql (`mysql_native_password`, `caching_sha2_password`, `auth_socket` is not supported over TCP — the same as every Go client). `--ssl-mode` maps:

- `preferred` — the default, matching the `mysql` client and `mysqlbinlog`, which both default to PREFERRED: connect as `required`; if, and only if, the server refuses TLS at handshake (go-mysql's "server does not support TLS" capability error, before credentials are sent), reconnect plain. Auth or network failures never fall back — that would downgrade the channel after credentials were sent. Replication traffic is sensitive data, so TLS-when-available is the right default posture, and the pre-auth-only fallback keeps it safe;
- `required` — `tls.Config{InsecureSkipVerify: true}`: MySQL `REQUIRED` encrypts without verifying, matching upstream semantics;
- `disabled` — no TLS (`TLSConfig` nil).

`verify_ca`/`verify_identity` and the certificate flags they need are out of scope for this change. The password comes from `--password` or `BINLOGVIZ_MYSQL_PASSWORD`; go-mysql's own config logging clears it, and BinlogViz never prints it in errors, help, or logs.

### Error taxonomy (no new kinds)

- Flag combination and value problems → `usage` (exit 3): remote flags without `--host`, remote combined with positional args/stdin `-`/`--from-dir`/`--prefix`/`--index-file`/`--start-position`/`--stop-position`, `--start-pos` without `--start-file`, `--start-file`/`--start-pos` with `--start-gtid`, a `--host` with an empty host (`:3306`) or empty port (`db:`) half, an invalid `--ssl-mode`/`--start-gtid`/`--port`/`--server-id` value, `--start-pos` past the probed file end.
- Connect, TLS negotiation, auth, manifest probe, privilege, and mid-stream failures → `input` (exit 4), with `details.reason` distinguishing `connect`, `tls`, `auth`, `manifest`, `privilege`, `start_file`, and `stream` for scripts that want the fix hint machine-readably. `start_file` also covers the server refusing the dump with `ER_MASTER_FATAL_ERROR_READING_BINLOG` (1236) — a purged binlog name or GTIDs it no longer retains; `start_pos` stays `usage`.
- Corrupt event bytes on the wire → `corrupt` (exit 5) through the unchanged `CorruptEventError` path — the same verification guards the stream.
- A stream that yields no events falls through the existing no-data guards exactly like an empty-window file run (exit 2); it does not pretend a file was missing.

### Package layout (ADR 0012 direction)

- `internal/remote` — leaf protocol client: `Config`, `Open(ctx, Config) → *Source` (handshake, manifest probe, dump issue), `Source.Next(ctx) → []byte` event frames, `Source.Manifest()`, `Source.Close()`, `ParseStartGTID`, SSL-mode mapping, and typed `Error{Reason}` values. It imports go-mysql and the standard library only; `clierr` kinds stay in `cmd`.
- `internal/binlog` — new `remote.go`: `NewRemoteParser(ctx, src FrameSource, boundary RemoteBoundary) Parser`. `FrameSource` (defined here) is the mockable seam — `Next(ctx) ([]byte, error)` — which `remote.Source` satisfies. The parser tracks the current binlog file from ROTATE events, stamps `BinlogPath` as `remote://host:port/<file>`, resets table maps on rotation, enforces the boundary, and pushes every frame through the same checked decode as files.
- `cmd/binlogviz` — `analyze_remote.go`: flag registration, mode detection, mutual exclusion, option validation, connect/dump wiring into the existing `runAnalysisStreaming...` path with the remote parser injected, and `remote.Error` → `clierr` mapping.

The analyze pipeline is untouched: remote injects one parser implementation and one synthetic path entry (`remote://host:port`), so progress, coverage, snapshot metadata (`input_mode: remote`), and report rendering flow unchanged.

## Considered options

- **`BINLOG_DUMP_NON_BLOCK` for stop-at-end.** Rejected: go-mysql swallows the EOF packet and the server then closes the connection; the resulting read error is indistinguishable from a network drop, which would truncate the analysis silently.
- **Time-based "end" detection** (stop when no event arrives for N seconds). Rejected: a stall is not an end marker; it confuses a quiet server with a finished one.
- **Vendoring a hand-rolled protocol client** instead of go-mysql's syncer. Rejected: the module is already a direct dependency, pure Go, and battle-tested on auth/TLS/packet plumbing; the vendored surface would duplicate `client.Conn` for no gain.
- **`--stop-at-end`/`--follow` flags now.** Rejected for this change; see flag surface. Analyze is bounded by definition, and follow mode needs reconnection and memory decisions that this ADR deliberately does not make.
- **`--flavor` flag.** Rejected: file:pos is flavor-agnostic and GTID start derives flavor from the set's format. A flag would only let operators pick a wrong value.
- **Remote in `internal/binlog` end to end** (client inside the parser package). Rejected by ADR 0012's leaf direction: protocol plumbing and event dispatch are different modules; the `FrameSource` seam keeps the parser network-free and unit-testable.
- **Reuse `--start-position`/`--stop-position` for remote.** Rejected: they are exact-boundary file selectors validated against a local file size; remote positions are server coordinates with manifest semantics. Sharing a flag would blur both contracts.

## Consequences

- `analyze` gains nine remote flags and a fourth input mode; positional/discovery behavior is unchanged.
- The server account needs `REPLICATION SLAVE` plus `REPLICATION CLIENT` (`SHOW BINARY LOGS`); a missing grant fails `input` with `reason: privilege` naming it.
- A remote run reports the binlog as `remote://host:port/<file>` on events and `remote://host:port` as the resolved input; snapshots record `input_mode: remote`.
- Every remote event passes the file path's byte checks before decode — a hostile or corrupted stream meets `corrupt`, not a decoder panic.
- Unit tests cover the protocol boundary via the `FrameSource` seam and `BinlogStreamer.AddEventToStreamer`; no test needs a server. The live-server path is exercised by an env-gated e2e in the `BINLOGVIZ_FLASHBACK_E2E` pattern.
- Not in this change: `--follow`, reconnect/resume, certificate-verification TLS modes, `flashback` remote input, SSH tunnels, compressed replication protocol. Each is a deliberate follow-up, not an accident.
