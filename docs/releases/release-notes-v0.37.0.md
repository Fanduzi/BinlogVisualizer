# BinlogViz v0.37.0 Release Notes

Release date: 2026-10-30

## Overview

v0.37.0 is a feature release: `binlogviz analyze` can now stream binlog events directly from a live MySQL or MariaDB server over the replication protocol. This closes the biggest gap with `mysqlbinlog -R`, `binlog2sql`, and `my2sql` — the binlog no longer has to be copied off the host first, and there is no lossy text round trip: raw replication event frames feed the same checked decoder used for file input. `report_version` stays 4, `plan_version` stays 2, and `flashback verify --format json` `schema_version` stays 1.

## Added

- **`analyze --host HOST` — remote replication read (#256, ADR 0021).** BinlogViz registers as a pseudo-replica (go-mysql `BinlogSyncer`, pure Go — `CGO_ENABLED=0` still builds) and streams a bounded slice of the server's binlog:
  - Connection flags: `--port` (3306), `--user` (OS login default), `--password`, `--server-id` (`0` = cryptographically random nonzero pseudo-replica id).
  - Start positioning: `--start-file`/`--start-pos` (file must be in the probed manifest; position defaults to `4` and may not exceed the probed size) or `--start-gtid` ("the set already consumed" — the stream begins at the first transaction after it; a bare `uuid:N` expands to `uuid:1-N`). With no start selector the dump covers the server's whole retained history.
  - The GTID set's format selects the wire flavor — MySQL `uuid:intervals` versus MariaDB `domain-server-seq` — so no `--flavor` flag exists.
  - `--ssl-mode` is `preferred` by default (TLS when the server offers it, like the `mysql` client): `preferred` falls back to plain only when the server refuses TLS at handshake, before credentials are sent; `required` encrypts without verifying the certificate (MySQL `REQUIRED` semantics) or fails; `disabled` never attempts TLS.
  - The password comes from `--password` or `BINLOGVIZ_MYSQL_PASSWORD` (the flag wins; the variable keeps it out of the command line) and is never logged or printed.
- **Bounded reads by construction.** At connect time the client snapshots `SHOW BINARY LOGS`; the last listed file and its size bound the read. Events written after the probe, and a server rotation past the probed list, are excluded rather than racing the report — a report always terminates at a defined point. When the final event lands exactly on the manifest boundary it is emitted and the stream stops cleanly without waiting for a frame that would never come.
- **Fail-loud stream errors.** A dropped connection mid-read fails `input` (`reason: stream`) instead of producing a silently short report. Remote failures carry typed reasons — `connect`, `tls`, `auth`, `manifest`, `privilege`, `start_file`, `start_pos`, `stream` — mapped into the existing `clierr` taxonomy for both locales.
- **Mutual exclusion.** Remote flags cannot mix with positional files, `-`, `--from-dir`, `--prefix`, `--index-file`, or the local `--start-position`/`--stop-position` selectors (`usage`); remote flags without `--host` are `usage`; `--start-gtid` with `--start-file`/`--start-pos` is `usage`.
- **Report and snapshot integration.** Coverage lines label sources `remote://host:port/<name>`; files before `--start-file` are reported as skipped. Snapshots record `input_mode: "remote"`.

## Architecture

- **`internal/remote`** — connection config, manifest probe, GTID-set parsing/flavor detection, TLS policy, and the `remote.Error` reason taxonomy behind `Source.Next`/`Manifest`/`Close`.
- **`internal/binlog/remote.go`** — `FrameSource` seam and `NewRemoteParser`: complete replication event frames go through the existing checked event decode, so remote and file input share one code path downstream.
- **`cmd/binlogviz`** — `analyze_remote.go` wires remote flags into the input-mode selector (`analyze_options.go`), with flag registration extracted to `analyze_flags.go`.

## Tests and CI

- Unit coverage for every validation rule in the mutual-exclusion matrix, GTID parsing (MySQL and MariaDB forms), TLS-mode mapping, password precedence (flag over env), and remote error → `clierr` classification — all via a replaceable `remoteOpenSource` seam with no network access.
- Remote-parser fixtures cover rotate events, boundary overshoot, and the exact-boundary case (a `hangingSource` panics if `Next` is called after the boundary frame — the exact CI hang this release's development caught).
- `TestAnalyzeRemoteMySQL80` runs in the `flashback e2e` job against real MySQL 8.0: creates a replication user, runs a workload, and checks a remote analyze end to end, plus `--start-file`, absent-start-file, and wrong-password error shapes.

## Verification

- **CI**: PR #457 run 40921665686 (ci: verify, vet and race, darwin verify, flashback e2e, difftest, landing overflow, release-prep self-test) and run 40921665414 (lint: golangci-lint, codehealth) are green on the candidate; the rebase merge to main is `c6140a3..c1a665e`, and the main-push runs 40924667089 (ci) and 40924667080 (lint) are green on that tree.
- **Local**: `go build ./...`, `CGO_ENABLED=0 go build ./...`, `go test -count=1 ./...` (25 packages), `go vet ./...`, `make codehealth-compare`, and `check_three_level_doc.sh` all pass.

## Compatibility

- `report_version` 4, `plan_version` 2, `schema_version` 1 unchanged; the only new JSON value is `input_mode: "remote"` in snapshot metadata.
- All remote flags are additive; existing file/stdin/`--from-dir` modes are unchanged. `--ssl-mode` defaults to `preferred` — matching `mysql` client behavior — so default remote connections attempt TLS and only fall back if the server refuses it at handshake.
- Required server privileges: `REPLICATION SLAVE` and `REPLICATION CLIENT`.

## Known issues / out of scope

- Remote input serves `analyze` only for now — remote `flashback`, `--follow` live tailing, reconnect/resume, SSH tunnels, compressed replication protocol, and certificate-verifying TLS modes are documented non-goals of this release (see ADR 0021).
- **#256 (P3, roadmap)** remains open: remote read is done here; forward/redo SQL, CSV/TSV statistics, per-table/transaction split output, row-level filters, statement fingerprinting, and relay-log index reading remain.
