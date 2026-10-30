# ADR 0023: Remote binlog read for flashback

Status: accepted (2026-10-31). Roadmap #256: the undo side of the remote-input triangle — `analyze` and `redo` read a live replication stream; `flashback` was still file-only.

## Context

`binlogviz flashback` generates undo SQL by running the analyze pipeline with the flashback planner attached: it resolves input paths, streams normalized events through `internal/analyzer`, and renders the retained transaction groups in reverse into a single guarded XA script (`cmd/binlogviz/flashback.go`, `writeFlashbackSQL` in `analyze_finish.go`). Its inputs today are the three local modes — positional files, stdin `-` materialized, and `--from-dir` discovery.

Remote binlog read (ADR 0021) already landed for `analyze`, and `redo` (ADR 0022) took the same seam on top: `openRemoteInput` + `buildRemoteRunConfig` + `validateRemoteArgs` + `analyzeRemoteRequested` in `analyze_remote.go`, `binlog.NewRemoteParser` in `internal/binlog/remote.go`, `remoteFileCoverage`/`printResolvedPaths` in `analyze_paths.go`. `redo.go` shows the plumbing is pure orchestration — inject one parser implementation and one synthetic `remote://host:port` path entry into the unchanged streaming runner. Flashback's only missing piece is that same injection; the undo planner consumes `NormalizedEvent` values and cannot tell file frames from remote frames.

Constraints that shaped the decisions:

- The planner's contract — reverse order, retained groups, conflict hashes, DDL-in-range refusal (ADR 0015), refusal-before-publish (ADR 0014), split-transaction warnings — is input-agnostic. No planner or output-contract change is in scope.
- A remote read is bounded by the connect-time `SHOW BINARY LOGS` manifest (ADR 0021). A transaction may straddle that window: BEGIN before the remote start point, or COMMIT after the manifest end. The undo semantics of a cut group must be defined, not left to accident.
- `flashback`'s local-input `Args` still enforces the pre-`--index-file` contract (`--from-dir` xor `--prefix`); `resolveAnalyzePaths`/`readIndexFilePaths` already implement the current contract (`--from-dir` + exactly one of `--prefix`/`--index-file`) and `redo` already runs it. Remote work is the moment to converge or to record why the divergence stays.
- `flashback verify` consumes a sealed script file and `workflow` drives analyze/compare/trend — neither reads binlog events. Remote flags on them would be dead surface.

## Decision

### Command surface: nine remote flags, same seam

`flashback` registers the identical nine flags `analyze`/`redo` expose — `--host`, `--port`, `--user`, `--password`, `--server-id`, `--start-file`, `--start-pos`, `--start-gtid`, `--ssl-mode` — bound to the same `analyzeOptions` fields and documented through the shared `cmd.analyze.flag.*` i18n keys (wording is identical, so no new keys). Any of them set requests remote mode via `analyzeRemoteRequested`; `validateRemoteArgs` and `buildRemoteRunConfig` validate and normalize exactly as for `analyze`/`redo`:

- Password resolution is unchanged: `--password` wins; otherwise `BINLOGVIZ_MYSQL_PASSWORD` fills it; it is never logged or printed.
- `--ssl-mode` defaults to `preferred` (pre-auth-only fallback to plain, per ADR 0021).
- `--server-id` unset derives a crypto-random value per run.
- The run wires `openRemoteInput` → `binlog.NewRemoteParser` → `remoteFileCoverage` → `printResolvedPaths` → `runAnalysisStreamingFastWithOutput`, mirroring `redo.go` line for line: `defer src.Close()`, `paths := []string{runCfg.Origin}`, `stage = clierr.KindInput` only around the remote half, and `run.remote = true`. `internal/remote` internals and the undo planner are untouched.

### Selection semantics: remote bounds the stream; filters select inside it

The undo scope is the events the bounded stream delivers, intersected with the selection filters:

- `--start-file` + `--start-pos` or `--start-gtid` (mutually exclusive, per ADR 0021) choose where the dump begins; the connect-time manifest end is where it always stops. `--start`/`--end` remain selection filters over the delivered events — they do not move the stream bounds. Both compose: a run may dump from `--start-file` and still keep only rows inside `--start`/`--end`.
- `--start-position`/`--stop-position` stay `usage`-rejected under remote. They are exact-boundary file selectors validated against a local `os.Stat` size (`validateAnalyzeSelectionInput`, `newPositionBoundaryValidator`); a `remote://` origin has no local file to bound. Remote positions are server coordinates the operator supplies through `--start-pos`. ADR 0021 already rejected reusing them for remote; flashback follows.
- Conflict detection and `--plan` are unchanged: the planner's `Observe`/`Note`/`NoteSkipped` seam sees normalized events regardless of source, so GTID/xid conflict hashes and the plan JSON work verbatim. `BinlogPath` is stamped `remote://host:port/<file>` by `eventPath` in `internal/binlog/remote.go`; the `-- binlog:` comments render the binlog basename like they do for file input (`filepath.Base` in `render.go`), so the full remote origin surfaces on the stderr resolved-paths line and in report/snapshot path fields rather than inside the script.

### Local-input contract: converge on the analyze/redo parity

**Decided: yes.** `flashback` `Args` moves to the current contract — positional args mutually exclusive with discovery; `--from-dir` requires exactly one of `--prefix`/`--index-file` — replacing the legacy `--from-dir` xor `--prefix` pair-check:

- The machinery already exists: `resolveAnalyzePaths`/`readIndexFilePaths` implement the whole contract (`cmd/binlogviz/analyze_paths.go`, `index_file.go`), `redo` already validates with it, and the `error.fromDirPrefixOrIndex`/`error.combineArgsWithDir` i18n keys already exist in both locales. Converging is deleting special-casing, not adding code.
- One input contract across `analyze`/`redo`/`flashback` is the documented surface (`docs/reference/cli.md` describes discovery uniformly); keeping `flashback` on the old rule would leave `--index-file` as a mystery rejection on one command.
- Compatibility: every input the old contract accepted is still accepted; the only behavioral delta is that `--from-dir` + `--index-file` now works and the missing-selector message names `--index-file` as the alternative. No valid invocation becomes invalid.

### `verify` and `workflow`: untouched

`flashback verify` reads a sealed undo script from disk and replays it — no binlog parse, no remote flags. `workflow` orchestrates analyze/compare/trend report runs — same. Neither file changes.

### Bounded read and cut transactions

A transaction group that straddles the remote window is handled by machinery that already exists, with no new warning types:

- **BEGIN before the remote start (head-cut).** ROWS events arrive with no open group; the analyzer starts an implicit transaction for them (`accumulateRowEvent` → `startTransaction(false)` in `internal/analyzer/transactions.go`), and the closing XID finalizes it like any group. The planner undoes the observed suffix — the undo scope is the delivered events by definition. This is the same shape a file-local `--start-position` mid-transaction produces today.
- **COMMIT beyond the manifest end (tail-cut).** The group is still open when the stream ends; `Finalize` flushes it as an open explicit group and records the `openDML`/`OpenDMLAlerts` diagnostics (which flashback does not render — same as a `--stop-position` mid-transaction file cut today). The planner emits undo for the observed prefix.
- The `partial_transaction` warning (`WarnPartialTransaction`, kept+skipped via `NoteSkipped`) fires only when the parser delivered events the selection dropped. A boundary cut is not a selection skip — the events never entered the pipeline — so no warning fires for either cut, matching the file-mode contract. Documented behavior, not silent loss: the stderr resolved-paths line always names the `remote://host:port` origin so the bounded window is visible.

### Snapshot metadata: nothing to stamp

`buildSnapshotMetadata` sets `input_mode: remote` for analyze reports. Flashback renders no report and passes `snapshotMeta` as nil today; under remote it stays nil — there is no artifact to stamp. Remote provenance surfaces through the stderr resolved-paths line (`- remote://host:port`); inside the script the `-- binlog:` comments carry the binlog basename under the same render rule as file input.

### Error taxonomy (no new kinds)

| Condition | Kind | Exit | `details.reason` |
|---|---|---|---|
| Flag combinations/values (below) | usage | 3 | — |
| Remote connect/TLS/auth/manifest/privilege/start/stream failures | input | 4 | remote reasons per ADR 0021 via `remoteClierr` |
| Unreadable/missing local binlog, stdin | input | 4 | — |
| Checked-decode failure (local or remote bytes) | corrupt | 5 | — |
| Flashback refusals — conflict (ADR 0013), DDL-in-range (ADR 0015), unrenderable row/schema | refused/conflict | 6/7 | unchanged |
| `BINLOGVIZ_SPILL_BYTES`/`BINLOGVIZ_MEMORY_BYTES` ceilings | resource | 10 | unchanged |
| Zero emitted events / no-data guards (remote yields none → same as empty-window file run) | no_data | 2 | unchanged |

Mutual-exclusion matrix (all `usage`): positional args ⊥ `--from-dir`/`--prefix`/`--index-file`; `--from-dir` needs exactly one of `--prefix`/`--index-file`; remote flags require `--host`; remote ⊥ positional/stdin/discovery/`--start-position`/`--stop-position`; `--start-pos` requires `--start-file`; `--start-gtid` ⊥ `--start-file`/`--start-pos`; a `--host` with empty host or port half is invalid; remote stale `start_pos` is `usage` while server-side `start_file` refusal stays `input` (ADR 0021 mapping).

### Package layout

Everything lands in `cmd/binlogviz/flashback.go` plus one new unit-test file and one new e2e file: flag registration, `Args` contract update, the remote branch in `RunE`, and the `refreshFlashbackHelp` usage map. `internal/remote`, `internal/binlog`, `internal/analyzer`, `internal/flashback`, `internal/model` get zero changes — remote flashback is `redo`'s plumbing pointed at the existing flashback capture/planner path.

## Considered options

- **Keep `flashback` on the legacy `--from-dir` xor `--prefix` contract.** Rejected: the current contract is already implemented once and shared; the legacy check is a special case that hides `--index-file` from one command for no benefit. Converging is the smaller change.
- **A `--follow`/`--stop-at-end` remote mode for flashback.** Rejected, same as ADR 0021/0022: bounded-to-manifest is the only defined remote mode; follow changes termination, reconnect, and memory semantics and arrives with its own design.
- **Warn when a remote stream ends mid-transaction.** Rejected as new surface: the open-group diagnostics already record it, `partial_transaction` is reserved for selection splits, and a boundary cut matches the file-mode `--stop-position` semantics exactly. Documented instead.
- **Reuse `--start-position`/`--stop-position` as remote byte coordinates.** Rejected (ADR 0021 already decided): they are local-file selectors with `os.Stat` validation; remote positions go through `--start-pos` against the probed manifest.
- **Remote flags on `flashback verify`/`workflow`.** Rejected: neither parses binlog events; remote input there is dead surface.

## Consequences

- `binlogviz flashback --host db --user repl` generates the identical undo script the same events would produce from copied files — same planner, same refusals, same XA contract — without staging binlogs locally.
- The undo/redo/remote triangle closes: `analyze`, `redo`, and `flashback` share one input surface and one remote protocol path; `internal/remote` and the undo planner did not change.
- The server account needs `REPLICATION SLAVE` + `REPLICATION CLIENT`; missing grants fail `input`/`privilege` per ADR 0021.
- Flashback's `Args` now accepts `--from-dir` + `--index-file`; all previously valid invocations stay valid.
- A remote read that begins or ends inside a transaction undoes the delivered prefix/suffix with no partial-transaction warning — the same semantics file-position selectors already produce; docs state this explicitly.
- Unit tests exercise the validation matrix and the full parse→plan→emit path through the `FrameSource` seam with fixture frames; the live path is covered by an env-gated `TestFlashbackRemoteMySQL80` under `BINLOGVIZ_FLASHBACK_E2E`, wired into the CI `flashback-e2e` job.
- Not in this change: `--follow`, reconnect/resume, `verify`/`workflow` remote modes, undo output contract changes, `internal/remote` internals.
