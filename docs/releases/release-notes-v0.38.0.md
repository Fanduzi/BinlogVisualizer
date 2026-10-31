# BinlogViz v0.38.0 Release Notes

Release date: 2026-11-02

## Overview

v0.38.0 completes the parity roadmap against `mysqlbinlog`, `binlog2sql`, `my2sql`, and `pt-query-digest`: every remaining feature gap tracked on the roadmap epic is now delivered. `binlogviz redo` emits replayable forward SQL; `flashback` reads binlogs straight from a live server; `--where`, `--thread-id`, and `--output-dir` give incident recovery surgical scope; and `analyze` gains CSV/TSV dataset exports plus pt-query-digest-style statement fingerprints. `report_version` stays 4, `plan_version` stays 2, and `flashback verify --format json` `schema_version` stays 1.

## Added

- **`binlogviz redo`** — forward (redo) SQL for selected changes, the binlog2sql default mode: real `UPDATE` statements (not DELETE+INSERT), PK `<=>` matching or full-image `LIMIT 1` fallback, DDL emitted bare, `USE` context tracking, XA pass-through, BEGIN/COMMIT boundary mirroring, and whole-script staging so a partially-generated script never lands on stdout. Works on every input mode including `--host` for bounded PITR replay (ADR 0022).
- **`flashback --host`** — the remote bounded read shipped for `analyze`/`redo` now covers flashback; `--index-file` plus `--from-dir`/`--prefix` parity across all three commands (ADR 0023).
- **`flashback`/`redo --where <expr>`** — row-level predicate filtering on MySQL comparison semantics: before-images for UPDATE/DELETE, after-images for INSERT, conservative three-valued logic, explicit refusal on unverifiable columns (MINIMAL metadata inherits the `names` refusal chain) (ADR 0024).
- **`--output-dir` split output** — atomic staged directory publication with `MANIFEST`; `--file-per-table` (flashback only: each file is a complete sealed guard script, verifiable and applicable independently) and `--max-statements-per-file` (redo: transaction-continuation comments plus cross-file GTID warnings) (ADR 0025).
- **`analyze --format csv|tsv` + `--csv-dataset`** — machine-readable exports of `tables`, `transactions`, `patterns`, and `fingerprints` datasets with JSON-identical bounding (ADR 0026).
- **Statement fingerprints** — Rows_query statement text normalized (literals to `?`, `IN`/`VALUES` collapsed to `(?)`, keywords folded, identifier case preserved), grouped and ranked by rows written across the whole workload, in JSON (`statement_fingerprints`), text (`--show-fingerprints`, implied by `--details`), and the `fingerprints` CSV/TSV dataset. Bounded at 8192 groups with an explicit overflow count (ADR 0026).
- **`--thread-id` selector** — select transaction groups by binlog connection id on all three SQL/analysis commands, with the same whole-group semantics as the GTID selector (ADR 0027).
- **`--index-file` trust-boundary hardening** — manifest entries must resolve to distinct regular files inside `--from-dir`: `..`/absolute escapes, self-listing, duplicates (by file identity, catching hardlinks and symlink aliases), and in-dir symlinks pointing out are all named refusals; relay-log inputs documented (ADR 0028).

## Fixed

- `codehealth lint` local gate was un-runnable: golangci-lint's content-keyed cache returned finding positions from stale base-extraction dirs or other worktrees, so every finding diffed as new (or passed deceptively under symmetric staleness). Each lint run now uses an isolated `GOLANGCI_LINT_CACHE`, and finding paths normalize against the linted root (#466).

## Architecture

- `internal/sqlemit` shared cell-literal emitters serve both undo and redo — no forked SQL formatting.
- `internal/splitout` staged atomic directory publication (fsync, MANIFEST-last, rename).
- `internal/mysqlexpr` gained a boolean/predicate layer beside the scalar grammar — one comparison engine, not two.
- Fingerprint capture rides a transient `RetainedStatements` fold at the builder seam — per-statement coverage of all transactions, identical scoping to `query_summary`.
- ADRs 0022–0028 document the surface decisions; L2 module READMEs now cover every internal package.

## Tests and CI

- New live MySQL 8.0 e2e coverage for redo replay, remote redo/flashback, `--where`, split round-trips, statement fingerprints, and `--thread-id` — all registered in the mandatory PASS-verification loop.
- macOS CI runners migrated `macos-14` → `macos-15` ahead of the GitHub brownout (protects darwin/arm64 release builds).
- 20 dead i18n keys removed; the tree is `gofmt -s` clean.

## Verification

- `go test -count=1 ./...`, `go vet ./...`, `CGO_ENABLED=0 go build ./...`, `make codehealth-compare`, golangci-lint — all green on the tagged commit.
- Live e2e verified against MySQL 8.0.45 in CI (and locally during development).

## Compatibility

- CLI additions are flag-scoped and additive; no flags removed or renamed.
- JSON report stays `report_version` 4 — all keys added are additive; `--sql-context off` suppresses fingerprint samples the same way it suppresses query summaries.
- `--csv-dataset` under a non-delimited format, `--thread-id 0`, and malformed index manifests are explicit usage/input errors, not silent behavior.

## Known issues / out of scope

- `--file-per-table` is intentionally flashback-only; redo keeps mirrored transaction order.
- Fingerprint input is bounded by the existing 4096-byte Rows_query capture; truncated statements fingerprint their stored prefix.
- `--thread-id` selects binlog connection ids (reused by the server), not application identities — see `docs/reference/cli.md`.
