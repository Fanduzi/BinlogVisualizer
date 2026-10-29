# BinlogViz v0.36.0 Release Notes

Release date: 2026-10-28

## Overview

v0.36.0 is a quality release: seventeen P3 issues closed, several real correctness fixes in generated-column verification and DDL reporting, one new input-discovery flag, and a demonstrably hardened release gate. `report_version` stays 4, `plan_version` stays 2, and `flashback verify --format json` `schema_version` stays 1.

## Added

- **`analyze --from-dir DIR --index-file NAME` (#256).** Discovery now reads a server's `binlog.index` manifest instead of only prefix matching. Entries resolve relative to `--from-dir` (absolute paths allowed), are read in manifest order, and a missing or rotated-away entry fails loudly rather than silently dropping part of the history. `--index-file` is mutually exclusive with `--prefix`; exactly one selector is required with `--from-dir`.
- **`landing-overflow` CI job (#451).** The landing page is loaded in headless Chromium at 320/768/1280px and the run fails when `documentElement.scrollWidth` exceeds the viewport — layout overflow the markup-level guard cannot see.
- **macOS CI.** The full test suite now runs on `macos-14` as `darwin verify`, so a Linux-only assumption in a test helper cannot quietly break darwin users.
- **Difftest nightly workflow** (`difftest-nightly.yml`) with pinned MySQL images (`8.0.46`/`8.4.13`) instead of floating `8.0`/`8.4` tags.
- **`go test -count=1` in CI** (`verify`, `darwin verify`, `vet and race`) so a cached test result can never stand in for a fresh run.

## Fixed — correctness

- **UNSIGNED integer arithmetic in generated-column verification (#450).** A column's `unsigned` flag was dropped before reaching the expression checker, so `a-b` on unsigned columns could verify against a negative result MySQL would refuse (ERROR 1690). `+`, `-`, `*`, `DIV`, `%`, and unary minus now apply unsigned bounds `[0, 2^64-1]` in both evaluation domains; out-of-range operands or results fail verification.
- **`/` division result is DECIMAL at scale `div_precision_increment` (#450).** `CONCAT(7/7)` now verifies as `'1.0000'` and `7/2` as `'3.5000'`, matching MySQL.
- **Four more assignment coercions (#450).** YEAR accepts `0`/`0000`, DATETIME literal assignment rounds fractional seconds per MySQL half-up rules, fixed-length BINARY pads with `0x00`, and `_latin1`-style introducer literals on generated expressions are not evaluated — the bytes are already lossy in `SHOW CREATE TABLE`. Introducers in the dump-safe UTF family (`utf8`, `utf8mb3`, `utf8mb4`, `ascii`) still evaluate.
- **`CREATE/ALTER/DROP/RENAME USER` fan-out (#421).** Every account a USER DDL names is reported with the correct table attribution, including multi-account statements and multi-pair `RENAME TABLE`.
- **`describe` failure output pins `valid:false`** in the failure schema instead of a stale default.
- **`--start`/`--end` offset-change errors** are localized instead of leaking the English template.
- **HTML minute-chart labels** get a date prefix when the analyzed span crosses midnight.
- **A timestamp-looking `workload_id`** survives `snapshot save` instead of being folded.
- **GTID ceiling message** no longer says "can need fewer" when the per-server ceiling is not the binding one.
- **Refusal excerpts** collapse whitespace; single-line statements stay single-line.
- **Report ordering (#255).** In text and Markdown, Top Transactions now precede Hot Rows — transaction ranking is the primary answer during spike triage. HTML keeps its DBA navigation sections.

## Fixed — release process and gates

- **The release gate cannot be silenced (#420).** Verified on real GitHub runs: an injected e2e failure on a tag skips all four builds and the release job (run 40718489351); a PR-only `if:` on the e2e job and a `continue-on-error:` on it are both caught by `TestFlashbackE2EJobCannotBeSilenced` before the e2e even runs (runs 40718744800, 40722108847); re-running a single skipped build leaves it skipped and re-running failed jobs re-runs `ci` first.
- **`TestVetRaceAndGuardWiringPresent`** pins the vet-race job and the guard-SQL wiring; **`TestBuildVerifiesCheckoutMatchesTag`** pins `git rev-parse HEAD` and a clean-tree check (`git status --porcelain`) before every release build.
- **`release-prep` self-check** runs under `pipefail` (a mid-pipe failure used to pass), and its version-pin scanner shares one allowlist (`scripts/landing-allowed-numbers.txt`) and one token-shape rule with the Go landing guard (#451).
- **README guard is an allowlist (#447)**, not a banned-pattern list — 38 of QA's 122 probes used to slip through. Archive-wide offline link checking now covers every shipped Markdown page, and twelve release-archive links that pointed at non-shipped files are absolute GitHub links.
- **`codehealth-compare`** no longer updates the baseline on failure paths, lint line numbers attribute to the right file, trailing assigned identifiers normalise (`mark` → `pos`), and a committed submodule or nested repository is a finding instead of a silent miss (#454).
- **The codehealth ledger** derives identity from content, tracks per-entry ceilings, and records file-move lineage instead of flagging a move as a new oversized file.

## Architecture

- **`internal/detailstore` (ADR 0012 step 6, #257).** The DuckDB adapter moved out of `internal/analyzer`; analyzer no longer imports `go-duckdb`, and cmd injects `*detailstore.DuckDBStore`. `CGO_ENABLED=0` still builds through the stub.
- God-file paydown continued: `analyze.go` split into four domain files, `workflow.go` into output and exec halves, `planner.go` into render and refusal halves, `expr.go` into JSON and DECIMAL domains — each under the size ceiling with no behaviour change.

## Tests and CI

- New regression coverage: index-manifest resolution and every invalid selector combination; unsigned arithmetic bounds; the four assignment coercions; USER DDL account fan-out; report section ordering; e2e-gate mutations; checkout verification.
- Mutant pins added for lexer quote-skip, digit-led words, versioned-comment digit boundaries, GTID limit reasons, and second failure shapes for `analyze` and `workflow describe` (#402, #403, #418).
- `docs/production-readiness.md`: 24 MET / 0 PARTIAL / 0 NOT MET.

## Verification

- **CI**: PR #455 run 40725552689 (ci: verify, vet and race, darwin verify, flashback e2e 73 pass, difftest, landing overflow, release-prep self-test) and run 40725552716 (lint, codehealth) are green; the merge to main is a fast-forward of that tree.
- **Real GitHub gate checks** (#420): tag push with an injected e2e failure produced no Release and no builds; dispatch runs confirmed both silencing mutations are caught by the static lock.
- **Local**: `go test ./...` green, `go vet` clean, `make codehealth-compare` passes against baseline, the release-prep self-test suite passes 68 cases.

## Compatibility

- `report_version` stays 4, `plan_version` stays 2, `schema_version` stays 1. No JSON field is added or removed.
- Text and Markdown reports order Top Transactions before Hot Rows; JSON and HTML content is unchanged.
- `analyze` gains `--index-file` (additive) and replaces the `--from-dir`/`--prefix` pairing error with a selector-pairing error naming both selectors.
- Artifact names and supported platforms are unchanged.

## Known issues

- **#256 (P3, roadmap)** remains open: remote replication read, forward/redo SQL, CSV/TSV statistics, per-table/transaction split output, row-level filters, and statement fingerprinting are feature-sized and each needs its own design pass. Two items from it are done here: `--index-file` discovery and `--tz`.
