# Orchestration: fuzz harness over attacker-controlled parse surfaces

Date: 2026-11-02. Runtime-quality hardening unit (evidence-driven).

## Evidence (orchestrator audit)

- **Zero `func Fuzz` in the entire repo** — no fuzz coverage at all.
- Identifier injection: SAFE — `sqlemit.QuoteIdent` doubles backticks correctly.
- Decoder posture: `internal/binlog` has ~8 error-return paths plus a recover→error wrapper (`parser.go:244` re-panics runtime errors, `remote.go:252` same). Fuzz validates whether the recover net catches *every* malformed-input panic or leaks real crashes.
- Attacker/user-controlled parse surfaces:
  1. `internal/binlog` event decoder — raw binlog bytes (remote server can send anything)
  2. `internal/mysqlexpr` — `--where` predicate text (user input)
  3. `readIndexFilePaths` manifest parser — `--index-file` contents (user file)
  4. GTID set parser — `--start-gtid` / remote GTID strings
  5. `internal/flashback/ddlrefs` DDL parser — binlog-sourced statements

## Scope

Add `testing.F` fuzz tests that run in normal `go test` mode (seed corpus — catches regressions in CI); document `-fuzz` for longer local runs. Assertions: **no panic** (error returns are correct behavior); round-trip invariants where meaningful (parse→serialize→parse = identity for valid inputs).

- `internal/binlog`: fuzz `Parse*`/decoder entry on raw byte slices; seed corpus from existing `testdata/*.binlog` (headers, event frames, truncated/mutated variants).
- `internal/mysqlexpr`: fuzz `ParseWhere`/expression parse on strings; seeds from ADR 0024 grammar examples + existing test inputs.
- Index-file: fuzz the manifest line parser on byte content.
- GTID: fuzz `ParseGTIDSet`-equivalent on strings; seeds: valid sets, bare `uuid:N`, MariaDB forms, malformed near-misses.
- `ddlrefs`: fuzz the DDL parser on statement strings; seeds from real DDL in tests/fixtures.

## Rules

- If a fuzz crash surfaces a REAL bug (panic on malformed input, infinite loop, unbounded memory): fix it if surgical (bounds check, error path); if the fix needs design, report it honestly and skip that fuzz target's assertion rather than hiding it.
- Fuzz tests must be fast in seed mode (<seconds); no network/MySQL.
- Do NOT add a CI fuzz job this round (workflow change = separate decision); document `go test -fuzz` invocation in the package README or test comment.
- Keep seeds minimal — derive from existing testdata, don't handcraft huge corpora.
- Commit plan doc first (force-add; docs/plans/ gitignored). No AI co-author.

## Gates

Standard: build/vet/`gofmt -s`/`go test -count=1 ./...`/codehealth compare+lint/doc checker/i18n untouched. Plus: run each Fuzz for a short bounded window locally (`-fuzztime=20s` per target) to smoke beyond seeds, report crashes honestly.
