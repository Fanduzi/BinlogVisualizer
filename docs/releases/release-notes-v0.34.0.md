# BinlogViz v0.34.0 Release Notes

Release date: 2026-10-20

## Overview

v0.34.0 is a minor release for ADR 0012 step 5: text, Markdown, JSON, and HTML now read one transaction view built in `internal/report/view`, and `report.Write` is the only format switch (#353, #356). The rest is test hardening: the flashback and verify memory-bound tests no longer depend on scheduling and pass at `GOMAXPROCS=1` (#358, #361, closes #351), and `docs/production-readiness.md` is new (#359). There are no breaking changes and no user-visible output change. Analyze output, flashback scripts and plans, `--help`, exit codes, `schema_version` 1, `plan_version` 2 and `report_version` 3 are the same as v0.33.1.

## Changed

- **One transaction view for every analyze format (ADR 0012 step 5, #353, part of #257).** `view.Build` is the only place that selects transactions. Text, Markdown, JSON, and HTML read that value instead of slicing the transaction lists themselves. `report.Write` normalizes options, builds the view, then dispatches the format. The selection rules and output bytes are unchanged. Text and Markdown still choose different top transactions, as in v0.33.1 (#352). `scripts/compare-analyze-formats.sh` renders text, JSON, Markdown, HTML, flashback SQL, and flashback `--plan` for every binlog in the repository on two commits and diffs them.
- **The format switch fails instead of falling through (#356, fixes #354).** `knownFormats` is the single list `ParseFormat` accepts. The `report.Write` switch has no default arm, so a listed format with no case returns `unhandled format` and writes nothing instead of silently rendering text. `TestWriteRendersEveryKnownFormat` renders every value in the list. An unknown `--format` still exits 3 (usage) before input is read, with the same message.

## Fixed

- **Memory-bound tests are stable on one CPU (#358, #361, closes #351).** These are test-only changes; the release binary does not change.
  - `TestGeneratedReviewRSS` (`internal/flashback`) asserted that a child process's VmHWM stayed under a fixed 64MB, and it failed in CI on a docs-only commit (#351). It now reads `/gc/heap/live:bytes` in the review's own goroutine at the end of the generated-column review, and again after Prepare and the write, so the result does not depend on scheduling. The limit is 100 bytes/row (streaming measures about 75), so keeping half, a quarter, or an eighth of the row images fails. The test fails if the review never reported, and a `BINLOGVIZ_GENMEM=measure` run skips instead of passing without asserting. The hook it uses, `reviewProbe` in `internal/flashback/review.go`, is nil unless a test sets it.
  - `TestVerifyDoesNotBufferTheScript` (`internal/guard`) compares the live-heap peak of verifying a script and one four times larger. It fails when the large run's extra peak is more than a tenth of the extra script bytes, and caps the small run's peak at 512KiB. Partial buffering, `io.ReadAll`, and 1MiB, 16MiB, and 64MiB read buffers fail at 1, 2, and 8 CPUs. Streaming peaks at about 90KB.
  - `TestRunAnalysisParsesMultipleFilesConcurrentlyAndConsumesInOrder` (`cmd/binlogviz`) proves parse overlap by blocking instead of sleeping, and sets `GOMAXPROCS` to one per file, since the parse pool is sized from it. It failed at `GOMAXPROCS=1` before this change ("peak concurrency was 1").

## Added

- **`docs/production-readiness.md` (#359).** A 24-item production-readiness checklist (in Chinese) for analysis correctness, flashback safety, the MySQL version matrix, the agent contract, resource bounds, release integrity, and docs. An item is MET only when an automated check runs in CI. Docs only.

## Verification

BinlogQA's PR QA passed #353, #356, #358 (`ab53d654`, landed on main as `6559ba65`), and #361 (`14bd41b3`). Tip QA on main `14bd41b3` passed with 0 P0, 0 P1, 0 P2 and 0 P3.

- **Unchanged output**: analyze output matches v0.33.1 on 2442/2442 renderings. Flashback SQL, stderr, and `--plan` JSON for the mf, mi, n316, pc, and rt325 corpora are byte-identical. On MySQL 8.0, `verify | mysql` prints `binlogviz: undo committed, 4 statements` and `CHECKSUM TABLE` after the undo equals the value before the DML. On 5.7, flashback refuses with exit 6 and the same message as v0.33.1.
- **Agent contract**: `--help` for all 21 commands is byte-identical in en and zh-CN (42/42). Exit codes 3, 4, 5, 6, and 9 with their JSON error envelopes match v0.33.1. The keys of `version`, `snapshot`, `compare`, and `trend` JSON are unchanged.
- **Memory tests**: the three rewritten tests failed 0 of 750 targeted runs at `GOMAXPROCS` 1, 2, and 8, under `taskset -c 0`, with go1.26.1, with `GOGC` 50, 200, and off, and under memory and CPU hogs. The 512KiB cap had 0 false failures in 720 streaming runs; the worst peak was 89,800 bytes.
- `go test ./...` passes at the default `GOMAXPROCS` and at `GOMAXPROCS=1`; `-race` on the touched packages is clean. CI green.

## Compatibility

- No breaking changes. `report_version` stays 3, `plan_version` stays 2, and `flashback verify --format json` `schema_version` stays 1. Exit codes, snapshots, artifact names, and supported platforms are unchanged from v0.33.1.
- v0.33.0 and v0.33.1 flashback scripts verify and apply unchanged. Scripts from v0.32.0 or earlier still need to be regenerated.

## Known issues

- **#303 (P2)**: the Linux release binaries report an empty `commit` and `commit_time` in `binlogviz version`. The fix is in `.github/workflows/release.yml` and needs a maintainer with workflow access. Check a Linux binary against `binlogviz_0.34.0_checksums.txt`.
- **#352**: text and Markdown choose different top transactions. `compare` and `trend` still have a default arm in their format switch (#357).
- `flashback verify` does not accept `-` for stdin (exit 4).
- **#270**, **#255**, **#256**, and **#257** (P3) carry over.

See `docs/concept/limitations.md` and `docs/adr/0014-flashback-no-partial-commit.md` for the flashback guarantees and their limits.
