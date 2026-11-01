# v0.39.0 Delivery Evidence

## Evidence Boundary

This record describes the immutable released source and its verified CI,
artifacts, and tap update. The later documentation-only commit carrying
this record has separate Git provenance and does not move the release tag.
Its identity is resolved with `git log -1 --format=%H -- docs/plans/release-v0.39.0-evidence.md`,
rather than a self-referential hash.

## Refs And Authorization

- The user explicitly approved v0.39.0 and the complete publication workflow.
- Base/source before delivery: `f637c308b89c1c449cd5db77e1cc350792b0f963`.
- Reviewed candidate head: `b1527bb718067163cbdd31b045107008b0515270`.
- PR synthetic merge: `b3b473131f2f1b5d63461f7f06db3348f21889a9`.
- Merged, release, and origin/main at source-publication boundary: `09317d97360489bf28fae1482f269d0bcfb6d53c`.
- Annotated tag object: `14833b133bb415efb24d2248e7815fe186dbdb09`; `v0.39.0^{}` is `09317d97360489bf28fae1482f269d0bcfb6d53c`.
- Candidate, synthetic merge, and merged source have the same complete tree:
  `e44ac89633acdf1624beded47f4533111fca3844`.
- PR: https://github.com/Fanduzi/BinlogVisualizer/pull/473, verified merged/closed.
- Merge policy: GitHub rebase-merge, then local `git merge --ff-only origin/main`.
- Main advanced through the protected PR path; no direct-main bypass was used.
- Main delivery range: `f637c308b89c1c449cd5db77e1cc350792b0f963..09317d97360489bf28fae1482f269d0bcfb6d53c`. New feature ref and the new tag
  were pushed normally; no force push or existing-ref rewrite occurred.

## Gate Results

| Gate | Exact command or source | Result |
|------|-------------------------|--------|
| Local build | `go build ./...` | exit 0 |
| Ordinary CI | `go test -count=1 ./...` | exit 0; conditional skips are not counted as passes |
| Vet | `go vet ./...` | exit 0 |
| Race | `go test -race -count=1 -timeout 40m ./...` | exit 0; intentional race-lane skips remain explicit |
| Tidy | two `go mod tidy` passes with byte comparison | exit 0, no further module changes |
| Full local lint | unfiltered `golangci-lint run ./...` | exit 0, zero issues |
| CI lint | `go run ./internal/codehealth/cmd/codehealth -base f637c308b89c1c449cd5db77e1cc350792b0f963 lint` | exit 0, differential gate |
| Codehealth | `go run ./internal/codehealth/cmd/codehealth -deadcode check`, `regen`, `-base f637c308b89c1c449cd5db77e1cc350792b0f963 compare` | exit 0; baseline only tightens |
| GoReleaser | `goreleaser check` | exit 0 locally and in release CI |
| Release source | `make release-check VERSION=v0.39.0 REF=09317d97360489bf28fae1482f269d0bcfb6d53c` | exit 0 |
| Release self-test | `make release-check-test` | exit 0, disposable fixtures only |
| Documentation | L2 sync, real Cobra flag guard, README/landing/archive contracts | exit 0 |
| Four native/manylinux archives | versioned `release.yml` matrix and packaged smoke | all four jobs succeed |
| Published host smoke | source `scripts/release_smoke.sh` with expected commit and required identity | exit 0 |
| Formatting | gofmt and `git diff --check` | no changes/errors |

The local statement profile covers 20,404 of 25,445 statements:
80.18864216938495% (Go display 80.2%). It is the passing full profile with
the entire subsequently updated mysqlexpr package replaced, including its
zero-count blocks; it is not a package-average or successful-counter union.
This is local Darwin/arm64 coverage, not a per-platform coverage promise.
The release adds test/config/documentation only after that production
source was frozen. Lint rules and coverage thresholds were not weakened.

## CI And Review

- Candidate CI: https://github.com/Fanduzi/BinlogVisualizer/actions/runs/41293632037, success.
- Candidate lint: https://github.com/Fanduzi/BinlogVisualizer/actions/runs/41293631961, success.
- Merged main CI: https://github.com/Fanduzi/BinlogVisualizer/actions/runs/41295150496, success.
- Merged main lint: https://github.com/Fanduzi/BinlogVisualizer/actions/runs/41295150502, success.
- Manual release rehearsal: https://github.com/Fanduzi/BinlogVisualizer/actions/runs/41295153251, success.
- Tagged release: https://github.com/Fanduzi/BinlogVisualizer/actions/runs/41297027042, success.
- Protected checks: codehealth, golangci-lint, verify, vet and race,
  flashback e2e, flashback difftest, release-prep self-test.
- Additional gates: MySQL 5.7 compatibility, darwin verify, landing overflow,
  release-prep, test, validate-homebrew, four build jobs, github-release.
- CodeRabbit was optional and skipped, not counted as successful review.
- Manual github-release was intentionally skipped because only tags publish;
  platform-inapplicable build steps also skipped by design. Their applicable
  build steps and the tagged github-release job succeeded.
- Review tools: lead incremental source/diff review, CodeGraph context,
  compiler/tests/vet, golangci-lint, and codehealth. Verdict: APPROVE.
- No CRITICAL/HIGH or open P1/P2 finding remained in the reviewed delivery.

## Database Execution

All original 28 locally skipped root tests have real applicable-lane PASS
records in the tagged run. Failed applicable roots: 0; root/child skips: 0.
The two differential roots each pass on both 8.0 and 8.4. Ordinary/race
invocations with environment-specific skips are not reclassified as passes.

| Role | Actual server versions | Serving CWD | Checkout SHA |
|------|------------------------|-------------|--------------|
| e2e | 8.0.46-0ubuntu0.24.04.4 | `/home/runner/work/BinlogVisualizer/BinlogVisualizer` | `09317d97360489bf28fae1482f269d0bcfb6d53c` |
| diff | 8.0.46, 8.4.13 | `/home/runner/work/BinlogVisualizer/BinlogVisualizer` | `09317d97360489bf28fae1482f269d0bcfb6d53c` |
| compat | 5.7.44 | `/home/runner/work/BinlogVisualizer/BinlogVisualizer` | `09317d97360489bf28fae1482f269d0bcfb6d53c` |

Exact selected invocations (job-level environment is defined in
`09317d97360489bf28fae1482f269d0bcfb6d53c:.github/workflows/ci.yml` and retained in the public job logs):

```bash
BINLOGVIZ_FLASHBACK_E2E=1 go test -count=1 -timeout 12m -v ./cmd/binlogviz \
            -run 'TestFlashbackRoundTripMySQL80|TestNumericDecodeMySQL80|TestFlashbackFailedApplyDropsAndSignals|TestAnalyzeRemoteMySQL80|TestRedoReplayMySQL80|TestRedoRemoteMySQL80|TestFlashbackRemoteMySQL80|TestRedoWhereReplayMySQL80|TestFlashbackWhereMySQL80|TestSplitFlashbackPerTableRoundTrip|TestSplitRedoPartsRoundTrip|TestAnalyzeFingerprintsMySQL80|TestAnalyzeThreadIDMySQL80' 2>&1 | tee "$log"
```

```bash
BINLOGVIZ_GUARD_MYSQL="$BINLOGVIZ_MYSQL" go test -count=1 -timeout 4m -v ./internal/guard \
            -run 'OnServer' 2>&1 | tee "$log"
```

```bash
go test -count=1 -timeout 4m -v ./cmd/binlogviz \
            -run '^TestFlashbackMySQL57DeprecatedMode$' 2>&1 | tee "$log"
```

```bash
go test -count=1 -timeout 15m -v ./internal/mysqlexpr ./cmd/binlogviz \
            -run 'TestDiffMySQLExpressions|TestFlashbackDiffMySQL' 2>&1 | tee "$log"
```

```bash
go test -count=1 -timeout 15m -v ./internal/mysqlexpr ./cmd/binlogviz \
            -run 'TestDiffMySQLExpressions|TestFlashbackDiffMySQL' 2>&1 | tee "$log"
```

| Package | Root test | Role | Actual root PASS records |
|---------|-----------|------|--------------------------|
| `binlogviz/internal/guard` | `TestGuardMessageOnServer` | e2e | 1 |
| `binlogviz/internal/guard` | `TestGuardMessageCutSweepOnServer` | e2e | 1 |
| `binlogviz/internal/guard` | `TestGuardMatchOnServer` | e2e | 1 |
| `binlogviz/cmd/binlogviz` | `TestAnalyzeRemoteMySQL80` | e2e | 1 |
| `binlogviz/cmd/binlogviz` | `TestAnalyzeFingerprintsMySQL80` | e2e | 1 |
| `binlogviz/cmd/binlogviz` | `TestFlashbackRoundTripMySQL80BinaryPad` | e2e | 1 |
| `binlogviz/cmd/binlogviz` | `TestFlashbackDiffMySQL` | diff | 2 |
| `binlogviz/cmd/binlogviz` | `TestFlashbackRoundTripMySQL80PreXAGrantForce` | e2e | 1 |
| `binlogviz/cmd/binlogviz` | `TestFlashbackFailedApplyDropsAndSignals` | e2e | 1 |
| `binlogviz/cmd/binlogviz` | `TestFlashbackRoundTripMySQL80Integrity` | e2e | 1 |
| `binlogviz/cmd/binlogviz` | `TestFlashbackRoundTripMySQL80PartialCommit` | e2e | 1 |
| `binlogviz/cmd/binlogviz` | `TestFlashbackRoundTripMySQL80MarkerNewline` | e2e | 1 |
| `binlogviz/cmd/binlogviz` | `TestFlashbackRoundTripMySQL80NoPKGuardRerun` | e2e | 1 |
| `binlogviz/cmd/binlogviz` | `TestFlashbackRoundTripMySQL80GuardSkipMarker` | e2e | 1 |
| `binlogviz/cmd/binlogviz` | `TestFlashbackRoundTripMySQL80RawApplyNoErrorZero` | e2e | 1 |
| `binlogviz/cmd/binlogviz` | `TestFlashbackMySQL57DeprecatedMode` | compat | 1 |
| `binlogviz/cmd/binlogviz` | `TestFlashbackRoundTripMySQL80PadCharModes` | e2e | 1 |
| `binlogviz/cmd/binlogviz` | `TestFlashbackRemoteMySQL80` | e2e | 1 |
| `binlogviz/cmd/binlogviz` | `TestFlashbackRoundTripMySQL80` | e2e | 1 |
| `binlogviz/cmd/binlogviz` | `TestNumericDecodeMySQL80` | e2e | 1 |
| `binlogviz/cmd/binlogviz` | `TestRedoReplayMySQL80` | e2e | 1 |
| `binlogviz/cmd/binlogviz` | `TestRedoRemoteMySQL80` | e2e | 1 |
| `binlogviz/cmd/binlogviz` | `TestSplitFlashbackPerTableRoundTrip` | e2e | 1 |
| `binlogviz/cmd/binlogviz` | `TestSplitRedoPartsRoundTrip` | e2e | 1 |
| `binlogviz/cmd/binlogviz` | `TestAnalyzeThreadIDMySQL80` | e2e | 1 |
| `binlogviz/cmd/binlogviz` | `TestRedoWhereReplayMySQL80` | e2e | 1 |
| `binlogviz/cmd/binlogviz` | `TestFlashbackWhereMySQL80` | e2e | 1 |
| `binlogviz/internal/mysqlexpr` | `TestDiffMySQLExpressions` | diff | 2 |

## Public Artifacts

- Public stable release: https://github.com/Fanduzi/BinlogVisualizer/releases/tag/v0.39.0.
- Published at: `2026-11-01T10:35:36Z`; the latest-release endpoint resolves here.
- Exactly four platform archives and one checksum manifest are uploaded.
- All four downloads match the manifest and GitHub asset digests.

| Archive | SHA-256 |
|---------|---------|
| `binlogviz_0.39.0_darwin_amd64.tar.gz` | `5f25bb47b20ae1390e68a3a58d0fe3fec37f61facadb3ae5350dadd9a2b81239` |
| `binlogviz_0.39.0_darwin_arm64.tar.gz` | `5867c6363d7265c7675a156c7e38d1877232e7e6685961fb9ec79cc32b7bacf3` |
| `binlogviz_0.39.0_linux_amd64.tar.gz` | `5a4a9ee20ccbdb7dccfc68f8adb95452f164714da7fb6f5f19cb438df6787883` |
| `binlogviz_0.39.0_linux_arm64.tar.gz` | `2d3a8669395fefbd6f7c0e0e6da2b921a2d2e8bbc820d3589a7aa4d09d844f74` |

- Executed archive: `binlogviz_0.39.0_darwin_arm64.tar.gz`.
- Runtime `version --format json`: version `0.39.0`,
  commit `09317d97360489bf28fae1482f269d0bcfb6d53c`, modified `false`,
  commit time `2026-11-01T09:54:48Z`, Go `go1.26.10`.
- Homebrew tap commit: `9a35e102dcefd2848fb1e30d92d9537cfcde733e`.
- Homebrew cask blob: `09448f8d96ac67f030ec5a0907262002fbb2d59e`.
- Cask version and all four exact URLs/checksums match the downloaded assets.
- Public landing page loads as BinlogViz in an isolated read-only browser session.

## Preservation And Cleanup

- Root dirty-path whitelist: empty; root stayed clean through source delivery.
- No user work was stashed, reset, cleaned, relocated, or overwritten.
- Historical 44 local/remote tag disagreements were preserved unchanged;
  only main was fetched without tags. The relevant v0.38.0 baseline matches.
- Manual and tagged credential-probe refs were independently confirmed absent
  using exact GET requests, rather than inferring deletion from the probe job.
- Disposable CI services were runner-owned; no existing local MySQL/Docker
  service was started, connected to, or modified.
- The clean review worktree and release branch are intentionally retained
  for audit; no deletion of existing worktrees/branches was authorized.
- The read-only browser session is closed. Raw public CI, downloaded
  artifacts, checksums, and observation logs remain available for inspection.
