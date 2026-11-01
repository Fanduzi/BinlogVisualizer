# Delivery: issues #73–#76

## Refs

- Repository: `Fanduzi/BinlogVisualizer`
- Base: `7c6dae4dbb0e75136d09495aba044819c5f3defd`
- Candidate and fast-forwarded `main`: `06674dbfb44fc90605a692dc44c10d359591dceb`
- Merge type: direct fast-forward push to `main` (PR https://github.com/Fanduzi/BinlogVisualizer/pull/77 auto-closed)
- Push range: `7c6dae4dbb0e75136d09495aba044819c5f3defd..06674dbfb44fc90605a692dc44c10d359591dceb`
- Issues:
  - https://github.com/Fanduzi/BinlogVisualizer/issues/73
  - https://github.com/Fanduzi/BinlogVisualizer/issues/74
  - https://github.com/Fanduzi/BinlogVisualizer/issues/75
  - https://github.com/Fanduzi/BinlogVisualizer/issues/76

## Root-cause evidence

- #74: independent management QUERY that was not ADMIN was dropped at normalize, so a GTID-started non-explicit group stayed open and the next GTID failed as a conflicting GTID. Analyze now emits Unclassified QUERY and fails with a bounded statement prefix (exit 1). Ignored QUERY (`SET timestamp`, `SET NAMES`, other non-ROLE `SET`) stays a counted drop and never closes.
- #75: a transaction payload wrapper had no canonical kind, so inner ROW images never reached analyze. The parser expands the wrapper; partial-update maps to UPDATE ROW images; anonymous GTID opens a group with empty identity.
- #76: exact `FLUSH TABLES` is ADMIN only with a committed MySQL 8.0.46 ROW+GTID dialect fixture driven through `ParseFiles`. `CHECK TABLE` and `SET ROLE` stay Unclassified QUERY.
- #73 is the parent spec for the three tickets above.

## Local gates

CWD: `/Users/fan/.cursor/worktrees/binlogvisualizer/issue-73-land`  
SHA under test: `06674dbfb44fc90605a692dc44c10d359591dceb`

| Command | Result |
|---|---|
| `go test ./... -count=1` | pass: 1320 tests in 12 packages |
| `go vet ./...` | pass |
| `goreleaser check` | pass: one configuration validated |
| `go build -trimpath -o dist/binlogviz .` then `bash scripts/pack_release_archive.sh dist/binlogviz dist/binlogviz_ci_local.tar.gz` then `bash scripts/release_smoke.sh dist/binlogviz_ci_local.tar.gz` | pass; smoke analyzed the bundled sample, saved current and baseline snapshots, and ran analyze/compare/trend workflow steps |
| `check_three_level_doc.sh` | not run: script is not in this repository and not on `PATH` |
| `git diff --check origin/main...HEAD` | pass after dropping one extra EOF blank line in `cmd/binlogviz/unclassified_query_test.go` |

No tests were skipped. This repository has no browser E2E suite; the packaged-archive smoke is the install-path check.

## Review

Independent `/code-review` workers (Standards + Spec) on isolated worktrees:

- #74: pass, findings none, head `dabf5994ec3349d4ec4d4323602811bfe21d6b06`
- #75: pass, findings none, head `c1d50c7cbbe2069c6502ceac029c438b774e6a42`
- #76: first review fail (standards: “GTID group” wording); fix `8744cab7d67a99a92969868abe75c94de0c8d866`; re-review pass, S1/S2 resolved, findings none

Integration merge `1f24ab9722aed80df2753a46e150e26e182a47f1` resolved doc/`normalize_test.go` header conflicts only. No extra product behavior.

Commit trailer scan on `7c6dae4..06674db`: worker commits include `Co-authored-by: Cursor <cursoragent@cursor.com>` (already on `origin/main`; history not rewritten).

## CI

- Required workflow/job: `ci` / `verify`
- PR run (head `06674db`): https://github.com/Fanduzi/BinlogVisualizer/actions/runs/35097137153
- PR job: https://github.com/Fanduzi/BinlogVisualizer/actions/runs/35097137153/job/104797185815
- `main` run: https://github.com/Fanduzi/BinlogVisualizer/actions/runs/35097339815
- `main` job: https://github.com/Fanduzi/BinlogVisualizer/actions/runs/35097339815/job/104797862242
- Head SHA: `06674dbfb44fc90605a692dc44c10d359591dceb`
- Conclusion: `success`
- Cloudflare Pages check: `success` on PR #77 for the same SHA.

## Root worktree preservation and cleanup

Allowed dirty/untracked paths on `/Users/fan/GolangProjects/BinlogVisualizer`, unchanged and excluded from this delivery:

- `CONTEXT.md` (modified; orchestrator grill copy, not the landed file)
- `docs/adr/0002-transaction-group-close-and-retain.md` (modified; orchestrator grill copy)
- `docs/adr/0003-unclassified-query-and-dialect-fixtures.md` (untracked orchestrator copy)
- `AGENTS.md.bak-pre-gitnexus-uninstall`
- `CLAUDE.md.bak-pre-gitnexus-uninstall`
- `dist/` (local gate artifact from pack/smoke; not committed)

No stash, reset, clean, or force-push was used. Task worktrees `issue-74`, `issue-75`, `issue-76`, and `issue-73-land` were kept.
