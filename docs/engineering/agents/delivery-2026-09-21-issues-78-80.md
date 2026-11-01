# Delivery: issues #78–#80

## Refs

- Repository: `Fanduzi/BinlogVisualizer`
- Base: `a66b9461fd56466c328bf5cb2a75979f954274cb`
- Candidate and fast-forwarded `main`: `eb2369f6ec259640bd9d98fad059683309a77390`
- Merge type: merge commit on `integration/issues-78-80`, then direct fast-forward push to `main`
- Merge parents: `3397d17748b298e9805425caf7e0ba228b17a86a` (#78/#79), `cab2fa5ac99341352cdc32a1e2c009c6cfe9cea8` (#80)
- Push range: `a66b9461fd56466c328bf5cb2a75979f954274cb..eb2369f6ec259640bd9d98fad059683309a77390`
- Issues:
  - https://github.com/Fanduzi/BinlogVisualizer/issues/78
  - https://github.com/Fanduzi/BinlogVisualizer/issues/79
  - https://github.com/Fanduzi/BinlogVisualizer/issues/80

## Root-cause evidence

- #78: an anonymous GTID opens a transaction group with empty identity. Unclassified QUERY as that group's only work was silently finalized on the next GTID. Analyze now fails as Unclassified QUERY (prefix, exit 1, empty stdout) on that next GTID, same as a named GTID.
- #79: an after-window Unclassified QUERY that never intersected the selected window failed the whole analyze. Clip flags now keep the in-window report (exit 0). An unclassified-only group that intersects the window still fails.
- #80: expanded transaction-payload inners invented uncompressed positions or copied the wrapper byte size each. Inners now share the wrapper's file-relative span once.

## Local gates

CWD: `/Users/fan/.cursor/worktrees/binlogvisualizer/issue-78-80-land`  
SHA under test: `eb2369f6ec259640bd9d98fad059683309a77390`

| Command | Result |
|---|---|
| `go test ./... -count=1` | pass: 1346 tests run, 1346 pass, 0 fail, 0 skip, 12 packages (2 with no test files) |
| `go vet ./...` | pass |
| `goreleaser check` | pass: one configuration validated |
| `go build -trimpath -o dist/binlogviz .` then `bash scripts/pack_release_archive.sh dist/binlogviz dist/binlogviz_ci_local.tar.gz` then `bash scripts/release_smoke.sh dist/binlogviz_ci_local.tar.gz` | pass; smoke analyzed the bundled sample and ran analyze/compare/trend workflow steps |
| `check_three_level_doc.sh` | not run: script is not in this repository and not on `PATH` |
| `git diff --check origin/main...HEAD` | pass |

No tests were skipped. This repository has no browser E2E suite; the packaged-archive smoke is the install-path check.

## Review

Independent `/code-review` workers (Standards + Spec) on isolated worktrees:

- #78: pass, findings none, head `58e2c45ca62cb22e8b1f7a5751ddf44d60b4f439`
- #79: pass, findings none, head `3397d17748b298e9805425caf7e0ba228b17a86a`
- #80: pass, findings none, head `cab2fa5ac99341352cdc32a1e2c009c6cfe9cea8`

Integration merge `eb2369f6ec259640bd9d98fad059683309a77390` resolved L3 header/`README.md` wording only. No extra product behavior.

## CI

- Required workflow/job: `ci` / `verify`
- `main` run: https://github.com/Fanduzi/BinlogVisualizer/actions/runs/35611263466
- `main` job: https://github.com/Fanduzi/BinlogVisualizer/actions/runs/35611263466/job/106370674735
- Head SHA: `eb2369f6ec259640bd9d98fad059683309a77390`
- Conclusion: `success`

## Root worktree preservation and cleanup

Allowed dirty/untracked paths on `/Users/fan/GolangProjects/BinlogVisualizer`, unchanged and excluded from this delivery:

- `CONTEXT.md` (modified; orchestrator grill copy, not the landed file)
- `docs/adr/0002-transaction-group-close-and-retain.md` (modified; orchestrator grill copy)
- `docs/adr/0003-unclassified-query-and-dialect-fixtures.md` (untracked orchestrator copy)
- `AGENTS.md.bak-pre-gitnexus-uninstall`
- `CLAUDE.md.bak-pre-gitnexus-uninstall`
- `dist/` (local gate artifact from pack/smoke; not committed)

No stash, reset, clean, or force-push was used. Task worktrees `issue-78`, `issue-79`, `issue-80`, and `issue-78-80-land` were kept.
