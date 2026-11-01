# Delivery: issues #65–#69

## Refs

- Repository: `Fanduzi/BinlogVisualizer`
- Base: `61c4bb45608244885cef1c967a8b986c0b7b2ca8`
- Candidate and fast-forwarded `main`: `6ecd0bbb2e2d398b634191f3b006113287a70e3e`
- Merge type: direct fast-forward push to `main`
- Push range: `61c4bb45608244885cef1c967a8b986c0b7b2ca8..6ecd0bbb2e2d398b634191f3b006113287a70e3e`
- Issues:
  - https://github.com/Fanduzi/BinlogVisualizer/issues/65
  - https://github.com/Fanduzi/BinlogVisualizer/issues/66
  - https://github.com/Fanduzi/BinlogVisualizer/issues/67
  - https://github.com/Fanduzi/BinlogVisualizer/issues/68
  - https://github.com/Fanduzi/BinlogVisualizer/issues/69

## Root-cause evidence

- #65: `--include-table` compared the token to the bare table name, so `dogfood.orders` never matched `schema=dogfood table=orders`.
- #66: `README_ZH.md` omitted the English SHOW MASTER / GTID quick-start section even though the flags already existed.
- #67: Query `GRANT`/`REVOKE` were skipped at normalize, leaving the MariaDB `ddl` GTID group open; the next legal GTID hit the real conflict guard. `CREATE USER` already matched the `CREATE` prefix.
- #68: `workflow export` registered `--output` without the `-o` shorthand used by `analyze`.
- #69: discovery required a non-empty numeric suffix after `--prefix`, so a complete filename missed; the no-match hint then appended `.` to the same filename.

## Local gates

CWD: `/Users/fan/GolangProjects/BinlogVisualizer`

| Command | Result |
|---|---|
| `go test ./... -count=1` | pass: 1,243 tests in 12 packages |
| `go vet ./...` | pass |
| `goreleaser check` | pass: one configuration validated |
| `go build -trimpath -o dist/binlogviz .` then `bash scripts/pack_release_archive.sh dist/binlogviz dist/binlogviz_ci_local.tar.gz` then `bash scripts/release_smoke.sh dist/binlogviz_ci_local.tar.gz` | pass; smoke analyzed the bundled sample, saved current and baseline snapshots, and ran analyze/compare/trend workflow steps |
| `check_three_level_doc.sh` | pass |
| `git diff --check` | pass |

No tests were skipped. This repository has no browser E2E suite; the packaged-archive smoke is the install-path check.

## Review

No independent review subagent was run for this delivery. Commit trailer scan on `6ecd0bb`: zero `Co-authored-by:` trailers.

## CI

- Required workflow/job: `ci` / `verify`
- Run: https://github.com/Fanduzi/BinlogVisualizer/actions/runs/33818447514
- Job: https://github.com/Fanduzi/BinlogVisualizer/actions/runs/33818447514/job/100855593061
- Head SHA: `6ecd0bbb2e2d398b634191f3b006113287a70e3e`
- Conclusion: `success`
- Cloudflare Pages check: `success` for the same SHA.

## Root worktree preservation and cleanup

Allowed untracked paths on `/Users/fan/GolangProjects/BinlogVisualizer`, unchanged and excluded from this delivery:

- `AGENTS.md.bak-pre-gitnexus-uninstall`
- `CLAUDE.md.bak-pre-gitnexus-uninstall`
- `dist/` (local gate artifact from pack/smoke; not committed)

No stash, reset, clean, or force-push was used.
