# Delivery: v0.22.0 dogfood issues #33–#40

## Refs

- Repository: `Fanduzi/BinlogVisualizer`
- Base (`origin/main` at gate time): `2acdc80411c409d506973bc17b1523c927ff2091`
- Candidate (CI-green PR head before this evidence commit): `a035fa499add24636b7edc3176458c13cb8bf79a`
- Merge type: fast-forward only (`origin/main` is an ancestor of the candidate)
- Push range: `2acdc80411c409d506973bc17b1523c927ff2091..` candidate then this evidence commit
- PR: https://github.com/Fanduzi/BinlogVisualizer/pull/41

## Local gates (CWD `/Users/fan/GolangProjects/BinlogVisualizer/.worktrees/integration-dogfood`, SHA `a035fa499add24636b7edc3176458c13cb8bf79a`)

| Command | Result |
|---|---|
| `go test ./... -count=1` | pass (packages with tests all `ok`; `binlogviz` and `internal/version` have no test files) |
| `go vet ./...` | pass (exit 0) |
| `go build -trimpath -o dist/binlogviz .` then `bash scripts/pack_release_archive.sh dist/binlogviz dist/binlogviz_ci_linux_amd64.tar.gz` then `bash scripts/release_smoke.sh dist/binlogviz_ci_linux_amd64.tar.gz` | pass; smoke analyzed bundled sample and ran bundled `incident.yaml` |

No tests skipped. No E2E browser suite exists in this repo; the packaging smoke is the install-path check.

## Review

- Standards + Spec on each issue worktree vs `2acdc80`: pass
- Unresolved P1: 0
- Unresolved P2: 0
- Documented nits (not blockers): `#33` `README_ZH.md`/`--help` still mention always-file HTML; `#34` unquoted paths with spaces; `#39` test name still `WarnsOnStderr`

## CI (required)

- Required job: `verify` (`.github/workflows/ci.yml`)
- PR run: https://github.com/Fanduzi/BinlogVisualizer/actions/runs/33227634825
- Required job URL: https://github.com/Fanduzi/BinlogVisualizer/actions/runs/33227634825/job/99034328758
- Conclusion at gate time: `verify` SUCCESS
- Optional: Cloudflare Pages SUCCESS; CodeRabbit SUCCESS with “Review skipped: manual review required for this OSS repository” (not a required job)

## Root worktree preservation

Allowed dirty/untracked on root `/Users/fan/GolangProjects/BinlogVisualizer` (not part of this delivery):

- `.codegraph/`
- `.omo/`
- `.scratch/`
- `AGENTS.md.bak-pre-gitnexus-uninstall`
- `CLAUDE.md.bak-pre-gitnexus-uninstall`
- `CONTEXT.md` (copy committed on the candidate; root copy remains untracked because it was authored on the root checkout)
- `docs/adr/`, `docs/agents/` on the root checkout (same)
- gitignored `CLAUDE.md` / `AGENTS.md`

No stash, reset, or clean of those paths.

## Cleanup (intentional)

Preserve until after merge+CI on `main`: issue worktrees `.worktrees/issue-33` … `issue-40`, integration worktree, and branches `fix/issue-3*` / `integration/v0.22-dogfood`.
