# Releasing BinlogViz

The full step-by-step procedure is the release skill:
[.agents/skills/binlogviz-release/SKILL.md](../.agents/skills/binlogviz-release/SKILL.md).
Workflow details (runners, manylinux2014, `-buildvcs=false`, the Homebrew
token) are in [.github/workflows/README.md](../.github/workflows/README.md).
This page is the short checklist. If they disagree, fix them in the same PR.

## Rules

- At most one release per day, and only when `main` has at least one
  user-visible change since the last tag. A P0/P1 fix may ship as a hotfix
  right away. Never release once per PR.
- `main` moves only by fast-forward. No merge commits, no force-push.
- Tags are annotated, with the message `Release vX.Y.Z` and the tagger
  `Fan() <18501341937@163.com>`, on the prep commit. A pushed tag is never moved
  or deleted; a bad release is fixed by the next patch. If only the Homebrew
  sync failed, re-run that job.
- No AI-tool attribution or names in commits, branches, PR titles or bodies.
- No delivery evidence files (QA transcripts, run logs) in the repository.

## 1. Gates

On a clean checkout of the `main` commit that tip QA passed:

```bash
go test ./...
go vet ./...
go test -race ./...
goreleaser check
make release-check-test
```

Then build and smoke-test a packaged archive the way `ci.yml` does
(`scripts/pack_release_archive.sh`, then `scripts/release_smoke.sh`). The
`flashback e2e` job must be green on the same commit. After any change to
`release.yml`, `.goreleaser.yml`, `install.sh` or the packing script, run
`release.yml` with `workflow_dispatch` first.

## 2. The prep PR

One commit, `docs: prepare BinlogViz vX.Y.Z`:

| File | Change |
|---|---|
| `docs/releases/release-notes-vX.Y.Z.md` | `# BinlogViz vX.Y.Z Release Notes`, release date, Overview, New Features, Bug Fixes, Compatibility Notes, Breaking Changes. 15 to 40 lines. |
| `docs/releases/release-notes-vX.Y.Z.zh-CN.md` | `# BinlogViz vX.Y.Z 发布说明`, same content. |
| `CHANGELOG.md` | Rename `## [Unreleased]` to `## [vX.Y.Z] - YYYY-MM-DD` (15 lines at most) and add a fresh empty `## [Unreleased]`. |

Do not change `README.md`, `README_ZH.md`, `install.sh` or
`docs/landing/index.html` for a release. They install from `releases/latest`
and name no version.

Then run `make release-check VERSION=vX.Y.Z`. It must print
`all release surfaces ready for vX.Y.Z`.

## 3. Tag

After the prep PR is fast-forwarded to `main` and its `verify` and
`Cloudflare Pages` checks pass:

```bash
git pull --ff-only origin main
make release-check VERSION=vX.Y.Z REF=HEAD
git tag -a vX.Y.Z -m "Release vX.Y.Z"
git push origin vX.Y.Z
```

## 4. Verify

- The `release.yml` run is green: test, ci, release-prep, validate-homebrew,
  all four builds, and release.
- `gh release view vX.Y.Z` lists 4 archives and `binlogviz_X.Y.Z_checksums.txt`.
- One downloaded archive passes its checksum, and `binlogviz --version`
  reports X.Y.Z.
- `Casks/binlogviz.rb` in `Fanduzi/homebrew-binlogviz` has version X.Y.Z and
  matching darwin sha256 values.
- The `Cloudflare Pages` check succeeded, and https://binlogviz.pages.dev is
  live with no version string.
