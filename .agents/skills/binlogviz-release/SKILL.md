---
name: binlogviz-release
description: Release BinlogViz. One prep PR (bilingual notes, the CHANGELOG Unreleased section renamed to the version), make release-check, fast-forward land, annotated "Release vX.Y.Z" tag by Fan(), watch release.yml, verify assets and the Homebrew cask, then check the Cloudflare Pages deploy and that the version-free landing page is live. Use when asked to release, tag or ship BinlogViz (发布, 发版, 上线, 打tag).
---

# BinlogViz Release

Publish BinlogViz from a tag through `release.yml` (native macOS runners,
manylinux2014 Linux builds, Homebrew cask sync). The steps follow the
maintainer's original `go-release` workflow (version, merge, notes, surface
audit, commit, tag, push, clean), adapted to this repo: a reviewed prep PR,
`make release-check`, fast-forward-only landing, and checks after publishing.
The human checklist is [docs/RELEASING.md](../../../docs/RELEASING.md);
workflow details are in [.github/workflows/README.md](../../../.github/workflows/README.md).

Passing tests does not make a release. Verify the whole chain:
`prep commit -> tag -> release.yml -> archives -> GitHub Release assets -> Homebrew cask -> install docs`.

## Hard rules

- Every commit has author **and** committer `Fan() <18501341937@163.com>`.
  Every tag has tagger `Fan() <18501341937@163.com>`.
- No AI-tool attribution or names in commits, branches, PR titles or bodies,
  tag messages or release notes. No `Co-authored-by` or `Generated-by` trailers.
- Never force-push. Never move, delete or recreate a pushed tag or a published
  GitHub Release. A broken release is fixed by the next patch version. If only
  the Homebrew sync failed, re-run that one job.
- `main` moves only by fast-forward:
  `gh api -X PATCH repos/Fanduzi/BinlogVisualizer/git/refs/heads/main -f sha=<sha> -F force=false`.
  No merge button, no `git push origin main`, no merge commits.
- Do not land while another agent is still pushing to the branch.
- Push only the one tag: `git push origin vX.Y.Z`. Never `--tags`.
- Never commit delivery evidence (QA transcripts, `docs/agents/delivery-*.md`,
  run logs, screenshots taken as proof). Evidence goes in the PR or QA thread.

## Cadence

- At most one release per day.
- Release only when `main` has at least one user-visible change since the
  last tag (`CHANGELOG.md` `## [Unreleased]` is not empty).
- A P0/P1 fix may ship as a hotfix release right away, even on the same day.
- Never release once per PR. A release is one batch: one tip QA, one prep PR,
  one tag.

## Step 0: Pre-flight

```bash
git fetch origin --tags
git switch main && git pull --ff-only origin main
git status --porcelain              # must be empty
git rev-parse HEAD origin/main      # must be equal
LAST_TAG=$(git describe --tags --abbrev=0)
git log -1 --format=%cs "$LAST_TAG" # not today, unless this is a P0/P1 hotfix
git log --oneline "${LAST_TAG}..origin/main"   # the batch; must have a user-visible change
git tag --points-at HEAD            # must be empty
gh release view vX.Y.Z -R Fanduzi/BinlogVisualizer   # must be "release not found"
git ls-remote --tags origin "vX.Y.Z*"                 # must be empty
```

- **Tip QA PASS on the exact SHA.** The QA verdict must name the current
  `origin/main` SHA and say it is OK to tag. Re-run it if `main` moved.
- Push CI (`ci.yml`: verify, release-prep self-test, vet and race, flashback
  e2e) is green on that SHA: `gh run list -R Fanduzi/BinlogVisualizer --commit <sha>`.
- If `release.yml`, `.goreleaser.yml`, `install.sh` or
  `scripts/pack_release_archive.sh` changed since `LAST_TAG`, run
  `gh workflow run release.yml -R Fanduzi/BinlogVisualizer --ref main` first.
  `test`, `validate-homebrew` and all four builds must pass, and the release
  job must be skipped.
- Identity and attribution scan over the batch; both commands must print nothing:

```bash
git log --format='%H %an <%ae> | %cn <%ce>' "${LAST_TAG}..origin/main" \
  | grep -v 'Fan() <18501341937@163.com> | Fan() <18501341937@163.com>'
git log --format='%B' "${LAST_TAG}..origin/main" \
  | grep -inE 'co-authored-by|generated-by|generated with|🤖'
```

## Step 1: Choose the version

Semver over the batch:

| Commit pattern | Bump |
|---|---|
| `feat!:`, `fix!:`, or a breaking change to output or exit codes (report_version, plan_version, schema_version, flashback script format) | MINOR while < 1.0 |
| `feat:` | MINOR |
| `fix:` and everything else | PATCH |

Put the reasoning and the chosen `vX.Y.Z` in the prep PR body.

## Step 2: Run the gates on the candidate SHA

On a clean checkout of the candidate SHA. A skip counts as a failure.

```bash
go test ./...
go vet ./...
go test -race ./...
goreleaser check
make release-check-test
# Packaged smoke test, as in the ci.yml verify job:
mkdir -p dist
COMMIT=$(git rev-parse HEAD)
COMMIT_TIME="$(TZ=UTC0 git log -1 --format=%cd --date=format-local:%Y-%m-%dT%H:%M:%SZ HEAD)"
go build -buildvcs=false -trimpath \
  -ldflags "-X binlogviz/internal/version.Commit=${COMMIT} -X binlogviz/internal/version.CommitTime=${COMMIT_TIME}" \
  -o dist/binlogviz .
bash scripts/pack_release_archive.sh dist/binlogviz dist/binlogviz_local.tar.gz
BINLOGVIZ_SMOKE_EXPECT_COMMIT=$COMMIT BINLOGVIZ_SMOKE_REQUIRE_COMMIT=1 \
  bash scripts/release_smoke.sh dist/binlogviz_local.tar.gz
```

Flashback against a real MySQL runs in the `flashback e2e` job of `ci.yml`
(MySQL 8.0, `BINLOGVIZ_FLASHBACK_E2E=1`; every named test must print
`--- PASS`). It must be green on the candidate SHA, and `release.yml` runs it
again on the tag. If the batch changed analyzer output or memory use, the tip
QA must have covered the regression corpus.

## Step 3: The prep PR (one PR, one commit)

Branch `release/vX.Y.Z`. One commit, `docs: prepare BinlogViz vX.Y.Z`.
Release surfaces only; no code.

### 3a. Release notes (EN and zh)

The paths and H1 lines are fixed by `release.yml` and
`scripts/check-release-prep.sh`. Read them in the script before writing.

- `docs/releases/release-notes-vX.Y.Z.md`, H1 `# BinlogViz vX.Y.Z Release Notes`
- `docs/releases/release-notes-vX.Y.Z.zh-CN.md`, H1 `# BinlogViz vX.Y.Z 发布说明`

15 to 40 lines each. No QA transcript, no PR dump, and no benchmark tables
beyond one headline number.

English template:

```markdown
# BinlogViz vX.Y.Z Release Notes

Release date: YYYY-MM-DD

## Overview

<one or two sentences: what this release is for>

## New Features

- <one bullet per user-visible change>

## Bug Fixes

- <one bullet each, or "None.">

## Compatibility Notes

- report_version / plan_version / schema_version: <unchanged | N to M>
- Exit codes and CLI flags: <unchanged | what changed>
- Known issues: <#NNN, one line each, or "None.">

## Breaking Changes

None.
```

Chinese template:

```markdown
# BinlogViz vX.Y.Z 发布说明

发布日期：YYYY-MM-DD

## 概述

## 新功能

## Bug 修复

## 兼容性说明

## 破坏性变更

无。
```

### 3b. CHANGELOG.md

Keep a Changelog style. PRs append their user-visible changes to
`## [Unreleased]` as they land. The prep commit:

1. Renames `## [Unreleased]` to `## [vX.Y.Z] - YYYY-MM-DD`.
2. Adds a fresh, empty `## [Unreleased]` above it.
3. Trims the entry to at most 15 lines, user-facing only (`### Added`,
   `### Changed`, `### Fixed`), and ends it with links to both notes files.

Do not rewrite older entries in a release PR.

### 3c. Install surfaces

- `README.md`, `README_ZH.md` and `install.sh` install from `releases/latest`.
  They carry no version pins, and a release does not add any.
- Do not add "What's new in vX", per-version sections or changelog text to
  either README. The README size and section guard in CI must stay green.
- Keep artifact names in sync across `release.yml`, `.goreleaser.yml`,
  `install.sh` and the README.
- The Homebrew cask is generated and pushed by `release.yml`. Never edit
  `Fanduzi/homebrew-binlogviz` by hand during a release.

### 3d. Landing page (`docs/landing/index.html`)

Do not touch the landing page for a release. It names no version, has no
release cards, and installs the latest release. Edit it only when the batch
changed what the product does, and then in its own PR, not the prep PR.

The landing page is a product page, not a release log and not a README dump.
`TestLandingPageHasNoReleaseHistory` fails on any `vX.Y.Z`, a release
section, a reveal threshold above 0, or a page over 128 KiB.

```bash
rg -n 'v[0-9]+\.[0-9]+\.[0-9]+' docs/landing/index.html   # must print nothing
```

### 3e. Surface audit

```bash
NEXT=X.Y.Z
SURFACE="README.md README_ZH.md install.sh docs/landing/index.html"
rg -n 'v?[0-9]+\.[0-9]+\.[0-9]+' $SURFACE | grep -v 'release-notes-v'
```

Every hit must be a dependency version (MySQL 8.0, Go 1.x, manylinux2014,
glibc 2.17) or an `install.sh` usage example. A BinlogViz release version in
any of these files is a pin that should not be there. Older versions may
remain only in history: `docs/releases/*`, older CHANGELOG entries, ADRs and
plans.

### 3f. The gate

```bash
go test ./cmd/binlogviz/ -run 'TestLandingPageHasNoReleaseHistory'
make release-check VERSION=vX.Y.Z
git diff --check origin/main...HEAD
```

`make release-check` must print
`[release-check] all release surfaces ready for vX.Y.Z`. The `release-prep`
job in `release.yml` runs the same check on the tag before any build starts.

### 3g. Open and land the prep PR

1. Push the branch and open the PR, titled with the commit subject. The body
   holds the version reasoning, the batch (PR numbers, one line each) and the
   gate results. Apply a clean body with
   `gh api -X PATCH repos/Fanduzi/BinlogVisualizer/pulls/N -F body=@body.md`.
2. All PR checks are green on the head SHA.
3. A quick QA pass on the prep PR: content and links.
4. Land by fast-forward only:

```bash
gh api repos/Fanduzi/BinlogVisualizer/compare/main...release/vX.Y.Z --jq '.behind_by,.ahead_by'   # 0 and 1
PREP_SHA=$(gh api repos/Fanduzi/BinlogVisualizer/git/refs/heads/release/vX.Y.Z --jq .object.sha)
gh api -X PATCH repos/Fanduzi/BinlogVisualizer/git/refs/heads/main -f sha=$PREP_SHA -F force=false
gh api -X DELETE repos/Fanduzi/BinlogVisualizer/git/refs/heads/release/vX.Y.Z
```

5. Push CI on `main` at `$PREP_SHA` is green, including `verify` and
   `Cloudflare Pages`.

## Step 4: Tag

Annotated, on the prep commit:

```bash
git fetch origin && git switch main && git pull --ff-only origin main
test "$(git rev-parse HEAD)" = "$PREP_SHA"
make release-check VERSION=vX.Y.Z REF=HEAD
GIT_COMMITTER_NAME='Fan()' GIT_COMMITTER_EMAIL='18501341937@163.com' \
  git -c user.name='Fan()' -c user.email='18501341937@163.com' \
  tag -a vX.Y.Z -m "Release vX.Y.Z" "$PREP_SHA"
git cat-file -p vX.Y.Z | head -5   # tagger Fan() <18501341937@163.com>; message "Release vX.Y.Z"
git push origin vX.Y.Z             # starts the release
```

## Step 5: Watch release.yml

```bash
gh run list -R Fanduzi/BinlogVisualizer --workflow release.yml --limit 3
gh run watch <run-id> -R Fanduzi/BinlogVisualizer --exit-status
gh run view <run-id> -R Fanduzi/BinlogVisualizer --json status,conclusion,jobs,url
```

All jobs must succeed: `test`, `ci` (verify and flashback e2e on the tagged
commit), `release-prep`, `validate-homebrew`, the four build jobs, and
`release` (publish and cask sync). A green build is an Actions artifact, not a
Release asset. On failure, find the first failed job and diagnose it before
changing anything. If only the tap push failed, re-run that job. Otherwise fix
on `main` and ship the next patch.

## Step 6: Verify the release and the cask

```bash
gh release view vX.Y.Z -R Fanduzi/BinlogVisualizer --json name,tagName,isDraft,isPrerelease,assets,url
```

- Public, not a draft or prerelease. The body is the English notes.
- Exactly 5 assets: `binlogviz_X.Y.Z_{darwin,linux}_{amd64,arm64}.tar.gz` and
  `binlogviz_X.Y.Z_checksums.txt`.

```bash
d=$(mktemp -d) && cd "$d"
gh release download vX.Y.Z -R Fanduzi/BinlogVisualizer -p 'binlogviz_X.Y.Z_linux_amd64.tar.gz' -p 'binlogviz_X.Y.Z_checksums.txt'
sha256sum -c --ignore-missing binlogviz_X.Y.Z_checksums.txt   # ": OK"
tar -xzf binlogviz_X.Y.Z_linux_amd64.tar.gz
./binlogviz --version                                        # reports X.Y.Z and commit $PREP_SHA
./binlogviz analyze testdata/minimal.binlog --format text >/dev/null
gh api repos/Fanduzi/homebrew-binlogviz/contents/Casks/binlogviz.rb --jq .content | base64 -d | grep -E 'version|sha256|url'
```

The cask has `version "X.Y.Z"`, both darwin URLs point at the vX.Y.Z assets,
and both sha256 values match the darwin lines in the checksums file.

## Step 7: Verify the Cloudflare Pages deploy and the live page

```bash
gh api repos/Fanduzi/BinlogVisualizer/commits/$PREP_SHA/check-runs \
  --jq '.check_runs[] | select(.name | test("Cloudflare Pages")) | "\(.name) \(.conclusion)"'   # success
curl -fsSL https://binlogviz.pages.dev -o /tmp/binlogviz-landing.html
rg -c 'main/install.sh' /tmp/binlogviz-landing.html                # >= 1 (installs the latest release)
rg -n 'v[0-9]+\.[0-9]+\.[0-9]+' /tmp/binlogviz-landing.html       # must print nothing
```

The live page must have no version string. Open it once in a browser in EN and
zh: no broken images, no blank gaps.

## Step 8: After the release

- `vX.Y.Z^{}` equals `$PREP_SHA`. The previous tag is unchanged.
- Scan the tag message and release body for AI-tool attribution.
- No follow-up "point docs at vX.Y.Z" PR should be needed. If one is, fix
  this skill or `check-release-prep.sh`.
- `git worktree prune`; delete the release branch if it is still there.
- No README or landing accretion: a release adds nothing to either.
- Report one status: `released and verified`, or `blocked with evidence`
  (the first unmet step, the exact SHA, the command output, the safe next step).

## Checklist

Before the tag:
- [ ] No release yet today (or a P0/P1 hotfix); a user-visible change since the last tag
- [ ] Tip QA PASS on the exact `main` SHA; push CI green, including flashback e2e
- [ ] Clean tree; behind 0; the tag and release do not exist
- [ ] Batch commits by Fan(), no AI-tool attribution
- [ ] Step 2 gates pass
- [ ] One prep commit: EN and zh notes, `Unreleased` renamed (≤ 15 lines)
- [ ] README, install.sh and landing page untouched; no evidence files committed
- [ ] `make release-check VERSION=vX.Y.Z` passes
- [ ] Prep PR green, checked, fast-forwarded; push CI and Cloudflare Pages green

After the tag:
- [ ] Annotated `Release vX.Y.Z` tag by Fan() on the prep commit, pushed alone
- [ ] release.yml fully green
- [ ] 4 archives and the checksums file; checksum OK; `binlogviz --version` reports X.Y.Z
- [ ] Homebrew cask version and sha256 values match
- [ ] Cloudflare Pages check succeeded; binlogviz.pages.dev is live with no version string

## Recovery

| Problem | What to do |
|---|---|
| `make release-check` fails | Fix the named surface in the prep PR; re-run |
| Prep branch is behind `main` | Rebase the prep branch; re-run the gates |
| `release-prep` or a build fails on the tag | Diagnose; fix on `main`; ship vX.Y.(Z+1). Never move the tag |
| Release published, cask sync failed | Re-run the failed job only |
| Live landing page shows a version or is stale | Check the Cloudflare Pages check; fix through a PR; verify again |
| The tag already exists | Stop. Do not delete it. Choose the next version |
