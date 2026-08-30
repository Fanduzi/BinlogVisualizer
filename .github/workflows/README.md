# GitHub Workflows

## Members

| File | Responsibility |
|------|----------------|
| `ci.yml` | Verifies tests, GoReleaser configuration, archive packaging, and the packaged smoke path on pushes to `main` and pull requests. |
| `release.yml` | Runs tests, validates `.goreleaser.yml` and Homebrew tap write access, builds darwin/linux archives via `scripts/pack_release_archive.sh`, computes checksums, publishes GitHub Releases, and synchronizes the cask on version tags. |

## Notes

- The release workflow is intentionally limited to the Phase 2 platform matrix:
  - `darwin/amd64` on `macos-15-intel`
  - `darwin/arm64` on `macos-14`
  - `linux/amd64` built in `quay.io/pypa/manylinux2014_x86_64`
  - `linux/arm64` built in `quay.io/pypa/manylinux2014_aarch64`
- The tag-triggered `release.yml` workflow is the only trusted release path. Local GoReleaser usage is limited to `goreleaser check` and optional single-target validation on the current host.
- `workflow_dispatch` exists only for pre-release pipeline validation. It is for smoke-testing the GitHub Actions path before cutting a tag, not for publishing an official release.
- Manual and tag runs both perform the real Homebrew token write check; only tag runs publish the Release and cask.
- Official publishing only happens for tag refs matching `v*`.
- Cloudflare Pages is an external GitHub integration, not a repository workflow. A successful deployment check on the release-preparation commit publishes `docs/landing/index.html` to `https://binlogviz.pages.dev`.
- GitHub Release publishing and Homebrew tap synchronization cannot be atomic across repositories. Before publishing, the workflow validates the tap token's real write access by creating and deleting a temporary branch ref; if the later tap push still fails, rerun the failed job instead of moving or recreating the tag.

## Release Checklist

Before pushing a new release tag:

- Confirm `go test ./...` passes locally.
- Update every versioned release surface in the same release-preparation commit:
  - `CHANGELOG.md`
  - install examples in `README.md` and `README_ZH.md`
  - `install.sh`
  - current release content and install commands in `docs/landing/index.html`
  - both release note files:
    - `docs/releases/release-notes-vX.Y.Z.md`
    - `docs/releases/release-notes-vX.Y.Z.zh-CN.md`
- Push the release-preparation commit to `main`, then confirm both the `verify` and `Cloudflare Pages` checks pass and the live landing page shows the target version.
- Run the `release.yml` workflow via `workflow_dispatch` after any workflow or packaging change, and verify:
  - `test` passes
  - `validate-homebrew` passes
  - both darwin build jobs pass
  - both Linux manylinux2014 build jobs pass
  - the `github-release` job is skipped when running manually
- Keep Linux release packaging aligned with the CentOS 7 / `glibc 2.17` compatibility target.
- Do not remove `-buildvcs=false` from the manylinux `go build` step unless the containerized build has verified access to VCS metadata. The `v0.8.1` release failed because Go VCS stamping could not resolve repository status inside the containerized tag build.
- Keep Linux and Darwin archives on the same extra-file set from `scripts/pack_release_archive.sh` (binary, `testdata/minimal.binlog`, `testdata/sample-binlog/mysql-bin.000001`, archive-relative `incident.yaml`).
- Keep release artifact names in sync across:
  - `release.yml`
  - `.goreleaser.yml`
  - `install.sh`
  - install examples in `README.md` and `README_ZH.md`

After pushing the version tag:

- Confirm the tag-triggered `release.yml` run succeeds, including all four build jobs and `github-release`.
- Confirm the GitHub Release is public and contains four platform archives plus the checksum manifest.
- Download one archive for the current host, verify its checksum, and confirm `binlogviz --version` reports the tag version.
- Confirm `Fanduzi/homebrew-binlogviz/Casks/binlogviz.rb` has the target version and the same four checksums.
- Recheck the public landing page and its tag-pinned release-note links.
