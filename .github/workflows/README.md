# GitHub Workflows

## Members

| File | Responsibility |
|------|----------------|
| `release.yml` | Runs tests, validates `.goreleaser.yml`, builds the darwin/linux release archives, computes checksums, and publishes GitHub Releases on version tags. |

## Notes

- The release workflow is intentionally limited to the Phase 2 platform matrix:
  - `darwin/amd64` on `macos-15-intel`
  - `darwin/arm64` on `macos-14`
  - `linux/amd64` built in `quay.io/pypa/manylinux2014_x86_64`
  - `linux/arm64` built in `quay.io/pypa/manylinux2014_aarch64`
- The tag-triggered `release.yml` workflow is the only trusted release path. Local GoReleaser usage is limited to `goreleaser check` and optional single-target validation on the current host.
- `workflow_dispatch` exists only for pre-release pipeline validation. It is for smoke-testing the GitHub Actions path before cutting a tag, not for publishing an official release.
- Official publishing only happens for tag refs matching `v*`.

## Release Checklist

Before pushing a new release tag:

- Confirm `go test ./...` passes locally.
- Confirm the target version has both release note files:
  - `docs/releases/release-notes-vX.Y.Z.md`
  - `docs/releases/release-notes-vX.Y.Z.zh-CN.md`
- Run the `release.yml` workflow via `workflow_dispatch` after any workflow or packaging change, and verify:
  - `test` passes
  - both darwin build jobs pass
  - both Linux manylinux2014 build jobs pass
  - the `github-release` job is skipped when running manually
- Keep Linux release packaging aligned with the CentOS 7 / `glibc 2.17` compatibility target.
- Do not remove `-buildvcs=false` from the manylinux `go build` step unless the containerized build has verified access to VCS metadata. The `v0.8.1` release failed because Go VCS stamping could not resolve repository status inside the containerized tag build.
- Keep release artifact names in sync across:
  - `release.yml`
  - `.goreleaser.yml`
  - `install.sh`
  - install examples in `README.md` and `README_ZH.md`
