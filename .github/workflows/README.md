# GitHub Workflows

## Members

| File | Responsibility |
|------|----------------|
| `release.yml` | Runs tests, validates `.goreleaser.yml`, builds native release archives via `goreleaser build --single-target`, computes checksums, and publishes GitHub Releases on version tags. |

## Notes

- The release workflow is intentionally limited to the Phase 2 platform matrix:
  - `darwin/amd64` on `macos-15-intel`
  - `darwin/arm64` on `macos-14`
  - `linux/amd64`
  - `linux/arm64`
- Native GitHub runners are the only trusted multi-platform release path; local GoReleaser usage is limited to `goreleaser check` and optional single-target validation on the current host.
- Publishing only happens for tag refs matching `v*`.
