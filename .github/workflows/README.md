# GitHub Workflows

## Members

| File | Responsibility |
|------|----------------|
| `release.yml` | Runs tests, builds native release archives for the supported darwin/linux amd64/arm64 matrix, computes checksums, and publishes GitHub Releases on version tags. |

## Notes

- The release workflow is intentionally limited to the Phase 2 platform matrix:
  - `darwin/amd64`
  - `darwin/arm64`
  - `linux/amd64`
  - `linux/arm64`
- Publishing only happens for tag refs matching `v*`.
