# BinlogViz v0.2.2 Release Notes

## Scope

`v0.2.2` is a repository polish release on top of the first successful public Phase 2 release.

It does not change the core analysis pipeline. Instead, it aligns the repository surface, documented toolchain requirement, and release-facing documentation with the current published state.

## Highlights

- Raised the documented and enforced Go version requirement to `1.26.1`
- Added a Chinese repository README
- Added repository-level changelog and security policy
- Updated README navigation and release-note links to match the latest published release

## Packaging Notes

- Release artifacts continue to target:
  - `darwin/amd64`
  - `darwin/arm64`
  - `linux/amd64`
  - `linux/arm64`
- Release downloads include a checksums manifest.
- Preferred installation path remains GitHub Release artifacts; source builds remain the fallback.
