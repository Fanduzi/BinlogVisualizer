# BinlogViz v0.8.1 Release Notes

Release date: 2026-04-02

## Scope

`v0.8.1` is a release hardening patch on top of the snapshot workflow introduced in `v0.8.0`. This release focuses on fixing stale version references in public-facing docs and tightening the Linux release pipeline so packaged binaries target an older glibc baseline.

## Highlights

- landing page hero copy and command examples now reflect `v0.8.0` snapshot workflow instead of the older visual-compare-only story
- `README.md` and `README_ZH.md` installation examples now point at the current release line instead of stale `v0.6.0` artifact URLs
- Linux release builds now run inside manylinux2014 userspace containers instead of inheriting the host runner glibc level
- release workflow adds a glibc symbol baseline check so Linux artifacts fail fast if they drift above `glibc 2.17`

## Compatibility Notes

- Linux release packaging is now aimed at CentOS 7 / `glibc 2.17` compatibility for both `linux/amd64` and `linux/arm64` build jobs
- macOS release packaging remains on native GitHub-hosted runners
- no CLI flags, report contracts, or snapshot commands changed in this patch release

## Breaking Changes

None.
