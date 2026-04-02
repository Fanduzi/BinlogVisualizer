# BinlogViz v0.8.2 Release Notes

Release date: 2026-04-02

## Scope

`v0.8.2` is a release pipeline hotfix on top of `v0.8.1`. This patch only fixes the Linux manylinux2014 packaging workflow so the release jobs can complete successfully for tagged builds.

## Highlights

- Linux manylinux2014 build jobs now disable Go VCS stamping inside the containerized `go build` step
- the fix addresses the `error obtaining VCS status: exit status 128` failure that blocked the `v0.8.1` release workflow
- no packaging names, install URLs, CLI flags, or snapshot workflow behavior changed in this release

## Compatibility Notes

- Linux release packaging still targets CentOS 7 / `glibc 2.17` compatibility
- macOS release packaging remains unchanged
- this patch does not change runtime behavior of the BinlogViz CLI

## Breaking Changes

None.
