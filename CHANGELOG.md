# Changelog

This file records user-visible changes for tagged releases.

## v0.2.1

Release date: 2026-03-19

Highlights:

- Switched the analysis pipeline to true streaming command execution:
  - `ParseFiles -> NormalizeRawEvent -> analyzer.Consume -> analyzer.Finalize`
- Added DuckDB-backed finalize-time result assembly for high-cardinality analysis data
- Added `--sql-context summary|off|full`
- Added bounded `Rows_query_log_event` SQL context support
- Added real binlog fixture coverage, broader streaming benchmarks, and release packaging workflow
- Fixed the release pipeline so GitHub Releases can publish downloadable artifacts

Related notes:

- [v0.2.1 release notes](docs/release-notes-v0.2.1.md)
- [v0.2.1 中文发行说明](docs/release-notes-v0.2.1.zh-CN.md)

## v0.2.0

This tag was superseded and is not a supported public release.

