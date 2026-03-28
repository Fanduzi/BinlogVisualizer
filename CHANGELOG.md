# Changelog

This file records user-visible changes for tagged releases.

## v0.5.0

Release date: 2026-03-28

Highlights:

- Exposed previously hidden CLI flags: `--top-minutes`, `--spike-window`, `--spike-factor`, `--spike-min-rows`
- Added schema/table filtering at analysis time: `--include-schema`, `--exclude-schema`, `--include-table`, `--exclude-table`

Related notes:

- [v0.5.0 release notes](docs/releases/release-notes-v0.5.0.md)
- [v0.5.0 中文发行说明](docs/releases/release-notes-v0.5.0.zh-CN.md)

## v0.4.0

Release date: 2026-03-28

Highlights:

- Added internationalization (i18n) support for English and Chinese
- Added `--lang` flag to switch output language
- Added automatic language detection from `LANG` and `LC_ALL` environment variables
- Localized error messages, report output, and alert messages

Related notes:

- [v0.4.0 release notes](docs/releases/release-notes-v0.4.0.md)
- [v0.4.0 中文发行说明](docs/releases/release-notes-v0.4.0.zh-CN.md)

## v0.3.0

Release date: 2026-03-27

Highlights:

- Added discovery mode with `--from-dir` and `--prefix` flags to automatically discover and order binlog files from a directory
- Added `binlogviz version` command (prints ASCII logo + version) and `--version` flag (prints version only)
- Restructured documentation into concept/recipe/reference sections with bilingual coverage
- Improved `validateFiles` error messages to distinguish "file not found" from other access errors

Related notes:

- [v0.3.0 release notes](docs/releases/release-notes-v0.3.0.md)
- [v0.3.0 中文发行说明](docs/releases/release-notes-v0.3.0.zh-CN.md)

## v0.2.3

Release date: 2026-03-24

Highlights:

- Added aggregate parse progress for `binlogviz analyze`, based on the summed size of ordered input binlog files
- Kept progress and finalization status on `stderr` so text and JSON reports remain clean on `stdout`
- Added parser progress plumbing and regression coverage for duplicate input paths and output-stream separation

Related notes:

- [v0.2.3 release notes](docs/releases/release-notes-v0.2.3.md)
- [v0.2.3 中文发行说明](docs/releases/release-notes-v0.2.3.zh-CN.md)

## v0.2.2

Release date: 2026-03-19

Highlights:

- Raised the documented and enforced Go toolchain requirement to `1.26.1`
- Added a Chinese repository README
- Added repository-level `CHANGELOG.md` and `SECURITY.md`
- Updated top-level documentation navigation and release entry links

Related notes:

- [v0.2.2 release notes](docs/releases/release-notes-v0.2.2.md)
- [v0.2.2 中文发行说明](docs/releases/release-notes-v0.2.2.zh-CN.md)

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

- [v0.2.1 release notes](docs/releases/release-notes-v0.2.1.md)
- [v0.2.1 中文发行说明](docs/releases/release-notes-v0.2.1.zh-CN.md)

## v0.2.0

This tag was superseded and is not a supported public release.
