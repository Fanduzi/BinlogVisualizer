# Changelog

This file records user-visible changes for tagged releases.

## v0.15.0

Release date: 2026-04-11

Highlights:

- Hardened workflow trust boundary: `workflow resume` and `workflow status` now validate that `plan_path` resolves to `<output_dir>/plan.yaml` inside the workflow root before opening any file
- Added symlink escape detection so malicious manifests cannot reference files outside the workflow root
- Tightened plan path acceptance to rooted `plan.yaml` only — nested paths and renamed files are rejected
- All plan-path consumers (`resume`, `status`) now use the canonical resolved path instead of the raw manifest value

Related notes:

- [v0.15.0 release notes](docs/releases/release-notes-v0.15.0.md)
- [v0.15.0 中文发行说明](docs/releases/release-notes-v0.15.0.zh-CN.md)

## v0.14.0

Release date: 2026-04-11

Highlights:

- Added workflow-level summary aggregation so workflow manifests and HTML landing pages now surface cross-report findings and recommendations
- Added `workflow status` support for persisted `workflow_summary` in both text and JSON outputs
- Added `workflow export` for deterministic workflow handoff bundles with optional snapshot inclusion
- Added operator recommendation surfaces for compare and trend outputs
- Hardened explanation evidence refs, workflow summary contracts, and export containment/path normalization behavior

Related notes:

- [v0.14.0 release notes](docs/releases/release-notes-v0.14.0.md)
- [v0.14.0 中文发行说明](docs/releases/release-notes-v0.14.0.zh-CN.md)

## v0.9.1

Release date: 2026-04-04

Highlights:

- Fixed streaming `txn_count` propagation so table and minute summaries report real transaction counts
- Made `analyze --snapshot-name` snapshots immediately usable in `trend` even without explicit `--start` / `--end`
- Added trend fallback to `summary.start_time` / `summary.end_time` for older snapshots missing `snapshot.window`
- Turned `warnings` into a real bounded-degradation signal and surfaced it in text reports
- Upgraded `snapshot list --format text` into a readable inventory table and aligned current-version docs with `v0.9.1`

Related notes:

- [v0.9.1 release notes](docs/releases/release-notes-v0.9.1.md)
- [v0.9.1 中文发行说明](docs/releases/release-notes-v0.9.1.zh-CN.md)

## v0.8.3

Release date: 2026-04-02

Highlights:

- Added `binlogviz snapshot rename` and `binlogviz snapshot delete` for long-lived snapshot store management
- Added `snapshot list --format json` and `snapshot show --format json` for script-friendly snapshot inspection
- Added richer compare text/HTML context with snapshot input mode, source summary, filters, and requested window
- Added integration coverage for old analyze JSON import, default snapshot directory behavior, conflicts, invalid names, and missing snapshot flows
- Kept legacy `binlogviz compare <current.json> <baseline.json>` file mode and compare JSON structure backward-compatible

Related notes:

- [v0.8.3 release notes](docs/releases/release-notes-v0.8.3.md)
- [v0.8.3 中文发行说明](docs/releases/release-notes-v0.8.3.zh-CN.md)

## v0.8.0

Release date: 2026-04-02

Highlights:

- Added snapshot-aware analyze workflow with `--snapshot-name` and optional `--snapshot-dir`
- Added `binlogviz snapshot save`, `binlogviz snapshot list`, and `binlogviz snapshot show`
- Added compare snapshot mode with `--current-snapshot` and `--baseline-snapshot`
- Added top-level analyze JSON snapshot metadata plus compare JSON `current_snapshot` and `baseline_snapshot`
- Kept legacy `binlogviz compare <current.json> <baseline.json>` file mode compatible for existing automation

Related notes:

- [v0.8.0 release notes](docs/releases/release-notes-v0.8.0.md)
- [v0.8.0 中文发行说明](docs/releases/release-notes-v0.8.0.zh-CN.md)

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
