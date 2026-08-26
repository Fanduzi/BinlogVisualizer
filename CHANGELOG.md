# Changelog

This file records user-visible changes for tagged releases.

## v0.22.0

Release date: 2026-08-26

Highlights:

- Redesigned HTML report UI with dark-mode aesthetic, glowing status indicators, and responsive glassmorphism cards across Analyze, Compare, and Trend reports
- Added sortable `BINLOG BYTES` physical volume metrics in Top Tables, transaction diagnostics cards, and hot interval summaries
- Added synchronized multi-chart linkage (`echarts.connect`) across shared-dimension timelines
- Added interactive mouse-selection range zoom (Toolbox Area Zoom & Restore, bottom DataZoom Slider, and wheel zoom)
- Added floating back-to-top button, theme switcher, and one-click `mysqlbinlog` command copy buttons

Related notes:

- [v0.22.0 release notes](docs/releases/release-notes-v0.22.0.md)
- [v0.22.0 中文发行说明](docs/releases/release-notes-v0.22.0.zh-CN.md)

## v0.19.0

Release date: 2026-04-19

Highlights:

- Changed default `--detail-store` from `duckdb` to `none`; `binlogviz analyze` no longer creates a DuckDB temp store by default
- `none` mode does not create a DuckDB database, does not call `ResolveTransactionQuerySQL`, and does not write to disk
- `--detail-store duckdb` remains available as an experimental/debug compatibility backend
- Default `none` reduces max RSS by about 38% on the 988 MB real binlog sample (199–203 MB vs 320–323 MB); JSON output is identical to `duckdb` mode across all 10 top-level report fields

Related notes:

- [v0.19.0 release notes](docs/releases/release-notes-v0.19.0.md)
- [v0.19.0 中文发行说明](docs/releases/release-notes-v0.19.0.zh-CN.md)

## v0.18.1

Release date: 2026-04-19

Highlights:

- Added an external real-binlog benchmark gated by `BINLOGVIZ_REAL_BINLOG`
- Reduced finalize allocation hotspots and store transaction scan memory
- Tuned DuckDB transaction batch flushes from 5,000 to 10,000 rows

Related notes:

- [v0.18.1 release notes](docs/releases/release-notes-v0.18.1.md)
- [v0.18.1 中文发行说明](docs/releases/release-notes-v0.18.1.zh-CN.md)

## v0.18.0

Release date: 2026-04-16

Highlights:

- Added directory-based analyze discovery with `--from-dir`, `--prefix`, `--start`, and `--end`
- Added richer analyzer diagnostics, redesigned HTML reports, and compare/trend diagnostic deltas
- Complete English/Chinese localization across HTML surfaces

Related notes:

- [v0.18.0 release notes](docs/releases/release-notes-v0.18.0.md)

## v0.17.0

Release date: 2026-04-13

Highlights:

- Added bounded cross-window pattern drilldowns for both `compare` and `trend`, so high-signal pattern changes now carry short explanatory context across windows
- Added top-level `pattern_drilldowns` arrays to compare and trend JSON outputs; the field is always present and remains empty when nothing qualifies
- Compare drilldowns now explain new patterns, disappeared patterns, and dominant row-movement shifts with bounded key points
- Trend drilldowns now explain rising, falling, and concentrated cross-window share shifts with bounded key points
- HTML compare and trend reports now render labeled drilldown detail cards beneath the pattern sections
- Bounded payloads remain enforced: at most 2 drilldowns per report and at most 2 key points per drilldown

Related notes:

- [v0.17.0 release notes](docs/releases/release-notes-v0.17.0.md)
- [v0.17.0 中文发行说明](docs/releases/release-notes-v0.17.0.zh-CN.md)

## v0.16.0

Release date: 2026-04-12

Highlights:

- Added selective pattern drilldowns: an optional explanatory layer that appears when one or more write patterns cross a high-signal threshold
- New top-level JSON field `pattern_drilldowns` (always present as array, empty when no pattern qualifies)
- Text output renders indented drilldown blocks under qualifying patterns; HTML output renders collapsible drilldown cards with signal flags and metric help
- Bounded payloads: at most 2 drilldowns per analysis, 2 workload peak minutes per drilldown, 2 workload transactions per drilldown
- Mixed signal model: dominance (share thresholds) + anomaly (table-aligned alerts, high rows-per-txn ratio)
- Workload-scoped wording: busiest minutes and representative transactions are described as window-level context, not pattern-owned data

Related notes:

- [v0.16.0 release notes](docs/releases/release-notes-v0.16.0.md)
- [v0.16.0 中文发行说明](docs/releases/release-notes-v0.16.0.zh-CN.md)

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
