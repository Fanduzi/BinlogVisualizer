# BinlogViz v0.7.0 Release Notes

Release date: 2026-04-02

## Scope

`v0.7.0` adds the visual compare workflow on top of the existing local binlog analysis CLI. This release is centered on comparing two `binlogviz analyze --format json` reports and presenting the delta in text, JSON, and chart-based HTML output for DBA-style workload review.

## Highlights

- New `binlogviz compare <current.json> <baseline.json>` command
- Compare input loading and validation for BinlogViz JSON reports
- Compare result modeling with summary, table, operation, and alert deltas
- Text and JSON compare renderers for terminal and automation workflows
- Chart-based HTML compare report powered by embedded ECharts
- Minimal `analyze -> compare` integration coverage plus locale-stable command assertions
- Coverage uplift that raises total Go coverage above the 80% quality target

## Compare Outputs

- `--format text`
  - concise terminal-friendly compare summary
- `--format json`
  - machine-readable compare structure for downstream tooling
- `--format html`
  - visual compare report with charts for rows, transactions, warnings, top table deltas, operation mix, and alerts

## Packaging Notes

- Planned release artifacts:
  - `darwin/amd64`
  - `darwin/arm64`
  - `linux/amd64`
  - `linux/arm64`
- Release downloads include a checksums manifest.
- GitHub Release remains the primary installation path for packaged binaries.
- Source builds remain available as a fallback for local environments.
