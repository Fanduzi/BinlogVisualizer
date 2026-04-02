# BinlogViz v0.8.0 Release Notes

Release date: 2026-04-02

## Scope

`v0.8.0` adds a first-class snapshot workflow on top of the existing analyze and compare commands. This release focuses on making it easy to save named analysis windows, inspect them later, and compare them without manually managing JSON file paths.

## Highlights

- `binlogviz analyze --format json --snapshot-name <name>` saves the exact JSON report to the default snapshot store while still writing the payload to `stdout`
- default snapshot store at `~/.binlogviz/snapshots`, with `--snapshot-dir` as an override for analyze, snapshot commands, and compare snapshot mode
- new `binlogviz snapshot save <report.json> --name <name>` command for importing an existing analyze JSON report into the snapshot store
- new `binlogviz snapshot list` command for enumerating saved snapshots in stable name order
- new `binlogviz snapshot show <name>` command for printing snapshot metadata and a compact result summary
- new `binlogviz compare --current-snapshot <name> --baseline-snapshot <name>` mode for direct snapshot-to-snapshot comparisons
- compare JSON now includes `current_snapshot` and `baseline_snapshot` when the input reports carry analyze snapshot metadata

## Output Contract Notes

- analyze JSON gains an optional top-level `snapshot` object when `--snapshot-name` is used
- snapshot saves report `Saved snapshot "<name>" to <path>` on `stderr`
- compare file mode remains supported for existing automation and previously exported reports
- compare falls back to `current` / `baseline` labels when the input reports do not contain snapshot metadata

## Packaging Notes

- Planned release artifacts:
  - `darwin/amd64`
  - `darwin/arm64`
  - `linux/amd64`
  - `linux/arm64`
- Release downloads include a checksums manifest.
- GitHub Release remains the primary installation path for packaged binaries.
- Source builds remain available as a fallback for local environments.
