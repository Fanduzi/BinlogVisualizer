# BinlogViz v0.8.3 Release Notes

Release date: 2026-04-02

## Scope

`v0.8.3` is a snapshot workflow hardening release on top of `v0.8.2`. This patch focuses on making saved snapshots practical for long-lived operational use, especially when teams need to script against them and keep the snapshot store organized over time.

## Highlights

- added `binlogviz snapshot rename <old> <new>` and `binlogviz snapshot delete <name>` so users can manage snapshot history without manual file operations
- added `snapshot list --format json` and `snapshot show --format json` for machine-readable snapshot inventory and inspection
- compare text and HTML output now include snapshot context such as input mode, source summary, active filters, and requested window
- integration coverage now includes old analyze JSON import, default `~/.binlogviz/snapshots` behavior, name conflicts, invalid names, and missing snapshot flows

## Compatibility Notes

- the snapshot store remains rooted at `~/.binlogviz/snapshots` by default
- legacy `binlogviz compare <current.json> <baseline.json>` file mode remains supported
- compare JSON structure remains backward-compatible; the new context is primarily added to text and HTML output
- imports from older analyze JSON files without a `snapshot` object remain supported

## Breaking Changes

None.
