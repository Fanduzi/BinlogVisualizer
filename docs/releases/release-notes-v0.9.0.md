# BinlogViz v0.9.0 Release Notes

Release date: 2026-04-03

## Scope

`v0.9.0` is a feature release built on top of `v0.8.3`. This version turns saved snapshots into a multi-point analysis workflow so teams can look at operational drift across several windows instead of staying limited to one-off analysis or pairwise compare.

## Highlights

- added `binlogviz trend` as a new top-level command for multi-snapshot trend analysis
- trend supports explicit snapshot lists and `--from-snapshots <pattern>` selection, ordered by `snapshot.window.start_time`
- added optional `--baseline-snapshot` deltas so each point can be read against a known-good reference window
- added `text`, `json`, and `html` trend reports, including overall totals, operation mix movement, and hot table change summaries
- documentation and automated coverage now include the new trend workflow and output contracts

## Compatibility Notes

- the existing `analyze`, `compare`, and `snapshot` workflows remain supported without contract changes
- trend requires snapshots to provide a valid `snapshot.window.start_time`; snapshots missing it fail fast instead of being skipped silently
- baseline snapshots are optional and do not implicitly join the analyzed trend point set

## Breaking Changes

None.
