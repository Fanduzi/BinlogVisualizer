# BinlogViz v0.9.1 Release Notes

Release date: 2026-04-04

## Scope

`v0.9.1` is a workflow hardening release on top of `v0.9.0`. This patch focuses on making the advertised `analyze -> snapshot -> compare -> trend` path trustworthy by default, especially when teams save snapshots directly from `analyze` and keep older reports in the same history store.

## Highlights

- fixed real `txn_count` propagation in the streaming analyzer so table-level and minute-level transaction counts no longer collapse to zero in fixture-backed runs
- `analyze --snapshot-name` now writes an effective snapshot window even when users do not pass explicit `--start` and `--end`, which makes analyze-generated snapshots immediately usable in `trend`
- trend now falls back to `summary.start_time` and `summary.end_time` for older snapshots whose `snapshot.window` fields are missing
- `warnings` now reports truncated query-context degradations in JSON, snapshot-backed workflows, and text reports instead of staying hardcoded at `0`
- `snapshot list --format text` now renders a readable inventory table with name, label, created time, input mode, and window summary

## Compatibility Notes

- `v0.9.0` snapshots that already contain `snapshot.window.start_time` continue to work unchanged
- older snapshots without `snapshot.window.start_time` can now participate in `trend` when `summary.start_time` is present
- `compare` and `snapshot show` continue to accept the existing snapshot JSON structure; this release only strengthens fallback and presentation behavior
- text and JSON outputs remain backward-compatible, with `warnings` now carrying a real non-zero value when analysis had bounded query-context loss

## Breaking Changes

None.
