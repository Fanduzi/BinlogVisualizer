# BinlogViz v0.12.0 Release Notes

Release date: 2026-04-10

## Scope

`v0.12.0` is a feature release centered on the change explanation layer for compare and trend reports.

This release keeps the existing analysis and workflow surface intact, but makes the output easier to act on. Compare and trend now surface the main workload drivers first, before users read through full tables, pattern lists, and charts.

## Highlights

- added `key_findings` to compare reports so the main drivers of workload change appear before detailed table, pattern, and operation sections
- added `trend_summary` to trend reports so rising patterns, falling patterns, table movement, concentration shifts, and volume spikes are summarized first
- rendered the new explanation layer consistently across `text`, `json`, and `html` outputs
- kept findings deterministic, evidence-based, and capped at 5 items to avoid noisy prose
- fixed compare decline wording so contraction windows are described as decline rather than growth

## Compatibility Notes

- no new commands or parser capabilities were added in this release
- compare JSON now includes `key_findings`
- trend JSON now includes `trend_summary`
- existing detailed sections remain intact and continue to render after the new summary blocks

## Breaking Changes

None.
