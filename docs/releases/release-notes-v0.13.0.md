# BinlogViz v0.13.0 Release Notes

Release date: 2026-04-11

## Scope

`v0.13.0` is a feature release centered on evidence drilldown for compare and trend findings.

This release builds on the `v0.12.0` change explanation layer. Findings now carry structured `evidence_refs` that point back to the report sections that support each conclusion, so users can move from summary to supporting evidence without manually hunting through the report.

## Highlights

- added `evidence_refs` to compare `key_findings`
- added `evidence_refs` to trend `trend_summary`
- rendered evidence references in `text`, `json`, and `html` outputs
- added clickable HTML links from findings to the supporting report sections and rows
- hardened HTML finding rendering to use DOM APIs instead of `innerHTML`, avoiding unsafe interpretation of report data as markup
- added regression coverage for trend anchor targets, anchor uniqueness, and hostile finding content

## Compatibility Notes

- no new commands or parser capabilities were added in this release
- compare JSON finding objects may now include `evidence_refs`
- trend JSON finding objects may now include `evidence_refs`
- existing `key_findings` and `trend_summary` fields remain compatible; low-signal findings can still omit `evidence_refs`

## Breaking Changes

None.
