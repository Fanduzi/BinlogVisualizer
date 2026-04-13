# BinlogViz v0.17.0 Release Notes

Release date: 2026-04-13

## Scope

`v0.17.0` extends pattern drilldowns from single-window analyze output into the cross-window surfaces: **compare** and **trend**.

This release adds bounded explanatory drilldowns for high-signal pattern changes and pattern trends, while keeping the existing compare/trend findings and recommendation layers intact.

## Highlights

- added `pattern_drilldowns` as a top-level array in both compare and trend JSON outputs
- compare now emits bounded drilldowns for dominant, new, disappeared, or row/txn-divergent pattern changes
- trend now emits bounded drilldowns for dominant share shifts, steady rises/falls, and concentrated share jumps
- text output renders short indented `drilldown:` blocks under qualifying compare/trend patterns
- HTML compare and trend reports now render labeled drilldown detail cards beneath the pattern sections
- bounded payloads remain enforced:
  - at most 2 drilldowns per compare or trend report
  - at most 2 key points per drilldown

## JSON Contract Additions

Compare and trend now always include:

```json
{
  "pattern_drilldowns": [
    {
      "pattern_key": "...",
      "label": "...",
      "why_selected": "...",
      "signal_flags": { "...": true },
      "key_points": [
        { "label": "...", "summary": "..." }
      ]
    }
  ]
}
```

Compare drilldowns focus on row/txn deltas between current and baseline windows. Trend drilldowns focus on rows/share movement across ordered snapshots.

## Compatibility Notes

- `pattern_drilldowns` is a new field in compare and trend JSON; existing consumers that ignore unknown fields remain compatible
- no new CLI flags or configuration options were added
- workflow behavior is unchanged; workflow continues consuming compare/trend outputs as before
- markdown behavior is unchanged; these drilldowns are carried by compare/trend text, JSON, and HTML outputs

## Breaking Changes

None.
