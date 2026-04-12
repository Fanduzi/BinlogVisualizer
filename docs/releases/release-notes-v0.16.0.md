# BinlogViz v0.16.0 Release Notes

Release date: 2026-04-12

## Scope

`v0.16.0` adds an optional explanatory layer to the analyze report: **selective pattern drilldowns**. When one or more write patterns cross a high-signal threshold, BinlogViz now expands them into bounded drilldown summaries that explain *why* the pattern deserves attention and provide workload context.

This is a new feature release. Existing report structure, CLI flags, and output contracts are unchanged.

## Highlights

- added `pattern_drilldowns` as a new top-level array in JSON analyze output (always present, empty when nothing qualifies)
- text output renders an indented `drilldown:` block under qualifying patterns with `workload minute` and `workload txn` lines
- HTML output renders collapsible drilldown cards with signal flags (`dominance`, `anomaly`), metric help tooltips, and workload context sections
- drilldown selection uses a mixed signal model combining dominance and anomaly signals, scored and capped at 2 drilldowns per analysis
- each drilldown carries at most 2 workload peak minutes and 2 workload transactions (window-level context, not pattern-owned data)
- anomaly signals are pattern-local: table-aligned alert matching and high rows-per-txn ratios, not global minute overlap

## New JSON Contract Field

```json
{
  "pattern_drilldowns": [
    {
      "pattern_key": "...",
      "label": "...",
      "why_selected": "...",
      "share_of_rows": 0.80,
      "share_of_txns": 0.70,
      "avg_rows_per_txn": 500,
      "signal_flags": { "dominance": true, "anomaly": true },
      "busiest_minutes": [...],
      "representative_transactions": [...]
    }
  ]
}
```

## Compatibility Notes

- `pattern_drilldowns` is a new field; existing JSON consumers that ignore unknown fields are unaffected
- no new CLI flags or configuration options — drilldowns are automatic when signal thresholds are met
- Markdown output intentionally omits drilldowns (Top Patterns section is not rendered in Markdown)
- no changes to compare, trend, or workflow outputs

## Breaking Changes

None.
