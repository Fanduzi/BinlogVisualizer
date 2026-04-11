# BinlogViz v0.14.0 Release Notes

Release date: 2026-04-11

## Scope

`v0.14.0` is a feature release centered on workflow handoff and operator-facing workflow summaries.

This release turns the workflow surface into a fuller delivery path. Workflow outputs now carry aggregated findings and recommendations, `workflow status` exposes that summary directly for CLI and automation users, and `workflow export` packages a workflow root into a deterministic zip bundle for sharing and archival.

## Highlights

- added workflow-level summary aggregation in `manifest.json` and `index.html`
- added workflow-level recommendations and findings sourced from compare/trend report summaries
- added `workflow status` support for persisted `workflow_summary` in both text and JSON outputs
- added `workflow export` to bundle manifest-declared workflow artifacts into a deterministic zip archive
- added optional snapshot inclusion for workflow export with explicit `--include-snapshots`
- hardened workflow summary contracts, export path normalization, and workflow export containment rules with regression coverage
- documented the new workflow summary and export contracts in CLI/output-format references

## Compatibility Notes

- no existing compare/trend finding kinds were removed or renamed
- `workflow_summary` remains best-effort and does not change workflow success/failure semantics
- `workflow status` remains read-only and does not rebuild workflow summary
- `workflow export` packages only manifest-declared workflow outputs plus best-effort `index.html` and rooted `plan.yaml`
- export archives default to `<output_dir>.zip` and reject archive targets inside the workflow root

## Breaking Changes

None.
