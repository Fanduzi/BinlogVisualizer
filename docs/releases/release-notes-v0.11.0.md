# BinlogViz v0.11.0 Release Notes

Release date: 2026-04-10

## Scope

`v0.11.0` is a feature release centered on workflow v1.

This release turns BinlogViz from a set of strong standalone commands into a complete workflow-driven investigation toolchain. Operators can now validate plans before execution, run multi-step investigations from one YAML file, inspect workflow state after execution, recover incomplete runs, and safely clean stale generated outputs.

## Highlights

- added `workflow run <plan.yaml>` to execute multi-window analyze, compare, and trend investigations from one declarative YAML plan
- added `manifest.json` and `index.html` as first-class workflow outputs for machine-readable state and human-friendly navigation
- added `workflow resume <output_dir>` with resumable manifest metadata, explicit `--rerun` selectors, and dependency-aware reuse/rerun planning
- added `workflow validate <plan.yaml>` and `workflow describe <plan.yaml>` for pre-run plan validation and static execution preview
- added `workflow status <output_dir>` for read-only runtime inspection, artifact presence checks, and dry resume preview
- added `workflow clean <output_dir>` as a dry-run-first maintenance command for orphaned workflow artifacts and optional orphaned snapshots
- hardened workflow result integrity, manifest semantics, snapshot overwrite behavior, and release-readiness regression coverage

## Workflow v1 Surface

The workflow lifecycle is now covered end to end:

- pre-run: `workflow validate`, `workflow describe`
- execution: `workflow run`
- landing page: `index.html`
- recovery: `workflow resume`
- runtime inspection: `workflow status`
- maintenance: `workflow clean`

## Compatibility Notes

- workflow manifests now use manifest v2 metadata to support resume-safe execution and inspection
- legacy pre-v2 workflow manifests remain inspectable in `workflow status`, but they are not resumable
- `workflow clean` uses the current manifest as the source of truth and only removes orphaned generated files inside the workflow artifact scope
- existing analyze, compare, trend, and snapshot commands remain available and continue to work directly outside workflow mode

## Breaking Changes

None.
