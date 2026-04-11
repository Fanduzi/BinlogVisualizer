# BinlogViz v0.15.0 Release Notes

Release date: 2026-04-11

## Scope

`v0.15.0` is a security hardening release focused on the workflow trust boundary.

This release ensures that `workflow resume` and `workflow status` only open plan files that live inside the workflow output root. Before this change, a crafted `manifest.json` could point `plan_path` at an arbitrary file on disk — including files outside the workflow root via symlinks, relative escapes, or absolute paths.

## Highlights

- added `ValidateWorkflowPlanPath` which canonicalizes and validates `plan_path` before any file I/O
- added symlink escape detection: manifests whose `plan_path` resolves outside the workflow root are rejected
- tightened plan acceptance to rooted `plan.yaml` only — nested paths (`sub/plan.yaml`) and renamed files (`other-plan.yaml`) are rejected
- `workflow resume` now hard-fails on trust-boundary violations before opening any plan file
- `workflow status` degrades gracefully: untrusted plans result in non-resumable status without crashing
- all callers now consume the canonical resolved path instead of the raw `manifest.PlanPath` value

## Compatibility Notes

- `workflow resume` and `workflow status` will now reject manifests with `plan_path` values that previously worked if those paths pointed outside the workflow root
- only `<output_dir>/plan.yaml` is accepted — this is already the default produced by `workflow run`, so existing workflows are unaffected
- manifests with empty or missing `plan_path` continue to be handled as non-resumable (unchanged behavior)

## Breaking Changes

None for workflows created by `workflow run`. Manually crafted manifests with non-standard `plan_path` values will be rejected.
