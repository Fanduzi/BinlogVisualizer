# Workflow Package

Orchestration primitives for `binlogviz workflow run <plan.yaml>` and `binlogviz workflow resume <output_dir>`.

## Files

| File | Responsibility |
|------|----------------|
| `plan.go` | YAML plan schema and decoder. |
| `validate.go` | Structural and cross-reference validation rules. |
| `layout.go` | Deterministic artifact directory and file path helpers. |
| `manifest.go` | Manifest and step-record structs plus JSON serialization. |
| `index.go` | Workflow index HTML renderer built from manifest data. |
| `resume.go` | Resume planner: selector parsing, manifest validation, resume plan builder. |

## Manifest Versioning

The manifest uses an explicit `manifest_version` field. The current version is **2**.

| Version | Changes |
|---------|---------|
| 2 | Added `manifest_version`, `mode`, `attempt`, `plan_sha256`, `resolved_input_files`, `snapshot_dir` fields; added `execution` field to `StepRecord`. |

### Resume-Safe Metadata (v2)

These fields enable safe resume and partial rerun:

| Field | Purpose |
|-------|---------|
| `manifest_version` | Schema version for forward-compatible parsing. |
| `mode` | `"run"` for fresh execution, `"resume"` for partial rerun. |
| `attempt` | Monotonically increasing attempt counter (1-based). |
| `plan_sha256` | SHA-256 of the plan YAML; resume refuses if the plan changed. |
| `resolved_input_files` | Discovered input file paths; reused on resume without re-discovery. |
| `snapshot_dir` | Snapshot storage directory override. |

### Step Execution Labels

Each `StepRecord` carries an `execution` field:

| Value | Meaning |
|-------|---------|
| `"executed"` | Step ran during this attempt. |
| `"reused"` | Step output was reused from a previous attempt. |

## Exports

- `LoadPlan(io.Reader) (Plan, error)` — Decodes and validates a YAML workflow plan.
- `ValidatePlan(Plan) error` — Checks structural and cross-reference rules.
- `ArtifactPath(root, kind, name, format string) string` — Resolves deterministic artifact file paths.
- `EnsureLayout(root string) error` — Creates the analyze/compare/trend directory tree.
- `Manifest` / `StepRecord` — Manifest structs.
- `WriteManifest(path string, m Manifest) error` — Writes manifest.json.
- `RenderIndex(input IndexInput) (string, error)` — Renders a self-contained HTML workflow index page from manifest data.
- `IndexInput` — Renderer input holding the output root and manifest.

### Resume Planner (Task 2)

The following will be exported from `resume.go`:

- `ResumeOptions` — Options for resume execution (output dir, rerun selectors, snapshot dir override).
- `PlannedStep` — A step with a resolved execution decision (execute or reuse).
- `ResumePlan` — Full plan listing steps to execute and steps to reuse.
- `ParseRerunSelectors(raw []string) ([]RerunSelector, error)` — Parses `--rerun kind:name` flags.
- `ValidateResumableManifest(m Manifest, planPath string) error` — Checks manifest is resumable (v2, matching plan hash, has input files).
- `BuildResumePlan(plan Plan, prev Manifest, reruns []RerunSelector) (ResumePlan, error)` — Builds dependency-aware step list.

## Update Rule

If plan fields, validation rules, layout conventions, manifest fields, or resume planner exports change, update this file in the same change.
