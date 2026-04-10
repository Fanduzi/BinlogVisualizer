# Workflow Package

Orchestration primitives for `binlogviz workflow run <plan.yaml>`, `binlogviz workflow resume <output_dir>`, and workflow inspection/cleanup commands such as `workflow status` and `workflow clean`.

## Files

| File | Responsibility |
|------|----------------|
| `plan.go` | YAML plan schema and decoder. |
| `validate.go` | Structural and cross-reference validation rules. |
| `layout.go` | Deterministic artifact directory and file path helpers. |
| `manifest.go` | Manifest and step-record structs plus JSON serialization. |
| `index.go` | Workflow index HTML renderer built from manifest data. |
| `resume.go` | Resume planner: selector parsing, manifest validation, resume plan builder. |
| `describe.go` | Static workflow preview model and deterministic description builder. |
| `status.go` | Read-only runtime status model, artifact existence inspection, and dry resume preview builder. |
| `clean.go` | Orphan discovery and apply-mode deletion for workflow-generated artifacts plus opt-in snapshot cleanup. |

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
- `ValidatePlan(Plan) error` — Checks structural and cross-reference rules, including duplicate window, compare-job, and trend-job names.
- `ArtifactPath(root, kind, name, format string) string` — Resolves deterministic artifact file paths.
- `BuildDescription(plan Plan) Description` — Builds a static workflow preview from plan-only data using deterministic artifact naming.
- `Description` / `WindowDescription` / `CompareDescription` / `TrendDescription` — Structured static preview model for text/json rendering.
- `BuildStatus(outputDir string, manifest Manifest, plan *Plan) (Status, error)` — Builds a read-only runtime inspection model from manifest data, artifact presence checks, and dry resume planning.
- `Status` / `StepStatus` / `ArtifactStatus` / `ResumePreviewStep` — Structured runtime status model for text/json rendering.
- `DiscoverCleanCandidates(outputDir string, manifest Manifest, includeSnapshots bool) (CleanResult, error)` — Discovers orphaned workflow-generated artifacts and optional snapshot JSON files using the manifest as the source of truth.
- `ApplyClean(result CleanResult) CleanResult` — Applies best-effort deletion for discovered cleanup candidates and records deleted/skipped paths.
- `CleanOptions` / `CleanResult` / `CleanCounts` — Structured cleanup model for text/json rendering and apply-mode summaries.
- `EnsureLayout(root string) error` — Creates the analyze/compare/trend directory tree.
- `Manifest` / `StepRecord` — Manifest structs.
- `WriteManifest(path string, m Manifest) error` — Writes manifest.json.
- `RenderIndex(input IndexInput) (string, error)` — Renders a self-contained HTML workflow index page from manifest data.
- `IndexInput` — Renderer input holding the output root and manifest.

### Resume Planner

Exported from `resume.go`:

- `RerunSelector` — Parsed `kind:name` step selector.
- `PlannedStep` — A step with a resolved execution decision (execute or reuse) and reason.
- `ResumePlan` — Full plan listing steps to execute and reuse, plus updated manifest.
- `ParseRerunSelectors(plan Plan, raw []string) ([]RerunSelector, error)` — Parses `--rerun kind:name` flags and validates against plan.
- `ValidateResumableManifest(m Manifest, planPath string, planSHA256 string) error` — Checks manifest is resumable (v2, matching plan hash, has input files).
- `BuildResumePlan(plan Plan, m Manifest, selectors []string, outputDir string, snapshotDir string) (ResumePlan, error)` — Builds dependency-aware step list with invalidation propagation.

## Update Rule

If plan fields, validation rules, layout conventions, manifest fields, or resume planner exports change, update this file in the same change.
