# Workflow Package

Orchestration primitives for `binlogviz workflow run <plan.yaml>`.

## Files

| File | Responsibility |
|------|----------------|
| `plan.go` | YAML plan schema and decoder. |
| `validate.go` | Structural and cross-reference validation rules. |
| `layout.go` | Deterministic artifact directory and file path helpers. |
| `manifest.go` | Manifest and step-record structs plus JSON serialization. |

## Exports

- `LoadPlan(io.Reader) (Plan, error)` — Decodes and validates a YAML workflow plan.
- `ValidatePlan(Plan) error` — Checks structural and cross-reference rules.
- `ArtifactPath(root, kind, name, format string) string` — Resolves deterministic artifact file paths.
- `EnsureLayout(root string) error` — Creates the analyze/compare/trend directory tree.
- `Manifest` / `StepRecord` — Manifest structs.
- `WriteManifest(path string, m Manifest) error` — Writes manifest.json.

## Update Rule

If plan fields, validation rules, layout conventions, or manifest fields change, update this file in the same change.
