# CLI error taxonomy

The process-level failure taxonomy (ADR 0008, #254). Commands return typed
errors; the process boundary in `cmd/binlogviz` maps each error's `Kind` to
a stable exit code and a one-line JSON form for stderr. The `Kind` string
is a stable JSON value and does not change with `--lang`. The public exit
codes are documented in `docs/reference/exit-codes.md` and
`docs/agents/USING-BINLOGVIZ.md`.

## Members

| File | Responsibility |
|------|----------------|
| `clierr.go` | `Kind` and its eleven constants with exit codes 1-11, the `codes` map and `Code`/`KindForCode` lookups, the `Error` carrier (kind + wrapped error + optional machine-readable `Details`), the constructors (`Wrap`, `WithDetails`, `Reasonf`, `Errorf`, `New`), `Of` extraction, and `JSONLine` (the `{"error":{kind,code,message,details?}}` stderr line). |
| `clierr_test.go` | Verifies every kind maps to a distinct exit code, innermost-kind-wins wrapping, and the stderr JSON line shape. |

## Interfaces

| API | Contract |
|-----|----------|
| `Kind` constants | `error`=1, `no_data`=2, `usage`=3, `input`=4, `corrupt`=5, `refused`=6, `conflict`=7, `output`=8, `unsupported`=9 (valid input binlogviz does not analyze, ADR 0009, #277), `resource`=10 (an enforced ceiling, never "no data", ADR 0012, #317), `spill`=11 (a failed spill-file read, not a report-write failure, ADR 0019). |
| `Code(kind) int` | Exit code for a kind; an unmapped kind is 1. |
| `KindForCode(code) Kind` | Inverse mapping; an unmapped code is `KindError`. |
| `Wrap(kind, err) error` | Gives `err` a kind; an error that already has one keeps it — the innermost classification is the most specific. `nil` returns `nil`. |
| `Errorf(kind, format, args...) error` | `Wrap` + `fmt.Errorf`; a `%w` operand's existing kind survives added context. |
| `WithDetails(kind, err, details) error` | `Wrap` plus machine-readable `Details` for the JSON form. |
| `Reasonf(kind, reason, format, args...) error` | A kind with `details.reason` set, for failures that share a kind but need different fixes (`invalid_plan`, `plan_changed`, `trust_boundary`). |
| `New(kind, msg) error` | `Wrap` + `errors.New`. |
| `Of(err) (Kind, map[string]any)` | The kind and details carried by `err`, or `KindError` when it has none. |
| `JSONLine(kind, code, message, details) []byte` | The newline-terminated stderr JSON line; a marshal failure of `details` re-emits without it. |
| `Error{Kind, Err, Details}` | `Error()` delegates to `Err`, `Unwrap` returns it. |

## Dependencies

- Upstream: standard library only (`errors`, `encoding/json`, `fmt`).
- Downstream: `cmd/binlogviz` (the exit boundary in `exit.go`) and most
  `internal/*` packages produce `clierr`-typed errors.
