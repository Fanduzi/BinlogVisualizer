# Code health

The ratchet behind `make codehealth`, `make codehealth-compare`, `make lint`
and the lint workflow. `Measure` sizes the tree (file lines, function count,
length and complexity, issue references, deadcode); `Baseline` is the
checked-in ceiling those numbers must hold against; `DiffLint` reports
golangci-lint findings the base revision does not have, matched by linter,
enclosing function and normalized message so moved or renamed code keeps its
findings. `ParseLint` normalizes each finding's filename to the
repo-relative form `Measurement` keys by, in whatever form golangci-lint
emitted it — relative, absolute under the linted root, or a `../` escape
resolving back in — so a cache-poisoned or differently-anchored position
cannot turn an existing finding into a new one.

## Members

| File | Responsibility |
|------|----------------|
| `measure.go` | `Measure(root, generated)` walks git-tracked files into a `Measurement`: `Files`/`TestFiles` line counts, `Funcs`/`TestFuncs` keyed `pkgdir:Recv.Func` with `File`, `Start`..`End`, body text and complexity, `Refs` issue references, `Unparsed` files; `Limits`/`DefaultLimits`. |
| `baseline.go` | `Baseline` (the committed ceiling, `baseline.json`): `Load`/`Save`, `Initial`, `Check`, `Update` (refuses to raise), `Compare` (no looser than the base, `-paydown` requires recording drops). |
| `ledger.go` | `Ledger` and `kind`: per-metric ceilings, growth tolerance, split slack and how each problem reads. |
| `lineage.go` | `minDerived` (80%) content tracing: a new entry must come mostly from one base entry, so moves, splits and renames are not new debt. |
| `cyclo.go` | gocyclo-equivalent complexity over a function body. |
| `lint.go` | `LintIssue`, `ParseLint(data, root)` (filename normalization above), `DiffLint` (the base-vs-PR finding diff). |
| `deadcode.go` | `DeadcodeTool` (pinned deadcode), `RunDeadcode`, `ParseDeadcode`. |
| `refs.go` | `#123`/`GH-123` issue-reference collection into `Measurement.Refs`. |
| `lint-base.sh` | Prints `sha=<base revision>` for the lint workflow: merge base on pull requests, previous tip on pushes. |
| `baseline.json` | The committed baseline this module enforces. |
| `cmd/codehealth` | The command that wires measurement, extraction and reporting together. |

## Dependencies

- Upstream: standard library only (`go/ast`, `go/parser`), plus the pinned `deadcode` tool at run time.
- Downstream: `internal/codehealth/cmd/codehealth`, invoked by the Makefile targets and `.github/workflows/lint.yml`.
