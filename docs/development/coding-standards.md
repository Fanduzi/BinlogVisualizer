# Coding standards

中文版：[coding-standards.zh-CN.md](coding-standards.zh-CN.md)

These rules apply to every change, whether a person or an agent writes it. Where CI enforces a rule, the rule says how. Existing debt is recorded in a baseline that may only go down, so old code does not block a change, but no change may make it worse.

## 1. Size

- **Files: soft cap 800 lines**, test files included. A new file over 800 lines fails CI. A file that is already over the cap may sit at most 20 lines above its ceiling, and the ceiling comes down whenever the file shrinks. When a change needs more room, move code out first.
- **Functions: cyclomatic complexity at most 40; aim for 25 or less.** Test functions and function literals assigned to package-level vars count. A function that is already over 40 may not get more complex, not even by one.
- **Functions: body at most 150 lines, not counting `//` comment lines.** A function that is already longer may not grow, not even by one line. golangci-lint `funlen` also caps a new function at 100 statements. Test functions are exempt from length: a table-driven test is long by design, and its complexity still counts.
- Keep a long operation readable as a list of phases: the top-level function calls one function per phase, and each phase returns its result or error.

CI: `codehealth` (files, complexity, function length) and golangci-lint `gocyclo`, `funlen` on changed lines.

## 2. Locks

- **One lock site and one unlock site per critical section.** Write the locked part as its own function: `mu.Lock(); defer mu.Unlock()` at the top.
- Do not unlock on every early return. If a function must drop the lock in the middle, split it at that point into a locked phase function and an unlocked one.

```go
// Bad: every exit path has to remember the unlock.
s.mu.Lock()
if err := a(); err != nil {
	s.mu.Unlock()
	return err
}
s.mu.Unlock()
return b()

// Good: one critical section, one unlock.
if err := s.lockedA(); err != nil {
	return err
}
return b()

func (s *S) lockedA() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return a()
}
```

## 3. Comments

- **Comments explain why** the code is the way it is: the constraint, the invariant, the failure it prevents.
- **No issue numbers, QA rounds or ADR section history in code** (`#123`, `§29.10`, "fixed in round 3"). That history belongs in the commit message, the pull request or the ADR. A path to an ADR is fine when the reader needs the design (`see docs/adr/0011-....md`).

CI: `codehealth` counts references in Go comments, per file: `#123`, the full-width `＃123`, `owner/repo#123`, `PR#123`, `PR 123`, `PR-3`, `issue 123`, `GH-123`, issue and pull request URLs, `§4` and `round 3`. A file's count may not go up, and a file without references may not get one. Rewording a reference into another of these forms does not lower the count. A counter cannot catch every wording; the rule is about the history, not the syntax.

## 4. No dead code

- Delete unused code. Do not keep a function "for later", leave commented-out code, or keep a path nothing calls.
- **When you migrate to a new path, delete the old one** in the same change, or in the next pull request named in this one. No compatibility layer without a caller that needs it.

CI: golangci-lint `unused` on changed lines, and `deadcode`: every unreachable function is a baseline entry by name, and a new one fails with its name and position. deadcode runs without `-test`, so a function that only tests call counts as dead. List them with `go run ./internal/codehealth/cmd/codehealth deadcode`.

## 5. Tests assert behavior

- A test checks what a user of the code can observe: outputs, returned errors, stored state, emitted events.
- It fails when that behavior breaks and keeps passing when only the implementation changes. Do not assert on private call order or copy the implementation into the test.
- Guard tests that read source or docs (lint guards, doc pins) are fine when the guard is the point; give the failure message the fix.

## 6. No README or docs accretion

- The README and landing page describe the product: what it does, how to install the current release, where the docs are. They are not a changelog.
- Per-version notes go to the release notes and the upgrade guide; feature detail goes to a page under `docs/`, linked from the README.
- The README shape guard and `make release-check` (`scripts/check-release-prep.sh`) enforce this.

## 7. Design before the third patch

- **When the same area fails QA twice, stop patching.** Write or update an ADR under `docs/adr/`: the premise that failed, the alternatives, the decision. Get it reviewed, then implement it.
- A fix on top of a fix in the same function is a sign the model is wrong, not that one more branch is needed.

## 8. The baseline

`internal/codehealth/baseline.json` holds one ceiling per file or function over a limit: `files`, `test_files`, `complex_functions`, `test_complex_functions`, `long_functions`, `issue_refs` (per file) and `deadcode` (per function). There is no pooled total, so room freed in one entry cannot be spent in another.

- `codehealth check` fails when a value has no entry, sits above its ceiling (plus 20 lines for files), or sits below its ceiling. The last one means "run `make codehealth-baseline`": the ceiling comes down with the debt, so freed room cannot be spent later.
- `codehealth compare` runs against the base revision's baseline. An existing entry may only go down or disappear. A new entry fails, so removed entries cannot be reused and a new file over the cap fails even when another file shrank.
- **Splits and renames.** The only way to add an entry is as a successor of an existing one: `make codehealth-baseline FROM="internal/x/a.go=internal/x/old.go internal/x/b.go=internal/x/old.go"`. The entry records `split_from`, and compare checks it against the original in the same change: the successors plus what is left of the original, measured now, may not exceed the original's ceiling, plus 20 lines per extra file (package clause and imports), 1 complexity or 2 lines per extra function. A rename is a split into one piece and gets no slack. Functions are keyed `pkgdir:Recv.Func`; a file rename inside a package keeps its function keys. Unreachable functions cannot be renamed into the baseline; delete them.
- **Generated files.** A `Code generated ... DO NOT EDIT.` marker exempts nothing by itself. A file is skipped only when `generated_files` lists its path and its first line is exactly `// Code generated by <generator>. DO NOT EDIT.` (Go's regular expression, first line only). compare accepts a new `generated_files` path only for a file that did not exist at the base revision. golangci-lint ignores markers too (`generated: disable`) and skips exactly the listed paths. Today the list is empty.
- **Where Go compiles.** codehealth measures every `.go` file under the module, including `testdata`, `vendor`, `_` and `.` directories, because an import path can reach them. It skips `.git`, `node_modules` and nested modules.
- **`//nolint`** must name the linter and give a reason (`//nolint:errcheck // reason`); `nolintlint` enforces it. codehealth ignores `//nolint`.
- What CI does not check: the lock, comment-intent, test and design rules (sections 2, 3, 5, 7) are conventions for review. A pull request can also edit `.golangci.yml` or the lint workflow itself; review those changes.

## Checks and commands

| Check | Where | Fails when |
| --- | --- | --- |
| golangci-lint (`.golangci.yml`) | `lint` workflow, `make lint` | a new issue on a changed line: errcheck, govet, staticcheck, ineffassign, unused, misspell, nolintlint, gocyclo > 40, funlen, gofmt, goimports |
| `codehealth check` | `lint` workflow, `make codehealth`, and `go test ./...` (`TestRepoWithinBaseline`, without deadcode) | the tree does not match the baseline: a new file or function over a limit, an entry above its ceiling, an entry below its ceiling, a new issue reference, a new unreachable function, a `generated_files` path without its header |
| `codehealth compare` | `lint` workflow | the baseline is looser than the base revision's: a limit raised, an entry raised or added without valid `split_from`, a `generated_files` path added for an existing file |

- `make codehealth-baseline` lowers the baseline to today's numbers after you pay down debt and records splits and renames given in `FROM`. It refuses to raise anything.
- `go run ./internal/codehealth/cmd/codehealth refs` lists the issue references in comments; `go run ./internal/codehealth/cmd/codehealth deadcode` lists the unreachable functions.
- golangci-lint reports existing issues outside the changed lines only when run without `--new-from-rev`; fix them when you touch the code.
