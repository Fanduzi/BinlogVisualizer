# Coding standards

中文版：[coding-standards.zh-CN.md](coding-standards.zh-CN.md)

These rules apply to every change, whether a person or an agent writes it. Where CI enforces a rule, the rule says how. Existing debt is recorded in a baseline that may only go down, so old code does not block a change, but no change may make it worse.

## 1. Size

- **Files: soft cap 800 lines** (non-test, non-generated Go). A new file over 800 lines fails CI. A file that is already over the cap may grow by at most 20 lines. When a change needs more room, move code out first.
- **Functions: cyclomatic complexity at most 40 for new code; aim for 25 or less.** A function that is already over 40 may not get more complex.
- **New functions: at most 150 lines or 100 statements** (tests are exempt).
- Keep a long operation readable as a list of phases: the top-level function calls one function per phase, and each phase returns its result or error.

CI: `codehealth` (files, complexity) and golangci-lint `gocyclo`, `funlen` on changed lines.

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

CI: `codehealth` counts `#123` and `§4` references in Go comments; the count may not go up.

## 4. No dead code

- Delete unused code. Do not keep a function "for later", leave commented-out code, or keep a path nothing calls.
- **When you migrate to a new path, delete the old one** in the same change, or in the next pull request named in this one. No compatibility layer without a caller that needs it.

CI: golangci-lint `unused` on changed lines and the `deadcode` count, which may not go up.

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

## Checks and commands

| Check | Where | Fails when |
| --- | --- | --- |
| golangci-lint (`.golangci.yml`) | `lint` workflow, `make lint` | a new issue on a changed line: errcheck, govet, staticcheck, ineffassign, unused, misspell, gocyclo > 40, funlen, gofmt, goimports |
| `codehealth check` | `lint` workflow, `make codehealth`, and `go test ./...` (`TestRepoWithinBaseline`, without deadcode) | a new file over 800 lines, an oversized file growing more than 20 lines, a new function over complexity 40, an existing one growing, more issue references, more deadcode |
| `codehealth compare` | `lint` workflow | the pull request raises any ceiling in `internal/codehealth/baseline.json` |

- `make codehealth-baseline` lowers the baseline to today's numbers after you pay down debt. It refuses to raise anything. `TestRepoWithinBaseline` fails when the baseline still lists a file or function that is now under the cap, so the ceiling follows the debt down.
- `go run ./internal/codehealth/cmd/codehealth refs` lists the issue references in comments.
- golangci-lint reports existing issues outside the changed lines only when run without `--new-from-rev`; fix them when you touch the code.
