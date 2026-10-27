# Coding standards

中文版：[coding-standards.zh-CN.md](coding-standards.zh-CN.md)

These rules apply to every change, whether a person or an agent writes it. Where CI enforces a rule, the rule says how. Existing debt is recorded in a baseline that may only go down, so old code does not block a change, but no change may make it worse.

## 1. Size

- **Files: soft cap 800 lines**, test files included. A new file over 800 lines fails CI. A file that is already over the cap may sit at most 20 lines above its ceiling. When a change needs more room, move code out first.
- **Functions: cyclomatic complexity at most 40; aim for 25 or less.** Test functions and function literals assigned to package-level vars count. A function that is already over 40 may not get more complex, not even by one.
- **Functions: at most 150 lines and 100 statements**, counted the way golangci-lint `funlen` counts them: the lines between the `func` line and the closing brace, minus one per comment inside. A function that is already over may sit at most 5 lines or 5 statements above its ceiling, so a small fix does not force a split first; complexity gets no such room. Test functions are exempt from length: a table-driven test is long by design, and its complexity still counts.
- Keep a long operation readable as a list of phases: the top-level function calls one function per phase, and each phase returns its result or error.

CI: `codehealth` (files, complexity, function lines and statements) and golangci-lint `gocyclo`, `funlen` (new findings only).

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

CI: `codehealth` counts references in Go comments, per file, in the forms that are not ordinary prose: `#123` after a non-word character (not after words like `Column` or `Field`), `owner/repo#123`, `GH-123`, `PR #123`, `issue 123`, `issue: 123`, `issue number 123`, `Issues 123`, `issues/123` and `pull/123` (URLs), `QA r6` and `r3 review`. Full-width `＃１２３` counts as `#123`. A file's count may not go up, and a file without references may not get one. The counter favors precision: `round 3`, `§9.2`, `bug 12`, `see 12`, `PR-3` and a bare `(12)` are not counted, because the same words are prose, SQL or binlog positions (`Column #3`, `The server issues 2 events`, `mysql-bin/000001#4`). The rule still applies to them in review; it is about the history, not the syntax.

## 4. No dead code

- Delete unused code. Do not keep a function "for later", leave commented-out code, or keep a path nothing calls.
- **When you migrate to a new path, delete the old one** in the same change, or in the next pull request named in this one. No compatibility layer without a caller that needs it.

CI: golangci-lint `unused` (new findings only), and `deadcode`: every unreachable function is a baseline entry by name, and a new one fails with its name and position. deadcode runs without `-test`, so a function that only tests call counts as dead, and with `-generated`, so generated files count too, except the files in `generated_files`. An unreachable function cannot be renamed or split into the baseline; delete it. List them with `go run ./internal/codehealth/cmd/codehealth deadcode`.

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

`internal/codehealth/baseline.json` holds one ceiling per file or function over a limit: `files`, `test_files`, `complex_functions`, `test_complex_functions`, `long_functions`, `statement_functions`, `issue_refs` (per file) and `deadcode` (per function). There is no pooled total, so room freed in one entry cannot be spent in another. Nothing in it is declared by the author: CI derives where code came from by comparing content with the base tree.

- **check** (`codehealth check`, also `TestRepoWithinBaseline` in `go test ./...`) fails when a value over a limit has no entry or sits above its ceiling plus tolerance: 20 lines for files, 5 lines or statements for long functions, 0 for complexity, issue references and deadcode. A value at or below its ceiling always passes, so main stays green when two merged changes pay down the same entry.
- **compare** (`codehealth -base REV compare`) extracts the tree at the base revision with `git archive`, reads its baseline, and measures both trees. Limits may not loosen, `generated_files` may not change, and an existing entry may not rise.
- **Splits and renames are derived, not declared.** Run `make codehealth-baseline`; it lowers ceilings and adds an entry for every new file or function over a limit. compare then accepts a new entry only when at least 80% of its significant lines (not blank, not punctuation, not the package clause or imports) appear verbatim in one baseline entry of the same kind as it was at the base revision: its origin. For each origin, the new pieces plus what is left of the origin, all measured now, may not exceed the origin's ceiling plus tolerance plus slack for each piece after the first: 40 lines per file (package clause, imports, doc comment), 1 complexity, 3 lines or 2 statements per function, 0 issue references. A rename is one piece, so a rename plus small growth behaves like the same growth without the rename. Functions match by body, so renaming `App.Run` to `RunMain` keeps its entry. Copying a file keeps the original's lines in the original, so the copy fails.
- **Paydown.** On a pull request, compare also fails when the change lowered a value by at least 50 lines (files), 5 complexity, 20 lines or 10 statements (functions) or 3 issue references and did not lower its ceiling: run `make codehealth-baseline`. Only values the change itself moved count, so a paydown that reached main through another merge never fails a later pull request. A drop below that threshold may stay unrecorded, and those few lines can be spent again later; the tolerance already allows that much.
- **Generated files.** `generated_files` maps a path to the command that regenerates it. Only listed paths are skipped, and no header or marker exempts anything. The list may not change in a pull request: compare fails, by design, and adding a generator takes the repository owner's override. CI runs each command (`codehealth regen`) and then `git diff --exit-code`, so a listed file is byte for byte its generator's output and hand edits fail. golangci-lint ignores markers too (`generated: disable`) and skips exactly the listed paths. Today the list is empty.
- **Lint on moved code.** `make lint` and CI run golangci-lint on the base tree and on this tree with this tree's configuration, and fail on findings the base does not have. A finding is identified by linter, message (numbers and the function's own name normalized) and enclosing function, never by line. A function in this tree is the base function with the same key, else the base function that holds at least 80% of its body. So a finding that moves with its code, in a split or a rename, still matches, and a new one does not.
- **Where Go compiles.** codehealth measures every `.go` file except under `.git`, including `testdata`, `vendor`, `node_modules`, `_` and `.` directories, because some tool can compile or ship each of them. A nested `go.mod` outside `testdata` fails check: `go test ./...`, golangci-lint and deadcode do not cover a nested module, so move the code into the main module. A file under `testdata` that is not valid Go is counted by lines and reported; anywhere else it is an error.
- **`//nolint`** must name the linter and give a reason of at least three words (`//nolint:errcheck // close error is logged by the caller`). `nolintlint` requires the reason; codehealth check rejects a token one such as `// fine`.
- What CI does not check: the lock, comment-intent, test and design rules (sections 2, 3, 5, 7) are conventions for review. The gate's code and workflow run from the pull request's own tree, so a pull request can change them; review those changes.

## Checks and commands

| Check | Where | Fails when |
| --- | --- | --- |
| `codehealth -base REV lint` (golangci-lint, `.golangci.yml`) | `lint` workflow, `make lint` | a finding the base tree does not have: errcheck, govet, staticcheck, ineffassign, unused, misspell, nolintlint, gocyclo > 40, funlen, gofmt, goimports |
| `codehealth check` | `lint` workflow, `make codehealth`, and `go test ./...` (`TestRepoWithinBaseline`, without deadcode) | a new file or function over a limit with no entry, an entry above its ceiling plus tolerance, a new issue reference, a new unreachable function, a nested module, a token `//nolint` reason |
| `codehealth regen` and `git diff --exit-code` | `lint` workflow, `make codehealth-regen` | a file in `generated_files` (or anything else) differs from what its command generates |
| `codehealth -base REV compare` | `lint` workflow (`-paydown` on pull requests) | the baseline is looser than at the base revision: a limit raised, `generated_files` changed, an entry raised, a new entry that does not derive from an old one or with its origin grows past ceiling plus slack, an unrecorded paydown |

- `make codehealth-baseline` lowers the baseline to today's numbers, drops entries now under a limit, and adds entries for split or renamed code. It refuses to raise anything.
- `go run ./internal/codehealth/cmd/codehealth refs` lists the issue references in comments; `go run ./internal/codehealth/cmd/codehealth deadcode` lists the unreachable functions.
- golangci-lint run directly (`golangci-lint run ./...`) reports existing findings as well; fix them when you touch the code.
