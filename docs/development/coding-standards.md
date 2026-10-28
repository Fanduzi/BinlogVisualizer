# Coding standards

中文版：[coding-standards.zh-CN.md](coding-standards.zh-CN.md)

These rules apply to every change, whether a person or an agent writes it. Where CI enforces a rule, the rule says how. Existing debt is recorded in a baseline that may only go down, so old code does not block a change, but no change may make it worse.

## 1. Size

- **Files: soft cap 800 lines**, test files included. A new file over 800 lines fails CI. A file that is already over the cap may sit at most 20 lines above its ceiling. When a change needs more room, move code out first.
- **Functions: cyclomatic complexity at most 40; aim for 25 or less.** Test functions and function literals assigned to package-level vars count. A function that is already over 40 may sit at most 2 above its ceiling in `baseline.json`, so a one-line fix such as a nil or error guard does not force a refactor first. The 2 is measured against the stored ceiling, which no pull request can raise, so it does not compound: after one change uses it, the next change to that function has none left until someone pays the function down. When you need more, extract a branch first (example below).
- **Functions: at most 150 lines and 100 statements**, counted the way golangci-lint `funlen` counts them: the lines between the `func` line and the closing brace, minus one per comment inside. A function that is already over may sit at most 5 lines or 5 statements above its ceiling, so a small fix does not force a split first. Test functions are exempt from length: a table-driven test is long by design, and its complexity still counts.
- Keep a long operation readable as a list of phases: the top-level function calls one function per phase, and each phase returns its result or error.

CI: `codehealth` (files, complexity, function lines and statements) and golangci-lint `gocyclo`, `funlen` (new findings only).

Extracting a branch: when a fix to a baselined function needs more than 2 more complexity, move one existing branch, with the fix, into its own function. The caller gets simpler by the branch's complexity, and the new function is small enough to need no entry.

```go
// Before: App.Run is at its ceiling of 80, and the fix needs three more
// conditions in the recovery branch.
if task.Recovering {
	...
}

// After: the branch and its fix live in recoverTask. App.Run loses the
// conditions inside the branch and keeps one err check; recoverTask is far
// under 40, so the fix fits.
if task.Recovering {
	if err := a.recoverTask(ctx, task); err != nil {
		return err
	}
}
```

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

CI: `codehealth` counts references in Go comments, per file, in the forms that are not ordinary prose: `#123` after a non-word character (not after words that make it a position, an instance or a colour: `Column #3`, `trx #2`, `thread #3`, `worker #2`, `color: #333`; a word with clause punctuation after it does not count, so `the segment, #415` is a reference), `owner/repo#123`, `GH-123`, `PR #123`, `issue 123`, `issue: 123`, `issue number 123`, `Issues 123`, `issues/123` and `pull/123` (URLs), `QA r6` and `r3 review`. Full-width `＃１２３` counts as `#123`. A file's count may not go up, and a file without references may not get one. The counter favors precision: `round 3`, `§9.2`, `bug 12`, `see 12`, `PR-3` and a bare `(12)` are not counted, because the same words are prose, SQL or binlog positions (`Column #3`, `The server issues 2 events`, `mysql-bin/000001#4`). The rule still applies to them in review; it is about the history, not the syntax.

## 4. No dead code

- Delete unused code. Do not keep a function "for later", leave commented-out code, or keep a path nothing calls.
- **When you migrate to a new path, delete the old one** in the same change, or in the next pull request named in this one. No compatibility layer without a caller that needs it.

CI: golangci-lint `unused` (new findings only), and `deadcode`: every unreachable function is a baseline entry by name, and a new one fails with its name and position. deadcode runs without `-test`, so a function that only tests call counts as dead, and with `-generated`, so generated files count too, except the files in `generated_files`. An unreachable function cannot be renamed or split into the baseline; delete it. New code lands wired up: a package or function that only its tests call is dead, so "add it now, call it in the next pull request" fails. Add the caller in the same pull request. List them with `go run ./internal/codehealth/cmd/codehealth deadcode`.

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

- **check** (`codehealth check`, also `TestRepoWithinBaseline` in `go test ./...`) fails when a value over a limit has no entry or sits above its ceiling plus tolerance: 20 lines for files, 5 lines or statements and 2 complexity for baselined functions, 0 for issue references and deadcode. A value at or below its ceiling always passes, so main stays green when two merged changes pay down the same entry.
- **compare** (`codehealth -base REV compare`) extracts the tree at the base revision with `git archive`, reads its baseline, and measures both trees. Limits may not loosen, `generated_files` may not change, and an existing entry may not rise.
- **Splits and renames are derived, not declared.** Run `make codehealth-compare`: it applies the update `make codehealth-baseline` would write — lowered ceilings and an entry for every new file or function over a limit — in memory, and runs compare against the merge base with `origin/main`, as CI does on the committed baseline. Nothing is written, so a failed run leaves baseline.json as committed; run `make codehealth-baseline` and commit the result for CI to see it. compare accepts a new entry only when at least 80% of its significant lines (not blank, not punctuation, not the package clause or imports) appear verbatim in one baseline entry of the same kind as it was at the base revision: its origin. For each origin, the new pieces plus what is left of the origin, all measured now, may not exceed the origin's ceiling plus tolerance plus slack for each piece after the first: 40 lines per file (package clause, imports, doc comment), 1 complexity, 3 lines or 2 statements per function, 0 issue references. A rename is one piece, so a rename plus small growth behaves like the same growth without the rename. Functions match by body, so renaming `App.Run` to `RunMain` keeps its entry. Copying a file keeps the original's lines in the original, so the copy fails.
- **Paydown.** On a pull request, compare also fails when the change lowered a value, measured from the base tree, by at least 50 lines (files), 5 complexity, 20 lines or 10 statements (functions) or 3 issue references and did not lower its ceiling to the new value: run `make codehealth-baseline`. Only the change's own drop counts, never the gap between a ceiling and the base value, so a paydown that reached main through another merge never fails a later pull request: after two pull requests each cut 60 lines from one file and merge, the next one-line change passes. A drop below that threshold may stay unrecorded, and those few lines can be spent again later; the tolerance already allows that much.
- **Generated files.** `generated_files` maps a path to the command that regenerates it. Only listed paths are skipped, and no header or marker exempts anything. The list may not change in a pull request: compare fails, by design, and adding a generator takes the repository owner's override. CI runs each command (`codehealth regen`) and then `git diff --exit-code`, so a listed file is byte for byte its generator's output and hand edits fail. golangci-lint ignores markers too (`generated: disable`) and skips exactly the listed paths. Today the list is empty.
- **Lint on moved code.** `make lint` and CI run golangci-lint on the base tree and on this tree with this tree's configuration, and fail on findings the base does not have. A finding is identified by linter, message and enclosing function, never by line. In the message, numbers, the function's own name, every identifier inside backticks, double quotes or single quotes, and the identifier a message ends with an assignment to are normalized, so renaming a local variable (`mysqlStore.Close` to `metaStore.Close` in an errcheck finding, `mark` to `pos` in an ineffassign one) keeps the finding. A finding whose line and text match a base finding exactly is that finding; among same-key siblings, only what is left counts as new, so a new finding is reported at its own line. A second finding of the same shape in the same function, or the same text from another linter, is still new. A function in this tree is the base function with the same key, else the base function that holds at least 80% of its body. So a finding that moves with its code, in a split or a rename, still matches, and a new one does not.
- **Where Go compiles.** In a git checkout, codehealth measures the files git would commit: tracked files and untracked ones `.gitignore` does not exclude. A local worktree under `.worktrees/`, a `.claude/` or `.scratch/` directory, or any other ignored path does not count. A committed submodule or a nested repository is a directory to git: its files cannot be measured, so it fails check the way a nested `go.mod` does. Within those files it measures every `.go` file, including `testdata`, `vendor`, `node_modules`, `_` and `.` directories, because some tool can compile or ship each of them. A nested `go.mod` outside `testdata` fails check: `go test ./...`, golangci-lint and deadcode do not cover a nested module, so move the code into the main module. A file under `testdata` that is not valid Go is counted by lines and reported; anywhere else it is an error.
- **`//nolint`** must name the linter and give a reason of at least three words (`//nolint:errcheck // close error is logged by the caller`). `nolintlint` requires the reason; codehealth check rejects a token one such as `// fine`.
- What CI does not check: the lock, comment-intent, test and design rules (sections 2, 3, 5, 7) are conventions for review. The gate's code and workflow run from the pull request's own tree, so a pull request can change them; review those changes.

## Checks and commands

| Check | Where | Fails when |
| --- | --- | --- |
| `codehealth -base REV lint` (golangci-lint, `.golangci.yml`) | `lint` workflow, `make lint` | a finding the base tree does not have: errcheck, govet, staticcheck, ineffassign, unused, misspell, nolintlint, gocyclo > 40, funlen, gofmt, goimports |
| `codehealth check` | `lint` workflow, `make codehealth`, and `go test ./...` (`TestRepoWithinBaseline`, without deadcode) | a new file or function over a limit with no entry, an entry above its ceiling plus tolerance, a new issue reference, a new unreachable function, a nested module, a token `//nolint` reason |
| `codehealth regen` and `git diff --exit-code` | `lint` workflow, `make codehealth-regen` | a file in `generated_files` (or anything else) differs from what its command generates |
| `codehealth -base REV compare` | `lint` workflow (`-paydown` on pull requests), `make codehealth-compare` (`-paydown -update`) | the baseline is looser than at the base revision: a limit raised, `generated_files` changed, an entry raised, a new entry that does not derive from an old one or with its origin grows past ceiling plus slack, an unrecorded paydown |

- `make codehealth-baseline` lowers the baseline to today's numbers, drops entries now under a limit, and adds entries for split or renamed code. It refuses to raise anything.
- `make codehealth-compare` applies that update in memory (`-update`) and then runs CI's comparison (`-paydown`) against the merge base with `origin/main` (`LINT_BASE`), writing nothing. Use it whenever check reports a new entry: it tells you locally whether CI will accept it once the baseline is updated and committed; CI compares the committed baseline, so commit the `make codehealth-baseline` result for the same verdict.
- `go run ./internal/codehealth/cmd/codehealth refs` lists the issue references in comments; `go run ./internal/codehealth/cmd/codehealth deadcode` lists the unreachable functions.
- golangci-lint run directly (`golangci-lint run ./...`) reports existing findings as well; fix them when you touch the code.
