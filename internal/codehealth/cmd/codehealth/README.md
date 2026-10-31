# codehealth command

`codehealth` checks the tree against `internal/codehealth/baseline.json`:
`init`/`check`/`update`/`deadcode`/`refs` measure this tree;
`-base REV compare` and `-base REV lint` extract REV with `git archive` into
a temporary tree and measure both sides; `regen` reruns each generated
file's generator. Outside tools (`golangci-lint`, `deadcode`, `git archive`,
shell generators) sit behind the `deps` struct so tests replace them.
`golangci` runs with a fresh `GOLANGCI_LINT_CACHE` per invocation: the lint
cache is content-keyed but keeps first-computed positions, so a cache shared
across trees — including the base and pull-request runs of one `lint` —
answers with another tree's filenames and every finding diffs as new.

## Members

| File | Responsibility |
|------|----------------|
| `main.go` | Flag parsing and dispatch; `golangci` (cache isolation above), `gitArchive` base extraction, `lintTree`, `compare`, `regen`, reporting. |
| `main_test.go` | Fixture-driven command tests: baseline lifecycle, compare/lint over a real git base, tool-error propagation, lint-cache isolation. |

## Dependencies

- Upstream: `internal/codehealth`; `golangci-lint`, `git`, `tar` and `sh` at run time.
- Downstream: Makefile `lint`/`codehealth*` targets and `.github/workflows/lint.yml`.
