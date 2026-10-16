# ADR 0009: Every command error has a kind

Status: accepted (2026-10-16). Tracks #277. Extends ADR 0008 §4.

## Context

ADR 0008 introduced `internal/clierr` and exit codes 3–8, but only a few commands used it. The v0.24.0 review (BinlogQA, 2026-10-16) counted about 149 plain `fmt.Errorf`/`errors.New` sites in `cmd/binlogviz`, and only 7 files used `clierr`. So exit 1 still covered too much:

- `snapshot save` without `--name` exited 1 (cobra's untyped required-flag error).
- A snapshot source that is not valid JSON exited 1.
- A STATEMENT or MIXED binlog with no ROW images exited 1.
- `analyze /dev/null` exited 4 (input) and `analyze empty.binlog` exited 5 (corrupt), for the same failure.

An agent cannot tell "fix your arguments" from "this file is bad" from "this tool cannot read this format" without scraping localized text.

## Decision

1. **One new kind.** `unsupported`, exit 9: valid input that binlogviz does not analyze. Today that is a binlog whose DML is only in STATEMENT form, an unsupported report shape, and a workflow manifest version this build cannot resume. It differs from `corrupt` (5): the file is fine, re-running with a different tool or binlog format helps, and retrying the same input will not.
2. **Rule for choosing a kind.**
   - `usage` (3): the fix is in the command line: a flag, argument, name or format value, including a missing required flag and a value that is out of range for the given input.
   - `input` (4): an input the user named cannot be found, opened or read (file, directory, snapshot, manifest, plan).
   - `corrupt` (5): an input was read but its content is broken: not a binlog, empty (a 0-byte file, `/dev/null` and empty stdin alike), truncated, or JSON that does not decode.
   - `unsupported` (9): above.
   - `output` (8): writing a report, artifact, temp file or manifest failed.
   - `error` (1): only failures that have no better kind, chiefly ADR 0001's open BEGIN and unclassified QUERY, and internal invariants.
3. **Every error is built typed.** In `cmd/binlogviz` and `internal/snapshot`, errors are created with `clierr.Errorf(kind, …)` or `clierr.New(kind, …)`. `fmt.Errorf` and `errors.New` are allowed only for package-level sentinels. `TestNoUntypedErrors` parses the package and fails on any other use.
4. **Innermost kind wins.** `clierr.Errorf` with a `%w` operand that already has a kind keeps that kind, like `clierr.Wrap`. Context added on the way up (`analyze window "w1": …`) never hides the specific classification.
5. **Required flags are checked in `RunE`.** Cobra's `MarkFlagRequired` returns an untyped error before our code runs, so commands check required flags themselves and return `usage`.

## Consequences

- Scripts that matched exit 1 for these cases must read the new codes; the JSON error `kind` is the stable contract (`docs/reference/exit-codes.md`).
- `analyze /dev/null` now exits 5 like an empty file.
- Position selectors that are out of range or not on an event boundary exit 3 (usage) instead of 1.
- Adding a new error without a kind fails CI.
