# BinlogViz v0.43.1 Release Notes

Release date: 2026-11-05

## Install or upgrade

```bash
brew upgrade --cask binlogviz          # macOS
# Linux: rerun install.sh, which resolves the latest release
```

A patch on v0.43.0: no flag, exit code, JSON field, or report byte changed.

## Overview

v0.43.1 fixes `--help`, which now follows `--lang`. Until this release only
`flashback` translated its help; every other command printed English while the
report itself was Chinese. The command tree resolves its text when it is built,
before `--lang` is parsed, and help is printed before any run hook.

## Fixed

- **`--help` follows `--lang`.** `binlogviz --lang zh-CN analyze --help` printed
  the English description and English flag usage lines. The root help now builds
  the command tree again in the requested language, so the description, the
  subcommand list and every flag usage line follow `--lang` for `analyze`,
  `compare`, `trend`, `snapshot`, `workflow`, `schema`, `redo`, `version` and
  `help` itself. `flashback` keeps its own help path and is unchanged. Without
  `--lang` the language still comes from the environment, and report output is
  untouched.
- **The CLI reference says so.** The `--lang` row in `docs/reference/cli.md` and
  `.zh-CN.md` now reads that it also applies to `--help`.

## Compatibility

- No flag, exit code, section name, or text layout changed. `report_version` stays
  4, `plan_version` stays 2, and `flashback verify --format json`
  `schema_version` stays 1.
- Help text is the only output that changed, and only when `--lang` names a
  language other than the one the process started in.

## Tests and CI

- `TestHelpFollowsTheLanguageFlag` runs `--lang zh-CN analyze --help`, `--lang en
  analyze --help`, and a second command, and asserts that both the long
  description and a flag usage line are in that language.
- `TestFlashbackMarkedExperimental` (which already ran `--lang zh-CN flashback
  --help`) and `TestCommandExamplesIncludeSpikeRecipe` still pass, so the
  flashback path and the examples are unchanged.
- `go test ./cmd/binlogviz ./internal/...`, `make codehealth`, `make lint`, and
  `gofmt` are clean; the pull request passed all 11 required checks.

## Verification

The tagged commit's parent is the merged fix, and `main` passed `ci` and `lint` on
it (`verify`, `vet and race`, `flashback e2e`, `flashback difftest`, `MySQL 5.7
compatibility`, `codehealth`, `golangci-lint`, `release-prep self-test`, `landing
overflow`, `darwin verify`).
