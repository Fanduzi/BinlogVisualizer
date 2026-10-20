# BinlogViz v0.33.1 Release Notes

Release date: 2026-10-20

## Overview

v0.33.1 is a patch release. It closes the four #312 follow-ups: `snapshot list` shows a file with an empty `diagnostics_mode` instead of hiding it as `-`, `analyze --help` documents the summary DDL cap, README links that point outside the release archive work from an unpacked tarball, and zh-CN `flashback --help` is complete. There are no breaking changes. Analyze output, flashback scripts, exit codes, `schema_version` 1, `plan_version` 2 and `report_version` 3 are the same as v0.33.0.

## Fixed

- **`snapshot list` shows an empty `diagnostics_mode` (#312).** A snapshot file whose `diagnostics_mode` is `""` is listed as `(empty)`, with its label, created_at, and window, and JSON includes `"diagnostics_mode": ""`. A missing or null value is still shown as `-` with the key omitted. Readers still refuse that file with exit 9 (`reduced_diagnostics`), as ADR 0011 §3 says.
- **`analyze --help` documents the DDL cap (#312).** `--diagnostics summary` caps `ddl_events` at `--top` and reports `ddl_events_omitted`. `--top 0` is the default 10, not unlimited. Analyze JSON for a given `--top` is unchanged.
- **README links work from the release archive (#312).** `README.md` and `README_ZH.md` link to files the tarball does not ship with absolute GitHub URLs on `main`. Links to files inside the archive stay relative. A test fails when a relative README link is not in the archive file list.
- **zh-CN `flashback --help` is complete (#312).** `--conflict`, `--plan`, `--error-format`, and `-q` are translated. English help is unchanged.

## Verification

BinlogQA's PR QA on #350 (`4ea3ed05`) passed with 0 P0, 0 P1, 0 P2 and 0 P3. The PR was fast-forward merged, so main is the same `4ea3ed05`, and tip QA on main passed.

- **`snapshot list`**: a `""` marker shows `(empty)` and JSON `"diagnostics_mode": ""`; missing, null, and `full` show `-` with the key omitted. Reduced strings exit 9 and non-strings exit 5 across list, show, compare, trend, and save.
- **Help**: only the two documented `analyze --help` lines changed; the DDL cap behaves as documented (`--top 5` lists 5 DDL events, `--top 0` lists 10). All other help is byte-identical in en and zh-CN apart from the zh-CN `flashback` lines.
- **README**: 39 absolute `main` links, 0 dead; in-archive links are relative and resolve.
- **Unchanged**: analyze text, JSON, Markdown, and HTML on the tip-QA corpus are byte-identical to v0.33.0 (20/20, HTML apart from the `Generated` time). Flashback scripts are byte-identical with matching digests. `verify | mysql` prints `binlogviz: undo committed, 10 statements` on MySQL 8.0.36 and 5.7.44; rt325 on 8.0 (60000 statements) restores its `CHECKSUM TABLE`.
- `go test ./...` and `go vet` pass. CI green.

## Compatibility

- No breaking changes. `report_version` stays 3, `plan_version` stays 2, and `flashback verify --format json` `schema_version` stays 1. Exit codes, snapshots, artifact names, and supported platforms are unchanged from v0.33.0.
- v0.33.0 flashback scripts verify and apply unchanged. Scripts from v0.32.0 or earlier still need to be regenerated.

## Known issues

- **#303 (P2)**: the Linux release binaries report an empty `commit` and `commit_time` in `binlogviz version`. The fix is in `.github/workflows/release.yml` and needs a maintainer with workflow access. Check a Linux binary against `binlogviz_0.33.1_checksums.txt`.
- `flashback verify` does not accept `-` for stdin (exit 4).
- `internal/guard` `TestVerifyDoesNotBufferTheScript` failed once in about 11 full `go test ./...` runs under memory pressure; it passes in isolation and in CI. The binary is not affected.
- **#270**, **#255**, **#256**, and **#257** (P3) carry over.

See `docs/concept/limitations.md` and `docs/adr/0014-flashback-no-partial-commit.md` for the flashback guarantees and their limits.
