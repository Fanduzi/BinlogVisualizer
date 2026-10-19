# BinlogViz v0.27.0 Release Notes

Release date: 2026-10-19

## Overview

v0.27.0 versions every JSON output and makes the binary say which build it is, so a script or an agent can check what it reads and what it drives. ADR 0010 records the design (`docs/adr/0010-every-json-output-is-versioned.md`). There are no breaking changes: `analyze` and `flashback` output is byte-identical to v0.26.1.

## Added

- **`schema_version` on every JSON output (#282, #302, ADR 0010)**: `compare`, `trend`, `snapshot list|show` and `workflow validate|describe|status|clean|export` with `--format json` now start with `"schema_version": 1`. The compare and trend artifacts that `workflow run` writes carry it too. `analyze` keeps `report_version` 3, `flashback --plan` keeps `plan_version` 2 and the workflow manifest keeps `manifest_version`. See "JSON Versioning" in `docs/reference/output-format.md`.
- **Workflow refuses unknown artifact versions (#282, #302)**: `workflow status`, `workflow resume` and `workflow clean` exit 9 (`unsupported`, `details.reason: unsupported_artifact_version`) on a compare or trend artifact whose `schema_version` this build does not know. Stdout is empty and nothing is run, moved or deleted. Artifacts without the key, written by v0.26.1 and earlier, still read.
- **`binlogviz version` shows the build (#282, #302)**: the text form prints the git commit and commit time (with `-dirty` for a modified tree). `binlogviz version --format json` prints `schema_version`, `version`, `commit`, `commit_time`, `modified` and `go_version`. The commit comes from the Go build info, or from `-X binlogviz/internal/version.Commit`/`CommitTime` ldflags when the build has none.
- `scripts/release_smoke.sh` checks that `version --format json` is versioned, and fails when the binary does not report an expected commit (`BINLOGVIZ_SMOKE_EXPECT_COMMIT=<sha>` or `BINLOGVIZ_SMOKE_REQUIRE_COMMIT=1`).

## Verification

BinlogQA's pre-release QA on main `5b71d7da` passed, against both a v0.26.1 tag build and the released v0.26.1 linux_amd64 archive.

- **Unchanged output**: analyze text/json/markdown, `flashback --plan` (including guard and `--conflict-limit 0`), plain flashback SQL and the conflict refuse path, on 50 files × en/zh-CN: 654 runs, 0 diffs against each baseline. HTML differs only in the "Generated" timestamp. The 67-case exit-code matrix is line-identical.
- **schema_version coverage** matches the ADR 0010 table for every command.
- **Unknown artifact versions**: `schema_version` 99, 0, `"1"` and `null` on compare and trend, across status, resume and clean (text and json, en and zh-CN): 192 runs, all exit 9 with empty stdout and the directory unchanged. v0.26.1 workflow directories still read.
- **version**: a checkout build reports the commit and commit time; a `-buildvcs=false` build reports the injected values; a dirty tree reports `modified: true`.
- **Workflow end to end** on real MySQL 8.0.36 binlogs: validate, run, status, resume, rerun, clean, export and restore all pass.
- CI and `go test ./...` pass.

## Compatibility

- No breaking changes: no key is removed or changed. A consumer that rejects unknown JSON keys will see the new `schema_version`.
- `report_version` stays `3` and `plan_version` stays 2. Exit codes, snapshots, artifact names and supported platforms are unchanged from v0.26.1.
- `version --format json` is new; it was a usage error (exit 3) before.
- `flashback` never connects to MySQL. Review the script, test it, and apply it in one new session on the primary with `sql_log_bin=1`, with the script header.

## Known issues

- **#303 (P2)**: the Linux release binaries report an empty `commit` and `commit_time` in `binlogviz version`, because the release workflow does not pass the commit ldflags to the Linux builds yet. macOS archives and builds from a git checkout report the commit correctly. Until #303 is fixed, check a Linux binary against `binlogviz_0.27.0_checksums.txt` rather than by commit.
- The HTML report's "Generated" timestamp makes two HTML runs differ when they straddle a second (pre-existing).
- `install.sh` P3s from #285, and #255, #256, #257 carry over from v0.26.1.

See `docs/concept/limitations.md` for the flashback limitations carried over from earlier releases.
