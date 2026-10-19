# BinlogViz v0.30.0 Release Notes

Release date: 2026-10-19

## Overview

v0.30.0 finishes ADR 0012 steps 1–4: the expression evaluator, schema catalog, apply guard, and flashback planner each live in their own package. Analyze and flashback output stay byte-identical to v0.29.0 on the documented corpora. The user-visible fixes are the generated-column review memory crash (#317) and a short-digest panic in the guard (#319). Exit 10 (`resource`) is new so an enforced spill or memory ceiling is never reported as exit 2 (`no_data`).

## Changed

- **Internal package split (ADR 0012, #257, #315, #316, #320).**
  - Steps 1–2 (#315): `internal/mysqlexpr` and `internal/schemacatalog`.
  - Step 3 (#316): the apply guard moves to `internal/guard`.
  - Step 4 (#320): the flashback planner moves to `internal/flashback`.
  - Analyze JSON, text, Markdown, HTML, and flashback SQL / `--plan` are byte-identical to v0.29.0 on the tip-QA corpora. `report_version` stays 3 and `plan_version` stays 2.

## Added

- **Exit 10 `resource` for flashback ceilings (#317, #320, ADR 0012).**
  - `details.reason` is `spill_limit` when `BINLOGVIZ_SPILL_BYTES` is a positive byte count and the row spill plus the generated-column review spill grow past it.
  - `details.reason` is `memory_limit` when `HeapAlloc` stays above 90% of `GOMEMLIMIT` after one GC.
  - Unset or non-positive / unparseable `BINLOGVIZ_SPILL_BYTES` means no byte ceiling. A full disk (ENOSPC) stays exit 8 (`output`).
  - Exit 10 is never exit 2: a ceiling the process can see is not an empty result. Stderr is the usual JSON error when `--error-format json` is set. No plan and no script are written.
  - `flashback --help` and the agent usage guide document exit 10, the reason values, and `BINLOGVIZ_SPILL_BYTES` (#322).

## Fixed

- **Generated-column review streams instead of holding every row image (#317, #320).** With `--schema-file`, the review used to copy every before-image and after-image into memory. On a 1.07 GB binlog that produced a 1.13 GB undo script, v0.29.0 OOMed with exit 2 around 813 MB RSS; v0.30.0 finishes at about 128 MB RSS and applies cleanly on MySQL 8.0.36. Smaller scripts that formerly matched v0.29.0 stay byte-identical.
- **`guard.Seal` / `SealWriter` refuse a digest shorter than 8 bytes instead of panicking (#319, #320).**

## Verification

BinlogQA's PR QA on #320 (`f3f3387`) and release-gate QA on main `136a937` passed with no P0 or P1. Baselines were main `f66820d` and the released v0.29.0 linux_amd64 archive / tag build.

- **Unchanged output**: corpus 1806/1806, generated-column corpus 1500/1500, and the 67-case matrix are byte-identical to v0.29.0.
- **#317**: whole-file 1.13 GB script tip RSS 128 MB (v0.29.0 same range exit 2 OOM); 43 MB script hash matches; MySQL apply restores the pre-incident checksum. `BINLOGVIZ_SPILL_BYTES=100000000` → exit 10, `reason` `spill_limit`, empty stdout.
- **#319**: digests of 0–7 bytes error without panic; 8–32 are accepted.
- `go test` / `go vet` / `CGO_ENABLED=0` build pass; `version --format json` is correct. CI green.

## Compatibility

- No breaking changes for analyze or flashback consumers. Default analyze and flashback output are byte-identical to v0.29.0 on the tip-QA corpora.
- `report_version` stays 3, `plan_version` stays 2. Exit 10 is additive: agents that only know exits 0–9 should treat unknown non-zero codes as failure, not as `no_data`.
- `flashback` never connects to MySQL. Review the script, test it, and apply it in one new session on the primary with `sql_log_bin=1`, with the script header. Do not apply a script after an incomplete write (exit 8); check the exit code and the seal first (#321).

## Known issues

- **#303 (P2)**: the Linux release binaries report an empty `commit` and `commit_time` in `binlogviz version`, because the release workflow does not pass the commit ldflags to the Linux builds yet. macOS archives and builds from a git checkout report the commit. Until #303 is fixed, check a Linux binary against `binlogviz_0.30.0_checksums.txt` rather than by commit.
- **#321 (P2)**: a flashback script truncated by a full disk (exit 8) can still partially apply. Do not apply a script after an incomplete write; prefer checking the exit code and the seal. A truncated prefix from v0.29.0 behaves the same way.
- **#318 (P2 design)**: the flashback guard token distinguishes the wrong script or the wrong target database; it does not detect edits to statement content (changed literals, deleted undo rows, reordered statements).
- **#312 (P3)** and other P3 follow-ups from v0.29.0 carry over (dead relative README links inside the archive, `--top 0` means 10, zh-CN flashback flag help still partly English, and the `snapshot list` `""` marker display).
- The maintainer note in `.github/workflows/README.md` still lists the old archive file set. The fix is documentation only and waits on a change made with workflow scope. Release builds are not affected.
- `install.sh` P3s from #285, and #255 (items 3, 5, 7), #256, #257 (remaining ADR 0012 steps) carry over.

See `docs/concept/limitations.md` for the flashback limitations carried over from earlier releases.
