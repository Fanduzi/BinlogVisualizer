# BinlogViz v0.25.0 Release Notes

Release date: 2026-10-17

## Overview

v0.25.0 finishes the exit-code taxonomy across every command, makes `workflow clean` safe to run, makes `install.sh` fail closed, and parses real MySQL 8.0 compressed transactions of 16 MiB and up. ADR 0009 records the exit-code design (`docs/adr/0009-exit-code-taxonomy-everywhere.md`).

**This release is breaking for scripts that check exit codes.** Read "Breaking: exit codes" below before upgrading automation.

## Breaking: exit codes (#277, #284, #286, ADR 0009)

- Every command error now has a kind. A test fails the build if any command error is created without one.
- New exit code **9 `unsupported`**, for format and version limits only: a STATEMENT or MIXED binlog with no ROW images, a report with a newer `report_version`, or a workflow plan or manifest version this build cannot run.
- Many failures that exited 1 now exit 3 (usage), 4 (input), 5 (corrupt), 6 (refused) or 8 (output). Examples: `snapshot save` without `--name` (3), an unknown snapshot or missing workflow plan (4), a report or snapshot that is `{}` (5), `workflow resume` after plan.yaml changed (6, `plan_changed`), a failed snapshot or artifact write (8).
- `--error-format=` (empty) is now a usage error (3) instead of being ignored. `analyze /dev/null` and empty stdin exit 5, like an empty file.
- Exit 1 now covers only failures with no better kind: ADR 0001's open `BEGIN`, an unclassified QUERY, and internal errors.
- The JSON error `kind` (`--error-format json`) is the stable contract. The full table is in `CHANGELOG.md` and [docs/reference/exit-codes.md](../reference/exit-codes.md). Scripts that test `$? -eq 1` or `$? -eq 4` must be updated.

## Changes

- **`workflow clean` is ledger-based (#287, #288, #289, #290, #291)**:
  - `workflow run`/`resume` append every file they write (path, step, run id, sha256) to `<output_dir>/ledger.jsonl`. Clean deletes only ledger-recorded files whose sha256 still matches and that no current step owns; everything else is listed in `kept_orphans`. `--force` (new) also removes unattributed outputs of recorded step kinds.
  - Files are moved to `<root>/.trash/<id>/` instead of unlinked, and every target is checked before anything moves.
  - Clean refuses and deletes nothing on an untrusted manifest (`{}` exits 5, no recorded steps with artifacts present exits 6), after a failed run (6, `run_not_successful`), with a symlinked kind dir (6, `trust_boundary`), with a ledger path that is absolute or contains `..` (5, `untrusted_ledger`), or when `manifest.snapshot_dir` does not match `--snapshot-dir` (6, `snapshot_dir_mismatch`).
  - `workflow run`, `resume` and `clean` take an exclusive lock on `<root>/.lock` (6, `locked`). `workflow export` includes `ledger.jsonl` and writes `snapshot_dir` as the relative `snapshots`.
- **`install.sh` fails closed (#281, #283, #285, #292)**:
  - With no `--version` it installs the latest release, so the docs can no longer drift.
  - A checksum mismatch, an archive missing from `checksums.txt`, or no `sha256sum`/`shasum`/`openssl` exits 1 and installs nothing. `--no-verify` skips the check on purpose, with a warning.
  - `wget` works when `curl` is missing; `--version 0.25.0` (no `v`) is accepted; a release that does not exist names the URL. A failed or interrupted install leaves the old binary in place.
  - The README tarball snippets resolve the latest tag and stop before unpacking when the checksum fails.
- **Compressed transactions of 16 MiB and up (#278, #293)**: with `binlog_transaction_compression=ON`, MySQL writes the uncompressed size of a payload of 16 MiB or more as a 9-byte field, which v0.24.0 refused with exit 5. It now parses.
  - Decompression is streamed and bounded. Each inner event is checked and released as it decodes, and a payload reaches `analyze`/`flashback` only after it decodes cleanly, so a payload corrupt at its last event prints nothing and exits 5.
  - A payload may declare at most 1 GiB uncompressed and name at most 65536 distinct tables. Over the limit, the error reads like `payload declares 1.0 GiB (1073741825 bytes) uncompressed, more than the limit 1.0 GiB (1073741824 bytes)` (exit 5).
  - A valid payload is decompressed twice (check, then hand off). A ~1 GiB payload of tiny events takes about 45 s.

## Verification

BinlogQA's QA on main `353a2dd0` passed, on top of the full pre-release QA on `9fde909b`.

- **Compressed binlogs**: real MySQL 8.0.36 binlogs of 20, 21 and 30 MB with transactions over 16 MiB parse at 95–160 MB peak memory, and the flashback plan matches what was written. Binlogs under 16 MiB match v0.24.0 byte for byte.
- **Hostile input**: compression bombs (33 KB declaring 1 GiB) and a 99 KB file of ~14.7 million tiny events declaring ~1 GiB exit 5 at 44 MB peak memory or less, with empty stdout. 86 constructed files: no exit 2.
- **Exit codes**: the 67-case matrix matches CHANGELOG and `docs/reference/exit-codes.md` for every reason of codes 3/4/5/6/9.
- **analyze/flashback against v0.24.0**: 312 runs; the 13 differences are 12 STATEMENT binlogs now exiting 9 and one HTML timestamp.
- **Workflow**: validate, run, status, resume, rerun, clean (preview, `--apply`, `--force`), export and restore, concurrent lock and clean after a failure all pass.
- **install.sh**: tampered archives, missing checksums, no sha256 tool, wget-only and interrupted installs all behave as above.
- CI and `go test ./...` pass.

## Compatibility

- **Exit codes changed** (see above). The JSON error format itself is unchanged.
- Workflow directories from earlier versions have no `ledger.jsonl`, so `workflow clean` refuses (6, `no_ledger`). Re-run `binlogviz workflow run <plan.yaml>` into the directory to rebuild it, or pass `--force` (it never deletes snapshots).
- Clean JSON gains `force`, `ledger`, `kept_orphans` and `skip_reasons`; `--apply` adds `trash_dir` and `snapshot_trash_dir`. The manifest gains `run_id`.
- JSON `report_version` stays `3`. Snapshots, artifact names and supported platforms are unchanged from v0.24.0.
- `flashback` never connects to MySQL. Review the script, test it, and apply it in one new session on the primary with `sql_log_bin=1`, with the script header.

## Known gaps

- `install.sh` P3s from #285: an uppercase-hex checksum is reported as a mismatch, `--version` accepts `/`, and the `--dry-run` README example needs `sh ./install.sh`.
- #255, #256, #257, #270 carry over from v0.24.0.

See `docs/concept/limitations.md` for the flashback limitations carried over from earlier releases.
