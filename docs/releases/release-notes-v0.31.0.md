# BinlogViz v0.31.0 Release Notes

Release date: 2026-10-20

## Overview

v0.31.0 implements ADR 0013: a flashback script commits nothing unless it is complete and unmodified. The published undo is one transaction and the file never commits by itself. `binlogviz flashback verify` checks the digest and is what emits the conditional `COMMIT`. A partial, edited, reordered, appended, or truncated script is refused with exit 6 and commits nothing (#318, #321). `verify` streams the script, so its memory stays flat on large undo files (#327).

**This is a breaking change for anyone applying flashback output.** `mysql < flashback.sql` no longer changes data, including when the file is intact. Apply with:

```sh
binlogviz flashback verify flashback.sql | mysql --default-character-set=utf8mb4
```

## Breaking

- **Applying a flashback script is a new command (ADR 0013, #318, #321).**
  - The SQL output is the new sealed format and is not byte-compatible with v0.30.0. `plan_version` stays 2.
  - The undo is one transaction, not one per original transaction. A statement that fails rolls the whole undo back.
  - The file on disk ends with a trailer that errors, so `mysql < flashback.sql` (with or without `--force`) rolls back and commits nothing.
  - `binlogviz flashback verify <file>` recomputes the digest and, only when it matches, prints the script with a prepared `COMMIT` / `ROLLBACK` chosen inside MySQL from the sealed state. `--format json` prints one object (`schema_version` 1, `command`, `ok`, `statements`, `digest`) instead.
- **`BINLOGVIZ_SPILL_BYTES` and `BINLOGVIZ_MEMORY_BYTES` are validated.** When set, each must be a positive base-10 integer. `abc`, `0`, `-1`, `1MB`, and empty are exit 3 before a binlog is read. Unset still means no spill ceiling, and the memory ceiling stays `GOMEMLIMIT`.

## Fixed

- **A flashback script commits nothing unless it is complete and unmodified (#318, #321, ADR 0013).**
  - Edited values, a deleted undo statement, reordered statements, appended SQL, and a truncated file are refused by `flashback verify` with exit 6 (`details.reason` `not_flashback`, `truncated`, `missing_trailer`, `appended`, or `digest_mismatch`). Nothing is written to stdout and nothing commits under `mysql`, including `--force`.
  - A guard token that does not match inside one statement is ERROR 1242, not a silent zero-row update.
  - Generation writes the script to a temp file and copies it to stdout only after that write succeeds, so exit 8 does not leave an applyable prefix.
- **`flashback verify` streams instead of loading the script into memory (#327).** It hashes the undo while it reads and emits SQL from a temp file after the digest matches. On 56 MB–454 MB scripts, peak RSS is about 34–41 MB; a 1.13 GB legacy file is read through at about 34 MB. A heap ceiling (`BINLOGVIZ_MEMORY_BYTES`, or `GOMEMLIMIT` when unset) is exit 10 `memory_limit`; a full disk while writing the temp file is exit 8. Neither is exit 2.
- `TestGeneratedReviewRSS` skips under `-race`, and its failure text says 64MB.

## Verification

BinlogQA's PR QA on #326 (`325deb24`, after a FAIL on `d0ac9c81` that found #327) and release-gate QA on main `f1a62e23` passed with no P0 or P1. Baseline was the released v0.30.0 archive. MySQL 8.0.36, everything under a 3 GiB address-space limit.

- **Streaming verify (#327)**: 56.5 / 109.7 / 166.9 / 403.6 MB scripts and a 453.7 MB generated-column script verify at 34–41 MB VmHWM with exit 0 (at `d0ac9c81` the 166.9 MB and 403.6 MB scripts OOMed with exit 2). `BINLOGVIZ_MEMORY_BYTES=1` → exit 10 `memory_limit`, empty stdout.
- **Integrity (#318, #321)**: intact script verify exit 0; bare `mysql` with and without `--force` changes no rows; `verify | mysql` restores the pre-incident checksum. Tamper matrix 52/52 (72/72 on the PR) → exit 6, no commits. Exit 8 (spill full, `/dev/full`, stdout full) leaves no usable prefix; the 8 MB partial is refused as `truncated`.
- **Unchanged output**: analyze spot check 40/40 byte-identical to v0.30.0. `flashback --plan` differs only in wording. The 67-case matrix keeps rc and kind. SQL is the new sealed format by design.
- `go test` / `go vet` / `CGO_ENABLED=0` build pass; `version --format json` is correct. CI green.

## Compatibility

- **Breaking for flashback apply.** Scripts are not byte-compatible with v0.30.0, and `mysql < file` no longer applies them. Pipe through `binlogviz flashback verify`. Scripts written by v0.30.0 or earlier are not sealed and are refused by `verify`; regenerate them with v0.31.0.
- `report_version` stays 3, `plan_version` stays 2. `flashback verify --format json` is a new versioned document (`schema_version` 1).
- Analyze output is unchanged.
- `flashback` and `flashback verify` never connect to MySQL. Review and test the script, then apply it in one new session on the primary with `sql_log_bin=1`.
- Generated scripts are larger than v0.30.0 (roughly 2.15× plain and 1.21× with `--conflict guard`) because each statement carries its digest payload.

## Known issues

- **#328 (P2)**: `verify | mysql --force` can commit part of the undo when the target is missing a table or column. The trailer checks that the text is complete, not that every statement succeeded. Do not use `--force` when applying; without it nothing commits.
- **#329 (P2)**: the "file does not commit" guarantee is broken by an implicit commit. A truncated file followed by another script, `BEGIN`, or DDL in the same session can commit the truncated part even without `--force`; with `--force`, a complete file followed by another script, `BEGIN`, `START TRANSACTION`, or DDL can commit. Apply one script per session, through `verify`.
- **#303 (P2)**: the Linux release binaries report an empty `commit` and `commit_time` in `binlogviz version`. Check a Linux binary against `binlogviz_0.31.0_checksums.txt` rather than by commit.
- P3 follow-ups from #326: `flashback verify --help` prints the parent command help (`Usage` reads `flashback flashback`), and `verify` does not accept `-` for stdin (exit 4).
- **#312 (P3)** and other P3 follow-ups from earlier releases carry over, as do the `.github/workflows/README.md` archive list, the `install.sh` P3s from #285, and #255 (items 3, 5, 7), #256, #257.

See `docs/concept/limitations.md` and `docs/adr/0013-flashback-apply-integrity.md` for the full guarantee table and its limits.
