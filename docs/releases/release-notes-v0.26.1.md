# BinlogViz v0.26.1 Release Notes

Release date: 2026-10-17

## Overview

v0.26.1 is a patch release. It fixes `flashback --conflict guard` for a primary key that was moved and then reused, the last open item of #270. There are no breaking changes: exit codes, `plan_version` 2, analyze output and the default `--conflict refuse` behaviour are the same as v0.26.0.

## Fixed

- **`--conflict guard` and a reused primary key (#270, #299)**: when an UPDATE moved a row's primary key (for example `id 1 → 101`) and a later row reused the old key `1`, the guard script's move-back failed with `ERROR 1062` and stopped the block. The move-back is now guarded like any other row changed later: it changes no row, and the rest of the block applies. The guard script header explains this.
- The default `--conflict refuse` still exits 7 for this case, so nothing changes unless you chose `--conflict guard`.

## Verification

BinlogQA's pre-release QA on main `5a7b2e84` passed, against the v0.26.0 tag.

- **Live MySQL 8.0.36**: the classic move-and-reuse case refuses with exit 7 and the guard script applies with the expected rows; a composite primary key with a partial key move, `_ci` case-only undo, `_ci` reuse and a chain with a reused key all apply without 1062.
- **Against v0.26.0**: analyze output, plain flashback SQL and stderr, and the refuse path are byte-identical. Guard scripts differ only in the new header comment. The 67-case exit-code matrix differs only in that guard output.
- **#293 compression bombs** still exit 5 with empty stdout at memory in line with v0.26.0. Workflow smoke passes.
- CI and `go test ./...` pass.

## Compatibility

- No breaking changes. `plan_version` stays 2, `report_version` stays `3`, and exit codes, snapshots, artifact names and supported platforms are unchanged from v0.26.0.
- `flashback` never connects to MySQL. Review the script, test it, and apply it in one new session on the primary with `sql_log_bin=1`, with the script header.

## Known gaps

- `install.sh` P3s from #285, and #255, #256, #257 carry over from v0.26.0.

See `docs/concept/limitations.md` for the flashback limitations carried over from earlier releases.
