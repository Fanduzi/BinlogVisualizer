# BinlogViz v0.26.0 Release Notes

Release date: 2026-10-17

## Overview

v0.26.0 makes the `flashback --plan` JSON a contract an operator or an agent can act on without parsing stderr: every refusal prints a plan that says why, warnings carry stable codes, and the conflict list can be listed in full.

**This release is breaking for scripts that read `flashback --plan` JSON.** The plan is now `plan_version` 2. Check `plan_version` before reading the fields below.

## Breaking: `flashback --plan` is `plan_version` 2 (#270, #282, #296)

- Each `warnings` entry is now `{"code", "message"}` instead of a string. The code is stable across `--lang`: `partial_transaction`, `table_definition_missing`, `generated_column_unverified`, `schema_file_db_mismatch`, `schema_file_table_ambiguous`, `schema_file_case_folded`, `schema_file_case_mismatch`. The message has no localized `warning:` prefix.
- A refusal now prints the plan too. Exit 6 (DDL, non-FULL row image, FLOAT and the other refusals) used to print nothing on stdout; now the plan has `would_refuse: true` and `refusal: {"kind": "refused", "message": ...}`. Exit 7 sets `refusal.kind` to `"conflict"`. `refusal` is `null` when nothing is refused.
- So the JSON alone tells ok (exit 0, `refusal` null), refused (exit 6) and conflict (exit 7) apart.
- Usage, input, corrupt and output errors (exit 3/4/5/8) still print no plan.
- Stderr warnings and the flashback SQL are unchanged.

## Added

- **`flashback --conflict-limit N` (#270, #296)**: default 20, `0` lists every conflict. It bounds the conflicts listed in `--plan` and in the exit 7 error details; the `count` is always the full count. A negative or non-integer value exits 3. An operator can review every conflicting key before choosing `--conflict guard`.

## Verification

BinlogQA's pre-release QA on main `d87b14a5` passed, on top of the PR QA of #296 at `9bb00d45`, both against the v0.25.0 tag.

- **Plan v2** on a real MySQL 8.0.36 corpus with 25 later UPDATEs: exit 0/6/7 give `refusal` null/`refused`/`conflict`; the conflict plan reports count 25, lists 20 and omits 5; `--conflict-limit` 0, 1, 5, 20 and 100 behave as documented. Warning codes are identical under en and zh-CN.
- **Against v0.25.0**: analyze, 168 runs, 0 diffs; plain flashback SQL and stderr byte-identical on three corpora. The 67-case exit-code matrix differs only in the conflict-plan output size (v2 schema).
- **#293 compression bombs** still exit 5 at 43 MB peak memory or less with empty stdout. Workflow smoke passes.
- CI and `go test ./...` pass.

## Compatibility

- **`flashback --plan` JSON changed** (see above). Callers of v1 must handle `plan_version` 2.
- Exit codes, `report_version` `3`, snapshots, artifact names and supported platforms are unchanged from v0.25.0.
- `flashback` never connects to MySQL. Review the script, test it, and apply it in one new session on the primary with `sql_log_bin=1`, with the script header.

## Known gaps

- #270 item 1: if a primary key was changed and the old key reused later, the rollback fails with 1062; the guard does not skip it and the plan does not mention it. It fails safely.
- `install.sh` P3s from #285, and #255, #256, #257 carry over from v0.25.0.

See `docs/concept/limitations.md` for the flashback limitations carried over from earlier releases.
