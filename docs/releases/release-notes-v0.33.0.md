# BinlogViz v0.33.0 Release Notes

Release date: 2026-10-20

## Overview

v0.33.0 fixes the flashback apply that v0.32.0 shipped. The documented grants omitted `SELECT`, so `UPDATE` and `DELETE ... WHERE` undos failed with ERROR 1143 (#341). Under `PAD_CHAR_TO_FULL_LENGTH`, or with `BINARY(n)` keys, a keyed undo could match 0 rows and still commit (#347, #348). A failed apply under `mysql --force` rolled back silently and could leave its finish procedure behind (#338, #343). Each apply now ends with one line that says what happened, and automation checks that line.

**v0.32.0 flashback scripts must be regenerated.** `flashback verify` refuses them with exit 6 and `details.reason` `old_format` (#342). This is why the release is a minor version, not a patch.

Grant the applying account, on the target server:

```sql
GRANT CREATE, CREATE ROUTINE, ALTER ROUTINE ON `binlogviz`.* TO 'account'@'host';
GRANT SELECT, INSERT, UPDATE, DELETE ON `schema`.* TO 'account'@'host';
-- only when automatic_sp_privileges=0:
GRANT EXECUTE ON `binlogviz`.* TO 'account'@'host';
```

Apply and check the result:

```bash
set -o pipefail
binlogviz flashback verify flashback.sql | mysql --default-character-set=utf8mb4 \
  | grep -E '^binlogviz: undo committed, [0-9]+ statements$'
```

## Breaking

- **Flashback SQL from v0.32.0 or v0.31.0 does not verify. Regenerate it with v0.33.0.** `flashback verify` reports an intact old script as `details.reason` `old_format` and says to regenerate it, instead of calling it truncated (#342).
- **Exit 0 from `mysql` is not success.** Success is a line on stdout:
  - `binlogviz: undo committed, N statements` when nothing was skipped (`^binlogviz: undo committed, [0-9]+ statements$`).
  - `binlogviz: undo committed with skips, N applied, M skipped` when `--conflict guard` skipped rows (`^binlogviz: undo committed with skips, [0-9]+ applied, [0-9]+ skipped$`).
  - `binlogviz: undo rolled back, no rows changed` when the apply failed. Stderr then shows the real errno and message.
  - Use `set -o pipefail` and an anchored `grep -E`. `mysql --force -v` echoes the statement, and a plain grep of that echo is not a commit.

## Fixed

- **Apply grants include `SELECT` (#341).** The data grant on the tables the undo changes is `SELECT, INSERT, UPDATE, DELETE`. The v0.32.0 release notes have a correction. The least-privilege e2e grants exactly the documented list.
- **A keyed undo that does not change exactly one row rolls the apply back (#347).** `PAD_CHAR_TO_FULL_LENGTH` is removed before `CREATE PROCEDURE`, so `CHAR` keys match. Under the default `--conflict refuse`, a keyed `UPDATE` or `DELETE` undo that affects 0 or more than 1 rows, including a no-PK `LIMIT 1` match, rolls the whole apply back. The saved `sql_mode` is restored after the apply, including when it fails.
- **`BINARY(n)` values are padded to n bytes with `0x00` (#348),** in `WHERE`, `SET`, and `INSERT`. v0.32.0 wrote the short value, so a keyed undo matched 0 rows. `VARBINARY`, `BLOB`, and `CHAR` are not padded.
- **A failed apply drops its finish procedure and says so (#338, #343).** It prints `binlogviz: undo rolled back, no rows changed`, and stderr carries the real errno and message. A kill before the finish `CALL`, or a `CALL` that never enters, can still leave the procedure. `DROP DATABASE IF EXISTS binlogviz` needs `DROP` on `binlogviz.*`.
- **The commit line cannot be forged by row data.** The marker is built from `CONCAT` pieces, so the literal is not in the SQL, and control characters in a server message are replaced before the rollback line and its stderr copy.
- **`--conflict guard` counts honestly.** 1 row is applied, 0 rows is skipped, and any other count rolls back. A no-PK `INSERT` undo under `--conflict guard` inserts only when that image is absent, so a second apply skips it instead of duplicating the row.
- MySQL 5.7 warning 3090 (a deprecated `sql_mode` combination such as `MYSQL40` or `POSTGRESQL`) no longer rolls the apply back.
- A published file applied without `verify` does not report `ERROR 0`; the no-commit trailer is the error.
- `flashback --plan --sql-context off` reports `would_refuse` true with reason `sql_context_off` and exits 6.
- **Generation refusals say why (#344).** Exit-6 refusals set `details.reason` (`row_metadata`, `row_image`, `column_type`, `row_capture`, `ddl_in_range`, `generated_unknown`, `generated_mismatch`, `generated_row`, `schema_mismatch`, `sql_context_off`) and `details.table` when one table is named; `--plan` carries the same fields. A MySQL 5.7 binlog cannot be undone, and the message says to restore from a backup or PITR.
- README exit codes match the CLI (corrupt input is exit 5, a flashback refusal is exit 6). `trend` keeps argument order unless `--order time` is set. `install.sh --help` prints usage (#345).

## Verification

BinlogQA ran four rounds of PR QA on #346 (MySQL 8.0.36 and 5.7.44, all generations under a 3 GiB address-space limit). Rounds 1 to 3 found a P1 and then P2/P3 items that were fixed; round 4 on `41853984` passed with 0 P0, 0 P1, 0 P2 and 0 new P3. The PR was fast-forward merged, so main is the same `41853984`, and tip QA on main passed.

- **Least-privilege flow on 8.0.36 and 5.7.44**: with exactly the documented grants, the n316 and rt325 round trips restore the data (rt325 `CHECKSUM TABLE` 3356976996) with the clean commit line, no leftover routine, and an empty `XA RECOVER`. A guard partial skip prints the "with skips" line, and the clean anchored grep does not match it. A missing table prints the real ERROR 1146 on the rollback line and changes nothing.
- **#347 / #348**: all 17 `sql_mode` cases restore and put the saved mode back; `BINARY(6)` keys are written `0x00`-padded and restore.
- **Marker forgery**: 0 of 612 anchored false successes per server across separators (LF, CR, NEL, U+2028, U+2029 and others), client flags, and charsets.
- **Tamper**: truncated, appended, and edited scripts exit 6 with the right reason and write 0 bytes; a v0.32.0 script exits 6 `old_format`.
- **Size and speed**: a 1 GB binlog's script is 397 MiB; rt325 apply takes 16.9 s against 16.2 s on v0.32.0. `verify` stays streamed (VmHWM 50 to 66 MB on a 416 MB script).
- **Unchanged**: `analyze` in text, JSON, Markdown, and HTML is byte-identical to v0.32.0 apart from the HTML `Generated` time; `trend` output matches.
- `go test`, `go vet`, `-race`, and the `CGO_ENABLED=0` build pass. CI green.

## Compatibility

- **Upgrade steps:**
  1. Add `SELECT` to the applying account's grant on the tables the undo changes.
  2. Regenerate any flashback script written by v0.32.0 or earlier. `verify` refuses it.
  3. Change automation to check the anchored commit line, not the `mysql` exit code.
  4. Clean leftover finish procedures, if any: `DROP DATABASE IF EXISTS binlogviz;` (needs `DROP` on `binlogviz.*`).
- `report_version` stays 3, `plan_version` stays 2, and `flashback verify --format json` `schema_version` stays 1. `flashback --plan` adds `details.reason` and `details.table` to refusals. Analyze output is unchanged.
- `flashback` and `flashback verify` never connect to MySQL. Apply one script per session, on the primary, with `sql_log_bin=1`.
- A no-PK `INSERT` undo under the default `--conflict refuse` is not idempotent: applying the same script twice inserts the row twice. Use `--conflict guard` when a re-run is possible.

## Known issues

- **#303 (P2)**: the Linux release binaries report an empty `commit` and `commit_time` in `binlogviz version`. The fix is in `.github/workflows/release.yml` and needs a maintainer with workflow access. Check a Linux binary against `binlogviz_0.33.0_checksums.txt`.
- `flashback verify` does not accept `-` for stdin (exit 4).
- **#312**, **#270**, **#255**, **#256**, and **#257** (P3) carry over.

See `docs/concept/limitations.md` and `docs/adr/0014-flashback-no-partial-commit.md` for the full guarantee table and its limits.
