# BinlogViz v0.32.1 Release Notes

Release date: 2026-10-20

## Overview

v0.32.1 fixes flashback apply on top of ADR 0014 (#346; fixes #338, #341, #342, #343, #344, #345, #347, #348). The undo still runs in one XA transaction and a failed apply still commits nothing. What changes is how the undo matches rows and how the operator learns the outcome:

- `PAD_CHAR_TO_FULL_LENGTH` and `NO_BACKSLASH_ESCAPES` are stripped before `CREATE PROCEDURE` (#347).
- A keyed `UPDATE` or `DELETE` undo must affect exactly one row, or the apply rolls back (#347).
- `BINARY(n)` values are padded to n bytes with `0x00` (#348).
- Success is an anchored marker line, not exit 0. A failed apply prints a rollback line with the real error and drops its finish procedure (#338, #343).

**Regenerate every flashback script written by v0.32.0 or earlier.** `flashback verify` refuses it with exit 6 and `details.reason` `old_format`. Grant the applying account, on the target server:

```sql
GRANT CREATE, CREATE ROUTINE, ALTER ROUTINE ON `binlogviz`.* TO 'account'@'host';
GRANT SELECT, INSERT, UPDATE, DELETE ON `schema`.* TO 'account'@'host';
-- only when automatic_sp_privileges=0:
GRANT EXECUTE ON `binlogviz`.* TO 'account'@'host';
```

Apply, and check success by the marker line:

```sh
set -o pipefail
binlogviz flashback verify flashback.sql | mysql --default-character-set=utf8mb4 \
  | tee apply.log
grep -E '^binlogviz: undo committed, [0-9]+ statements$' apply.log
```

## Breaking

- **Flashback SQL from v0.32.0 or earlier does not verify. Regenerate it with v0.32.1.** `flashback verify` reports an intact v0.31 or v0.32.0 script as exit 6, `details.reason` `old_format`, and says to regenerate it, instead of `truncated` (#342). Editing the finish mode line in the published suffix is still `truncated`, because that line is outside the digest.
- **Exit 0 is not success.** Success is one of these lines on stdout:
  - `^binlogviz: undo committed, [0-9]+ statements$` when nothing was skipped.
  - `^binlogviz: undo committed with skips, [0-9]+ applied, [0-9]+ skipped$` when `--conflict guard` skipped rows.
  
  Automation uses `set -o pipefail` and `grep -E` with the anchored pattern. The marker is built from `CONCAT` pieces, so the literal is not in the SQL; `mysql --force -v` echoes the statement, and a plain grep of that echo is not a commit.
- `schema_version` stays 1, `plan_version` stays 2, `report_version` stays 3.

## Fixed

- **Apply grants include `SELECT` (#341).** The data grant on the tables the undo changes is `SELECT, INSERT, UPDATE, DELETE`. `UPDATE` and `DELETE ... WHERE` need `SELECT` (ERROR 1143). The v0.32.0 notes carry a correction.
- **A failed apply says so and cleans up (#338, #343).** The finish rolls back, the client drops the per-run finish procedure, and the output is `binlogviz: undo rolled back, no rows changed` followed by `(ERROR N: message)` when a statement error was captured. Without `--force` mysql exits 1. Stderr is ERROR 1231 whose text includes the real errno and message, including a client-side error such as 1370 or 1044. A procedure cannot drop itself (ERROR 1357), so the drop runs after the finish returns. A kill before the finish `CALL`, a `CALL` that never enters, or a `SYSTEM_USER` definer the account cannot drop can still leave the procedure; clean up with `DROP DATABASE IF EXISTS binlogviz` (`DROP` on `binlogviz.*`).
- **The real errno on MySQL 8.0.** The finish reads `GET STACKED DIAGNOSTICS`, so 1143, 1146, and 1054 are not reported as 1243.
- **A row value cannot forge the commit line.** Control characters (C0 and DEL) in a server message are replaced with spaces before the rollback line and the stderr copy. A duplicate-key value containing a newline and `binlogviz: undo committed, 9 statements` no longer prints a standalone marker line under `mysql --force` with `-r`, `-t`, or `-vvv`.
- **`sql_mode` cannot change what the undo matches (#347).** Under `PAD_CHAR_TO_FULL_LENGTH`, a keyed undo on a `CHAR` key with a NO PAD collation (`utf8mb4_0900_ai_ci`, `utf8mb4_0900_bin`) matched 0 rows, the `INSERT` undos still ran, and the apply committed wrong data with exit 0. The first statement now saves `@@SESSION.sql_mode` and strips `PAD_CHAR_TO_FULL_LENGTH` and `NO_BACKSLASH_ESCAPES` before `CREATE PROCEDURE`; the procedure runs with that mode. A successful or failed apply restores the saved mode, including under `--force`. MySQL 5.7 warning 3090 (a deprecated combination mode such as `MYSQL40` or `POSTGRESQL`) is not a failure.
- **A keyed undo must affect exactly one row (#347).** A keyed `UPDATE` or `DELETE`, including a no-PK `LIMIT 1` match, that changes 0 or more than 1 row rolls the apply back with `keyed undo affected N rows, expected 1`. `N` in the marker counts undo statements that changed exactly one row.
- **`BINARY(n)` values are padded (#348).** Values are written padded to n bytes with `0x00`, in `WHERE`, `SET`, and `INSERT`. v0.32.0 wrote the short value (`X'6B31'` for a stored `6B3100000000`), so a keyed undo matched 0 rows and v0.32.0 committed the rest. A full-length value ending in `0x00`, such as a `BINARY(16)` UUID, is covered. `VARBINARY`, `BLOB`, and `CHAR` are not padded.
- **`--conflict guard` counts honestly.** 1 row changed is applied, 0 rows is skipped, any other count rolls back. The clean marker prints only when at least one row changed and nothing was skipped; an apply that only skipped rows prints `binlogviz: undo skipped, N rows` and no commit marker. A no-PK `INSERT` undo inserts only when that image is absent, so a second apply skips it instead of duplicating the row.
- A published file applied without `verify` errors on the no-commit trailer and does not print `ERROR 0`. A block cut off from the header sets the session read-only and does not call the finish procedure.
- **Generation refusals are machine-readable (#344).** Exit-6 refusals set `details.reason` (`row_metadata`, `row_image`, `column_type`, `row_capture`, `ddl_in_range`, `generated_unknown`, `generated_mismatch`, `generated_row`, `schema_mismatch`, `sql_context_off`) and `details.table` when one table is named. `flashback --plan` carries the same fields; `--plan --sql-context off` prints the plan with `would_refuse` true and exits 6. A MySQL 5.7 binlog cannot be undone and `--schema-file` does not supply row metadata; the message says to restore from a backup or PITR and to use MySQL 8.0.14+ with `binlog_row_metadata=FULL`.
- **README and installer (#345).** README exit codes match the CLI (corrupt input is exit 5, a flashback refusal is exit 6). `trend` keeps argument order unless `--order time` is set. `install.sh --help` prints usage.

## Verification

BinlogQA ran four rounds of PR QA on #346 (FAIL on r2 and r3, PASS on r4 at `41853984`) and tip QA on main `41853984` on MySQL 8.0.36 and 5.7.44, with no P0, P1, P2, or new P3. Everything ran under a 3 GiB address-space limit.

- **Least-privilege flow**: the documented grants with the stock `mysql` client, `set -o pipefail`, and the anchored grep. n316 restores; rt325 restores to checksum 3356976996; a guard apply with 2 of 8 rows drifted prints `with skips, 6 applied, 2 skipped` and the clean grep does not match; a missing table prints the real ERROR 1146 on the rollback line, no marker, no leftover routine, and `XA RECOVER` is empty.
- **`BINARY(n)` (#348)**: refuse and guard both restore; `BINARY(6)` keys are written 0x00-padded.
- **`sql_mode` (#347)**: 17 modes on 8.0 and 16 on 5.7 restore and restore the saved mode.
- **Marker forgery**: 0/612 anchored false successes per server across separators, client flags, and character sets in PR QA; 0/12 in tip QA.
- **Failure matrix**: 22 cases per server surface the real errno and message; tamper cases still exit 6; a v0.32.0 script is `old_format`.
- **Unchanged**: analyze is byte-identical to v0.32.0 on 5 binlogs × 4 formats (the only difference is an HTML `Generated` timestamp); `trend` output is identical.
- `go test` / `go vet` / `-race` / `CGO_ENABLED=0` build pass. CI green.

## Compatibility

- **Upgrade steps:**
  1. Make sure the applying account has `SELECT, INSERT, UPDATE, DELETE` on the tables the undo changes, plus `CREATE`, `CREATE ROUTINE`, `ALTER ROUTINE` on `binlogviz.*` (and `EXECUTE` there if `automatic_sp_privileges=0`).
  2. Regenerate any flashback script written by v0.32.0 or earlier. `verify` refuses it as `old_format`.
  3. Change automation to check the anchored marker line, not the mysql exit code.
  4. Clean leftover finish procedures, if any, with `DROP DATABASE IF EXISTS binlogviz;`.
- `report_version` stays 3, `plan_version` stays 2, `flashback verify --format json` `schema_version` stays 1. Analyze output is unchanged.
- A stream cut after `COMMIT` leaves the undo committed with no marker.
- `flashback` and `flashback verify` never connect to MySQL. Apply one script per session, on the primary, with `sql_log_bin=1`.

## Known issues

- **#303 (P2)**: Linux release binaries may report an empty `commit` and `commit_time` in `binlogviz version`. The fix is in `.github/workflows/release.yml` and needs a maintainer with workflow access. Check a Linux binary against `binlogviz_0.32.1_checksums.txt` rather than by commit.
- `flashback verify` does not accept `-` for stdin (exit 4), carried over from #326.
- **#312 (P3)** and **#270 (P3)** carry over, as do #255, #256, #257.

See `docs/concept/limitations.md` and `docs/adr/0014-flashback-no-partial-commit.md` for the full guarantee table and its limits.
