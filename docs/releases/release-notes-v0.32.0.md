# BinlogViz v0.32.0 Release Notes

Release date: 2026-10-20

## Overview

v0.32.0 implements ADR 0014: a flashback apply that fails never commits part of the undo. The undo runs in one XA transaction, so a `BEGIN`, `START TRANSACTION`, or DDL in the script or after it cannot commit it, and the finish commits only when every statement succeeded (#328, #329, #332). The apply grants are now documented and minimal, each run gets its own finish procedure, and a failure before `XA START` stays recorded under `mysql --force` (#333, #334, #335, #337).

**Apply now needs new grants, and v0.31.0 scripts must be regenerated.** Grant the applying account, on the target server:

```sql
GRANT CREATE, CREATE ROUTINE, ALTER ROUTINE ON `binlogviz`.* TO 'account'@'host';
-- only when automatic_sp_privileges=0:
GRANT EXECUTE ON `binlogviz`.* TO 'account'@'host';
```

Apply is still `binlogviz flashback verify flashback.sql | mysql --default-character-set=utf8mb4`.

## Breaking

- **Flashback SQL from v0.31.0 does not verify. Regenerate it with v0.32.0 (ADR 0014).**
  - Apply creates the `binlogviz` schema if it is absent and a finish procedure in it. The applying account needs `CREATE`, `CREATE ROUTINE`, and `ALTER ROUTINE` on `binlogviz.*`, plus `EXECUTE` on `binlogviz.*` when `automatic_sp_privileges=0`. It still needs `INSERT`, `UPDATE`, and `DELETE` on the tables the undo changes.
  - `XA_RECOVER_ADMIN` is not required on 8.0, and `SUPER` is not required on 5.7. `DROP` on the schema is not required to apply.
  - The stock `mysql` client is required (`DELIMITER`).
  - `schema_version` stays 1 and `plan_version` stays 2.

## Fixed

- **A failed apply never commits a prefix of the undo (#328, #329, #332, ADR 0014).**
  - `verify | mysql --force` no longer commits part of the undo when the target is missing a table or a column (#328).
  - A truncated file followed by another script, `BEGIN`, `START TRANSACTION`, or DDL, and a complete file followed by those under `--force`, no longer commit the undo (#329). `BEGIN` inside the open XA transaction is ERROR 1399.
  - The undo is one XA transaction. The finish commits only when every statement succeeded; otherwise it rolls back. Killing the session mid-apply leaves `XA RECOVER` empty.
- **Apply grants are documented and minimal (#333).** `DROP PROCEDURE IF EXISTS` needs `ALTER ROUTINE` even when the procedure does not exist (ERROR 1370). `CREATE` on `binlogviz.*` covers `CREATE DATABASE IF NOT EXISTS binlogviz`. `XA RECOVER` is not used. The same grant works on MySQL 5.7 and 8.0. The agent guide (EN and zh-CN) lists `ALTER ROUTINE`.
- **A failure before `XA START` stays recorded under `mysql --force`.** Before, a missing `ALTER ROUTINE` failed the `DROP` with 1370, the following `CREATE PROCEDURE` succeeded and cleared the error, and the undo committed. Now every statement in the procedure section records its failure in a flag that a later success cannot clear, so the finish does not commit.
- **Each apply gets its own finish procedure (#334).** `verify` names it `binlogviz.binlogviz_fb_end_<token>_<nonce>` and drops it at the end. A leftover routine, including one owned by a `SYSTEM_USER` account (ERROR 1227 on MySQL 8.0), does not block the next apply. New scripts create the procedure `SQL SECURITY INVOKER`.
- `binlogviz flashback verify --help` prints that command's usage, `flashback verify <file>` (#335).

## Verification

BinlogQA's PR QA on #332 (`68c32d52`, MySQL 8.0.36) and tip QA on main `68c32d52` (MySQL 5.7.44), then PR QA on #337 (`3338ed61`, after a FAIL on `7f1d1212` that found the `--force` hole) on 8.0.36 and 5.7.44, passed with no P0 or P1. Main is `3338ed61`. Everything ran under a 3 GiB address-space limit.

- **No partial commit (#328, #329)**: missing table or column 4/4 with and without `--force` leave sibling tables unchanged (ERROR 1146 / 1054); the #329 shapes 14/14 on #337 (20/20 on 5.7 at `68c32d52`) leave the tables unchanged. On v0.31.0's main `f1a62e23` these committed a prefix or the full undo.
- **Grants (#333)**: the documented grants restore the pre-incident state with no leftover procedure on 8.0 and 5.7; `automatic_sp_privileges=0` plus `EXECUTE` also works. Every missing grant, with and without `--force`, leaves the data unchanged and `XA RECOVER` empty. In a same-run A/B, missing `ALTER ROUTINE` with `--force` commits on `7f1d1212` and leaves the tables unchanged on `3338ed61`, on both servers.
- **Leftovers (#334)**: root-owned leftovers do not block; 20 runs gave 20 distinct nonces.
- **Round trips**: n316 restores, and rt325 with generated columns restores to checksum 3356976996 on 8.0 and 5.7.
- **Unchanged**: tamper cases still exit 6; `verify` RSS 34.3 / 34.1 MB (SQL / JSON) on a 637 MB script; `BINLOGVIZ_MEMORY_BYTES=1` exits 10; analyze byte-identical to `68c32d52`; the 66-case matrix keeps rc and kind; `plan_version` 2.
- `go test` / `go vet` / `-race` / `CGO_ENABLED=0` build pass. CI green.

## Compatibility

- **Upgrade steps:**
  1. Grant the applying account `CREATE`, `CREATE ROUTINE`, `ALTER ROUTINE` on `binlogviz.*`, and `EXECUTE` on `binlogviz.*` if `automatic_sp_privileges=0`.
  2. Regenerate any flashback script written by v0.31.0 or earlier. `verify` refuses it.
  3. Clean leftover finish procedures, if any, as an account that can drop them (on 8.0, a `SYSTEM_USER` definer means the cleaner needs `SYSTEM_USER`; `DROP DATABASE` needs `DROP`): `DROP DATABASE IF EXISTS binlogviz;`. The next apply creates the schema again; it holds only these helpers.
- `report_version` stays 3, `plan_version` stays 2, `flashback verify --format json` `schema_version` stays 1. Analyze output is unchanged.
- The SQL `verify` prints is not byte-identical to the file: the finish procedure name and the trailer hex differ on each run.
- `flashback` and `flashback verify` never connect to MySQL. Apply one script per session, on the primary, with `sql_log_bin=1`.
- Hand-editing the SQL that `verify` prints is outside the guarantee (ADR 0014, "Not guaranteed").

## Known issues

- **#338 (P2)**: some missing-grant paths leave the per-run finish procedure `binlogviz.binlogviz_fb_end_<token>_<nonce>` behind (missing `ALTER ROUTINE` with `--force`, missing `INSERT`/`UPDATE`/`DELETE` without `--force`, missing `EXECUTE` with `automatic_sp_privileges=0`). No data changes and later applies are not blocked. Clean up with `DROP DATABASE IF EXISTS binlogviz`.
- **#303 (P2)**: the Linux release binaries report an empty `commit` and `commit_time` in `binlogviz version`. The fix is in `.github/workflows/release.yml` and needs a maintainer with workflow access. Check a Linux binary against `binlogviz_0.32.0_checksums.txt` rather than by commit.
- `flashback verify` does not accept `-` for stdin (exit 4), carried over from #326.
- **#312 (P3)** and **#270 (P3)** carry over, as do #255, #256, #257, the `.github/workflows/README.md` archive list, and the `install.sh` P3s from #285.

See `docs/concept/limitations.md` and `docs/adr/0014-flashback-no-partial-commit.md` for the full guarantee table and its limits.
