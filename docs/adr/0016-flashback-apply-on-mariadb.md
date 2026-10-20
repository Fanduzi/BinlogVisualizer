# ADR 0016: A flashback script stops on MariaDB

Status: accepted (2026-10-20). From BinlogQA's review of PR #366 (finding F2). Extends ADR 0014; changes nothing on MySQL.

## Context

Flashback reads MariaDB 10.11 and 11.8 binlogs, and the undo statements are exact: BinlogQA ran them alone in a plain transaction on MariaDB 10.11.19 and 11.8.9 and got the rows back byte for byte. The script around them is the ADR 0013/0014 apply protocol, and that protocol is MySQL syntax:

- The finish procedure's handler starts with `GET STACKED DIAGNOSTICS`. MariaDB has no `STACKED` and rejects the `CREATE PROCEDURE` with ERROR 1064. Without `--force` the client stopped there, an error that does not say why.
- After every raw piece the client runs `GET DIAGNOSTICS CONDITION 1 ...`. On MariaDB that reads differently when the previous statement left no condition (1758, Invalid condition number), so the three-line check does not record the right error. With `--force` the run rolled back with `ERROR 1758`, again not the reason.

Probed on the live servers: plain `GET DIAGNOSTICS` inside a handler reads the condition; `XA START`, `XA END`, `XA COMMIT` work inside a procedure; `BEGIN` or DDL while the XA is `ACTIVE` gives 1399, as on MySQL; `@@error_count` read by the next table-less statement is the previous statement's error count.

So a MariaDB apply path looks possible. It would need its own check line (`@@error_count` instead of the `GET DIAGNOSTICS` triple), a handler without `STACKED`, a verifier that accepts a second set of check lines, and the whole ADR 0014 table (no committed prefix under `--force`, under a cut file, under `BEGIN`/DDL, after a reconnect) proved again on MariaDB. That is a second safety protocol, and it does not belong in a PR that adds test fixtures.

## Decision

Apply on MariaDB is not supported, and the script says so first:

- The second raw piece of every script, right after the mode strip, is `guard.MariaDBStopSQL()`. When `@@version` contains `MariaDB` it sets `@binlogviz_applied = 0`, keeps the first error as `1235` with the stop text, and prepares `SET SESSION sql_mode = 'binlogviz: flashback apply does not support MariaDB; no rows were changed; see docs/concept/limitations.md'`, which fails with ERROR 1231 and that text. On MySQL it prepares `DO 0`.
- The flag is set inside the piece, not by the check lines after it, because on MariaDB those check lines are the part that cannot be trusted. With `--force` the client runs on, the finish sees `@binlogviz_applied = 0` and rolls back, and the last line is the rollback line with `ERROR 1235` and the same text.
- It is a raw piece, so it is digested like any other. `flashback verify` is unchanged, and a script written before this change still verifies.
- Generation warns `mariadb_apply_unsupported` on stderr and in the `--plan` warnings when a planned row came from a MariaDB binlog. The refusals (row image, row metadata, DDL) are unchanged.

Checked live (evidence in the PR): MariaDB 10.11.19 and 11.8.9 with the `mariadb` client and with the MySQL 8.0 client, with and without `--force`: ERROR 1231 with the text, tables unchanged, no XA left. MySQL 8.4.11 and 9.7.2, with and without `--force`: undo committed, tables restored.

## Consequences

- MariaDB users get exact undo statements to review and a clear stop if they try to apply them, instead of an unrelated syntax error.
- The `binlogviz` database can still be created by `CREATE DATABASE IF NOT EXISTS` under `--force` on MariaDB, as before.
- A MariaDB apply dialect is future work. It must keep the ADR 0014 guarantees and be tested on MariaDB in CI before the stop is removed.
