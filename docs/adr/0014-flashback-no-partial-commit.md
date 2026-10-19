# ADR 0014: A failed flashback apply does not commit a prefix

Status: accepted (2026-10-19). Tracks #328 and #329. Supersedes the commit and implicit-commit rows of ADR 0013. The seal, the digest, `flashback verify` exit codes, and `schema_version` 1 stay as ADR 0013 and ADR 0010 describe them.

## Context

ADR 0013 publishes a script that does not commit. The undo is one transaction. The last line sets `sql_mode` to a value MySQL rejects, so `mysql < file` stops and the server rolls the open transaction back on disconnect. `binlogviz flashback verify` replaces that line with a prepared `COMMIT` or `ROLLBACK` chosen from `@binlogviz_sealed`. The digest proves the text. It does not prove that every statement succeeded.

BinlogQA on PR #326 (comment 6271884791), reproduced here on MySQL 8.0.46, showed two ways a prefix still commits:

- `verify | mysql --force` on a target missing a table or a column. `--force` continues after ERROR 1146 or 1054 (and the ERROR 1243 from `EXECUTE` of a prepare that failed). `@binlogviz_sealed` is still 1, so the finish runs `COMMIT`. The statements that succeeded are committed. Without `--force` the client stops and nothing is committed.
- An implicit commit while that transaction is open. `BEGIN`, `START TRANSACTION`, `COMMIT`, `SET autocommit = 1`, and DDL each commit the open transaction and then do their own work. A file cut on a line boundary, followed by another script, `BEGIN`, or DDL, commits the cut prefix with no `--force`. With `--force`, an intact file followed by another script, `BEGIN`, `START TRANSACTION`, or DDL does the same: the trailer errors, `--force` continues, and the next statement commits the undo.

The `mysql` client sends one statement at a time. It cannot see SQL it has not read yet. `--force` skips a statement error and sends the next one. A statement that implicitly commits does so inside that statement, before any later statement of ours can run. `COMMIT` and `ROLLBACK` can be prepared. `XA START`, `XA END`, `XA COMMIT`, and `XA ROLLBACK` cannot (ERROR 1295). `XA COMMIT` without `ONE PHASE` requires the `PREPARED` state. `SIGNAL` is illegal outside a stored program. A procedure body needs `DELIMITER`, which the stock `mysql` client honors and which is not sent to the server.

Inside an `XA` transaction in `ACTIVE` state, `BEGIN`, `START TRANSACTION`, `COMMIT`, `SET autocommit = 1`, and DDL fail with ERROR 1399 (`XAER_RMFAIL`) and do not commit. Disconnect rolls an `ACTIVE` or `IDLE` XA transaction back. An `XA PREPARE` would survive disconnect; this script never prepares. `@@error_count` read by the next statement is the error count of the statement that just ran. A successful `DEALLOCATE PREPARE` sets it back to 0, so the check has to run before that. `PREPARE` and `EXECUTE` work inside an XA transaction. `XA END` / `XA COMMIT` / `XA ROLLBACK` work inside a stored procedure. A procedure name with no schema fails with ERROR 1046 when the session has no default database. `CREATE PROCEDURE` and `CREATE DATABASE` are implicit commits, so they have to run before `XA START`.

A non-transactional engine does not join the XA transaction. On MyISAM, the row change is durable at the statement, and `XA ROLLBACK` does not undo it.

## Decision

### 1. Every statement's success is a precondition of the commit

The digest init sets `@binlogviz_applied = 1`. After each `PREPARE` and each `EXECUTE`, and after each raw section:

```sql
SET @binlogviz_applied = IF(@binlogviz_applied <=> 1 AND @@error_count = 0, 1, 0);
```

`@binlogviz_applied` only moves from 1 to 0. A later successful statement does not set it back. The check lines are part of the piece format. `verify` rejects a script that drops them (exit 6, `truncated`).

The finish commits only when `@binlogviz_sealed <=> 1 AND @binlogviz_applied <=> 1 AND @binlogviz_want <=> 1`. `verify` is what sets `@binlogviz_want = 1`. The published file sets `@binlogviz_want = 0`. An intact text whose target rejects a statement keeps `@binlogviz_applied = 0`, so `--force` still rolls the undo back.

### 2. The undo is an XA transaction, so an implicit commit cannot land

`CREATE DATABASE IF NOT EXISTS binlogviz` and the finish procedure run before the undo, after the guard header. The procedure name and the XA id are the session token (16 hex characters), so two scripts do not share an id. The procedure is `binlogviz.binlogviz_fb_end_<token>`. Apply needs `CREATE` and `CREATE ROUTINE`. The stock `mysql` client is required because the body is introduced with `DELIMITER`.

The undo opens with `XA START '<token>'`. If that fails, the next statement is `SET SESSION TRANSACTION READ ONLY` unless `@binlogviz_applied` is still 1, so a client that continues does not autocommit the following undo. While the XA transaction is `ACTIVE`, `BEGIN`, `START TRANSACTION`, DDL, `COMMIT`, and `SET autocommit = 1` fail with ERROR 1399 and leave the undo uncommitted. Disconnect rolls it back.

The published finish calls the procedure with `@binlogviz_want = 0`, which is `XA END` and `XA ROLLBACK`, then sets the rejected `sql_mode` so the client exits non-zero. `verify` prints the same call with `@binlogviz_want = 1`. The procedure then `XA COMMIT '<token>' ONE PHASE` only when the digest matched and every checked statement succeeded. Otherwise it rolls back. It does not `XA PREPARE`.

`schema_version` stays 1. The SQL is not byte-compatible with v0.31.0. Regenerate the script.

### 3. What is guaranteed

| What happened | `mysql < file` | `verify` | `verify \| mysql` | `verify \| mysql --force` |
|---|---|---|---|---|
| Intact script, every statement succeeds | trailer error, rollback, no change | exit 0 | restores the pre-change rows | restores the pre-change rows |
| Intact script, a statement fails (missing table or column) | client stops, rollback, no change | exit 0 | client stops, rollback, no change | ERROR 1146 or 1054, then rollback, no change |
| Edited value, deleted statement, or reordered statements | no commit | exit 6, `digest_mismatch` | not produced | not produced |
| Appended statement, or bytes after the trailer | no commit | exit 6, `appended` | not produced | not produced |
| File cut short | no commit; a following `BEGIN`, `START TRANSACTION`, DDL, or second script in the same session is ERROR 1399 and does not commit the prefix | exit 6, `truncated` or `missing_trailer` | not produced | not produced |
| Intact file, then in the same session another script, `BEGIN`, `START TRANSACTION`, or DDL, with `--force` | the finish already rolled the XA back; the follower does not commit the undo | unchanged | the follower does not commit the undo | the follower does not commit the undo |
| Token changed inside one statement | that statement is ERROR 1242; no commit | exit 6, `digest_mismatch` | `ROLLBACK` when the session token is wrong | same |
| Generation exited 8 | no script bytes were written | | | |

## Not guaranteed

These refuse or leave the data unchanged when the tool can make them do that. They are not a silent commit of a prefix.

- An edit that recomputes the payloads and the trailer hex is a new script. `verify` accepts it.
- A hand edit of the guard header or of `XA START` that changes the raw SQL and not its digest payload is invisible to the in-SQL chain. `verify` rejects it. `mysql < file` still commits nothing.
- A hand-pasted `XA END '<token>'` and `XA COMMIT '<token>' ONE PHASE`, using the id printed in the file, commits the open undo. Plain `COMMIT` does not: inside the XA transaction it is ERROR 1399. The published file contains no `XA COMMIT` on the path `mysql` runs; the commit text is only in the procedure's branch that `verify` selects.
- A non-transactional table (MyISAM and anything that does not join the XA transaction) makes the row change durable at the statement. `XA ROLLBACK` does not undo it. The tool cannot see the engine in a row event. Do not point a flashback at a non-transactional table.
- A client that does not implement `DELIMITER` cannot apply the script. The documented client is stock `mysql`.
- Without `CREATE` and `CREATE ROUTINE`, creating the procedure fails before `XA START`. The undo does not run and nothing is committed.
- A cut file leaves its procedure behind and an `ACTIVE` XA transaction until the session disconnects. Disconnect rolls the XA back. Run the next script in a new session. A second apply of the same script drops its own procedure first.
- Two concurrent applies whose scripts hash to the same token share an XA id. The second `XA START` fails. With `--force` the session is set read-only, so that second script does not autocommit the undo.
- The undo holds locks until the finish and can exceed `max_binlog_cache_size`. The finish does not `XA PREPARE`. A failed finish does not leave a prepared transaction for another session to commit. There is no per-block resume.

## Considered options

- **Keep `START TRANSACTION` and only check `@@error_count` before a prepared `COMMIT`.** That fixes #328. It does not fix #329: `BEGIN` and DDL commit the open transaction before any later statement runs.
- **`ROLLBACK` before `START TRANSACTION`, and `ROLLBACK` before the trailer.** A following binlogviz script and a complete file under `--force` roll back. A cut file followed by a bare `BEGIN` or DDL still commits.
- **One stored procedure that holds the whole undo.** ADR 0013 rejected this. A 1 GB body does not fit in `max_allowed_packet`, and the procedure would be the undo text. The procedure here is a fixed finish. The undo stays one statement at a time.
- **Document the holes and leave the SQL.** That still commits a prefix. The requirement is to refuse those cases instead of committing them.

## Consequences

- Applying still goes through `binlogviz flashback verify`. `mysql < file` still changes nothing.
- The session needs `CREATE` and `CREATE ROUTINE`. The script creates the `binlogviz` database if it is absent and drops its procedure after the finish. `CREATE DATABASE`, `CREATE PROCEDURE`, and `DROP PROCEDURE` are their own binlog transactions. The undo is the `XA COMMIT`.
- v0.31.0 scripts do not verify. Regenerate them.
- `plan_version` stays 2. `flashback verify --format json` stays `schema_version` 1.
