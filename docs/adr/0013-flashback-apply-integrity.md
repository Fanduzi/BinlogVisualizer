# ADR 0013: A flashback script commits nothing unless it is complete and unmodified

Status: accepted (2026-10-19). Tracks #318 and #321. Supersedes the seal half of ADR 0006 and the "unparseable `BINLOGVIZ_SPILL_BYTES` means no ceiling" sentence of ADR 0012. The generated-column check in ADR 0006 is unchanged.

## Context

The apply guard token tells scripts and sessions apart. It is not a checksum of the undo. `Seal` / `SealWriter` hash the script while it is rendered and splice the token in, and nothing reads that hash again when the operator applies the file. BinlogQA on MySQL 8.0.36 (#318) showed what still runs: an edited row value, a deleted undo statement, an appended statement, and reordered statements. Changing the token inside one statement makes that statement match zero rows and succeed. The same pass (#321) filled the disk during the write (exit 8). The partial file was left in place. `mysql` committed 64 transactions of a 50 MB prefix and then stopped with ERROR 1064 on the cut statement, so the table was half rolled back. The README said a partial script is never written. That was only true for a refusal that happens before the first byte. It was not true for a short write.

Sealing was also a three-step contract the guard could not enforce: render into a hash, call `SealWriter` with that digest, render again. A caller could skip a step. ADR 0012 step 4 left that shape in place on purpose, so the planner would not buffer the script.

Two smaller notes came from the same QA. `BINLOGVIZ_SPILL_BYTES` values `abc`, `0`, `-1`, `1MB`, and empty are not a ceiling, and the code treated them as "no ceiling". `TestGeneratedReviewRSS` fails under `-race` (about 90–97 MB against a 64 MB VmHWM check) and the parent failure says 192 MiB. `GOMEMLIMIT` is also the flashback memory ceiling. A container often sets it only to tune GC.

The script is applied by the stock `mysql` client. The operator pipes a file. Scripts are large (a 1 GB undo is a real incident). `--force` continues after an error. `COMMIT` and `ROLLBACK` can be prepared. `START TRANSACTION` cannot (ERROR 1295). `SIGNAL` is illegal outside a stored program. There is no way for a statement to see SQL the client has not sent yet, and no way to take back a `COMMIT` that already ran.

## Decision

### 1. One write path seals the script

`guard.Write(w, render)` is the only way to emit a script. `render` is called twice and must be deterministic. The first pass hashes the placeholder text and chooses the session token (first 8 bytes of SHA-256, hex). The second pass substitutes the token, writes the script, and chains the apply digest. `Seal` and `SealWriter` are gone. The planner cannot print a script that skipped the seal.

The session token still does what ADR 0006 describes: the header sets `@binlogviz_ok` only when the generated-column check ran in this session and matched, and a block that is not inside that transaction sets the session read-only. It is not the content checksum.

A token check on one undo statement is an error, not a filter. The condition is `CASE WHEN @binlogviz_ok <=> '<token>' THEN 1 ELSE (SELECT @binlogviz_bad := 1 UNION ALL SELECT 1) END`. MySQL evaluates only the taken branch, so a match does not run the subquery. A mismatch is ERROR 1242 and sets `@binlogviz_bad`. With `--force` the flag is still 1 afterwards.

### 2. The file on disk does not commit

Every undo runs in **one** transaction, opened with a raw `START TRANSACTION` after the guard header (the header's own `COMMIT` only ends the `information_schema` read; no undo has run). There is no per-block `COMMIT`. The per-block lock from ADR 0006 started with `COMMIT`, so it is not repeated. A block whose session does not hold this script's token prepares `SET SESSION TRANSACTION READ ONLY` (that statement names no server variable, so MariaDB 10.x and MySQL 5.7.0–5.7.19 still lock) and the following write fails with ERROR 1792. A block inside this script's transaction prepares `DO 0` and leaves the header's unlock in place.

Each prepared statement is hashed and executed from the same string:

```sql
SET @binlogviz_sql = '<statement>';
SET @binlogviz_digest = SHA2(CONCAT(@binlogviz_digest, @binlogviz_sql), 256);
PREPARE binlogviz_fb_stmt_x9q FROM @binlogviz_sql;
EXECUTE binlogviz_fb_stmt_x9q;
DEALLOCATE PREPARE binlogviz_fb_stmt_x9q;
```

The chain starts at `SHA2('binlogviz-flashback-v1', 256)`. Go's SHA-256 of the same bytes matches MySQL's `SHA2(..., 256)` hex. The trailer folds in the constant `binlogviz-trailer-v1` and sets `@binlogviz_sealed` only when that hex matches and `@binlogviz_bad` is not 1. The file then sets `sql_mode` to a value MySQL rejects. It does not contain `COMMIT`.

`START TRANSACTION` and the generated-column header cannot be prepared (`START TRANSACTION` is ERROR 1295; the header itself prepares statements, and a prepared statement cannot contain `PREPARE`). Those parts are written raw, and the same bytes are the digest payload on the line above them. `binlogviz flashback verify` checks the raw bytes against that payload. The running digest inside MySQL does not see a hand edit that changes the raw text and not the payload. That limit is stated in the guarantee table.

The first executed statement removes `NO_BACKSLASH_ESCAPES`. Its payload is quoted with doubled quotes, so a session that already has that mode can parse the line. Later payloads may use backslash escapes, and the client parses them only after that statement has run.

### 3. `binlogviz flashback verify` is the commit

`verify <file>` reads the file and recomputes the digest from the payloads, in order. It does not connect to MySQL.

| `--format` | stdout on success |
|---|---|
| `sql` (default) | the script with the failing `sql_mode` line replaced by a prepared `COMMIT` or `ROLLBACK` chosen from `@binlogviz_sealed` |
| `json` | one object, `schema_version` 1, then `command`, `ok`, `statements`, `digest` |

Failure writes nothing to stdout. Exit 6 (`refused`), `details.reason`:

| reason | when |
|---|---|
| `not_flashback` | the file is not a script this build wrote |
| `truncated` | the trailer or a piece is cut |
| `missing_trailer` | the pieces end and the trailer is absent |
| `appended` | executable SQL outside the pieces, or bytes after the trailer |
| `digest_mismatch` | a payload was edited, deleted, or reordered, or the trailer hex does not match |

Exit 3 is a bad `--format`. Exit 4 is a missing file. Exit 5 is an empty file. `--error-format json` is the usual one-line stderr object.

The apply command is:

```bash
binlogviz flashback verify flashback.sql | mysql --default-character-set=utf8mb4
```

`mysql < flashback.sql` does not commit, including when the file is intact. The client stops on the trailer error (or, with `--force`, skips it) and the server rolls the open transaction back on disconnect.

The prepared finish statement is not an error when the digest is wrong: it runs `ROLLBACK`. `--force` cannot skip a successful `ROLLBACK` and reach a later `COMMIT`, because the `COMMIT` is that same statement.

### 4. A failed generation publishes nothing

`flashback` writes the sealed script to a temp file and copies it to stdout only after that write returns. On any error the temp file is removed and stdout receives no bytes. Exit 8 during generation no longer leaves a prefix that `mysql` will apply.

A failure while copying the finished temp file to a shell redirect can still leave a prefix in the file the shell opened. That prefix has no commit trailer. Applying it commits nothing. The README says this and does not say "a partial script is never written".

### 5. Byte ceilings are usage errors

`BINLOGVIZ_SPILL_BYTES`, when set, must be a positive base-10 integer. `abc`, `0`, `-1`, `1MB`, and empty exit 3 (`usage`) before a binlog is read. Unset still means no byte ceiling. A full disk while spilling is still exit 8.

`BINLOGVIZ_MEMORY_BYTES` is the same kind of value. When set, it is the flashback heap ceiling instead of `GOMEMLIMIT`. When unset, a finite `GOMEMLIMIT` remains the ceiling: it is the only limit that stops the runtime from calling `exit(2)` (#317). A number above `GOMEMLIMIT` does not raise the runtime limit; the process can still die. Invalid values exit 3.

### 6. The RSS test does not run under the race detector

`TestGeneratedReviewRSS` checks VmHWM against 64 MB. The race detector puts the same workload at about 90–97 MB, so the test skips when built with `-race`. The failure text says 64 MB, which is the check.

## Guarantee

| What happened | `mysql < file` | `verify` | `verify \| mysql` |
|---|---|---|---|
| Intact script | trailer error, rollback, no change | exit 0, SQL on stdout | restores the pre-change rows |
| Edited value, deleted statement, or reordered statements | no commit | exit 6, `digest_mismatch` | not produced |
| Appended statement, or bytes after the trailer | no commit | exit 6, `appended` | not produced |
| File cut short (disk full, or truncated by hand) | no commit | exit 6, `truncated` or `missing_trailer` | not produced |
| Token changed inside one statement | that statement is ERROR 1242 if it is executed; the file still has no commit | exit 6, `digest_mismatch` | if the file was not edited and the session token is wrong, `@binlogviz_bad` makes the finish statement `ROLLBACK` |
| `--force` on any of the above | still no commit | unchanged | rollback when the file is intact but the session token is wrong |
| Generation exited 8 | no script bytes were written | | |

These are not guaranteed, and the docs say so:

- An edit that recomputes the payloads and the trailer hex is a new script. `verify` accepts it.
- A raw section (the guard header and `START TRANSACTION`) is bound by `verify`'s byte check. A hand edit that changes the raw SQL and not its digest payload is invisible to the in-SQL chain. `verify` rejects it. `mysql < file` still commits nothing, because the file has no `COMMIT`.
- Stock SQL cannot stop a `COMMIT` that someone pastes into the open transaction after throwing the trailer away. The published file does not contain one.
- The undo is one transaction. It holds locks until the finish statement and it can exceed `max_binlog_cache_size`. A failed `COMMIT` rolls the transaction back. There is no per-block resume: a cut file does not verify, and a failed apply did not commit a prefix. Run the whole verified script again.
- `mysql < flashback.sql` on an intact file looks like a failure. That is the trailer. The rows are unchanged. Pipe `verify` into `mysql`.

## Considered options

- **Keep per-block `COMMIT` and only hash the file.** A cut file still commits every block that ended. That is #321.
- **One transaction and a `COMMIT` inside the file, gated by the digest.** An intact file would apply with `mysql < file`, which is the old habit. An appended statement after that `COMMIT` cannot be rolled back, and `--force` walks past an error into a later `COMMIT`. The requirement is that an appended statement commits nothing.
- **A stored procedure that holds the whole undo.** `CREATE PROCEDURE` cannot hold a 1 GB body, and it needs `DELIMITER` and `CREATE ROUTINE`.
- **Hash a comment and execute a second copy of the statement.** An edit to the executed copy that leaves the comment alone would apply. The prepared string is the statement that runs.
- **Treat `GOMEMLIMIT` as unset for the ceiling.** A container that hits the runtime limit dies with status 2 and no JSON, which #317 exists to avoid. The dedicated variable is how an operator sets a flashback ceiling on purpose; `GOMEMLIMIT` stays the backstop.

## Consequences

- Applying a script is a new command. Scripts and agents that piped `flashback` straight into `mysql` no longer change data. CHANGELOG marks that breaking.
- The undo is one binlog transaction, not one per original transaction. `plan_version` stays 2. The SQL text is not byte-compatible with v0.30.0.
- ADR 0006's header, message cut, and session token remain. The per-block `COMMIT` lock does not. A header-less block is read-only (ERROR 1792) instead of repeating that lock. A statement whose token does not match errors (ERROR 1242) instead of changing zero rows.
- `flashback verify --format json` is a new versioned document (`schema_version` 1). ADR 0010's rule applies.
- Exit 10 is unchanged. The spill and memory variables no longer treat garbage as "off".
