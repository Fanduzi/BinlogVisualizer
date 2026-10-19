# Limitations

This document explains the product boundaries of BinlogViz so operators know what the tool is designed to do and what it intentionally does not do.

## Supported Binlog Format

BinlogViz is built around local MySQL `ROW`-format binlog files.

That boundary matters because the analyzer is designed to consume normalized row-oriented events and derive workload statistics from row activity. If your operational question depends on statement-based replay semantics, a different tooling path is more appropriate.

MIXED-format files are undercounted: only ROW images are included, so Query-DML statements without row images are ignored. Do not treat a MIXED or STATEMENT report as a complete write workload. Analyze warns on stderr (`binlog appears STATEMENT or MIXED; only ROW images are counted`) and exits non-zero when the file has zero row images.

In practical terms, use BinlogViz when your input is a local ROW-format binlog sequence and your goal is workload inspection rather than logical replay.

## Input Scope

BinlogViz analyzes local files only.

The command accepts input in one of two ways:

- explicit positional binlog paths
- discovery mode with `--from-dir` plus `--prefix`

It does not attempt to fetch remote binlogs, connect to a live MySQL server, or manage replication coordinates for you. Discovery mode is also intentionally narrow: it scans one directory, filters by prefix plus numeric suffix, and orders the result set for analysis.

This makes the input contract predictable, but it also means operators must prepare the local file set themselves.

## Runtime Model

BinlogViz uses a single-pass streaming command path:

```text
parse -> normalize -> consume -> finalize -> render
```

This design keeps live in-memory state bounded, but it does not mean analysis is cost-free at the end of the run.

Important runtime implications:

- parsing is streaming
- final report assembly still happens after parsing completes
- a temporary DuckDB store is created per command when `--detail-store duckdb` is used (default: none)
- larger inputs may show noticeable finalization time and temporary disk usage

So the product is optimized for bounded streaming analysis, not for zero-disk or fully incremental interactive exploration.

## SQL Context Boundaries

SQL context is intentionally bounded and presentation-oriented.

Current limits include:

- stored SQL capped at `4096` bytes, with `… [truncated: <shown> of <original> bytes]` when that cap cuts the text
- query summaries capped at `160` characters of SQL, then the same marker when the text was cut
- query fields and DDL statement text shown or omitted according to `--sql-context` (`off` omits both)
- `--sql-context off` also omits `--show-rows` cell values, and the report says they were omitted
- `query_truncated` means the 4096-byte store cap, not the 160-character summary

This means:

- BinlogViz does not promise lossless storage of original SQL text
- `summary` mode is meant for quick operator orientation
- even `full` mode only exposes the bounded stored SQL, not unlimited original statements

If your workflow requires complete long-form SQL archival or forensic preservation, BinlogViz should not be treated as that system of record.

## Row values

`--show-rows` is off by default. When it is on, listed transactions carry a bounded image: DELETE before-image, UPDATE columns that differ, INSERT after-image. The report keeps at most 32 logical rows per transaction. Strings and blobs stop at 64 bytes. Integers, decimals, and `BIT` values are printed in full, so a `DECIMAL(65)` is not cut into a wrong number. A cut uses `… [truncated: shown of original bytes]`, and the transaction says how many rows were left out.

Column names are taken from the binlog when `binlog_row_metadata=FULL` (MySQL 8.0.1+). Otherwise columns are `@1`..`@N`, and the report says names are missing. Without that metadata, an integer whose signed and unsigned readings differ is printed as both, the same way `mysqlbinlog -v` does. With FULL metadata, a column is printed with the signedness the binlog recorded.

`--sql-context off` does not print those cells. Auth-DDL `<secret>` redaction is unchanged; it applies to statement text, not to row cells. `mysqlbinlog_cmd` on the same transaction is the cross-check. The analyze report does not print undo SQL. `binlogviz flashback` does; see below.

## Flashback SQL

`binlogviz flashback` prints SQL that undoes the selected row changes. It reads the local binlog and does not connect to a database. Review the script, test it, and apply it yourself in a single session on the primary.

A DELETE becomes `INSERT` of the before-image. An INSERT becomes `DELETE` of the after-image. An UPDATE sets every column back to the before-image and matches the primary key from the after-image. Statements are in reverse binlog order. The undo is one XA transaction and the file does not commit. A comment names the original GTID, or `GTID unavailable`, and `file:start-position` (the basename and the byte where that transaction starts).

The same table, schema, `--dml`, time, position, and GTID selectors as `analyze` choose which rows are included. The binlog must carry column names (`binlog_row_metadata=FULL`) and complete row images (`binlog_row_image=FULL`). The script sets `utf8mb4`, `time_zone='+00:00'`, and, before creating the finish procedure, removes `PAD_CHAR_TO_FULL_LENGTH` and `NO_BACKSLASH_ESCAPES`, then restores the saved mode after a successful apply. `TIMESTAMP` literals are the UTC wall clock of the stored instant.

A table with no primary key is still reversed: the `DELETE` or `UPDATE` matches every column and adds `LIMIT 1`, and a comment says so. The `INSERT` that puts a deleted row back does not use `LIMIT 1`.

JSON is rebuilt from the binary document (`JSON_OBJECT` / `JSON_ARRAY`, or `CAST(... AS JSON)` for a scalar), so a decimal stays a decimal, a datetime stays a datetime, and `-0.0` keeps its sign. That includes JSON inside a transaction recorded with `binlog_transaction_compression=ON`. A character column that is not `utf8mb4` is a charset introducer plus the raw bytes (`_latin1 0xE9`, `_utf16 0x00410042`). Collation `binary` stays `X'...'`. `utf8mb4` stays a quoted string. `ENUM` is the 1-based member index (`0` is the empty member) and `SET` is the bitmask (bit 0 is the first member). A quoted member name would be the column charset, which is not `utf8mb4` for `latin1` or `gbk`: strict `sql_mode` then returns `ERROR 1265`, and non-strict mode can store a blank. The number restores the same member in both modes. Index 0 is not a member. Strict `sql_mode` rejects it with `ERROR 1265`. Flashback restores that row by saving `@@SESSION.sql_mode`, dropping `STRICT_TRANS_TABLES`, `STRICT_ALL_TABLES`, and `TRADITIONAL` for that statement only, and restoring the saved mode. `TRADITIONAL` has to go too: MySQL expands it back into those two strict modes. Other statements stay strict. An empty `sql_mode` and a session that started with `NO_BACKSLASH_ESCAPES` still restore the row. The script header removes `PAD_CHAR_TO_FULL_LENGTH` and `NO_BACKSLASH_ESCAPES` before `CREATE PROCEDURE` and restores the saved mode after a successful apply.

Generated columns, virtual or stored, are omitted from `INSERT` lists and `UPDATE` assignments when the definition is known. A no-primary-key `WHERE` still compares them, together with every other column, so an INSERT undo cannot delete a different duplicate row. A generated column that is part of the primary key stays in the `WHERE`. They are not written in `INSERT` or `UPDATE` assignments. Names come from `CREATE TABLE` and `ALTER TABLE` in the files you pass, including transactions dropped by `--exclude-gtids` or a time window, and from `--schema-file` (`mysqldump --no-data`, or `SHOW CREATE TABLE`, including `mysql --batch` output whose newlines inside the statement are `\n`). Flashback does not connect to MySQL. MySQL 8 `TABLE_MAP` optional metadata ends at `COLUMN_VISIBILITY` (invisible columns, not generated columns), and `binlog_row_image=FULL` stores both virtual and stored values, so a missing cell is not a signal. A `FULL` image still stores the value, and flashback checks that value against a schema-file expression. If a definition was seen and cannot be read, flashback refuses the table and prints no SQL. If a selected table was never defined, flashback still prints the script, listing every column, and writes a warning on stderr before that script. The warning names the table, says generated columns cannot be ruled out, and says applying the script can stop at `ERROR 3105` and the undo rolls back.

`--schema-file` must describe the table as it was when the rows were written. Flashback compares that definition with the binlog `TABLE_MAP` for each selected event: column count, names, order, and, where the binlog has them, type, signedness, charset, `ENUM`/`SET` members, decimal precision, and fractional seconds. A mismatch names the table and the columns, says the file does not match, and prints no SQL. The error also says what to do next: use a dump from incident time, an older dump if an `ALTER` came later, or `--include-table` to leave the table out. A generated column is omitted when every logged value equals the expression. The check understands integer and `DECIMAL` `+`, `-`, `*`, `/`, `DIV`, `%`, `MOD`, parentheses, column names, and `NULL`, and also `UPPER`, `LOWER`, `CONCAT`, `CONCAT_WS`, `LENGTH`, `CHAR_LENGTH`, and simple JSON extraction (`->`, `->>`, `JSON_EXTRACT`, `JSON_UNQUOTE` with a constant path). `/` into an integer is rounded the way MySQL assigns to an integer, half away from zero, so `5 / 2` stored in an integer column is `3` and `-5 / 2` is `-3`. A logged `2` for that expression is a mismatch. `DIV` truncates toward zero. A `DECIMAL(p,s)` column uses the same operators and MySQL 8.0's division at the default `div_precision_increment` of 4. That default keeps 9 fractional digits for an integer divided by an integer: each operand scale is rounded up to a multiple of 9, the increment is spent on that padding, the sum is rounded up to a multiple of 9, and the division fills those digits and cuts the remainder off. The last kept digit is not rounded. Assignment then rounds half away from zero to scale `s`. `5 / 2` in `DECIMAL(40,4)` is `2.5000`. `15 / 4` is `3.7500`. `1 / 7` in `DECIMAL(40,4)` is `0.1429`, in `DECIMAL(40,6)` is `0.142857`, and in `DECIMAL(40,9)` is `0.142857142`. Rounding the repeating expansion to `0.142857143` does not match. `2 / 3` in `DECIMAL(40,9)` is `0.666666666`. `(1 / 10000000000) * 10000000000` in `DECIMAL(40,4)` is `0.0000`, and the same expression in a `BIGINT` column is `0`, because the 9-digit quotient is already 0. `NULL` and division by zero are `NULL`. The binlog does not record `div_precision_increment`. A value written while it was 0 (`5 / 2` stored as `2.0000`, or as the integer `2`) contradicts the default and is refused. Calling that unverifiable would also accept `DIV` written as `/`. Increments 1 through 9 keep the same 9 digits for integer operands, so those stored values match. An increment of 10 or more keeps 18 digits, and a scale wide enough to show the extra digits is refused. With `DECIMAL` operands, or a quotient that is multiplied or added afterwards, such as `(a / b) * b` or `(a / b) + c`, any non-default increment can change the stored digits, including in a narrow column such as `DECIMAL(12,4)`, and flashback refuses those rows. When the logged values of a `/` column match the expression at some other increment from 0 to 30, the error says which increments fit. Increments the checker could not evaluate, for example a quotient past 30 fractional digits or a value clipped to the column range, are named as not checked rather than left out, so a partial range does not read as exact. If no checked increment fits but some could not be checked, the error names those and keeps both explanations. The dump may then be right, and `--exclude-table` leaves the table out (or `--include-table` listing only the other tables). The table is still refused, because the binlog cannot say which increment the server used. A result that does not fit the column is unverifiable when the logged value is the endpoint a non-strict `sql_mode` stores, and a mismatch when it is anything else. The endpoints are `127` and `-128` for `TINYINT`, `0` and `255` for `TINYINT UNSIGNED`, `2147483647` and `-2147483648` for `INT`, and `99.99` and `-99.99` for `DECIMAL(4,2)`. The column is omitted, stderr warns once, and the header says it was not verified. That warning is not an acceptance: a real column that happens to store the endpoint stays unverified too. An in-range value, including a `0` the expression really produced, is still checked. `UPPER` and `LOWER` cover `ascii`, `latin1`'s one-to-one map, and `utf8mb4` for Latin-1 letters plus `µ` (U+00B5) to `Μ` (U+039C). `ß`, `ÿ`, and other code points above Latin-1 are not checked. `NULL` stays `NULL`, except `CONCAT_WS`, which skips `NULL` arguments. A logged value that is not that expression exits 6 and prints no SQL. The error names the table, the column, the expression, and one example row. `ENUM` and `SET` values in that example print the schema-file labels. The same refusal applies when two images share the referenced column values and differ in the generated column, including an `UPDATE` that changes only that column and an `INSERT`/`DELETE` pair. An expression that cannot be modelled exactly, and that the logged values do not contradict, is omitted. stderr warns once for that column, and the script header contains an English comment, `-- WARNING: generated column db.tbl.col not verified`, which says the guard checks the target. Anything the checker cannot model is unverifiable: it is never treated as a match and never as a mismatch. A proven contradiction, including two images that share the referenced values and differ in the generated column, is exit 6 (kind refused) and prints no SQL. When the logged values equal an expression the checker can evaluate, a real column still cannot be told apart from a generated one at flashback time. The script's guard is what stops that case on the target, including when the client continues after the error: the session is read-only and later writes fail. A generated column whose expression differs from the schema file still passes, and the target recomputes it. A `CREATE` in the parsed files replaces the file for that table. An `ALTER` in the parsed files, including one excluded by time or GTID, updates the definition used for later events. An `ALTER` inside the selected range is still refused, because flashback does not undo DDL. A plain `mysqldump --no-data db` dump has no `USE`. The database is taken from the `-- Host: ... Database:` header, from `--schema-file-db`, or from the binlog when that table name occurs in only one schema. Several `SHOW CREATE TABLE` results may sit in one file with no `;` between them, including `mysql --batch` output. If the header and `--schema-file-db` disagree, or the table name is in more than one schema, flashback warns and does not use the definition.

These limits stay even when the literals are exact:

- `ON DELETE CASCADE` and `ON UPDATE CASCADE` change child rows that are not in the binlog. Flashback does not restore those children.
- Triggers on the table fire when the undo runs.
- The match is the primary key, or every column and `LIMIT 1`. There is no conflict check. The script overwrites changes made after the incident.
- A table filter or `--dml` can undo only some row changes from one transaction. Flashback then prints a warning on stderr and still prints SQL for the rows it kept. Time, position, and GTID selectors drop whole transactions and do not warn.
- Review the script and test it before applying. Apply it in a single session on the primary with `sql_log_bin=1`, so replicas follow under new GTIDs. Do not apply it on a replica. A statement that fails rolls the undo back, including under `mysql --force`. Apply with `binlogviz flashback verify`.

### Known limitations

Review the script, test it, and apply it with `binlogviz flashback verify <file> | mysql --default-character-set=utf8mb4` in a single session on the primary. The file itself does not commit. A failed apply rolls the undo back, including under `mysql --force`, and prints `binlogviz: undo rolled back, no rows changed`. Without `--force` the mysql client exits 1. Success is the line `binlogviz: undo committed, N statements`. Exit 0 without that line is not success. Automation uses `set -o pipefail` and greps for the line. Apply needs `CREATE`, `CREATE ROUTINE`, and `ALTER ROUTINE` on `binlogviz.*`, `SELECT, INSERT, UPDATE, DELETE` on the tables the undo changes, and the stock `mysql` client.

- A `--schema-file` that marks a real column as generated still exits 0 and omits that column when the logged values equal an expression binlogviz can check, or when the expression cannot be checked and nothing contradicts it (`c` was always written as `a + 1`, `UPPER(x)`, or `CONCAT(first, ' ', last)`) ([#166](https://github.com/Fanduzi/BinlogVisualizer/issues/166)). MySQL 8 row metadata does not mark generated columns, so flashback cannot tell that case apart from a correct dump. The script then contains one guard, after the three `SET` lines and before the first transaction. Apply fails there, before any transaction, when `information_schema.COLUMNS.EXTRA` for that column is not `STORED GENERATED` or `VIRTUAL GENERATED`. The error says the schema file does not match the target and the error line names every mismatched `db.table.column`, not only the first. A long list shows the count and the first names. A column that is `GENERATED` on the target with a different expression passes the guard: only `EXTRA` is checked, the target recomputes the value, and the restored row is still correct. `mysql --force`, `source` or a paste into the mysql prompt, `mysqlsh --force`, `mysqlsh --interactive`, and a GUI runner set to continue on error keep going after that error. The guard switches the session to read-only first, so every later write fails with `ERROR 1792` and no row changes. Disconnect. A new session is not read-only. Fix the schema file and apply the script again. The lock is `SET SESSION TRANSACTION READ ONLY`, which names no server variable, so it works the same on MySQL 5.6.5 and newer and on MariaDB 10.0 and newer, including MariaDB 10.x–11.0, which have no `transaction_read_only`. The guard locks first and switches the session back only when the check ran in that session, every column matched, and the server is supported. If a guard step fails, for example a server that refuses `PREPARE` (`max_prepared_stmt_count` reached or 0), the session stays read-only and every write fails with `ERROR 1792`. Each later block stays inside that one transaction. Its gate prepares `SET SESSION TRANSACTION READ ONLY` unless `@binlogviz_txn` is 1 and the session holds this script's token, in which case it prepares `DO 0` and leaves the header's unlock in place. The gate does not `COMMIT`. The guard sets `@binlogviz_ok` to that token, a value derived from the script's own text, only when the check ran in that session and matched. Every undo statement checks the token too. The condition is `CASE WHEN @binlogviz_ok <=> '<token>' THEN 1 ELSE (SELECT @binlogviz_bad := 1 UNION ALL SELECT 1) END`. A match returns 1. A mismatch is `ERROR 1242` and sets `@binlogviz_bad`, which survives `--force`. A block run without the header, a block of another script pasted into a session that already applied a correct script, and a block run after a reconnect become read-only, and their writes fail with `ERROR 1792` or `ERROR 1242`. The undo is one transaction, so a reconnect in the middle rolls back every statement that had not been committed. There is no per-block resume. Run `binlogviz flashback verify` on the original file and apply that output in a new session ([#197](https://github.com/Fanduzi/BinlogVisualizer/issues/197), [#318](https://github.com/Fanduzi/BinlogVisualizer/issues/318)). The undo is an XA transaction (ADR 0014). `SET SESSION TRANSACTION READ ONLY` inside that XA transaction does not fail and does not make the current transaction read-only; it applies to the next one. While the XA transaction is open, `BEGIN`, `START TRANSACTION`, and DDL fail with ERROR 1399 and do not commit the undo. A script applied in a session that is already read-only, for example after an earlier script's guard failed, stops at its own guard with `binlogviz: this session is already read-only so the script cannot write. Disconnect and apply the script again in a new session` instead of a bare `ERROR 1792` on every write ([#191](https://github.com/Fanduzi/BinlogVisualizer/issues/191)). On a correct target, `PREPARE` failing means the script stops at `ERROR 1461` and restores nothing; raise `max_prepared_stmt_count` and apply again. A session that was read-only before the script stays read-only. Apply is supported on MySQL 5.7 and newer and on MariaDB 10.2 and newer; the guard is checked live on MySQL 5.7.19, 5.7.44 and 8.0 and on MariaDB 10.6, 10.11, 11.0 and 11.4. An older server (MySQL 5.6, MariaDB 10.1) fails the guard with `binlogviz: target server is older than MySQL 5.7.0 or MariaDB 10.2 and is not supported for apply` and is left read-only, so `mysql --force` cannot write there either. MySQL before 5.6.5 and MariaDB before 10.0 have no read-only session mode: the guard stops a client that stops on error, but one that continues is not locked. Clearing the read-only flag and continuing in that same session writes the wrong rows. That stops a wrong-environment dump and a dump whose expression happens to equal the logged rows, including a no-primary-key INSERT undo that would otherwise delete a different duplicate row. A logged value that contradicts the expression, including an `UPDATE` that changes only the claimed generated column, is exit 6 (kind refused) and prints no SQL. Omitting `--schema-file` is not a workaround for a real generated column: the script assigns that column, and apply stops at `ERROR 3105` and the undo rolls back.
- If the live table was altered so that a normal column became generated after the incident, the pre-incident value cannot be written back into that column. A dump taken after the `ALTER` is refused when the logged values contradict the new expression. A dump from incident time, when the column was still a normal column, assigns it, and apply stops at `ERROR 3105` because the target column is now generated. To restore the other columns, start from that incident-time script and delete the column from the `INSERT` lists and `SET` lists. MySQL then recomputes it. That recomputed value is not the stored original when the two differ. Applying the unedited script with `--force` still does not commit a prefix: ERROR 3105 sets `@binlogviz_applied` to 0 and the finish rolls the XA transaction back (ADR 0014).
- Rows written by a non-strict session are restored under a strict session ([#180](https://github.com/Fanduzi/BinlogVisualizer/issues/180)). Flashback sees these values offline and wraps only that statement in a saved and restored `@@SESSION.sql_mode` that drops just the flags that would reject the stored value, plus `TRADITIONAL`: a zero month or day in a `DATE`, `DATETIME` or `TIMESTAMP` (`'0000-00-00'`, `'2026-00-15'`) drops `NO_ZERO_DATE` and `NO_ZERO_IN_DATE`, and a generated column that the script leaves out and whose logged value is `NULL` drops `ERROR_FOR_DIVISION_BY_ZERO`, so `a/b`, `DIV` and `%` with `b` = 0 recompute to `NULL` again. Strict mode stays on for those statements, so any other value that does not fit the target still fails. Other values that only a non-strict session accepts (a truncated string or an out-of-range number) cannot be in the binlog, because the server stored the clipped value. A generated column the script does not know about is still assigned, and apply stops at `ERROR 3105` as described above.
- In the guard error line, a `|` in a column name is shown as `/`, so `a|b` and `a/b` look the same there ([#208](https://github.com/Fanduzi/BinlogVisualizer/issues/208)). The guard's `SELECT` on stdout and the SQL keep the real names.
- A column that is added in the parsed binlogs (`ADD COLUMN`) and later modified or renamed there (`MODIFY`, `CHANGE`, `RENAME COLUMN`) still makes a correct incident-time dump fail: flashback exits 6 with `reordered (schema file ...; binlog ...)` and the column listed twice (`c, c`). It fails safe and prints no SQL. Parse only the binlog that holds the incident, or use a dump taken after the `ADD` and before the later `ALTER`.
- When `--schema-file-db` conflicts with several `Database:` headers in a file of joined dumps, the warning `--schema-file Database header is ... and --schema-file-db is ...` names only the last conflicting header. Its ending, `unqualified tables were not used`, is also not exact: the tables of a dump whose header matches `--schema-file-db` are used. Check every `Database:` header in the file.
- [#215](https://github.com/Fanduzi/BinlogVisualizer/issues/215): when the binlog names a table in all lower case and `--schema-file` has it only under different letter case, flashback uses the file table ([#187](https://github.com/Fanduzi/BinlogVisualizer/issues/187)). That assumes the binlog server had `lower_case_table_names=1` or `2`, and the warning says so. On a `lower_case_table_names=0` server, where `t` and `T` can be two tables, a dump that has only `T` is used for `t`: the guard then stops the apply before any write, or flashback exits 6, in a case that v0.23.27 restored without a definition. When a folded table is refused, the error names both tables, for example `p187x.mixt (schema file table P187X.MixT, letter case differs): --schema-file does not match the binlog columns: ...`. Both cases fail safe. Use a dump that has the table under its exact binlog name, or leave the table out with `--include-table` or `--exclude-table`.

### After a failed apply

The published script does not commit the undo. The undo is one XA transaction. `mysql < flashback.sql` runs the finish with `@binlogviz_want = 0`, which is `XA END` and `XA ROLLBACK`, then stops on the trailer. `--force` skips the trailer and still does not commit: the finish already rolled the XA transaction back. A statement that fails, including a missing table (ERROR 1146) or a missing column (ERROR 1054), sets `@binlogviz_applied` to 0, so the finish rolls back even when `verify` allowed a commit. A cut file leaves the XA transaction `ACTIVE`. A following `BEGIN`, `START TRANSACTION`, DDL, or second script in that session is ERROR 1399 and does not commit the prefix. Disconnect rolls an `ACTIVE` or `IDLE` XA transaction back. An edited value, a deleted statement, reordered statements, and SQL appended after the trailer are refused by `binlogviz flashback verify` (exit 6, `details.reason` `truncated`, `missing_trailer`, `digest_mismatch`, `appended`, or `not_flashback`) and `mysql` of the file commits nothing.

Apply with:

```bash
binlogviz flashback verify flashback.sql | mysql --default-character-set=utf8mb4
```

Apply needs `CREATE`, `CREATE ROUTINE`, and `ALTER ROUTINE` on `binlogviz.*`, and `SELECT, INSERT, UPDATE, DELETE` on the tables the undo changes:

```sql
GRANT CREATE, CREATE ROUTINE, ALTER ROUTINE ON `binlogviz`.* TO 'account'@'host';
```

`CREATE` on that schema covers `CREATE DATABASE IF NOT EXISTS binlogviz`, including when the schema does not exist yet. `DROP PROCEDURE IF EXISTS` returns ERROR 1370 without `ALTER ROUTINE`, including when the procedure does not exist. `CALL` uses `EXECUTE`. With `automatic_sp_privileges=1` (the default on MySQL 5.7 and 8.0) the creator receives it on the new procedure. With `automatic_sp_privileges=0`, also grant `EXECUTE` on `binlogviz.*`. The script does not run `XA RECOVER`, so `XA_RECOVER_ADMIN` is not required, and the XA statements do not need `SUPER`. `CREATE PROCEDURE` with binary logging on does not need `SUPER` or `log_bin_trust_function_creators`. The undo needs `SELECT, INSERT, UPDATE, DELETE` on the tables it changes. `UPDATE` and `DELETE ... WHERE` need `SELECT` (ERROR 1143 without it). The stock `mysql` client is required (`DELIMITER`). A client that does not implement `DELIMITER` cannot apply the script.

`verify` names the finish procedure `binlogviz.binlogviz_fb_end_<token>_<nonce>` for that invocation. The procedure cannot drop itself (ERROR 1357). The finish returns, and the next client statement drops the procedure, on a commit and on a rollback. The XA id stays `<token>`. A kill or disconnect after `CREATE PROCEDURE` and before the finish `CALL` can leave the procedure from that run. So can a first `CALL` that never enters the body (missing `EXECUTE` when `automatic_sp_privileges=0`) if the client is not using `--force`. On MySQL 8.0, `DROP PROCEDURE` of a routine whose definer has `SYSTEM_USER` is ERROR 1227 for an account without that privilege. MySQL 5.7 has no `SYSTEM_USER` privilege. Those leftovers do not block the next apply. `DROP DATABASE IF EXISTS binlogviz` needs `DROP` on `binlogviz.*` and does not need `SYSTEM_USER` or `ALTER ROUTINE`:

```sql
DROP DATABASE IF EXISTS binlogviz;
```

The next apply creates the schema again. It holds only these helpers.

A failed apply did not commit a prefix of a transactional table. Run that command again on the original file, in a new session. Do not cut the file into blocks. verify rejects a file that is missing a piece or the trailer. There is no per-block resume.

A shell redirect can still keep a prefix if the copy from the finished temp file fails mid-write. That prefix has no commit trailer. A following `BEGIN` or DDL does not commit it. A generation that fails (exit 8, or exit 10 for a spill or memory ceiling) writes no script bytes.

The undo holds locks until the finish and can exceed `max_binlog_cache_size`. The finish does not `XA PREPARE`. A failed finish does not leave a prepared transaction for another session to commit.

These are not guaranteed. An edit that recomputes the payloads and the trailer hex is a new script, and verify accepts it. A hand edit of the guard header or of `XA START` that changes the raw SQL and not its digest payload is invisible to the chain MySQL computes; verify rejects it, and `mysql < file` still commits nothing. A hand-pasted `XA END '<token>'` and `XA COMMIT '<token>' ONE PHASE`, using the id printed in the file, commits the open undo. Plain `COMMIT` inside the XA transaction is ERROR 1399 and does not. A non-transactional table (MyISAM) makes the row change durable at the statement; `XA ROLLBACK` does not undo it. Do not point a flashback at a non-transactional table. Two concurrent applies of scripts that hash to the same token share an XA id: the second `XA START` fails, and with `--force` that session is set read-only. The full table is ADR 0014.

Flashback refuses rather than guessing. Exit 1, one `Error:` line that names the table and the reason, and no SQL on stdout, when:

- column names are unavailable
- a before-image or after-image is incomplete (`binlog_row_image` `MINIMAL` or `NOBLOB`)
- a column cannot be rendered exactly (`FLOAT`, `DOUBLE`, `GEOMETRY`, `VECTOR`, a partial JSON value, a JSON value that cannot be represented exactly, invalid UTF-8 in a `utf8mb4` column, an unknown collation, or missing signedness, collation, or ENUM/SET members). `BIT` is a `b'...'` literal of the column width. A `FLOAT` or `DOUBLE` whose decimal MySQL will not cast is `<FLOAT>` or `<DOUBLE>` in `--show-rows`, not a wrong number.
- a `CREATE` or `ALTER` was seen, in the binlog or in `--schema-file`, and cannot be read
- `--schema-file` does not match the binlog columns for a selected event
- the selected range contains DDL (no reverse DDL is emitted)
- `--sql-context off` is set, because the script is the row values

Nothing selected uses the same exit 2 and `Error:` line as `analyze` (`schema/table filter matched no events`, `dml filter matched no events`, or `window matched 0 events`), with empty stdout. Auth DDL in the selected range is refused as DDL, so those secrets are not copied into the script. Cell values are not redacted. `analyze` text, Markdown, JSON, and HTML stay unchanged when flashback is not used.

## Output and Contract Boundaries

BinlogViz separates channels deliberately:

- final report on `stdout`
- progress, resolved-file listings, finalization status, and command errors on `stderr`

That contract supports shell redirection and automation, but it also means operators should not treat `stderr` noise as part of the report payload. If you redirect only `stdout`, you capture the report. If you need runtime status logs too, capture `stderr` separately.

## Product Focus

BinlogViz is focused on workload inspection, not on full operational control planes.

It is designed to help answer questions like:

- which tables were hottest
- which transactions were largest
- when activity spiked
- what the overall write workload looked like

It is not positioned as:

- a MySQL replication manager
- a live binlog tailing service
- a statement replay engine (flashback emits inverse row DML for a selected range; it does not replay the original statements)
- a full historical data reconstruction tool
- a general-purpose SQL observability platform

## Non-Goals

To keep the tool focused, these are explicit non-goals for the product shape documented today:

- managing or modifying MySQL server state
- reading from remote MySQL instances directly during analysis
- preserving unlimited raw SQL text in reports
- turning progress output into part of the machine-readable report stream
- replacing deeper replication, forensic, or observability systems

Use BinlogViz when you need a fast operational summary of local ROW binlog workload, or SQL that undoes one selected set of row changes. Reach for other tooling when you need remote collection, replay of the original statements, or a broader database operations platform.
