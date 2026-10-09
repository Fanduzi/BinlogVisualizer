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

A DELETE becomes `INSERT` of the before-image. An INSERT becomes `DELETE` of the after-image. An UPDATE sets every column back to the before-image and matches the primary key from the after-image. Statements are in reverse binlog order. Each original transaction is one `START TRANSACTION` / `COMMIT`. A comment names the original GTID, or `GTID unavailable`, and `file:start-position` (the basename and the byte where that transaction starts).

The same table, schema, `--dml`, time, position, and GTID selectors as `analyze` choose which rows are included. The binlog must carry column names (`binlog_row_metadata=FULL`) and complete row images (`binlog_row_image=FULL`). The script sets `utf8mb4`, `time_zone='+00:00'`, and strips `NO_BACKSLASH_ESCAPES` for the session. `TIMESTAMP` literals are the UTC wall clock of the stored instant.

A table with no primary key is still reversed: the `DELETE` or `UPDATE` matches every column and adds `LIMIT 1`, and a comment says so. The `INSERT` that puts a deleted row back does not use `LIMIT 1`.

JSON is rebuilt from the binary document (`JSON_OBJECT` / `JSON_ARRAY`, or `CAST(... AS JSON)` for a scalar), so a decimal stays a decimal, a datetime stays a datetime, and `-0.0` keeps its sign. That includes JSON inside a transaction recorded with `binlog_transaction_compression=ON`. A character column that is not `utf8mb4` is a charset introducer plus the raw bytes (`_latin1 0xE9`, `_utf16 0x00410042`). Collation `binary` stays `X'...'`. `utf8mb4` stays a quoted string. `ENUM` is the 1-based member index (`0` is the empty member) and `SET` is the bitmask (bit 0 is the first member). A quoted member name would be the column charset, which is not `utf8mb4` for `latin1` or `gbk`: strict `sql_mode` then returns `ERROR 1265`, and non-strict mode can store a blank. The number restores the same member in both modes. Index 0 is not a member. Strict `sql_mode` rejects it with `ERROR 1265`. Flashback restores that row by saving `@@SESSION.sql_mode`, dropping `STRICT_TRANS_TABLES`, `STRICT_ALL_TABLES`, and `TRADITIONAL` for that statement only, and restoring the saved mode. `TRADITIONAL` has to go too: MySQL expands it back into those two strict modes. Other statements stay strict. An empty `sql_mode` and a session that started with `NO_BACKSLASH_ESCAPES` still restore the row. The script header removes `NO_BACKSLASH_ESCAPES` for the whole session and does not put it back.

Generated columns, virtual or stored, are omitted from `INSERT` lists and `UPDATE` assignments when the definition is known. A no-primary-key `WHERE` still compares them, together with every other column, so an INSERT undo cannot delete a different duplicate row. A generated column that is part of the primary key stays in the `WHERE`. They are not written in `INSERT` or `UPDATE` assignments. Names come from `CREATE TABLE` and `ALTER TABLE` in the files you pass, including transactions dropped by `--exclude-gtids` or a time window, and from `--schema-file` (`mysqldump --no-data`, or `SHOW CREATE TABLE`, including `mysql --batch` output whose newlines inside the statement are `\n`). Flashback does not connect to MySQL. MySQL 8 `TABLE_MAP` optional metadata ends at `COLUMN_VISIBILITY` (invisible columns, not generated columns), and `binlog_row_image=FULL` stores both virtual and stored values, so a missing cell is not a signal. A `FULL` image still stores the value, and flashback checks that value against a schema-file expression. If a definition was seen and cannot be read, flashback refuses the table and prints no SQL. If a selected table was never defined, flashback still prints the script, listing every column, and writes a warning on stderr before that script. The warning names the table, says generated columns cannot be ruled out, and says applying the script can stop at `ERROR 3105` with earlier transactions already committed.

`--schema-file` must describe the table as it was when the rows were written. Flashback compares that definition with the binlog `TABLE_MAP` for each selected event: column count, names, order, and, where the binlog has them, type, signedness, charset, `ENUM`/`SET` members, decimal precision, and fractional seconds. A mismatch names the table and the columns, says the file does not match, and prints no SQL. The error also says what to do next: use a dump from incident time, an older dump if an `ALTER` came later, or `--include-table` to leave the table out. A generated column is omitted when every logged value equals the expression. The check understands integer and `DECIMAL` `+`, `-`, `*`, `/`, `DIV`, `%`, `MOD`, parentheses, column names, and `NULL`, and also `UPPER`, `LOWER`, `CONCAT`, `CONCAT_WS`, `LENGTH`, `CHAR_LENGTH`, and simple JSON extraction (`->`, `->>`, `JSON_EXTRACT`, `JSON_UNQUOTE` with a constant path). `/` into an integer is rounded the way MySQL assigns to an integer, half away from zero, so `5 / 2` stored in an integer column is `3` and `-5 / 2` is `-3`. A logged `2` for that expression is a mismatch. `DIV` truncates toward zero. A `DECIMAL(p,s)` column uses the same operators and MySQL 8.0's division at the default `div_precision_increment` of 4. That default keeps 9 fractional digits for an integer divided by an integer: each operand scale is rounded up to a multiple of 9, the increment is spent on that padding, the sum is rounded up to a multiple of 9, and the division fills those digits and cuts the remainder off. The last kept digit is not rounded. Assignment then rounds half away from zero to scale `s`. `5 / 2` in `DECIMAL(40,4)` is `2.5000`. `15 / 4` is `3.7500`. `1 / 7` in `DECIMAL(40,4)` is `0.1429`, in `DECIMAL(40,6)` is `0.142857`, and in `DECIMAL(40,9)` is `0.142857142`. Rounding the repeating expansion to `0.142857143` does not match. `2 / 3` in `DECIMAL(40,9)` is `0.666666666`. `(1 / 10000000000) * 10000000000` in `DECIMAL(40,4)` is `0.0000`, and the same expression in a `BIGINT` column is `0`, because the 9-digit quotient is already 0. `NULL` and division by zero are `NULL`. The binlog does not record `div_precision_increment`. A value written while it was 0 (`5 / 2` stored as `2.0000`, or as the integer `2`) contradicts the default and is refused. Calling that unverifiable would also accept `DIV` written as `/`. Increments 1 through 9 keep the same 9 digits for integer operands, so those stored values match. An increment of 10 or more keeps 18 digits, and a scale wide enough to show the extra digits is refused. A result that does not fit the column is unverifiable when the logged value is the endpoint a non-strict `sql_mode` stores, and a mismatch when it is anything else. The endpoints are `127` and `-128` for `TINYINT`, `0` and `255` for `TINYINT UNSIGNED`, `2147483647` and `-2147483648` for `INT`, and `99.99` and `-99.99` for `DECIMAL(4,2)`. The column is omitted, stderr warns once, and the header says it was not verified. That warning is not an acceptance: a real column that happens to store the endpoint stays unverified too. An in-range value, including a `0` the expression really produced, is still checked. `UPPER` and `LOWER` cover `ascii`, `latin1`'s one-to-one map, and `utf8mb4` for Latin-1 letters plus `µ` (U+00B5) to `Μ` (U+039C). `ß`, `ÿ`, and other code points above Latin-1 are not checked. `NULL` stays `NULL`, except `CONCAT_WS`, which skips `NULL` arguments. A logged value that is not that expression exits 1 and prints no SQL. The error names the table, the column, the expression, and one example row. `ENUM` and `SET` values in that example print the schema-file labels. The same refusal applies when two images share the referenced column values and differ in the generated column, including an `UPDATE` that changes only that column and an `INSERT`/`DELETE` pair. An expression that cannot be modelled exactly, and that the logged values do not contradict, is omitted. stderr warns once for that column, and the script header contains an English comment, `-- WARNING: generated column db.tbl.col not verified`, which says the guard checks the target. Anything the checker cannot model is unverifiable: it is never treated as a match and never as a mismatch. A proven contradiction, including two images that share the referenced values and differ in the generated column, still exits 1 and prints no SQL. When the logged values equal an expression the checker can evaluate, a real column still cannot be told apart from a generated one at flashback time. The script's guard is what stops that case on the target, including when the client continues after the error: the session is read-only and later writes fail. A generated column whose expression differs from the schema file still passes, and the target recomputes it. A `CREATE` in the parsed files replaces the file for that table. An `ALTER` in the parsed files, including one excluded by time or GTID, updates the definition used for later events. An `ALTER` inside the selected range is still refused, because flashback does not undo DDL. A plain `mysqldump --no-data db` dump has no `USE`. The database is taken from the `-- Host: ... Database:` header, from `--schema-file-db`, or from the binlog when that table name occurs in only one schema. Several `SHOW CREATE TABLE` results may sit in one file with no `;` between them, including `mysql --batch` output. If the header and `--schema-file-db` disagree, or the table name is in more than one schema, flashback warns and does not use the definition.

These limits stay even when the literals are exact:

- `ON DELETE CASCADE` and `ON UPDATE CASCADE` change child rows that are not in the binlog. Flashback does not restore those children.
- Triggers on the table fire when the undo runs.
- The match is the primary key, or every column and `LIMIT 1`. There is no conflict check. The script overwrites changes made after the incident.
- A table filter or `--dml` can undo only some row changes from one transaction. Flashback then prints a warning on stderr and still prints SQL for the rows it kept. Time, position, and GTID selectors drop whole transactions and do not warn.
- Review the script and test it before applying. Apply it in a single session on the primary with `sql_log_bin=1`, so replicas follow under new GTIDs. Do not apply it on a replica. A statement that fails leaves earlier transactions in the script committed.

### Known limitations

Review the script, test it, and apply it in a single session on the primary. A statement that fails leaves earlier transactions in the script committed.

- A `--schema-file` that marks a real column as generated still exits 0 and omits that column when the logged values equal an expression binlogviz can check, or when the expression cannot be checked and nothing contradicts it (`c` was always written as `a + 1`, `UPPER(x)`, or `CONCAT(first, ' ', last)`) ([#166](https://github.com/Fanduzi/BinlogVisualizer/issues/166)). MySQL 8 row metadata does not mark generated columns, so flashback cannot tell that case apart from a correct dump. The script then contains one guard, after the three `SET` lines and before the first transaction. Apply fails there, before any transaction, when `information_schema.COLUMNS.EXTRA` for that column is not `STORED GENERATED` or `VIRTUAL GENERATED`. The error says the schema file does not match the target and the error line names every mismatched `db.table.column`, not only the first. A long list shows the count and the first names. A column that is `GENERATED` on the target with a different expression passes the guard: only `EXTRA` is checked, the target recomputes the value, and the restored row is still correct. `mysql --force`, `source` or a paste into the mysql prompt, `mysqlsh --force`, `mysqlsh --interactive`, and a GUI runner set to continue on error keep going after that error. The guard switches the session to read-only first, so every later write fails with `ERROR 1792` and no row changes. Disconnect. A new session is not read-only. Fix the schema file and apply the script again. The lock is `SET SESSION TRANSACTION READ ONLY`, which names no server variable, so it works the same on MySQL 5.6.5 and newer and on MariaDB 10.0 and newer, including MariaDB 10.x–11.0, which have no `transaction_read_only`. The guard locks first and switches the session back only when the check ran in that session, every column matched, and the server is supported. If a guard step fails, for example a server that refuses `PREPARE` (`max_prepared_stmt_count` reached or 0), the session stays read-only and every write fails with `ERROR 1792`. Each transaction block locks again before it starts, and switches back only when the session holds this script's token. The guard sets `@binlogviz_ok` to that token, a value derived from the script's own text, only when the check ran in that session and matched. Every undo statement checks the token too: an `INSERT` is `INSERT ... SELECT ... FROM DUAL WHERE @binlogviz_ok <=> '<token>'`, and an `UPDATE` or `DELETE` adds `AND @binlogviz_ok <=> '<token>'` to its `WHERE`. So a block run without the header, a block of another script pasted into a session that already applied a correct script, and a block run after a reconnect stay read-only, and their writes fail with `ERROR 1792`. If an interactive client reconnects in the middle of a block, the rest of that block runs in the new session but changes no row (`0 rows affected`), and every later block fails with `ERROR 1792` ([#197](https://github.com/Fanduzi/BinlogVisualizer/issues/197)). The same holds inside an `XA START` transaction, where the lock itself fails with `ERROR 1399`. On a correct target, a reconnect inside a block leaves that block not applied: the statements before the drop roll back and the rest change nothing. Resume from that block, with the header, in a new session (see below). A script applied in a session that is already read-only, for example after an earlier script's guard failed, stops at its own guard with `binlogviz: this session is already read-only so the script cannot write. Disconnect and apply the script again in a new session` instead of a bare `ERROR 1792` on every write ([#191](https://github.com/Fanduzi/BinlogVisualizer/issues/191)). Apply a cut only with the header, in a new session. On a correct target, `PREPARE` failing means the script stops at `ERROR 1461` and restores nothing; raise `max_prepared_stmt_count` and apply again. A session that was read-only before the script stays read-only. Apply is supported on MySQL 5.7 and newer and on MariaDB 10.2 and newer; the guard is checked live on MySQL 5.7.19, 5.7.44 and 8.0 and on MariaDB 10.6, 10.11, 11.0 and 11.4. An older server (MySQL 5.6, MariaDB 10.1) fails the guard with `binlogviz: target server is older than MySQL 5.7.0 or MariaDB 10.2 and is not supported for apply` and is left read-only, so `mysql --force` cannot write there either. MySQL before 5.6.5 and MariaDB before 10.0 have no read-only session mode: the guard stops a client that stops on error, but one that continues is not locked. Clearing the read-only flag and continuing in that same session writes the wrong rows. That stops a wrong-environment dump and a dump whose expression happens to equal the logged rows, including a no-primary-key INSERT undo that would otherwise delete a different duplicate row. A logged value that contradicts the expression, including an `UPDATE` that changes only the claimed generated column, still exits 1 and prints no SQL. Omitting `--schema-file` is not a workaround for a real generated column: the script assigns that column, and apply stops at `ERROR 3105` with earlier transactions already committed.
- If the live table was altered so that a normal column became generated after the incident, the pre-incident value cannot be written back into that column. A dump taken after the `ALTER` is refused when the logged values contradict the new expression. A dump from incident time, when the column was still a normal column, assigns it, and apply stops at `ERROR 3105` because the target column is now generated. To restore the other columns, start from that incident-time script and delete the column from the `INSERT` lists and `SET` lists. MySQL then recomputes it. That recomputed value is not the stored original when the two differ. Do not apply the script with `--force`: the client would continue past that error into later transactions.
- Rows written by a non-strict session are restored under a strict session ([#180](https://github.com/Fanduzi/BinlogVisualizer/issues/180)). Flashback sees these values offline and wraps only that statement in a saved and restored `@@SESSION.sql_mode` that drops just the flags that would reject the stored value, plus `TRADITIONAL`: a zero month or day in a `DATE`, `DATETIME` or `TIMESTAMP` (`'0000-00-00'`, `'2026-00-15'`) drops `NO_ZERO_DATE` and `NO_ZERO_IN_DATE`, and a generated column that the script leaves out and whose logged value is `NULL` drops `ERROR_FOR_DIVISION_BY_ZERO`, so `a/b`, `DIV` and `%` with `b` = 0 recompute to `NULL` again. Strict mode stays on for those statements, so any other value that does not fit the target still fails. Other values that only a non-strict session accepts (a truncated string or an out-of-range number) cannot be in the binlog, because the server stored the clipped value. A generated column the script does not know about is still assigned, and apply stops at `ERROR 3105` as described above.
- In the guard error line, a `|` in a column name is shown as `/`, so `a|b` and `a/b` look the same there ([#208](https://github.com/Fanduzi/BinlogVisualizer/issues/208)). The guard's `SELECT` on stdout and the SQL keep the real names.
- [#167](https://github.com/Fanduzi/BinlogVisualizer/issues/167): an `ALTER` in the parsed binlog is applied a second time on top of a schema file that already contains it, so a dump taken after that `ALTER` can be refused even though the `ALTER` came before the incident and the file is the incident-time definition. How to recognise it: the error says `reordered (schema file ...; binlog ...)` and lists the same column twice (`id, a, b, c, c`). Use a dump taken before that `ALTER`, which restores, or leave out the binlog file that holds the `ALTER`. A file made by joining several `mysqldump --no-data` outputs uses only the first `Database:` header, so later dumps are bound to that database. Put `USE db;` before each dump, or run one flashback per database with `--schema-file-db` and that database's own dump file. A per-database run against the joined file does not work: it either refuses (`--schema-file does not match the binlog columns`) or warns that the `Database` header and `--schema-file-db` disagree and does not use the definition. Both fail safe, but neither restores. When an unqualified table name occurs in more than one schema, the per-table warning names only one of the tables left without a definition. Qualify that table, or add `USE`, so the definition is used.
- [#193](https://github.com/Fanduzi/BinlogVisualizer/issues/193): with a non-default `div_precision_increment` on the incident server, a generated column that divides `DECIMAL` operands, or multiplies or adds a quotient afterwards, can hold different digits than flashback computes at the default. Flashback refuses those rows (exit 1, no SQL), and the message points at the dump even when the dump is correct. If the dump is right, leave the table out with `--include-table` or `--exclude-table`.
- [#215](https://github.com/Fanduzi/BinlogVisualizer/issues/215): when the binlog names a table in all lower case and `--schema-file` has it only under different letter case, flashback uses the file table ([#187](https://github.com/Fanduzi/BinlogVisualizer/issues/187)). That assumes the binlog server had `lower_case_table_names=1` or `2`. On a `lower_case_table_names=0` server, where `t` and `T` can be two tables, a dump that has only `T` is used for `t`: the guard then stops the apply before any write, or flashback exits 1, in a case that v0.23.27 restored without a definition. When a folded table is refused, the error names the binlog table (`p187x.mixt`), not the file table (`P187X.MixT`), and the fold warning is not printed. Both fail safe. Use a dump that has the table under its exact binlog name, or leave the table out with `--include-table` or `--exclude-table`.

### Resuming after a failed apply

A statement that fails leaves earlier transactions in the script committed. Running the whole script again inserts a second copy of each restored row in a table with no primary key. A table with a primary key stops at `ERROR 1062`.

A script has two parts. The header is every line before the first `-- gtid:` line: two comments, an optional English `-- WARNING: generated column ... not verified` line for each generated column the checker could not model, three session statements, and, when the script omits a schema-file generated column, the guard that follows those statements. Resume keeps the whole header, including the guard.

```sql
SET NAMES utf8mb4;
SET time_zone = '+00:00';
SET SESSION sql_mode = REPLACE(@@SESSION.sql_mode, 'NO_BACKSLASH_ESCAPES', '');
-- Guard: every generated column omitted below must be GENERATED on the target.
```

The transaction blocks follow, in reverse binlog order: the last original transaction is the first block. Each block starts with a `-- gtid:` comment (or `-- gtid: GTID unavailable`) and a `-- binlog: file:pos` comment, and ends with `COMMIT;`. The `SET @binlogviz_sql_mode` / `SET SESSION sql_mode` lines inside a block belong to that block.

Blocks above the failure have been applied. Blocks below it have not. The client reports the failing line (`ERROR 1265 (01000) at line 20: ...`). The failed block is the last `-- gtid:` line at or before that line number. To resume:

1. Keep the header. Every `TIMESTAMP` literal in the script is the UTC wall clock and needs `SET time_zone = '+00:00'`. If the header is dropped, the remainder runs in the session's own time zone: apply still exits 0, MySQL reports nothing, and every restored `TIMESTAMP` is shifted by that offset (on a `+08:00` server, `09:00:00.123` comes back as `01:00:00.123`).
2. Delete only the transaction blocks above the failed one.
3. Keep the failed block and every block below it.
4. Run the header plus that remainder in a new session, because the failed session aborted. If the guard switched the session to read-only, disconnect and open a new session, fix the schema file, and apply the whole script again. If the failure needed a session option, put that `SET SESSION` statement above the header, as the first line of the resume file.

Do not re-run a transaction that already committed.

Example. This script stopped at `ERROR 1265 (01000) at line 20`. The last `-- gtid:` line at or before line 20 is line 13. Lines 1–6 are the header, lines 7–12 are the block that committed, and line 13 onward has not run:

```text
 1  -- flashback reverses the selected row changes, last transaction first.
 2  -- TIMESTAMP literals are the UTC wall clock of the stored instant. Review this script before applying it.
 3  SET NAMES utf8mb4;                                   -- header: keep
 4  SET time_zone = '+00:00';                            -- header: keep
 5  SET SESSION sql_mode = REPLACE(@@SESSION.sql_mode, 'NO_BACKSLASH_ESCAPES', '');  -- header: keep
 6
 7  -- gtid: 3528e50c-c289-11f1-8861-0242ac110003:986    -- committed: delete lines 7-12
 8  -- binlog: mysql-bin.000124:542
 9  START TRANSACTION;
10  INSERT INTO `shop`.`r_nopk` (`n`, `ts`, `s`) VALUES (1, '2026-10-08 00:00:00', 'one');
11  COMMIT;
12
13  -- gtid: 3528e50c-c289-11f1-8861-0242ac110003:985    -- failed: keep from here to the end
14  -- binlog: mysql-bin.000124:197
15  START TRANSACTION;
...
```

The resume file is lines 1–6 plus line 13 to the end. This keeps the header, drops every block that starts before line 13, and keeps the rest:

```bash
awk -v n=13 'NR >= n || !seen { if (NR < n && /^-- gtid:/) { seen = 1; next } print }' flashback.sql > resume.sql
mysql --default-character-set=utf8mb4 < resume.sql
```

Check that `resume.sql` starts with the three `SET` lines and that its first `-- gtid:` line is the failed block before applying it.

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
