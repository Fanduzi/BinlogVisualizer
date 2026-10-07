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

`--show-rows` is off by default. When it is on, listed transactions carry a bounded image: DELETE before-image, UPDATE columns that differ, INSERT after-image. The report keeps at most 32 logical rows per transaction and 64 bytes of each value. A cut uses `… [truncated: shown of original bytes]`, and the transaction says how many rows were left out.

Column names are taken from the binlog when `binlog_row_metadata=FULL` (MySQL 8.0.1+). Otherwise columns are `@1`..`@N`, and the report says names are missing. Without that metadata, an integer whose signed and unsigned readings differ is printed as both, the same way `mysqlbinlog -v` does. With FULL metadata, a column is printed with the signedness the binlog recorded.

`--sql-context off` does not print those cells. Auth-DDL `<secret>` redaction is unchanged; it applies to statement text, not to row cells. `mysqlbinlog_cmd` on the same transaction is the cross-check. The analyze report does not print undo SQL. `binlogviz flashback` does; see below.

## Flashback SQL

`binlogviz flashback` prints SQL that undoes the selected row changes. It reads the local binlog and does not connect to a database. Apply the script yourself after review.

A DELETE becomes `INSERT` of the before-image. An INSERT becomes `DELETE` of the after-image. An UPDATE sets every column back to the before-image and matches the primary key from the after-image. Statements are in reverse binlog order. Each original transaction is one `START TRANSACTION` / `COMMIT`. A comment names the original GTID, or `GTID unavailable`, and `file:start-position` (the basename and the byte where that transaction starts).

The same table, schema, `--dml`, time, position, and GTID selectors as `analyze` choose which rows are included. The binlog must carry column names (`binlog_row_metadata=FULL`) and complete row images (`binlog_row_image=FULL`). The script sets `utf8mb4`, `time_zone='+00:00'`, and strips `NO_BACKSLASH_ESCAPES` for the session. `TIMESTAMP` literals are the UTC wall clock of the stored instant.

A table with no primary key is still reversed: the `DELETE` or `UPDATE` matches every column and adds `LIMIT 1`, and a comment says so. The `INSERT` that puts a deleted row back does not use `LIMIT 1`.

JSON is rebuilt from the binary document (`JSON_OBJECT` / `JSON_ARRAY`, or `CAST(... AS JSON)` for a scalar), so a decimal stays a decimal, a datetime stays a datetime, and `-0.0` keeps its sign. That includes JSON inside a transaction recorded with `binlog_transaction_compression=ON`. A character column that is not `utf8mb4` is a charset introducer plus the raw bytes (`_latin1 0xE9`, `_utf16 0x00410042`). Collation `binary` stays `X'...'`. `utf8mb4` stays a quoted string. `ENUM` is the 1-based member index (`0` is the empty member) and `SET` is the bitmask (bit 0 is the first member). A quoted member name would be the column charset, which is not `utf8mb4` for `latin1` or `gbk`: strict `sql_mode` then returns `ERROR 1265`, and non-strict mode can store a blank. The number restores the same member in both modes. Index 0 is not a member. Strict `sql_mode` rejects it with `ERROR 1265`. Flashback restores that row by saving `@@SESSION.sql_mode`, dropping `STRICT_TRANS_TABLES` and `STRICT_ALL_TABLES` for that statement only, and restoring the saved mode. Other statements stay strict.

Generated columns, virtual or stored, are omitted from `INSERT` lists and `UPDATE` assignments when the definition is known. They are omitted from a no-primary-key `WHERE` when a base column remains. A generated column that is part of the primary key stays in the `WHERE`. Names come from `CREATE TABLE` and `ALTER TABLE` in the files you pass, including transactions dropped by `--exclude-gtids` or a time window, and from `--schema-file` (`mysqldump --no-data`, or `SHOW CREATE TABLE`, including `mysql --batch` output whose newlines inside the statement are `\n`). Flashback does not connect to MySQL. MySQL 8 `TABLE_MAP` optional metadata ends at `COLUMN_VISIBILITY` (invisible columns, not generated columns), and `binlog_row_image=FULL` stores both virtual and stored values, so a missing cell is not a signal and the binlog alone cannot tell them apart. If a definition was seen and cannot be read, flashback refuses the table and prints no SQL. If a selected table was never defined, flashback still prints the script, listing every column, and writes a warning on stderr before that script. The warning names the table, says generated columns cannot be ruled out, and says applying the script can stop at `ERROR 3105` with earlier transactions already committed.

`--schema-file` must describe the table as it was when the rows were written. Flashback compares that definition with the binlog `TABLE_MAP` for each selected event: column count, names, order, and, where the binlog has them, type, signedness, charset, `ENUM`/`SET` members, decimal precision, and fractional seconds. A mismatch names the table and the columns and prints no SQL. A generated column is omitted only when every logged value equals the expression. The check understands integer `+`, `-`, `*`, `/`, parentheses, column names, and `NULL`. Any other expression is refused, because a generated flag on a real column would otherwise drop the stored value. A `CREATE` in the parsed files replaces the file for that table. An `ALTER` in the parsed files, including one excluded by time or GTID, updates the definition used for later events. An `ALTER` inside the selected range is still refused, because flashback does not undo DDL. A plain `mysqldump --no-data db` dump has no `USE`. The database is taken from the `-- Host: ... Database:` header, from `--schema-file-db`, or from the binlog when that table name occurs in only one schema. Several `SHOW CREATE TABLE` results may sit in one file with no `;` between them, including `mysql --batch` output. If the header and `--schema-file-db` disagree, or the table name is in more than one schema, flashback warns and does not use the definition.

These limits stay even when the literals are exact:

- `ON DELETE CASCADE` and `ON UPDATE CASCADE` change child rows that are not in the binlog. Flashback does not restore those children.
- Triggers on the table fire when the undo runs.
- The match is the primary key, or every remaining column and `LIMIT 1`. There is no conflict check. The script overwrites changes made after the incident.
- A table filter or `--dml` can undo only some row changes from one transaction. Flashback then prints a warning on stderr and still prints SQL for the rows it kept. Time, position, and GTID selectors drop whole transactions and do not warn.
- Apply the script on the primary with `sql_log_bin=1`, so replicas follow under new GTIDs. Do not apply it on a replica.

Flashback refuses rather than guessing. Exit 1, one `Error:` line that names the table and the reason, and no SQL on stdout, when:

- column names are unavailable
- a before-image or after-image is incomplete (`binlog_row_image` `MINIMAL` or `NOBLOB`)
- a column cannot be rendered exactly (`FLOAT`, `DOUBLE`, `BIT`, `GEOMETRY`, `VECTOR`, a partial JSON value, a JSON value that cannot be represented exactly, invalid UTF-8 in a `utf8mb4` column, an unknown collation, or missing signedness, collation, or ENUM/SET members)
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
