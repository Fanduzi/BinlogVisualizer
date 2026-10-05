# BinlogViz v0.23.13 Release Notes

Release date: 2026-10-05

## Overview

v0.23.13 counts index changes on the table the statement changes. `CREATE INDEX`, `CREATE UNIQUE INDEX`, `CREATE FULLTEXT INDEX`, `CREATE SPATIAL INDEX`, and `DROP INDEX` show the table after `ON` on the DDL timeline, in the per-table list, and under `--include-table`. `CREATE UNIQUE INDEX` and `CREATE FULLTEXT INDEX` stay on the DDL timeline, and `CREATE SPATIAL INDEX` does too. A schema written in the statement wins over the database selected with `USE`. `ON t(col)` and `CREATE TABLE t(id INT)` without a space are parsed as that table. Workflow and trend behavior are unchanged.

## Bug Fixes

- **Index DDL is counted on the table after `ON` (#112)**: `CREATE INDEX`, `CREATE UNIQUE INDEX`, `CREATE FULLTEXT INDEX`, `CREATE SPATIAL INDEX`, and `DROP INDEX` are counted on the table after `ON`. The DDL timeline, the per-table list, and `--include-table` use that table. `CREATE UNIQUE INDEX` and `CREATE FULLTEXT INDEX` stay on the DDL timeline. `CREATE SPATIAL INDEX` stays on the timeline the same way. `--include-table` keeps these index statements when the filter names that table.
- **A schema in the statement wins over `USE` (#112)**: When the statement names `schema.table`, the report counts that table, including when another database is selected.
- **A missing space before `(` still names the table (#112)**: `ON t(col)` and `CREATE TABLE t(id INT)` are parsed as table `t`. A backticked identifier that contains a parenthesis stays intact.

## Breaking Changes

None.

## Compatibility

- Exit codes 0, 1, and 2 keep ADR-0001 meaning.
- CLI flags are unchanged. `--include-table` keeps these index statements when the filter names the table after `ON`.
- The DDL timeline, the per-table list, and `--include-table` name the table after `ON` for `CREATE INDEX`, `CREATE UNIQUE INDEX`, `CREATE FULLTEXT INDEX`, `CREATE SPATIAL INDEX`, and `DROP INDEX`. `CREATE UNIQUE INDEX`, `CREATE FULLTEXT INDEX`, and `CREATE SPATIAL INDEX` appear on the DDL timeline.
- A schema written in the statement is the schema for that statement.
- Snapshots, workflows, artifact names, and supported platforms are otherwise unchanged from v0.23.12.
- Workflow and trend behavior are unchanged.
