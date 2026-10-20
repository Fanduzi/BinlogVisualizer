# ADR 0015: A table filter never hides a DDL from the flashback refusal

Status: accepted (2026-10-20). Fixes a P1 that v0.34.0 and earlier ship with (BinlogQA, PR #366 QA, private report).

## Context

Flashback refuses a selected range that contains DDL (`ddl_in_range`), because it does not undo schema changes and an undo written for the old table layout or the old table name can land in the wrong place.

The analyzer's object filter (`--include-table`, `--exclude-table`, `--include-schema`, `--exclude-schema`) decides whether an event is retained by one `schema.table`. For a DDL that is the first name the DDL parser finds. A filtered-out DDL never reached the planner. So a DDL whose selected table is not its first name was invisible to the refusal:

```sql
DELETE FROM rn.t1 WHERE id=1;
RENAME TABLE rn.t2 TO rn.tmp, rn.t1 TO rn.t2, rn.tmp TO rn.t1;
DELETE FROM rn.t1 WHERE id=5;   -- this row came from the old rn.t2
```

`flashback --include-table rn.t1` exited 0. `verify | mysql` on MySQL 8.4.11 printed `undo committed, 2 statements` and inserted the old rn.t2 row into rn.t1. `DROP TABLE rn.z, rn.t1` followed by `RENAME TABLE rn.t2 TO rn.t1` did the same. `--exclude-table rn.t2` hid the swap too, because the first name was the excluded table. With no filter, or when rn.t1 was named first, the range was refused.

## Decision

1. Every in-range DDL that the selection keeps by position and GTID reaches the planner, whether or not the object filter keeps it for the report. The analyzer hands a filtered-out DDL to `Planner.Note` (under GTID selectors, when its group matches, like a retained event). The report and DDL timeline are unchanged.
2. With no table or schema filter, every DDL in range refuses, as before, and the message and `details.table` are unchanged.
3. With a table or schema filter, the planner reads the DDL's whole text (`internal/flashback/ddlrefs.go`) and collects every table it names as a source or a target, and every schema it creates, alters, or drops. The DDL refuses when any of them could hold a selected row (`Selection.MayHoldTable` / `MayHoldSchema`, implemented by the analyzer's `EventFilter`). The first name gets no special role.
4. Fail closed. The reader knows these shapes completely: `RENAME TABLE` lists, `DROP [TEMPORARY] TABLE` lists, `DROP VIEW` lists, `TRUNCATE`, `ALTER [ONLINE] [IGNORE] TABLE` (including `RENAME [TO|AS] name` and `REFERENCES name` anywhere in the body), `CREATE [OR REPLACE] [TEMPORARY] TABLE` (including `LIKE name` and `REFERENCES name`), `CREATE`/`DROP INDEX ... ON name`, `CREATE`/`ALTER`/`DROP DATABASE`, and account and privilege statements (`GRANT`, `REVOKE`, `CREATE`/`ALTER`/`DROP`/`RENAME USER`, roles, `SET PASSWORD`), which name no table. Anything else refuses with a message that says the statement could not be read completely: views, triggers, routines, events, `EXCHANGE PARTITION`, `CREATE TABLE ... SELECT | TABLE | VALUES | WITH`, executable comments (`/*! */`, `/*M! */`, which the server runs), double-quoted names (identifiers under `ANSI_QUOTES`), a second statement, an unterminated quote or comment, and text the binlog layer truncated.
5. Lean to refusing when a name could match. An include list matches with letter case folded (`lower_case_table_names` is not in the binlog). An exclude list removes a name only on the exact spelling. An unqualified name resolves in the query event's default database, which the analyzer now keeps on the event as `QuerySchema` when a qualified first name replaces `Schema`. With no default database, an unqualified name matches any schema. A schema-level statement refuses when a selected table could live in that schema. The schema catalog's DDL replay reads the same default database.

`--dml` alone is not an object filter: every DDL in range still refuses. A DDL left out by GTID or position is not in range and does not refuse, as before.

## Consequences

- The two witnesses and `--exclude-table` of the other table now exit 6 with `ddl_in_range` naming the selected table. A DDL on another table, with the selected table not named anywhere in it, still passes.
- Some filtered runs that passed before now refuse: a view, trigger, or routine DDL in range, or a `CREATE TABLE ... SELECT`. The message names the cure: narrow the range with `--start-position`, `--stop-position`, or `--exclude-gtids` so it leaves out the DDL.
- Tests: `TestFlashbackDDLUnderTableFilter` and `TestFlashbackDDLWithoutFilterUnchanged` run BinlogQA's live MySQL 8.4.11 binlogs (`internal/binlog/testdata/mysql-8.4.11-ddlfilter-*.binlog`). `TestDDLReferencesReadsEveryName`, `TestDDLReferencesFailsClosed`, `TestDDLRefusalUnderSelection`, and `TestFilterMayHoldLeansToRefuse` cover the reader and the predicate. Checking only the first name, dropping the analyzer hand-off, or dropping the GTID-path hand-off each fails a test.

## Not covered

A DDL outside the selected range (after `--stop-position`, or excluded by GTID) can still rename or drop a selected table before the script is applied. The script's statements then fail or hit the wrong table on the target exactly as they would for any later schema change. That is the "changes outside the scanned binlogs are not seen" limit, not this decision.
