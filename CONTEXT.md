# BinlogViz

A local CLI that summarizes MySQL and MariaDB **ROW** binlog files for operators: hot tables, write shapes, and before/after compare. It is not a full STATEMENT/MIXED analyzer.

## Language

**ROW image**:
The before/after row payload in a binlog rows event. This is what analyze counts.
_Avoid_: row event (the parser event), SQL row

**Query-DML**:
A QUERY event whose statement is INSERT, UPDATE, DELETE, or REPLACE, not BEGIN/COMMIT/DDL. STATEMENT and MIXED files carry writes this way.
_Avoid_: query event (includes DDL and transaction control)

**Input format**:
ROW, STATEMENT, or MIXED, guessed from Query-DML versus ROW images. ROW is the supported case; STATEMENT has no images; MIXED is undercounted (images only).
_Avoid_: binlog_format (the server variable)

**No-data result**:
The binlog parsed and a Format Description was present, but analyze counted no ROW images and retained no transactions — an empty time window, a file that is only Format Description / rotate, or an ADMIN-only file. Distinct from a corrupt or incomplete file, and from Unclassified QUERY (exit 1).
_Avoid_: empty success, empty report

**TPS**:
Transactions per second on the operator-facing summary. When the analyzed span is shorter than one second, it is not shown as a number.
_Avoid_: avg TPS/min (the per-minute chart series)

**Transaction group**:
A start-to-close span in a ROW binlog, opened by a GTID or BEGIN (including XA START). It may or may not appear in the report.
_Avoid_: GTID group (BEGIN-only spans are groups too), transaction (the report item)

**Transaction**:
A transaction group that analyze puts in the report: it has ROW image rows, or an XA identity with a recorded file location.
_Avoid_: in-flight group, discarded DDL group

**XA identity**:
The SQL-form XA xid (`gtrid`, `bqual`, format id). Empty means unknown.
_Avoid_: XID (the InnoDB engine xid)

**ADMIN**:
An independent management QUERY that closes a GTID-started transaction group that never saw BEGIN or XA START. It is not DDL and does not become a report transaction.
_Avoid_: admin query, DDL (GRANT is DDL; ANALYZE is not)

**Ignored QUERY**:
A QUERY analyze drops on purpose because it is session prefix, not a group boundary and not counted work. Typical forms are `SET timestamp` and `SET NAMES`. A group that holds only Ignored QUERY stays open; the next GTID fails analyze. That failure is not a missing COMMIT.
_Avoid_: skipped event (includes unmapped physical kinds), unclassified QUERY

**Unclassified QUERY**:
A QUERY that is neither a known boundary (BEGIN, COMMIT, plain ROLLBACK, XA, DDL, ADMIN, LOAD DATA) nor Ignored QUERY. If it is the only work in a GTID-started non-explicit group, analyze fails rather than skipping into a conflicting GTID. `ROLLBACK TO SAVEPOINT` stays in this class and does not close the group. Inside an open BEGIN, the next GTID fails as an open BEGIN whose Error line says the SAVEPOINT rollback is not a group close.
_Avoid_: unknown query, skipped query

**Unmapped event**:
A parser event with no canonical kind. Distinct from a classified QUERY that was ignored.
_Avoid_: ignored QUERY, skipped QUERY

**Transaction payload**:
A binlog event that wraps a transaction group's inner events, optionally compressed. Analyze counts the inner ROW images as if they were written separately.
_Avoid_: compressed binlog (MariaDB compressed row events are a different physical kind)

**Schema catalog**:
The flashback module that answers which table definition applies to a binlog `db.table`, and why. It loads `--schema-file` text (USE, `Database:` headers, `--schema-file-db`), replays binlog DDL and table maps, and resolves a name to one typed result: exact, case-fold (naming the schema-file table), ambiguous, missing, or refused with a reason. Refusal and warning text is built by callers from that result. Lives in `internal/analyzer/schema_catalog.go`; see ADR-0005.
_Avoid_: generatedTables (its old name), schema binding, label rewrite

**Apply guard**:
The SQL a flashback script runs before its first transaction when it omits generated columns: it checks those columns are GENERATED on the target, cuts the mismatch message to whole names within MySQL's 200-byte sql_mode value, and leaves the session read-only unless the check matched; each block repeats the lock and each undo statement checks the script's token. One module, `internal/analyzer/flashback_guard.go` (header, block lock, row condition, token seal); the message cut exists only in the emitted SQL and is tested by running it on MySQL. See ADR-0006.
_Avoid_: guard twin, guardErrorValue (removed Go copy of the cut)

**Expression evaluator**:
The code that computes a schema-file generated-column expression from logged values, to check the logged generated value. One grammar parsed once per expression text into a cached tree; one walk in the general domain (`evalGenExpr`: integers, text, JSON) or the exact-DECIMAL domain (`evalDecExprInc`, which takes div_precision_increment and accepts only numbers, columns, NULL/TRUE/FALSE, arithmetic, MOD and parentheses). One module, `internal/analyzer/flashback_eval.go`. See ADR-0007.
_Avoid_: gen parser, dec parser, evalIntExpr (removed integer evaluator)
