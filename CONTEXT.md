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
The binlog parsed and a Format Description was present, but zero events were counted — an empty time window, or a file that is only Format Description / rotate. Distinct from a corrupt or incomplete file.
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
