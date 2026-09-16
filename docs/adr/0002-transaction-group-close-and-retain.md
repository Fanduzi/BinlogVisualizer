# Transaction groups close on a fixed table; only some become report transactions

Analyze reconstructs **transaction groups** from GTID, BEGIN, XA, DDL, and independent management QUERY, then decides which groups appear as report **transactions**. A group closes on COMMIT, XID, XA PREPARE, XA COMMIT, XA ROLLBACK, a GTID-started DDL with no BEGIN, or a GTID-started `ADMIN` event with no BEGIN (`ANALYZE TABLE`, `OPTIMIZE TABLE`, `FLUSH PRIVILEGES`, `SET DEFAULT ROLE`, exact `FLUSH TABLES`). XA END does not close. A group becomes a transaction if it has ROW image rows, or an XA identity with a recorded file path and a physical position span. DDL-only and admin-only groups stay off the report transaction list; admin QUERY is not DDL. A second GTID in an open group fails analyze (exit 1). GTID selectors filter after grouping. The next GTID does not force-close an open explicit transaction.

## Considered Options

- Treat XA PREPARE as continue until COMMIT: the next GTID conflicts (#62).
- Drop zero-row XA COMMIT: operators lose prepare/commit GTID and xa_xid for recovery (#63).
- On conflicting GTID, force-close and continue: hides a missing close event.
- Label ANALYZE/OPTIMIZE/FLUSH/SET DEFAULT ROLE as DDL so the existing close branch runs: pollutes the DDL timeline and table DDL counts (#72).
- Treat every unknown QUERY as an independent group: would close on `SET timestamp` / `SET ROLE` and hide a missing boundary.

## Consequences

- Tests for this contract go through `Analyzer.Analyze` (events in, report transactions out), not the grouping state machine alone.
- Exit 2 (No-data result, ADR-0001) still applies when retain leaves zero transactions and zero counted ROW images.
- Normalize must emit XA END and XA ROLLBACK as `XA_END` / `XA_ROLLBACK`. Silent skip of those Query events would hide the close table.
- Normalize must emit `ANALYZE TABLE`, `OPTIMIZE TABLE`, `FLUSH PRIVILEGES`, `SET DEFAULT ROLE`, and exact `FLUSH TABLES` as `ADMIN`, not `DDL`. Silent skip leaves the GTID group open and the next GTID fails analyze (#72). `FLUSH TABLES` is ADMIN only because a committed MySQL 8.0.46 dialect fixture proved it as that group's only work (ADR-0003). Other `SET` and `FLUSH` forms stay Ignored QUERY or Unclassified QUERY.
