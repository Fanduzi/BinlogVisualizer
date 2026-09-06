# Transaction groups close on a fixed table; only some become report transactions

Analyze reconstructs **transaction groups** from GTID, BEGIN, XA, and DDL, then decides which groups appear as report **transactions**. A group closes on COMMIT, XID, XA PREPARE, XA COMMIT, XA ROLLBACK, or a GTID-started DDL with no BEGIN. XA END does not close. A group becomes a transaction if it has ROW image rows, or an XA identity with a recorded file location. DDL-only groups stay on the DDL timeline. A second GTID in an open group fails analyze (exit 1). GTID selectors filter after grouping.

## Considered Options

- Treat XA PREPARE as continue until COMMIT: the next GTID conflicts (#62).
- Drop zero-row XA COMMIT: operators lose prepare/commit GTID and xa_xid for recovery (#63).
- On conflicting GTID, force-close and continue: hides a missing close event.

## Consequences

- Tests for this contract go through `Analyzer.Analyze` (events in, report transactions out), not the grouping state machine alone.
- Exit 2 (No-data result, ADR-0001) still applies when retain leaves zero transactions and zero counted ROW images.
- Normalize must emit XA END and XA ROLLBACK as `XA_END` / `XA_ROLLBACK`. Silent skip of those Query events would hide the close table.
