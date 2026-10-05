# Plain ROLLBACK closes a group, and XA END releases it on the next GTID

A GTID group closes on plain `ROLLBACK`, `ROLLBACK WORK`, and either form with one trailing semicolon. `ROLLBACK TO SAVEPOINT` stays Unclassified QUERY and does not close the group. A zero-row plain `ROLLBACK` is not a report transaction. Row images already present in that group stay in the report.

`XA END` does not finalize the group by itself. When the group has an XA identity, `XA END` records an end boundary. A later GTID with a different identity finalizes that group and starts a new one. End of file finalizes it too. `XA PREPARE`, `XA COMMIT`, and `XA ROLLBACK` still finalize in place, so `XA END` followed by `XA PREPARE` stays one group whose end position is the prepare event.

This decision does not change STATEMENT or MIXED handling, and it does not add a report transaction that the retain rule would have dropped.

## Considered options

- Force-close every open group when the next GTID arrives. That hides a missing `COMMIT` after `BEGIN`, and it hides Ignored QUERY such as `SET timestamp`. ADR-0002 already rejected that option.
- Leave plain `ROLLBACK` as Unclassified QUERY. The group is already explicit because of `BEGIN`, so analyze aborts with conflicting GTID instead of the Unclassified QUERY error.
- Finalize on `XA END` itself. The following `XA PREPARE` would fall outside the group.

## Consequences

- ADR-0002 still holds for `XA END` followed by `XA PREPARE` in the same group. A different GTID after `XA END`, with no prepare, commit, or XA rollback, releases the group instead of aborting.
- ADR-0003 is unchanged. Unclassified QUERY that is the only work of a GTID-started non-explicit group still fails analyze. `CHECK TABLE` and `SET ROLE` are ADMIN and close the group. `FLUSH TABLES WITH READ LOCK` stays Unclassified QUERY. ADMIN verbs already admitted by dialect fixtures stay on the close table. `ANALYZE TABLE`, `OPTIMIZE TABLE`, `FLUSH PRIVILEGES`, `SET DEFAULT ROLE`, and exact `FLUSH TABLES` are unchanged.
- `BEGIN` with no `COMMIT` and no plain `ROLLBACK` still fails on the next GTID (exit 1). The group stays open. The Error line says `open BEGIN without close`.
- `ROLLBACK TO SAVEPOINT` still does not close. When it is inside that open `BEGIN`, the Error line says `ROLLBACK TO SAVEPOINT is not a group close`.
- Ignored QUERY still never closes. A next GTID after only Ignored QUERY still fails (exit 1). The Error line says Ignored QUERY does not close the group and that this is not a missing `COMMIT`. It does not say `open BEGIN without close`.
