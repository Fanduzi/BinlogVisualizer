# Unclassified QUERY fails analyze; ADMIN grows from dialect fixtures

ADR-0002 left unknown QUERY skipped. That is the #61–#72 cascade: a skipped independent statement leaves the transaction group open, and the next GTID fails as a conflict. Analyze will not close on every QUERY — `SET timestamp` before BEGIN is prefix noise, and closing it would detach the following BEGIN from its GTID or hide a missing COMMIT after BEGIN. QUERY is classified as **ADMIN** (closes a GTID-started non-explicit group), **Ignored QUERY** (drop, count, never close), or **Unclassified QUERY**. Unclassified QUERY that is the only work of a GTID-started non-explicit group fails analyze with the statement prefix, exit 1, empty stdout. ADMIN membership grows only when a committed dialect fixture — a real ROW binlog from a named server flavor and version — shows that statement as that group's only work, and the test drives `ParseFiles`, not a hand-built event list. A second GTID in an explicit group, or in a group that already had ROW images or BEGIN, stays a conflicting GTID.

## Considered Options

- Close every QUERY in a non-explicit GTID group: `SET timestamp` before BEGIN would close and detach the following BEGIN from its GTID.
- Keep silent skip (ADR-0002): the next legal dialect is exit 1 conflicting GTID with no cause.
- Speculative ADMIN laundry list (`FLUSH TABLES`, `CHECK TABLE`, …) without a dialect fixture: untested verbs become close boundaries without proof they are that group's only work.

## Consequences

- Tests that expect `FLUSH TABLES` / `CHECK TABLE` / `SET ROLE` to produce “conflicting GTID” must change: either the statement is added to ADMIN with a dialect fixture, or the error is unclassified QUERY.
- Ignored QUERY starts as `SET timestamp`, `SET NAMES`, and other `SET` that is not `SET ROLE` or `SET DEFAULT ROLE`. Those never close. A next GTID after a group that held only Ignored QUERY is still a conflicting GTID.
- JSON diagnostics count Ignored QUERY separately from unmapped events (empty kind). Transaction payload wrappers are expanded to inner events before classification; that is ROW-image completeness, not a QUERY class.
- Synthetic `RawEvent` tests stay for the state machine. They are not the admission ticket for a new ADMIN verb or a new physical kind.
- BinlogQA retest is a release gate after the fixture is green, not the first detector.
- ADR-0002's “Unknown QUERY … stays skipped” sentence is superseded by this decision.
