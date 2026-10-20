# GTID filters stream each transaction (#376)

Status: implemented on top of v0.34.1 + #374. Covers `analyze` and `flashback` with `--include-gtids` / `--exclude-gtids`.

## Problem

With a GTID selector the analyzer appended every in-window event of the open transaction group to `pendingGroupEvents` and only decided include or exclude when the group closed. The buffered `NormalizedEvent` copies (flashback row data included) cost about 4.3KB of RSS per event. One transaction of 300k row events (a 43MB binlog) ran out of memory and exited 2 with a Go runtime crash. That is the usual way to look at one big transaction: find its GTID, then analyze only that GTID.

## Why it buffered

The comment called the buffer a stopgap ("spool if real transaction sizes make this a memory ceiling"). Nothing needed lookahead:

- **Only a GTID event carries a GTID, and it opens its group.** The parser sets `GTID` on MySQL `GTID_LOG_EVENT` / `GTID_TAGGED_LOG_EVENT` and MariaDB `GTID_EVENT` only. In `TransactionBuilder`, a GTID event either starts a new group or joins an open group that already has the same GTID. It never names a group that has none: an implicit group without a GTID is released, and an explicit one without a GTID is an error. `mergeProvenance` refuses a second, different GTID. So the group's GTID is final by the time any of its other events arrives.
- **MariaDB.** `GTID_EVENT` is the group's first event and carries `domain-server-seq`. Same rule.
- **Anonymous GTIDs and groups with no GTID event.** The GTID is empty and an active selector never matches it (`GTIDSelector.Match`), which is final too. The old code dropped these events the same way.
- **`--start-position` or `--start` in the middle of a transaction.** The builder still observes the out-of-window GTID event, so the open group has its GTID before its first in-window event.
- **Unresolved boundaries** (a group cut by the start of the input or by EOF). These were and still are rejected when the group closes ("cannot apply GTID selectors: transaction ... has unresolved boundaries"). That error ends the run with no report, so having counted the group's earlier events changes nothing.
- **Events attributed to a group that is neither open nor just closed.** These can't happen: the event's key is the open group or the group this event closed. The old buffer would never have drained such a key, so they are dropped, as before.

## Design

`Analyzer.gtidGroups` holds one small decision per transaction key: the GTID, whether it matched, and whether any in-window event was kept. An entry is created at the group's first in-window event, from `TransactionBuilder.groupGTID` (the open group or a completed group not yet drained), and removed when the group drains. At most two entries exist at once, so memory is O(1) per transaction.

- An in-window event of a matched group goes straight to `aggregateRetainedEvent`. An event of an unmatched group is dropped. Nothing is held per event.
- The order the old buffer replayed in is kept. An event that closes its group (XID, COMMIT, XA PREPARE, a GTID-started DDL) is aggregated before that group drains. An event of the open group is aggregated after any group this same event closed.
- A filter-excluded DDL goes to the flashback planner at once if its group matched, as it does without a selector.
- Drain still runs the old checks in the old order: unresolved boundaries, then `Match` (whose parse error is reported there), then `matched_gtids`. As a guard, if the GTID or decision at drain differs from the one used for streaming, analyze fails ("was filtered as GTID ... but closed as ...") instead of printing a wrong report. The rule above means this cannot happen.

No case needed lookahead, so there is no bounded buffer and no new refusal or exit code.

## What still grows

JSON `selection.matched_gtids` lists every matched GTID, so it grows with the number of matches, as the alert list grows with alerts. The memory test therefore keeps the matched set fixed.

## Evidence

- `TestAnalyzeGTIDFilterRetainedHeap` (CI `verify`, listed in `TestEveryMemoryTestRunsInCIVerify`) covers two cases. First, 25k vs 100k transactions with `--include-gtids` picking the same 2000. Second, one transaction of 75k vs 300k `UPDATE_ROWS` events, under include and under include+exclude. Each larger run may peak at most 256KiB above the smaller one. Before: the 300k transaction peaked 310MB above the 75k one (about 1.4KB per event in this test's small rows). After: under 12KB.
- On QA's one-transaction binlogs (374-pr F1) with `--include-gtids` of the big transaction under a 3GiB address-space cap, the base binary took 134MB / 504MB of max RSS for 30k / 100k row events and crashed at 300k (`fatal error: out of memory`, exit 2). The new binary used 36MB / 45MB / 42MB with exit 0. The 30k and 100k JSON is byte-identical to the base output.
- Output is unchanged. Base and new binaries were compared on the 48 repo fixtures: analyze json/text/markdown, detailed text, flashback `--plan` and flashback SQL, with first/middle/last/range include and exclude selectors, and a start/stop position in the middle of a transaction. All runs were identical in rc, stdout and stderr.
