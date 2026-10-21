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
- Drain still runs the old checks in the old order: unresolved boundaries, then `Match` (whose parse error is reported there), then the matched GTID set. As a guard, if the GTID or decision at drain differs from the one used for streaming, analyze fails ("was filtered as GTID ... but closed as ...") instead of printing a wrong report. The rule above means this cannot happen.

No case needed lookahead, so there is no bounded buffer and no new refusal or exit code.

## Matched GTID evidence (#379)

Report_version 3 listed every matched GTID in JSON `selection.matched_gtids` and kept them in a map until the report was built: about 117 bytes per matched transaction in the memory test below, 190-375 bytes in QA's runs. `--exclude-gtids` of one GTID on a 3M-transaction binlog ran out of memory (exit 2).

The analyzer now keeps `matchedGTIDSet` (`internal/analyzer/gtid_matched_set.go`): sorted, merged sequence ranges per MySQL server UUID or MariaDB domain and server, plus `matched_transactions`: the matched transaction groups with at least one event in the window, once per group read (repeats included; groups without retained rows, such as DDL, included, unlike `summary.total_transactions`). Report_version 4 writes them as `selection.matched_gtid_set` (`uuid:1-4:6-3000000`; MariaDB `0-7-1..3`, `0-7-8`) and `selection.matched_transactions`, and drops `matched_gtids`. Adding a GTID that extends the last range is O(1); any other GTID is a binary search over chunks of at most 256 ranges, so out-of-order GTIDs cost O(log n + 256) instead of shifting the whole list (#381 QA: 100,000 descending GTIDs took 14s), and repeated GTIDs are merged.

MariaDB lets several servers write one domain with one sequence (#381 QA F1). Ranges per domain-server pair then leave one range per transaction when writers alternate, and 100,001 such transactions hit the ceiling. Each MariaDB domain therefore also keeps ranges over its sequence numbers plus the set of servers that wrote matched transactions, rendered as the same ranges once per server (`0-7-1..9`, `0-8-1..9`), and the set renders whichever form has fewer entries. The shared form also names identities the input does not contain (`0-8-1` when server 8 wrote only even numbers); they select nothing. It is used only while no sequence number of the domain was read twice, matched or not, so it can never name an unmatched transaction of the input: the analyzer records the sequence numbers of every MariaDB transaction it reads in a third range list per domain. A repeat (another server with the same number, or the same identity in two files) makes the domain use the per-server form. Each form holds at most 100,000 ranges; a form that grows past that is dropped, and analyze stops with exit 10 when a domain has no form left or the rendered entries exceed the limit. The error counts what the matched transactions read need, the stopping one included: per domain the smaller of its per-server and shared counts (the per-server count is derived from the shared ranges after its own ranges are freed). The message says which case applies (#390 QA): more than 100,000 in any form; a sequence number repeats after the per-server form was dropped; the shared form was dropped earlier, so the count is per server and the shared form could need fewer; or the count fits but the form that holds it was dropped. The seen ranges are checked before a matched transaction is added, as for an unmatched one. `--include-gtids` and `--exclude-gtids` accept `d-s-first..last`, so the list round-trips (#381 QA F2). Memory is O(intervals): a contiguous run of matches costs one range, whatever its length.

The set renders at most `MatchedGTIDIntervalLimit` (100,000) entries (intervals) across all sources. The next one stops analyze with exit 10 `resource`, `details.reason` `matched_gtid_intervals`, and no report. 100,000 intervals take a few MB; reaching them needs selectors or input that leave 100,000 gaps among the matched GTIDs, for example an exclude list of 100,000 separate GTIDs.

## Warning order under selectors (#378 F1)

A GTID selector does not reorder flashback warnings. Retained rows reach the planner in binlog order, as without a selector, so "no table definition" warnings follow the order in which the planner settles each table: at its first undo row, or, for an unqualified `--schema-file` name, when the binlog shows the name in a second schema and the file entry is set aside. With `t1` unqualified in the schema file and rows for `q378_a.t1`, `q378_a.t2`, `q378_b.t1`, the order is `q378_a.t2`, `q378_a.t1`, `q378_b.t1`, with or without a selector. Before #378, a selector buffered the group and so resolved the name earlier, giving `q378_a.t1` first; that order is gone on purpose. `TestFlashbackWarningOrderSameUnderGTIDSelector` pins the rule for include and exclude selectors.

## Evidence

- `TestAnalyzeGTIDFilterRetainedHeap` (CI `verify`, listed in `TestEveryMemoryTestRunsInCIVerify`) covers two cases. First, 25k vs 100k transactions with `--include-gtids` picking the same 2000. Second, one transaction of 75k vs 300k `UPDATE_ROWS` events, under include and under include+exclude. Each larger run may peak at most 256KiB above the smaller one. Before: the 300k transaction peaked 310MB above the 75k one (about 1.4KB per event in this test's small rows). After: under 12KB.
- On QA's one-transaction binlogs (374-pr F1) with `--include-gtids` of the big transaction under a 3GiB address-space cap, the base binary took 134MB / 504MB of max RSS for 30k / 100k row events and crashed at 300k (`fatal error: out of memory`, exit 2). The new binary used 36MB / 45MB / 42MB with exit 0. The 30k and 100k JSON is byte-identical to the base output.
- `TestAnalyzeMatchedGTIDSetHeap` (CI `verify`) covers #379: 262,144 vs 1,048,576 transactions (the same 256 carry rows), under `--exclude-gtids` of one GTID and under an include of everything with three GTIDs excluded. The matched count grows 4x while the interval count stays 2 or 4; the larger run may peak at most 256KiB above the smaller one. Before: 92MB more (about 117 bytes per match). After: 13-20KB, at GOMAXPROCS 1 and 8.
- Output is unchanged. Base and new binaries were compared on the 48 repo fixtures: analyze json/text/markdown, detailed text, flashback `--plan` and flashback SQL, with first/middle/last/range include and exclude selectors, and a start/stop position in the middle of a transaction. All runs were identical in rc, stdout and stderr.
