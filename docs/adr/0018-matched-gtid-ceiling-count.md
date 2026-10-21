# ADR 0018: The matched GTID ceiling count is one lower bound

Status: accepted (2026-10-21). Tracks the ceiling follow-ups on #383, #384, and #390.

## Context

`selection.matched_gtid_set` holds at most `MatchedGTIDIntervalLimit` intervals (100,000). Past that, analyze stops with exit 10, `details.reason` `matched_gtid_intervals`, and no report. MariaDB keeps two forms per domain: per-server ranges, and one range list over the domain's sequence numbers rendered once per server. Either form is dropped, for every domain, when its ranges pass the limit. A domain left with no form also stops the run.

The count in that error was taken at the call site that noticed the overflow. After the per-server form was dropped, the next check priced the shared rendering (ranges times servers). With three gappy writers that number is three times the per-server count; with two writers, twice. The message then said a sequence number repeats whenever a domain had no form left, including when both forms were dropped because a list overflowed and nothing repeated. Checking the seen list and returning before the matched transaction was stored also dropped that transaction from the count.

## Decision

1. **What N means.** `MatchedGTIDLimitError.Intervals` is a lower bound on the intervals the matched transactions read so far need, the transaction that stopped the run included. It is as tight as the forms still held allow, and it is never higher than the true per-server count.

2. **One function.** `needed` is the only place a limit error gets that number. Seen overflow, `dropExact`, `dropShared`, a repeated sequence number, and a MySQL gap all call it through `stopError`. A domain contributes the smaller of the candidates the tracker can still justify:
   - the per-server ranges, while they are stored (exact);
   - the same count read back from the shared labels once those ranges are gone (exact until a sequence number repeats, then the `repeatDelta` lower bound);
   - the shared rendering, while that form is still allowed and no sequence number has repeated. It is exact, and it is ignored when it is only larger than the per-server count.

3. **The wording is the recorded reason.** `stopError` reads flags the tracker stored when the input was read: a domain's sequence number repeated (`conflict`), and whether the shared form is already off. It does not treat a count at or below the limit as proof of a repeat. The four messages are:
   - `repeat`, only when a sequence number repeated and the per-server form is already gone;
   - `per_server`, when the shared form was dropped earlier and the per-server count is what remains;
   - `form_dropped`, when the tight count fits in the limit but the form that would hold it was dropped;
   - plain, when the tight count itself passes the limit.

4. **The stopping transaction is in the state before the ceilings run.** `read` stores a matched identity, then `add` drops a seen overflow before it prices a per-server overflow. While the shared form is still trusted it stays in the bound, so the count is the smaller of the two rather than the shared cost. Returning on the seen list before that store leaves the transaction out of `needed`.

## Consequences

- Three gappy writers at the real limit report 100001, not 300003. Two report 100001, not 200002. Neither message says a sequence number repeats.
- A repeat after the per-server form was dropped still uses the repeat message, and the count stays the lower bound `repeatDelta` already provided.
- A seen overflow after the per-server form was dropped reports the tight count of the transactions read, including the one that crossed the limit, and the form-dropped message when that count fits.
- The run still stops when the form it would render does not fit, even if the tight bound is under the limit. It does not emit a set past the limit.
