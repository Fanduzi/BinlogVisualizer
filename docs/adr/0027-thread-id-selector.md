# ADR 0027: `--thread-id` — select transaction groups by binlog connection id

Status: accepted (2026-11-02). Roadmap #256: select transactions by the connection that wrote them (`analyze --thread-id 1875`, same flag on `flashback` and `redo`). ADR 0024 deferred this selector; the deferred surface lands here. This ADR is a pair with ADR 0028 (index/discovery hardening); they are separate ADRs because the two surfaces are disjoint — an analyzer group-selection seam and a CLI input-path trust boundary share no state, code, or rollback unit.

## Context

Code evidence that shaped the decisions:

- **The parser already captures the value.** `internal/binlog/parser.go` copies `QueryEvent.SlaveProxyID` onto `raw.ThreadID`, and `normalize.go` carries it to `NormalizedEvent.ThreadID`. Only `QUERY`-derived canonical events carry it: `BEGIN`, `XA_*`, `COMMIT`, `ROLLBACK`, `DDL`, `ADMIN`, `UNCLASSIFIED_QUERY`. `GTID`, `XID`, `TABLE_MAP`, `ROWS_QUERY`, and `ROWS` events never carry one.
- **The transaction already owns the value.** `TransactionBuilder.mergeProvenance` merges the first nonzero event `ThreadID` onto the in-flight group (first-wins), and `finalizeTransaction` copies it to `model.Transaction.ThreadID`. `0` is the "no attribution" marker — a group whose events never carried a QUERY thread id keeps `0`.
- **Attribution arrives later than GTID identity.** The GTID selector (docs/gtid-filter-streaming.md, #376) works because the GTID event both *opens* its group and *carries* the identity — the match is decided at the group's first event and everything streams straight through. A thread id is only visible once the group's first QUERY event arrives, and a GTID-opened group has one event — its opening `GTID` event — strictly before that. Any streaming design must decide what the pre-attribution window means.
- **`--where` composes at row level, not group level** (ADR 0024): `Options.RowWhere` is forwarded into both planners and applies to staged rows inside retained groups. A group selector must AND with it the way object/DML/time/position/GTID selectors already do — each kept event still runs every check.
- **Selector conventions are fixed.** Value flags use `StringSliceVar`: comma-separated and repeated forms both work, whitespace trims, canonical order comes out of the parser (`ParseDMLKinds`, `ParseGTIDSelector`). Unparseable values are usage errors (exit 3) wrapped at the command's flag-parsing stage.
- **Anonymous-group precedent exists.** Under GTID selectors a group with no GTID matches nothing — a filter selects evidence, not absence of evidence (`selection_test.go` pins it). `thread_id == 0` is the same shape: unattributed, therefore unselectable.
- **`selection` is the selector-evidence home in JSON** (`model.AnalysisSelection` → `jsonSelection`); `scope.filters` holds the object filters only. Compare reads `InputSelection` and hashes it into `canonicalScopeKey`, so two snapshots must differ when their group selectors differ or comparability would lie.

## Decision

### Value form and validation

`--thread-id` is a `StringSliceVar` on `analyze`, `flashback`, and `redo`: comma-separated and/or repeated flags, whitespace trimmed. `analyzer.ParseThreadIDs` returns a sorted, deduplicated `[]uint32`. Each token must parse as a decimal uint32 and be **nonzero** — `0` is the unattributed marker and selecting it could never match, so it is a usage error, not an empty result. An empty/whitespace-only flag is also a usage error. Errors surface as usage (exit 3) at each command's existing selector-parsing stage, identical to `--dml` and `--include-gtids` handling.

### Semantics: complete-group selector decided on first attribution

`Options.ThreadIDs` turns on a group-level selector with the same shape as the GTID selector:

- The group's selector identity is its **first nonzero merged thread id** — exactly what `Transaction.ThreadID` ends up carrying (the first QUERY event's `SlaveProxyID`, normally the `BEGIN`). `TransactionBuilder.groupThreadID(key)` mirrors `groupGTID`: the open group's merged value, else the completed-but-undrained group's.
- A group **matches** when its identity is nonzero and in the set. It then keeps every in-window event — boundaries included — and drops entirely otherwise, for report aggregation *and* for the flashback/redo planner `Note` stream (both go through the same `aggregateRetainedEvent` seam, so all three commands get one semantics for free).
- **`thread_id == 0` never matches.** Unattributed groups — anonymous GTID-less groups, server-generated groups — are excluded like anonymous GTID groups, not silently admitted.
- Composition is AND with everything else: schema/table include/exclude, `--dml`, time/position windows, GTID include/exclude, and `--where` (which still applies per-row inside retained groups).
- Held per group: `{decided, tid, matched, held}` in `a.threadGroups`, created on first in-window event, drained at `persistCompletedTransactions` alongside `gtidGroups`.

### The pre-attribution window: a bounded held slot, not a guess

Before attribution, only the group's opening `GTID` event(s) can legitimately exist — every other event type either carries a thread id itself (QUERY-derived) or follows one in a valid binlog. So while a group is undecided:

- `GTID` events queue in `held` (cap 8 — covers pathological same-GTID repeats without exposing unbounded memory).
- Any other event type while still unattributed marks the group **decided-not-matched** and clears `held`: in a valid stream this state is unreachable, because the first QUERY already carried or preceded the attribution the builder merged.

On the first event that observes a nonzero merged id, the group decides: matched groups replay `held` through `aggregateRetainedEvent` (planner `Note` order stays GTID-then-BEGIN, summary event/byte counts stay group-exact), then the current event aggregates; unmatched groups drop `held` and keep dropping.

**Drain guard.** At `persistCompletedTransactions`, the recorded decision is checked against the finalized `Transaction.ThreadID`: a group filtered as id `X` that closes as `Y != X` fails the run — `cannot apply --thread-id: transaction %s was filtered as thread_id %d but closed as thread_id %d`. This mirrors the GTID drift guard and is the honest answer for streams where attribution genuinely arrives only after rows (rows-before-any-QUERY is outside valid producer output; guessing mid-group is worse than failing).

`noteFilteredDDL` gets the same gate: an object-filter-dropped DDL still reaches the flashback planner only when its group matches — now under the thread decision too (the DDL carries its own attribution, so the decision is always resolvable there). Because the flush can fail, it becomes error-returning.

**Events with no `TxnKey`** (FORMAT, ROTATE, file-level events) are not group members and drop under any active group selector — same rule the GTID selector applies.

### Evidence and comparability

- `model.AnalysisSelection.ThreadIDs []uint32`; JSON emits `selection.thread_ids` (additive key, report stays v4 per ADR 0010).
- `compare.InputSelection` gains `ThreadIDs`, `cloneSelection` copies it, and `canonicalSelection` normalizes it into the scope key: two reports selected under different thread-id sets are not comparable.
- `HasSelectionFilters()` covers it.

### The honest caveat, stated everywhere the flag is documented

Binlog `thread_id` is the server-assigned **connection id** stamped on QUERY events. The server reuses ids as connections churn, so `--thread-id 42` selects every transaction written while *some connection held id 42* — possibly several unrelated logical connections over the file's history. It is a session-boundary selector for incident scoping ("everything this connection did"), not an application identity. The flag help and `docs/reference/cli.*` say exactly this.

## Considered options

- **Filter at `filteredWorkloadEvent` / row level.** Rejected: the selector is defined on *transactions*, and boundaries (GTID/BEGIN/XID) must follow the group whole — dropping non-workload events of matched groups would corrupt the group's byte and event evidence, the bug the GTID streaming design exists to avoid.
- **Buffer every event of a group until attribution.** Rejected: unbounded memory on exactly the inputs that need protection; the only legitimately pre-attribution events are GTID-class.
- **Drop the pre-attribution GTID event instead of holding it.** Rejected: a matched group would silently lose its opener — `summary.total_events`/`binlog_bytes` would differ from the same group unfiltered, and the redo planner would lose its staged `recGTID` record. The held slot is ~one event in valid input; exactness is cheap.
- **Allow `--thread-id 0` to mean "unattributed".** Rejected: it cannot match under the streaming decision and silently admits nothing — an explicit usage error beats a flag that can only ever produce an empty report.
- **Attribute each event to the *event's own* thread id rather than the group's.** Rejected: row events carry none, so per-event semantics would mean "QUERY events only" — not the transaction-group selector operators need.

## Consequences

- `analyze --thread-id 1875` reports only groups whose first QUERY carried connection id 1875 — transactions, threads ranking, tables, minutes, patterns, alerts all scope to the same retained set, exactly as GTID selection does today.
- `flashback --thread-id` / `redo --thread-id` emit SQL only for matched groups' retained rows; planner `Observe` still sees the whole stream (schema learning is unaffected — same as GTID).
- `--thread-id` composes with `--where` (AND: group's id in set AND row predicate provably true), object/DML/time/position/GTID selectors.
- A fully-unmatched run keeps today's "no analyzable events" exit 2.
- Memory cost: ≤8 held events per pre-attribution group; decisions are O(1) per event.
- Failure mode on attribution-ordered-after-rows input is a loud exit-1 error, never a silent partial selection.
- New kinds: none. New exit codes: none.
