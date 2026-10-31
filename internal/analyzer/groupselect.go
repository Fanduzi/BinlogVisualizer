// Package analyzer decides the transaction-group selectors (GTID, --thread-id).
// input: in-window events of keyed groups plus the builder's merged per-group identities.
// output: matched groups' events aggregated whole; unmatched and unattributed groups dropped.
// pos: the group-selector seam — a GTID decision is fixed at the group opener, a thread-id
// decision can only land once a QUERY-derived event merges the id, so pre-attribution
// events are held per group (ADR 0027); the drain helpers re-check each finalized group.
// note: if this file changes, update this header and module README.md.
package analyzer

import (
	"fmt"

	"binlogviz/internal/model"
)

// gtidGroup is the GTID selector decision for one transaction group.
type gtidGroup struct {
	gtid     string
	matched  bool
	retained bool // at least one in-window event was aggregated
}

// gtidGroupFor returns the decision for the open group or a group closed by
// the current event, or nil for any other key (the old buffer dropped such
// events too). A Match error counts as not matched here; drain reports it,
// after the unresolved-boundary check, as before.
func (a *Analyzer) gtidGroupFor(key string) *gtidGroup {
	if key == "" {
		return nil
	}
	if group, ok := a.gtidGroups[key]; ok {
		return group
	}
	gtid, ok := a.txnBuilder.groupGTID(key)
	if !ok {
		return nil
	}
	matched, err := a.opts.GTIDSelector.Match(gtid)
	group := &gtidGroup{gtid: gtid, matched: err == nil && matched}
	a.gtidGroups[key] = group
	return group
}

// retainSelectedGroupEvent aggregates one in-window event of a group every
// active group selector keeps, and drops it otherwise. The GTID stage decides
// from the opener; the thread stage may hold a pre-attribution GTID event in
// its group's slot instead of aggregating (ADR 0027), so retained only marks
// when the event was actually aggregated.
func (a *Analyzer) retainSelectedGroupEvent(ev model.NormalizedEvent) error {
	var group *gtidGroup
	if a.opts.HasGTIDSelectors() {
		group = a.gtidGroupFor(ev.TxnKey)
		if group == nil || !group.matched {
			return nil
		}
	}
	kept, err := a.retainThreadSelected(ev)
	if kept && group != nil {
		group.retained = true
	}
	return err
}

// threadIDHeldLimit bounds the GTID-class events a group may hold while its
// thread id is still unseen. Valid input needs one; same-GTID repeats raise
// it a little. Past the cap the group is undecidable, like any other event
// that precedes attribution.
const threadIDHeldLimit = 8

// threadGroup is the --thread-id decision for one transaction group: pending
// until a nonzero merged thread id is seen, then matched or not (ADR 0027).
type threadGroup struct {
	decided bool
	tid     uint32
	matched bool
	held    []model.NormalizedEvent // pre-attribution GTID events
}

// threadGroupFor returns the selector state for one transaction group,
// deciding the first time the builder exposes a nonzero thread id for it. A
// nil return mirrors gtidGroupFor: events of keys the builder no longer owns
// drop the same way.
func (a *Analyzer) threadGroupFor(key string) *threadGroup {
	if key == "" {
		return nil
	}
	g := a.threadGroups[key]
	if g == nil {
		g = &threadGroup{}
		a.threadGroups[key] = g
	}
	if !g.decided {
		if tid, ok := a.txnBuilder.groupThreadID(key); ok && tid != 0 {
			g.decided = true
			g.tid = tid
			_, g.matched = a.threadIDSet[tid]
		}
	}
	return g
}

// retainThreadSelected is the thread-id stage of the group-selector leaf: it
// aggregates one in-window event the selector keeps and reports whether
// anything aggregated. With no thread selector it is a pass-through.
func (a *Analyzer) retainThreadSelected(ev model.NormalizedEvent) (bool, error) {
	if !a.opts.HasThreadIDSelector() {
		return true, a.aggregateRetainedEvent(ev)
	}
	return a.retainThreadEvent(ev)
}

// retainThreadEvent applies the group's thread-id decision to one in-window
// event. Only GTID events may arrive before attribution in a valid stream, so
// a pending group holds them; any other event while still unattributed marks
// the group undecidable — matched=false — and the drain guard fails if the
// finalized transaction disagrees.
func (a *Analyzer) retainThreadEvent(ev model.NormalizedEvent) (bool, error) {
	g := a.threadGroupFor(ev.TxnKey)
	if g == nil {
		return false, nil
	}
	if g.decided {
		if !g.matched {
			return false, nil
		}
		if err := a.flushThreadHeld(g); err != nil {
			return true, err
		}
		return true, a.aggregateRetainedEvent(ev)
	}
	if ev.EventType == "GTID" && len(g.held) < threadIDHeldLimit {
		g.held = append(g.held, ev)
		return false, nil
	}
	g.decided = true // event arrived with no attribution: undecidable
	g.held = nil
	return false, nil
}

// threadSelectedDiverted resolves the selector for an event that bypasses the
// retain path — the object-filter-dropped DDL the flashback planner still
// inspects (ADR 0015). The event already passed through the builder, so the
// group's attribution is final: it decides now if it had not already.
func (a *Analyzer) threadSelectedDiverted(key string) (bool, error) {
	g := a.threadGroupFor(key)
	if g == nil {
		return false, nil
	}
	if !g.decided {
		g.decided = true // a QUERY event that merged no thread id
	}
	if !g.matched {
		g.held = nil
		return false, nil
	}
	return true, a.flushThreadHeld(g)
}

// flushThreadHeld replays a matched group's pre-attribution events through
// the normal aggregate path exactly once.
func (a *Analyzer) flushThreadHeld(g *threadGroup) error {
	held := g.held
	g.held = nil
	for _, ev := range held {
		if err := a.aggregateRetainedEvent(ev); err != nil {
			return err
		}
	}
	return nil
}

// threadKeepsFinalized applies the selector to a completed transaction's
// merged thread id. A group decided-undecidable at stream time whose merge
// later gains an id — rows preceding every QUERY, which no valid producer
// emits — disagrees with the preselection and fails rather than mis-selecting.
func (a *Analyzer) threadKeepsFinalized(txn model.Transaction, g *threadGroup) (bool, error) {
	if g != nil && g.tid != txn.ThreadID {
		return false, fmt.Errorf("cannot apply --thread-id: transaction %s was filtered as thread_id %d but closed as thread_id %d", txn.TxnKey, g.tid, txn.ThreadID)
	}
	if txn.ThreadID == 0 || a.threadIDSet == nil {
		return false, nil
	}
	_, keep := a.threadIDSet[txn.ThreadID]
	return keep, nil
}

// groupThreadID returns the merged thread id of the open group or of a
// completed group not yet drained, by key. Unlike a GTID it can arrive late —
// only QUERY-derived events carry one — so a zero return means "no attribution
// seen yet", which is also the finalized unattributed value.
func (b *TransactionBuilder) groupThreadID(key string) (uint32, bool) {
	if b.current != nil && b.current.txnKey == key {
		return b.current.threadID, true
	}
	for i := len(b.completed) - 1; i >= 0; i-- {
		if b.completed[i].TxnKey == key {
			return b.completed[i].ThreadID, true
		}
	}
	return 0, false
}
