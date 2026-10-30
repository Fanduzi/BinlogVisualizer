// Package analyzer: SQL-planner wiring shared by undo (flashback) and redo.
// input: normalized events plus the txnBuilder's open-group position.
// output: Observe calls on every scanned event, Note on retained events, and NoteSkipped on row events a filter dropped — the seam both planners consume (ADR 0013, ADR 0022).
// pos: the planner fan-out lives here so consume and reset stay small; flashback and redo are mutually exclusive via Options.
// note: if this file changes, update this header and module README.md.
package analyzer

import (
	"binlogviz/internal/flashback"
	"binlogviz/internal/model"
	"binlogviz/internal/redo"
)

// observePlanners feeds the pre-selection seam of every active SQL planner:
// each sees all scanned events (schema changes must be learned even for
// filtered-out tables).
func (a *Analyzer) observePlanners(ev model.NormalizedEvent) {
	if a.flash == nil && a.redo == nil {
		return
	}
	gtid, _, _ := a.txnBuilder.touchLocation()
	key := a.txnBuilder.CurrentTxnKey()
	if a.flash != nil {
		a.flash.Observe(ev, key, gtid)
	}
	if a.redo != nil {
		a.redo.Observe(ev, key, gtid)
	}
}

// notePlannerSkipped records a row event a filter dropped while its
// transaction group was open. Both planners use it to warn that a group was
// only partly kept.
func (a *Analyzer) notePlannerSkipped(ev model.NormalizedEvent) {
	if (a.flash == nil && a.redo == nil) || ev.EventType != "ROWS" {
		return
	}
	gtid, _, _ := a.txnBuilder.touchLocation()
	key := a.txnBuilder.CurrentTxnKey()
	if a.flash != nil {
		a.flash.NoteSkipped(key, gtid)
	}
	if a.redo != nil {
		a.redo.NoteSkipped(key, gtid)
	}
}

// noteRetained records one event the selection kept for every active
// planner.
func (a *Analyzer) noteRetained(ev model.NormalizedEvent) {
	if a.flash != nil {
		a.flash.Note(ev)
	}
	if a.redo != nil {
		a.redo.Note(ev)
	}
}

// initPlanners closes any previous planners and constructs the ones Options
// asks for. Flashback and Redo are mutually exclusive.
func (a *Analyzer) initPlanners() {
	if a.flash != nil {
		a.flash.Close()
		a.flash = nil
	}
	if a.redo != nil {
		a.redo.Close()
		a.redo = nil
	}
	if a.opts.Flashback {
		a.flash = flashback.New(flashback.Options{
			SchemaSQL:     a.opts.SchemaSQL,
			SchemaFileDB:  a.opts.SchemaFileDB,
			Conflict:      a.opts.FlashbackConflict,
			Plan:          a.opts.FlashbackPlan,
			ConflictLimit: a.opts.FlashbackConflictLimit,
			Selection:     a.flashSelection(),
			Where:         a.opts.RowWhere,
		})
	}
	if a.opts.Redo {
		a.redo = redo.New(redo.Options{
			SchemaSQL:    a.opts.SchemaSQL,
			SchemaFileDB: a.opts.SchemaFileDB,
			Where:        a.opts.RowWhere,
		})
	}
}

// Planner is the flashback planner for this run, or nil when flashback is off.
func (a *Analyzer) Planner() *flashback.Planner {
	if a == nil {
		return nil
	}
	return a.flash
}

// RedoPlanner is the redo planner for this run, or nil when redo is off.
func (a *Analyzer) RedoPlanner() *redo.Planner {
	if a == nil {
		return nil
	}
	return a.redo
}
