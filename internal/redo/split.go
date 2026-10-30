// Package redo rotates the emitted script into bounded part files (ADR 0025).
// input: the split spec (max statements per file) and the staged record stream.
// output: part-NNNNNN.sql files inside the staging directory, each a complete
// script — session preamble, USE context, BEGIN for groups that continue —
// plus a MANIFEST in apply order.
// pos: the file-boundary half of the emit layer; emit.go owns the per-record
// rotation decision.
// note: if this file changes, update this header and module README.md.
package redo

import (
	"bufio"
	"fmt"
	"os"

	"binlogviz/internal/clierr"
	"binlogviz/internal/i18n"
	"binlogviz/internal/splitout"
	"binlogviz/internal/sqlemit"
)

// fileSplit rotates the output stream into part-NNNNNN.sql files at
// statement-count boundaries (ADR 0025). count is the statements emitted
// into the current file.
type fileSplit struct {
	sink  *splitout.Sink
	max   int64
	idx   int
	count int64
	f     *os.File
	w     *bufio.Writer
}

// open starts the next part file: the session preamble, the last emitted
// USE context, and — for groups spanning the boundary — a continuation
// comment plus BEGIN so the file applies on its own. A group whose BEGIN is
// still deferred has emitted nothing yet: it is not continued here, its
// own records open it in this file.
func (sp *fileSplit) open(st *emitState) error {
	sp.idx++
	f, err := sp.sink.Create(fmt.Sprintf("part-%06d.sql", sp.idx))
	if err != nil {
		return err
	}
	sp.f, sp.w = f, bufio.NewWriter(f)
	if err := writeScriptHead(sp.w); err != nil {
		return err
	}
	if st.useSet {
		if _, err := fmt.Fprintf(sp.w, "USE %s;\n", sqlemit.QuoteIdent(st.useCtx)); err != nil {
			return err
		}
	}
	for _, key := range st.order {
		if !st.open[key] || st.pending[key] {
			continue
		}
		gtid := st.gtidByKey[key]
		if gtid == "" {
			gtid = "GTID unavailable"
		}
		if _, err := fmt.Fprintf(sp.w, "-- gtid: %s (continued in part %06d)\n", gtid, sp.idx); err != nil {
			return err
		}
		// A group that never logged BEGIN emits its rows bare, here too.
		if st.begun[key] {
			if _, err := fmt.Fprintln(sp.w, "BEGIN;"); err != nil {
				return err
			}
		}
	}
	return nil
}

// seal flushes, fsyncs and closes the current part file.
func (sp *fileSplit) seal() error {
	if sp.w == nil {
		return nil
	}
	if err := sp.w.Flush(); err != nil {
		_ = sp.f.Close()
		return clierr.Wrap(clierr.KindOutput, err)
	}
	err := sp.sink.Seal(sp.f)
	sp.f, sp.w = nil, nil
	return err
}

// PrepareSplit replays the emit bookkeeping without rendering, so the
// spanning-transaction warnings are complete before the first file opens.
// The pass mirrors emitAll step for step — same deferred-BEGIN, implicit
// close-out, and rotation order — so a group marked here is exactly a group
// a file boundary will cut (ADR 0025).
func (a *Planner) PrepareSplit(spec splitout.Spec) error {
	if spec.PerTable {
		return clierr.Errorf(clierr.KindUsage, "%s", i18n.T("error.redoFilePerTable"))
	}
	if _, err := a.Prepare(); err != nil {
		return err
	}
	if a.splitReady {
		return nil
	}
	a.splitReady = true
	st := &emitState{open: map[string]bool{}, closed: map[string]bool{}, xa: map[string]bool{}, gtidByKey: map[string]string{}, pending: map[string]bool{}}
	var count int64
	spanned := map[string]bool{}
	return a.spill.forward(func(rec *stagedRecord) error {
		a.settleBinds(rec)
		if rec.kind == recGTID || rec.kind == recComment {
			st.track(rec)
			return nil
		}
		st.closeForeign(rec.key)
		count += int64(len(st.pendFlush))
		st.pendFlush = st.pendFlush[:0]
		if rec.kind == recBegin && rec.key != "" {
			// Deferred BEGIN: nothing emitted, nothing counted — the count
			// lands where flushPending emits it.
			st.track(rec)
			st.pending[rec.key] = true
			return nil
		}
		extra := int64(0)
		if st.pending[rec.key] {
			extra = 1
		}
		if st.splitNeedsRotate(spec.MaxStatements, count+extra, rec) {
			for _, key := range st.order {
				if st.open[key] && !st.pending[key] && !spanned[key] {
					spanned[key] = true
					a.splitSpan = append(a.splitSpan, st.gtidByKey[key])
				}
			}
			count = 0
		}
		if st.pending[rec.key] {
			delete(st.pending, rec.key)
			count++
		}
		st.track(rec)
		count += recStatementCount(rec)
		return nil
	})
}

// WriteSplit streams the script into the staging sink as part-NNNNNN.sql
// files of at most spec.MaxStatements each (ADR 0025). Refusals decided by
// Prepare return before the first file; a rendering refusal aborts the set.
func (a *Planner) WriteSplit(sink *splitout.Sink, spec splitout.Spec) (int64, error) {
	if spec.PerTable {
		return 0, clierr.Errorf(clierr.KindUsage, "%s", i18n.T("error.redoFilePerTable"))
	}
	rows, err := a.Prepare()
	if err != nil {
		return 0, err
	}
	if a.stmtCount == 0 {
		return 0, nil
	}
	if err := a.PrepareSplit(spec); err != nil {
		return 0, err
	}
	st := &emitState{
		open:      map[string]bool{},
		closed:    map[string]bool{},
		xa:        map[string]bool{},
		gtidByKey: map[string]string{},
		begun:     map[string]bool{},
		pending:   map[string]bool{},
		split:     &fileSplit{sink: sink, max: spec.MaxStatements},
	}
	if err := st.split.open(st); err != nil {
		return 0, err
	}
	if err := a.emitAll(st); err != nil {
		return 0, err
	}
	if err := st.drainPend(); err != nil {
		return 0, err
	}
	if err := st.flushComments(); err != nil {
		return 0, err
	}
	if err := st.emitTrailers(); err != nil {
		return 0, err
	}
	if err := st.split.seal(); err != nil {
		return 0, err
	}
	return rows, nil
}
