// Package flashback partitions the undo stream into per-table or
// statement-bounded files, each a complete sealed guard script (ADR 0025).
// input: the split spec and the staged row groups.
// output: one file per unit inside the staging directory, written through
// the same writeFlashbackScript seam the monolith uses.
// pos: the enumeration and unit-filter half of the writer; render.go keeps
// the per-script rendering untouched.
// note: if this file changes, update this header and module README.md.
package flashback

import (
	"errors"
	"fmt"
	"io"
	"strings"

	"binlogviz/internal/guard"
	"binlogviz/internal/splitout"
)

// genNote is a generated-column review note attributed to the table it
// belongs to, so a split file lists only its own notes.
type genNote struct {
	schema, table string
	text          string
}

func genNoteTexts(notes []genNote) []string {
	out := make([]string, len(notes))
	for i := range notes {
		out[i] = notes[i].text
	}
	return out
}

// tableKey identifies one (schema, table) for per-table partitioning.
type tableKey struct{ schema, table string }

// splitUnit is one output file: the undo rows of (schema, table) — or every
// row when table is "" — whose index in that stream falls in [from, to).
// to < 0 means unbounded. cols is this file's guard header set.
type splitUnit struct {
	name   string
	schema string
	table  string
	from   int64
	to     int64
	cols   []guard.Col
	notes  []string
}

// PrepareSplit enumerates the output units on one backward pass: per-table
// counts and guard columns, chunk membership, and the transactions whose
// undo lands in more than one file (the file-split warnings). It runs after
// Prepare, which settled every bind the enumeration reads (ADR 0025).
func (a *Planner) PrepareSplit(spec splitout.Spec) error {
	if _, err := a.Prepare(); err != nil {
		return err
	}
	if a.splitReady {
		return nil
	}
	type tableAcc struct {
		count  int64
		cols   []guard.Col
		colSet map[string]struct{}
	}
	tables := map[tableKey]*tableAcc{}
	var order []tableKey
	var global int64
	var chunkTables []map[tableKey]struct{}
	type spanKey struct {
		group int64
		table tableKey
	}
	lastUnit := map[spanKey]int64{}
	spanned := map[int64]bool{}
	err := a.eachFlashGroup(true, func(g *flashGroup) error {
		for j := len(g.rows) - 1; j >= 0; j-- {
			row := g.rows[j]
			key := tableKey{row.Schema, row.Table}
			acc := tables[key]
			if acc == nil {
				acc = &tableAcc{colSet: map[string]struct{}{}}
				tables[key] = acc
				order = append(order, key)
			}
			if j < len(g.binds) {
				for _, col := range g.binds[j].cols {
					if !col.Generated {
						continue
					}
					ck := strings.ToLower(row.Schema) + "\x00" + strings.ToLower(row.Table) + "\x00" + strings.ToLower(col.Name)
					if _, ok := acc.colSet[ck]; !ok {
						acc.colSet[ck] = struct{}{}
						acc.cols = append(acc.cols, guard.Col{Schema: row.Schema, Table: row.Table, Column: col.Name})
					}
				}
			}
			var unitIdx int64
			var sk spanKey
			if spec.PerTable {
				unitIdx = 0
				sk = spanKey{g.id, key}
				if spec.MaxStatements > 0 {
					unitIdx = acc.count / spec.MaxStatements
				}
			} else {
				unitIdx = global / spec.MaxStatements
				sk = spanKey{g.id, tableKey{}}
				for len(chunkTables) <= int(unitIdx) {
					chunkTables = append(chunkTables, map[tableKey]struct{}{})
				}
				chunkTables[unitIdx][key] = struct{}{}
			}
			if spec.MaxStatements > 0 {
				if last, seen := lastUnit[sk]; seen && last != unitIdx {
					if !spanned[g.id] {
						spanned[g.id] = true
						a.splitSpan = append(a.splitSpan, g.gtid)
					}
				} else if !seen {
					lastUnit[sk] = unitIdx
				}
			}
			acc.count++
			global++
		}
		return nil
	})
	if err != nil {
		return err
	}
	used := map[string]bool{}
	nameFor := func(base string) string {
		name := base
		for i := 2; used[strings.ToLower(name)]; i++ {
			name = fmt.Sprintf("%s-%d", base, i)
		}
		used[strings.ToLower(name)] = true
		return name
	}
	var units []splitUnit
	if spec.PerTable {
		for _, key := range order {
			acc := tables[key]
			base := splitout.TableFileBase(key.schema, key.table)
			notes := a.splitNotes(func(k tableKey) bool { return k == key })
			if spec.MaxStatements <= 0 {
				units = append(units, splitUnit{name: nameFor(base + ".sql"), schema: key.schema, table: key.table, to: -1, cols: acc.cols, notes: notes})
				continue
			}
			chunks := (acc.count + spec.MaxStatements - 1) / spec.MaxStatements
			for c := int64(0); c < chunks; c++ {
				units = append(units, splitUnit{
					name:   nameFor(fmt.Sprintf("%s-%06d.sql", base, c+1)),
					schema: key.schema, table: key.table,
					from: c * spec.MaxStatements, to: (c + 1) * spec.MaxStatements,
					cols: acc.cols, notes: notes,
				})
			}
		}
	} else {
		for c, set := range chunkTables {
			var cols []guard.Col
			seen := map[string]struct{}{}
			for _, k := range order {
				if _, ok := set[k]; !ok {
					continue
				}
				for _, col := range tables[k].cols {
					ck := strings.ToLower(col.Schema) + "\x00" + strings.ToLower(col.Table) + "\x00" + strings.ToLower(col.Column)
					if _, dup := seen[ck]; !dup {
						seen[ck] = struct{}{}
						cols = append(cols, col)
					}
				}
			}
			units = append(units, splitUnit{
				name: nameFor(fmt.Sprintf("part-%06d.sql", c+1)),
				from: int64(c) * spec.MaxStatements, to: (int64(c) + 1) * spec.MaxStatements,
				cols:  cols,
				notes: a.splitNotes(func(k tableKey) bool { _, ok := set[k]; return ok }),
			})
		}
	}
	for i := range units {
		label := fmt.Sprintf("-- binlogviz split file: %s (%d of %d)", units[i].name, i+1, len(units))
		if units[i].table != "" {
			label = fmt.Sprintf("-- binlogviz split file: %s (%d of %d) — undo for %s", units[i].name, i+1, len(units), flashTable(units[i].schema, units[i].table))
		}
		units[i].notes = append([]string{label + "\n"}, units[i].notes...)
	}
	a.splitUnits = units
	a.splitReady = true
	return nil
}

// splitNotes returns the generated-column review notes for the tables in
// match — a split file names only the notes its own statements can touch.
func (a *Planner) splitNotes(match func(tableKey) bool) []string {
	var out []string
	for _, n := range a.flashGenNotes {
		if match(tableKey{n.schema, n.table}) {
			out = append(out, n.text)
		}
	}
	return out
}

// WriteSplit writes one sealed script per enumerated unit into the staging
// sink (ADR 0025). Every file runs the full writeFlashbackScript path —
// digest init, procedure, one XA, expectations, trailer — so each verifies
// and applies exactly like the monolithic script.
func (a *Planner) WriteSplit(sink *splitout.Sink, spec splitout.Spec) (int64, error) {
	rows, err := a.Prepare()
	if err != nil || rows == 0 {
		return 0, err
	}
	if err := a.PrepareSplit(spec); err != nil {
		return 0, err
	}
	for _, unit := range a.splitUnits {
		u := unit
		err := sink.WriteFile(u.name, func(w io.Writer) error {
			return writeFlashbackScript(w, func(fn func(*flashGroup) error) error {
				var idx int64
				return a.eachFlashGroup(true, func(g *flashGroup) error {
					fg, ok := filterSplitGroup(g, u, &idx)
					if !ok {
						return nil
					}
					return fn(&fg)
				})
			}, u.notes, u.cols, a.postImageGuard())
		})
		if err != nil {
			var rowErr *generatedRowRefusal
			if errors.As(err, &rowErr) {
				return 0, rowErr.named(a.refusalTable(rowErr.schema, rowErr.table))
			}
			return 0, err
		}
	}
	return int64(rows), nil
}

// filterSplitGroup projects g onto the unit's row range: rows matching the
// unit's table — or every row when table is "" — whose running index in the
// unit's backward stream falls in [from, to). ok is false when no row of g
// belongs to the file.
func filterSplitGroup(g *flashGroup, u splitUnit, idx *int64) (flashGroup, bool) {
	out := flashGroup{key: g.key, gtid: g.gtid, path: g.path, pos: g.pos, id: g.id, chunk: g.chunk}
	for j, row := range g.rows {
		if u.table != "" && (row.Schema != u.schema || row.Table != u.table) {
			continue
		}
		i := *idx
		*idx++
		if i < u.from || (u.to >= 0 && i >= u.to) {
			continue
		}
		out.rows = append(out.rows, row)
		var bind flashBind
		if j < len(g.binds) {
			bind = g.binds[j]
		}
		out.binds = append(out.binds, bind)
		var seq int64
		if j < len(g.seqs) {
			seq = g.seqs[j]
		}
		out.seqs = append(out.seqs, seq)
	}
	return out, len(out.rows) > 0
}
