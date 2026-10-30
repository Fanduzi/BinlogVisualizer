// Package redo: the --where row predicate applied at staging.
// input: the compiled predicate and one row event's checked RedoRows after
// their table binding resolved (names and merged type facts).
// output: the rows whose matched image the predicate proves — before for
// DELETE/UPDATE, after for INSERT (ADR 0024) — or a where_column refusal when
// the predicate names a column the row does not have.
// pos: inside noteRows, after every check that already ran on the raw event
// and before appendRows; dropped rows are never staged, so no statement is
// emitted for them. Only the referenced cells of the matched image are
// rendered — the same EmitLiteral path the emitter later replays in full.
// note: if this file changes, update this header and module README.md.
package redo

import (
	"fmt"
	"strings"

	"binlogviz/internal/binlog"
	"binlogviz/internal/i18n"
	"binlogviz/internal/model"
	"binlogviz/internal/mysqlexpr"
)

// selectRedoRows drops the rows of one checked row event that fail the
// --where predicate. The binding came from the same resolve emitRows later
// re-applies, so a referenced cell renders exactly once here and once there.
func (a *Planner) selectRedoRows(rows []model.RedoRow, bind redoBind, flavor string) ([]model.RedoRow, error) {
	w := a.opts.Where
	if w == nil || len(rows) == 0 {
		return rows, nil
	}
	kept := make([]model.RedoRow, 0, len(rows))
	for _, row := range rows {
		columns, metas, _, err := a.rowFacts(row, bind, flavor)
		if err != nil {
			return nil, err
		}
		if missing := w.MissingRef(columns); missing != "" {
			return nil, whereColumnError(a.refusalTable(row.Schema, row.Table), missing)
		}
		image := row.Before
		docs := row.BeforeDocs
		if row.Op == "INSERT" {
			image = row.After
			docs = row.AfterDocs
		}
		index := make(map[string]int, len(columns))
		for i, name := range columns {
			index[strings.ToLower(name)] = i
		}
		env := make(map[string]mysqlexpr.GenVal, len(w.Refs()))
		for _, ref := range w.Refs() {
			i := index[ref]
			if i >= len(image) || i >= len(metas) {
				return nil, whereColumnError(a.refusalTable(row.Schema, row.Table), ref)
			}
			var doc []byte
			if i < len(docs) {
				doc = docs[i]
			}
			lit, problem := binlog.EmitLiteral(metas[i], image[i], doc)
			if problem != "" {
				return nil, literalProblemError(row, i, problem)
			}
			env[ref] = mysqlexpr.LiteralCell(lit, metas[i])
		}
		if w.Match(env) {
			kept = append(kept, row)
		}
	}
	return kept, nil
}

// whereColumnError refuses a predicate that names a column the row does not
// have — the MySQL ERROR 1054 shape (ADR 0024).
func whereColumnError(table, column string) error {
	return refuse("where_column", table, fmt.Errorf("%s", i18n.Tf("error.whereColumn", map[string]any{
		"Table":  table,
		"Column": column,
	})))
}
