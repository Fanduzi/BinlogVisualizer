// Package flashback: the --where row predicate applied at staging.
// input: the compiled predicate and the row event's checked FlashRows.
// output: the rows whose matched image the predicate proves — before for
// DELETE/UPDATE, after for INSERT (ADR 0024) — or a where_column refusal when
// the predicate names a column the row does not have.
// pos: inside noteFlashback, after every check that already ran on the raw
// event and before appendFlashRows; dropped rows are never staged, so no
// statement, conflict key, or guard expectation exists for them.
// note: if this file changes, update this header and module README.md.
package flashback

import (
	"fmt"
	"strings"

	"binlogviz/internal/i18n"
	"binlogviz/internal/model"
	"binlogviz/internal/mysqlexpr"
)

// selectFlashRows drops the rows of one checked row event that fail the
// --where predicate. The checked-image checks ran before this; the predicate
// only decides which rows are staged.
func (a *Planner) selectFlashRows(rows []model.FlashRow) ([]model.FlashRow, error) {
	w := a.opts.Where
	if w == nil || len(rows) == 0 {
		return rows, nil
	}
	kept := make([]model.FlashRow, 0, len(rows))
	for _, row := range rows {
		if missing := w.MissingRef(row.Columns); missing != "" {
			return nil, whereColumnError(a.refusalTable(row.Schema, row.Table), missing)
		}
		image := row.Before
		if row.Op == "INSERT" {
			image = row.After
		}
		env := make(map[string]mysqlexpr.GenVal, len(w.Refs()))
		for i, name := range row.Columns {
			lname := strings.ToLower(name)
			if i >= len(image) || !w.References(lname) {
				continue
			}
			var col model.FlashCol
			if i < len(row.Cols) {
				col = row.Cols[i]
			}
			env[lname] = mysqlexpr.LiteralCell(image[i], col)
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
