// Package binlog formats exact SQL literals from an already-decoded rows event.
// pos: schema-definition resolver hook for undo capture (ADR 0030).
// input: a table name and, when the binlog has no column names, a definition
// supplied by --schema-file.
// output: the ordered columns, their literal-rendering facts, and the
// primary-key column indexes, or "not resolvable".
// note: if this file changes, update this header and README.md.
package binlog

import (
	"binlogviz/internal/model"
	"binlogviz/internal/mysqlexpr"
)

// FlashSchemaResolver supplies a table definition when the binlog carries no
// column names: binlog_row_metadata=MINIMAL, or MySQL 5.7, which has none
// (ADR 0030). cols are in table order; pk holds the primary-key column
// indexes, or nil when the definition declares no key.
//
// The capture asks before rendering a row's literals, because under MINIMAL the
// signedness, charset, and ENUM/SET members the literals need are missing from
// the table map as well as the names. A definition that contradicts a
// comparable binlog fact is still refused later: the flashback planner
// validates every row against the same definition before any SQL is emitted.
type FlashSchemaResolver interface {
	FlashColumns(schema, table string) (cols []mysqlexpr.SchemaCol, pk []int, ok bool)
}

// FlashSchemaParser is implemented by parsers that accept a resolver.
type FlashSchemaParser interface {
	SetFlashSchemaResolver(r FlashSchemaResolver)
}

// resolveFlashColumns asks the resolver for a definition that describes width
// columns. A definition of the wrong width cannot name this row, so it counts
// as unresolved rather than being stretched.
func resolveFlashColumns(r FlashSchemaResolver, schema, table string, width int) ([]mysqlexpr.SchemaCol, []int, bool) {
	if r == nil || width <= 0 {
		return nil, nil, false
	}
	cols, pk, ok := r.FlashColumns(schema, table)
	if !ok || len(cols) != width {
		return nil, nil, false
	}
	return cols, pk, true
}

// flashLabelsFromCols names columns from a resolved definition.
func flashLabelsFromCols(cols []mysqlexpr.SchemaCol) []string {
	labels := make([]string, len(cols))
	for i := range cols {
		labels[i] = cols[i].Name
	}
	return labels
}

// mergeFlashColumns fills the facts a MINIMAL table map does not carry —
// signedness, charset, ENUM/SET members, binary affinity — the same merge redo
// performs (ADR 0022), so EmitLiteral can render from the result.
func mergeFlashColumns(metas []model.FlashCol, cols []mysqlexpr.SchemaCol) {
	for i := range metas {
		if i < len(cols) {
			mysqlexpr.FillFlashCol(cols[i], &metas[i])
		}
	}
}

// flashLiteralsFromCols renders literals from merged column facts instead of
// the table-map maps, the path redo already uses (ADR 0022, ADR 0030).
func flashLiteralsFromCols(cols []model.FlashCol, labels []string, values []any, cells []jsonCell, schema, tableName string) ([]string, *model.FlashRow) {
	lits := make([]string, len(labels))
	for i := range labels {
		var value any
		if i < len(values) {
			value = values[i]
		}
		var doc []byte
		if i < len(cells) && cells[i].ok {
			doc = cells[i].doc
		}
		var meta model.FlashCol
		if i < len(cols) {
			meta = cols[i]
		}
		lit, typeProblem := EmitLiteral(meta, value, doc)
		if typeProblem != "" {
			return nil, &model.FlashRow{
				Schema:        schema,
				Table:         tableName,
				Columns:       labels,
				ProblemKind:   model.FlashProblemType,
				ProblemColumn: labels[i],
				ProblemType:   typeProblem,
			}
		}
		lits[i] = lit
	}
	return lits, nil
}
