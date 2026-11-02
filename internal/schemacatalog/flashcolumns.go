// Package schemacatalog resolves a table definition for undo capture.
// pos: the definition half of the cross-package lookups: names, facts, and the
// primary key it declares (ADR 0030).
// input: a binlog schema.table name and the loaded schema file.
// output: resolved columns, the key column indexes, or a refusal.
// note: if this file changes, update this header and README.md.
package schemacatalog

import (
	"strings"

	"binlogviz/internal/model"
	"binlogviz/internal/mysqlexpr"
)

// FlashColumns is the resolver hook the undo capture uses when the binlog has
// no column names (ADR 0030): the definition's columns in table order, and the
// primary-key column indexes it declares (empty when it declares no key, which
// sends the undo to its all-columns + LIMIT 1 path). It reports false when no
// definition covers the table, which leaves the existing row_metadata refusal
// in place.
func (g *Catalog) FlashColumns(schema, table string) ([]mysqlexpr.SchemaCol, []int, bool) {
	if g == nil {
		return nil, nil, false
	}
	res := g.Resolve(schema, table)
	switch res.Kind {
	case Exact, Fold:
		if len(res.Cols) == 0 {
			return nil, nil, false
		}
		return res.Cols, PrimaryKeyIndexes(res.Cols), true
	default:
		return nil, nil, false
	}
}

// PrimaryKeyIndexes lists the columns a definition declares as its primary key,
// in table order. An empty result means it declares no key (ADR 0030).
func PrimaryKeyIndexes(cols []mysqlexpr.SchemaCol) []int {
	var pk []int
	for i := range cols {
		if cols[i].Primary {
			pk = append(pk, i)
		}
	}
	return pk
}

// dropDefinitionKey forgets the primary key a definition declared, because a
// later ALTER changed it and the catalog does not model key changes: the
// columns stay usable, and undo loses the key, so it matches every column
// instead of addressing a row by a key the definition no longer owns.
func (g *Catalog) dropDefinitionKey(key string) {
	cols, ok := g.defs[key]
	if !ok {
		return
	}
	next := mysqlexpr.CloneSchemaCols(cols)
	dropped := false
	for i := range next {
		if next[i].Primary {
			next[i].Primary = false
			dropped = true
		}
	}
	if !dropped {
		return
	}
	g.setDef(key, next, g.fromFile[key])
	schema, table, _ := strings.Cut(key, "\x00")
	label := table
	if schema != "" {
		label = schema + "." + table
	}
	g.keyNotes = append(g.keyNotes, model.NewFlashWarning(model.WarnSchemaFileKeyUnmodelled, "warning.flashbackSchemaKey", map[string]any{"Table": label}))
}
