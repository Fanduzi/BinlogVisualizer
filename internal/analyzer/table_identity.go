// Package analyzer keeps internal table identities allocation-light until final model projection.
// input: schema and table names from normalized row and DDL events.
// output: reusable package-local tableIdentity keys and string projection helpers.
// pos: internal analyzer helper for hot-path map keys in minute, table, and transaction aggregation.
// note: if this file changes, update this header and module README.md.
package analyzer

type tableIdentity struct {
	schema string
	table  string
}

func newTableIdentity(schema, table string) tableIdentity {
	return tableIdentity{schema: schema, table: table}
}

func (k tableIdentity) String() string {
	if k.schema == "" {
		return k.table
	}
	if k.table == "" {
		return k.schema
	}
	return k.schema + "." + k.table
}
