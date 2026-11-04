// Package binlog formats exact SQL literals from an already-decoded rows event.
// pos: schema-definition resolver hook for undo capture (ADR 0030).
// input: a table name and, when the binlog has no column names, a definition
// supplied by --schema-file.
// output: the ordered columns, their literal-rendering facts, and the
// primary-key column indexes, or "not resolvable".
// note: if this file changes, update this header and README.md.
package binlog

import (
	"github.com/go-mysql-org/go-mysql/replication"

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

// resolveFlashColumns asks the resolver for the table's definition. The caller
// compares the column count: a definition of the wrong width is a schema
// mismatch, which is a different refusal from "no definition covers this".
func resolveFlashColumns(r FlashSchemaResolver, schema, table string) ([]mysqlexpr.SchemaCol, []int, bool) {
	if r == nil {
		return nil, nil, false
	}
	cols, pk, ok := r.FlashColumns(schema, table)
	if !ok || len(cols) == 0 {
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

// definitionKey is one table's key metadata from --schema-file. ok=false records
// that the definition does not answer there, so the lookup runs once per table
// and never per row (ADR 0032).
type definitionKey struct {
	meta pkMeta
	ok   bool
}

// flashKeyMeta builds the key metadata RowKeys needs from a resolved definition.
func flashKeyMeta(cols []mysqlexpr.SchemaCol, metas []model.FlashCol, pk []int) pkMeta {
	meta := pkMeta{
		indexes: make([]int, 0, len(pk)),
		names:   make([]string, 0, len(pk)),
		sign:    make([]*bool, 0, len(pk)),
		types:   make([]byte, 0, len(pk)),
	}
	for _, idx := range pk {
		if idx < 0 || idx >= len(cols) {
			return pkMeta{}
		}
		meta.indexes = append(meta.indexes, idx)
		meta.names = append(meta.names, cols[idx].Name)
		var typ byte
		var sign *bool
		if idx < len(metas) {
			typ = metas[idx].Typ
			if metas[idx].HasSign {
				unsigned := metas[idx].Unsigned
				sign = &unsigned
			}
		}
		meta.types = append(meta.types, typ)
		meta.sign = append(meta.sign, sign)
	}
	return meta
}

// rowKeys returns the UPDATE/DELETE key identities for one rows event and
// whether a definition supplied them instead of the table map. The binlog's own
// metadata always wins (ADR 0032).
func rowKeys(resolver FlashSchemaResolver, cache map[string]definitionKey, e *replication.RowsEvent, kind string, tableNames map[uint64]cachedTableName) ([]string, bool) {
	if e == nil {
		return nil, false
	}
	if meta := pkMetaForRows(e, tableNames); len(meta.indexes) > 0 {
		return primaryKeyValues(e, kind, meta), false
	}
	meta, ok := definitionKeyFor(resolver, cache, e, tableNames)
	if !ok {
		return nil, false
	}
	return primaryKeyValues(e, kind, meta), true
}

// definitionKeyFor resolves the key a schema definition declares for this
// event's table, or reports that it cannot, once per table.
func definitionKeyFor(resolver FlashSchemaResolver, cache map[string]definitionKey, e *replication.RowsEvent, tableNames map[uint64]cachedTableName) (pkMeta, bool) {
	if resolver == nil || e == nil || e.Table == nil {
		return pkMeta{}, false
	}
	schema, table := string(e.Table.Schema), string(e.Table.Table)
	if cached, ok := tableNames[e.TableID]; ok {
		if schema == "" {
			schema = cached.schema
		}
		if table == "" {
			table = cached.table
		}
	}
	if schema == "" || table == "" {
		return pkMeta{}, false
	}
	key := schema + "\x00" + table
	if entry, seen := cache[key]; seen {
		return entry.meta, entry.ok
	}
	entry := definitionKey{}
	width := int(e.ColumnCount)
	if width == 0 && len(e.Rows) > 0 {
		width = len(e.Rows[0])
	}
	if cols, pk, ok := resolver.FlashColumns(schema, table); ok && len(pk) > 0 && width > 0 && len(cols) == width {
		metas := flashColumnMeta(e.Table, width)
		mergeFlashColumns(metas, cols)
		if meta := flashKeyMeta(cols, metas, pk); len(meta.indexes) > 0 {
			entry = definitionKey{meta: meta, ok: true}
		}
	}
	if cache != nil {
		cache[key] = entry
	}
	return entry.meta, entry.ok
}
