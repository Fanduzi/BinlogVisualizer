// Package mysqlexpr: the check-then-merge pair redo uses under
// binlog_row_metadata=MINIMAL — a schema definition supplies the names and
// row-metadata facts the table map lacks, but only after every comparable
// fact is validated (ADR 0022).
// input: one parsed schema column and the binlog's per-column descriptor.
// output: a diff string when the facts contradict, or the filled descriptor.
// pos: MINIMAL-merge helpers beside schema.go's ValidateSchemaRow; split out so schema.go stays inside the line ceiling.
// note: if this file changes, update this header and module README.md.
package mysqlexpr

import "binlogviz/internal/model"

// CheckSchemaCol is the per-column half of ValidateSchemaRow: the comparable
// facts a schema definition must not contradict (base type, signedness,
// charset, member list, precision/scale, FSP). Facts absent on either side
// are skipped. Used by redo under binlog_row_metadata=MINIMAL, where names
// come from the definition and the check runs before they are merged
// (ADR 0022).
func CheckSchemaCol(col SchemaCol, meta model.FlashCol) string {
	return schemaTypeDiff(col, meta)
}

// FillFlashCol fills the row-metadata facts a MINIMAL table map does not
// carry — signedness, charset, ENUM/SET members, binary affinity — from a
// schema definition column. Wire facts (Typ, Meta, Base, precision, FSP)
// stay untouched; call CheckSchemaCol first so a contradiction refuses
// before the merge hides it (ADR 0022).
func FillFlashCol(col SchemaCol, meta *model.FlashCol) {
	if col.charset == "binary" {
		meta.Binary = true
	} else if meta.Charset == "" {
		meta.Charset = col.charset
	}
	if !meta.HasSign {
		meta.Unsigned = col.unsigned
		meta.HasSign = true
	}
	if len(meta.Members) == 0 && len(col.members) > 0 {
		meta.Members = append([]string(nil), col.members...)
	}
}
