// Package analyzer types flashback warnings.
// input: a warning code, its i18n message key, and the message arguments.
// output: FlashbackWarning, a stable code plus the localized message; Line is the stderr form with the localized "warning:" prefix.
// pos: every flashback warning source (schema catalog, schema file review, unknown tables, split transactions) builds one; the plan prints them as JSON (#270, #282).
// note: if this file changes, update this header, module README.md and docs/reference/cli.md (--plan warning codes).
package analyzer

import "binlogviz/internal/i18n"

// Flashback warning codes. The strings are stable JSON values; they do not
// change with --lang. Add a code; never rename one.
const (
	WarnPartialTransaction        = "partial_transaction"
	WarnTableDefinitionMissing    = "table_definition_missing"
	WarnGeneratedColumnUnverified = "generated_column_unverified"
	WarnSchemaFileDBMismatch      = "schema_file_db_mismatch"
	WarnSchemaFileTableAmbiguous  = "schema_file_table_ambiguous"
	WarnSchemaFileCaseFolded      = "schema_file_case_folded"
	WarnSchemaFileCaseMismatch    = "schema_file_case_mismatch"
)

// FlashbackWarning is one flashback warning. Message is localized and has
// no "warning:" prefix.
type FlashbackWarning struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func newFlashWarning(code, key string, args map[string]any) FlashbackWarning {
	return FlashbackWarning{Code: code, Message: i18n.Tf(key, args)}
}

// Line is the warning as flashback prints it on stderr.
func (w FlashbackWarning) Line() string {
	return i18n.Tf("warning.flashbackLine", map[string]any{"Message": w.Message})
}

// appendWarningOnce appends w unless an equal warning is already in list.
func appendWarningOnce(list []FlashbackWarning, w FlashbackWarning) []FlashbackWarning {
	for _, have := range list {
		if have == w {
			return list
		}
	}
	return append(list, w)
}
