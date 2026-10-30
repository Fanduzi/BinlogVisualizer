// Package redo refusal types and labels: a failed script must not be applied.
// input: a stable reason code, the table it names, and the human-readable cause.
// output: a *Refusal the CLI maps to exit 6 with details.reason and details.table.
// pos: refusal vocabulary of the redo planner; mirrors flashback's GenerationRefusal shape so redoExit reads like flashbackExit (ADR 0022).
// note: if this file changes, update this header and module README.md.
package redo

import (
	"errors"
	"fmt"
	"strings"

	"binlogviz/internal/i18n"
	"binlogviz/internal/model"
	"binlogviz/internal/schemacatalog"
	"binlogviz/internal/sqlemit"
)

// Refusal is an exit-6 redo refusal with a stable reason.
// Table is set when the refusal names one table.
type Refusal struct {
	Code  string
	Table string
	Err   error
}

func (e *Refusal) Error() string {
	if e == nil || e.Err == nil {
		return ""
	}
	return e.Err.Error()
}

func (e *Refusal) Unwrap() error { return e.Err }

func refuse(code, table string, err error) error {
	if err == nil {
		return nil
	}
	return &Refusal{Code: code, Table: table, Err: err}
}

// RefusalDetails reports the stable reason of a redo refusal.
func RefusalDetails(err error) (reason, table string, ok bool) {
	var gen *Refusal
	if errors.As(err, &gen) && gen != nil {
		return gen.Code, gen.Table, true
	}
	return "", "", false
}

// rowProblemError is the refusal for one row the binlog layer could not
// capture fully. flavor is the binlog's server flavor; a MariaDB binlog
// names the MariaDB setting.
func rowProblemError(row model.RedoRow, table, flavor string) error {
	switch row.ProblemKind {
	case model.FlashProblemImage:
		return refuse("row_image", table, fmt.Errorf("%s", i18n.Tf("error.flashbackImage", map[string]any{"Table": table})))
	case model.FlashProblemType:
		column := row.ProblemColumn
		if column == "" {
			column = "?"
		}
		named := redoTable(row.Schema, row.Table)
		return refuse("column_type", named, fmt.Errorf("%s", i18n.Tf("error.flashbackType", map[string]any{
			"Table":  named,
			"Column": column,
			"Type":   row.ProblemType,
		})))
	default:
		return refuse("capture", table, fmt.Errorf("%s", i18n.Tf("error.flashbackCapture", map[string]any{"Table": table})))
	}
}

// namesError is the refusal for a MINIMAL-metadata row with no resolvable
// table definition: neither names nor the type facts literals need are
// available. The message names both remediations (ADR 0022).
func namesError(row model.RedoRow, table, flavor string) error {
	key := "error.redoNames"
	if strings.EqualFold(strings.TrimSpace(flavor), "mariadb") {
		key = "error.redoNamesMariaDB"
	}
	return refuse("names", table, fmt.Errorf("%s", i18n.Tf(key, map[string]any{"Table": table})))
}

// literalProblemError is the refusal for a cell the shared emitter could not
// render exactly. problem is the type name it reported.
func literalProblemError(row model.RedoRow, column int, problem string) error {
	name := "?"
	if column >= 0 && column < len(row.Columns) {
		name = row.Columns[column]
	}
	named := redoTable(row.Schema, row.Table)
	return refuse("column_type", named, fmt.Errorf("%s", i18n.Tf("error.flashbackType", map[string]any{
		"Table":  named,
		"Column": name,
		"Type":   problem,
	})))
}

// truncatedQueryError refuses an unclassified QUERY whose stored text was
// cut at the model bound: the emitted prefix would be a different statement.
func truncatedQueryError(statement string) error {
	return refuse("truncated_query", "", fmt.Errorf("%s", i18n.Tf("error.redoTruncatedQuery", map[string]any{
		"Statement": statement,
	})))
}

// schemaError is the refusal for a table definition that contradicts a
// comparable binlog fact.
func schemaError(table string, err error) error {
	return refuse("schema", table, err)
}

func redoTable(schema, table string) string {
	switch {
	case schema != "" && table != "":
		return sqlemit.TableName(schema, table)
	case table != "":
		return table
	case schema != "":
		return schema
	default:
		return "selected range"
	}
}

// refusalTable names schema.table in a refusal, from its current catalog binding.
func (a *Planner) refusalTable(schema, table string) string {
	return refusalLabel(a.cat.Peek(schema, table), schema, table)
}

// refusalLabel is the table name every refusal prints. A table bound by a
// letter-case fold also names the schema-file table, matching undo.
func refusalLabel(res schemacatalog.Resolution, schema, table string) string {
	name := redoTable(schema, table)
	if res.FileTable == "" {
		return name
	}
	return i18n.Tf("error.flashbackSchemaFoldLabel", map[string]any{"Binlog": name, "File": res.FileTable})
}
