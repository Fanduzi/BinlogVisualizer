// Package analyzer collects selected row images for undo SQL.
// input: retained normalized events that already passed time, position, GTID, schema, table, and DML filters, plus flashback images captured by the parser.
// output: one SQL script that reverses those row changes, or one error and no script when a selected row cannot be rendered exactly or the selected range contains DDL.
// pos: optional collector on Analyzer. It runs only when Options.Flashback is set.
// note: if this file changes, update this header and module README.md.
package analyzer

import (
	"fmt"
	"path/filepath"
	"strings"

	"binlogviz/internal/i18n"
	"binlogviz/internal/model"
)

type flashGroup struct {
	key  string
	gtid string
	path string
	pos  int64
	rows []model.FlashRow
}

func (a *Analyzer) noteFlashback(ev model.NormalizedEvent) {
	if a == nil || !a.opts.Flashback || a.flashErr != nil {
		return
	}
	if ev.EventType == "DDL" {
		a.flashErr = flashDDLError(ev)
		return
	}
	if ev.EventType != "ROWS" {
		return
	}
	if len(ev.FlashRows) == 0 {
		a.flashErr = fmt.Errorf("%s", i18n.Tf("error.flashbackCapture", map[string]any{
			"Table": flashTable(ev.Schema, ev.Table),
		}))
		return
	}
	for _, row := range ev.FlashRows {
		if row.ProblemKind != "" {
			a.flashErr = flashRowError(row)
			return
		}
	}
	a.appendFlashRows(ev, ev.FlashRows)
}

func (a *Analyzer) appendFlashRows(ev model.NormalizedEvent, rows []model.FlashRow) {
	copied := append([]model.FlashRow(nil), rows...)
	key := ev.TxnKey
	if n := len(a.flashGroups); n > 0 && key != "" && a.flashGroups[n-1].key == key {
		a.flashGroups[n-1].rows = append(a.flashGroups[n-1].rows, copied...)
		return
	}
	path := ev.TxnStartPath
	pos := ev.TxnStartPos
	if path == "" {
		path = ev.BinlogPath
	}
	if pos == 0 {
		pos = ev.PositionStart
	}
	a.flashGroups = append(a.flashGroups, flashGroup{
		key:  key,
		gtid: ev.TxnGTID,
		path: path,
		pos:  pos,
		rows: copied,
	})
}

// FlashbackSQL returns undo SQL for the selected row changes.
// A non-nil error means the script must not be applied; the string is empty.
func (a *Analyzer) FlashbackSQL() (string, error) {
	if a == nil {
		return "", fmt.Errorf("%s", i18n.T("error.flashbackCapture", map[string]any{"Table": "selected range"}))
	}
	if a.flashErr != nil {
		return "", a.flashErr
	}
	var kept []flashGroup
	for _, group := range a.flashGroups {
		if len(group.rows) > 0 {
			kept = append(kept, group)
		}
	}
	if len(kept) == 0 {
		return "", nil
	}
	return renderFlashbackSQL(kept), nil
}

func flashRowError(row model.FlashRow) error {
	table := flashTable(row.Schema, row.Table)
	switch row.ProblemKind {
	case model.FlashProblemNames:
		return fmt.Errorf("%s", i18n.Tf("error.flashbackNames", map[string]any{"Table": table}))
	case model.FlashProblemImage:
		return fmt.Errorf("%s", i18n.Tf("error.flashbackImage", map[string]any{"Table": table}))
	case model.FlashProblemType:
		column := row.ProblemColumn
		if column == "" {
			column = "?"
		}
		return fmt.Errorf("%s", i18n.Tf("error.flashbackType", map[string]any{
			"Table":  table,
			"Column": column,
			"Type":   row.ProblemType,
		}))
	default:
		return fmt.Errorf("%s", i18n.Tf("error.flashbackCapture", map[string]any{"Table": table}))
	}
}

func flashDDLError(ev model.NormalizedEvent) error {
	statement := strings.TrimSpace(ev.QuerySQL)
	if statement == "" {
		statement = "DDL"
	}
	statement = oneLine(statement, 120)
	return fmt.Errorf("%s", i18n.Tf("error.flashbackDDL", map[string]any{
		"Table":     flashTable(ev.Schema, ev.Table),
		"Statement": statement,
	}))
}

func flashTable(schema, table string) string {
	switch {
	case schema != "" && table != "":
		return schema + "." + table
	case table != "":
		return table
	case schema != "":
		return schema
	default:
		return "selected range"
	}
}

func oneLine(value string, limit int) string {
	value = strings.TrimSpace(value)
	if i := strings.IndexAny(value, "\r\n"); i >= 0 {
		value = strings.TrimSpace(value[:i])
	}
	if limit > 0 && len(value) > limit {
		value = value[:limit] + "..."
	}
	return value
}

func renderFlashbackSQL(groups []flashGroup) string {
	var b strings.Builder
	b.WriteString("-- flashback reverses the selected row changes, last transaction first.\n")
	b.WriteString("-- TIMESTAMP literals are the UTC wall clock of the stored instant. Review this script before applying it.\n")
	b.WriteString("SET NAMES utf8mb4;\n")
	b.WriteString("SET time_zone = '+00:00';\n")
	b.WriteString("SET SESSION sql_mode = REPLACE(@@SESSION.sql_mode, 'NO_BACKSLASH_ESCAPES', '');\n")
	for i := len(groups) - 1; i >= 0; i-- {
		group := groups[i]
		b.WriteByte('\n')
		gtid := group.gtid
		if gtid == "" {
			gtid = "GTID unavailable"
		}
		fmt.Fprintf(&b, "-- gtid: %s\n", oneLine(gtid, 0))
		file := filepath.Base(group.path)
		if file == "" || file == "." {
			file = "binlog"
		}
		fmt.Fprintf(&b, "-- binlog: %s:%d\n", oneLine(file, 0), group.pos)
		b.WriteString("START TRANSACTION;\n")
		for j := len(group.rows) - 1; j >= 0; j-- {
			b.WriteString(renderUndoStatement(group.rows[j]))
			b.WriteByte('\n')
		}
		b.WriteString("COMMIT;\n")
	}
	return b.String()
}

func renderUndoStatement(row model.FlashRow) string {
	table := quoteIdent(row.Schema) + "." + quoteIdent(row.Table)
	switch row.Op {
	case "DELETE":
		return "INSERT INTO " + table + " (" + quoteColumnList(row.Columns) + ") VALUES (" + strings.Join(row.Before, ", ") + ");"
	case "UPDATE":
		sets := make([]string, len(row.Columns))
		for i, column := range row.Columns {
			sets[i] = quoteIdent(column) + " = " + row.Before[i]
		}
		return undoWhere(row, "UPDATE "+table+" SET "+strings.Join(sets, ", "), row.After)
	default:
		return undoWhere(row, "DELETE FROM "+table, row.After)
	}
}

func undoWhere(row model.FlashRow, head string, image []string) string {
	indexes := row.PK
	if row.NoPK || len(indexes) == 0 {
		indexes = make([]int, len(row.Columns))
		for i := range indexes {
			indexes[i] = i
		}
	}
	parts := make([]string, 0, len(indexes))
	for _, index := range indexes {
		if index < 0 || index >= len(row.Columns) || index >= len(image) {
			continue
		}
		parts = append(parts, quoteIdent(row.Columns[index])+" <=> "+image[index])
	}
	statement := head + " WHERE " + strings.Join(parts, " AND ")
	if row.NoPK || len(row.PK) == 0 {
		statement += " LIMIT 1"
		return "-- no primary key on " + flashTable(row.Schema, row.Table) + "; this matches every column and LIMIT 1\n" + statement + ";"
	}
	return statement + ";"
}

func quoteColumnList(columns []string) string {
	quoted := make([]string, len(columns))
	for i, column := range columns {
		quoted[i] = quoteIdent(column)
	}
	return strings.Join(quoted, ", ")
}

func quoteIdent(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}
