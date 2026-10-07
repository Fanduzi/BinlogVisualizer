// Package analyzer collects selected row images for undo SQL.
// input: retained normalized events that already passed time, position, GTID, schema, table, and DML filters, plus flashback images captured by the parser.
// output: one SQL script that reverses those row changes, or one error and no script when a selected row cannot be rendered exactly, generated columns cannot be identified, or the selected range contains DDL. Generated columns learned from parsed CREATE/ALTER are omitted from INSERT and UPDATE SET.
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
	gens []map[string]struct{}
}

type flashSplit struct {
	gtid    string
	kept    bool
	skipped bool
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
	if a.flashGen.unknown(ev.Schema, ev.Table) {
		a.flashErr = fmt.Errorf("%s", i18n.Tf("error.flashbackGenerated", map[string]any{
			"Table": flashTable(ev.Schema, ev.Table),
		}))
		return
	}
	a.appendFlashRows(ev, ev.FlashRows, copyNames(a.flashGen.columns(ev.Schema, ev.Table)))
	a.noteFlashbackKept(ev)
}

func (a *Analyzer) appendFlashRows(ev model.NormalizedEvent, rows []model.FlashRow, gen map[string]struct{}) {
	copied := append([]model.FlashRow(nil), rows...)
	gens := make([]map[string]struct{}, len(copied))
	for i := range gens {
		gens[i] = gen
	}
	key := ev.TxnKey
	if n := len(a.flashGroups); n > 0 && key != "" && a.flashGroups[n-1].key == key {
		a.flashGroups[n-1].rows = append(a.flashGroups[n-1].rows, copied...)
		a.flashGroups[n-1].gens = append(a.flashGroups[n-1].gens, gens...)
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
		gens: gens,
	})
}

func (a *Analyzer) noteFlashbackKept(ev model.NormalizedEvent) {
	a.noteFlashSplit(ev.TxnKey, ev.TxnGTID, true)
}

func (a *Analyzer) noteFlashbackSkipped() {
	if a == nil || a.txnBuilder == nil {
		return
	}
	gtid, _, _ := a.txnBuilder.touchLocation()
	a.noteFlashSplit(a.txnBuilder.CurrentTxnKey(), gtid, false)
}

func (a *Analyzer) noteFlashSplit(key, gtid string, kept bool) {
	if a == nil || key == "" {
		return
	}
	if a.flashSplit == nil {
		a.flashSplit = map[string]*flashSplit{}
	}
	split, ok := a.flashSplit[key]
	if !ok {
		split = &flashSplit{}
		a.flashSplit[key] = split
		a.flashSplitOrder = append(a.flashSplitOrder, key)
	}
	if gtid != "" {
		split.gtid = gtid
	}
	if kept {
		split.kept = true
	} else {
		split.skipped = true
	}
}

// FlashbackWarnings reports transactions a table or --dml filter undid only in part.
// Empty when flashback has nothing to print.
func (a *Analyzer) FlashbackWarnings() []string {
	if a == nil {
		return nil
	}
	var out []string
	for _, key := range a.flashSplitOrder {
		split := a.flashSplit[key]
		if split == nil || !split.kept || !split.skipped {
			continue
		}
		gtid := split.gtid
		if gtid == "" {
			gtid = "GTID unavailable"
		}
		out = append(out, i18n.Tf("warning.flashbackSplit", map[string]any{"GTID": gtid}))
	}
	return out
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
	sql, err := renderFlashbackSQL(kept)
	if err != nil {
		return "", err
	}
	return sql, nil
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

func renderFlashbackSQL(groups []flashGroup) (string, error) {
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
			var gen map[string]struct{}
			if j < len(group.gens) {
				gen = group.gens[j]
			}
			statement, err := renderUndoStatement(group.rows[j], gen)
			if err != nil {
				return "", err
			}
			b.WriteString(statement)
			b.WriteByte('\n')
		}
		b.WriteString("COMMIT;\n")
	}
	return b.String(), nil
}

func renderUndoStatement(row model.FlashRow, gen map[string]struct{}) (string, error) {
	table := quoteIdent(row.Schema) + "." + quoteIdent(row.Table)
	switch row.Op {
	case "DELETE":
		cols, vals := assignColumns(row.Columns, row.Before, gen)
		if len(cols) == 0 {
			return "", generatedRowError(row)
		}
		return "INSERT INTO " + table + " (" + quoteColumnList(cols) + ") VALUES (" + strings.Join(vals, ", ") + ");", nil
	case "UPDATE":
		cols, vals := assignColumns(row.Columns, row.Before, gen)
		if len(cols) == 0 {
			return "", generatedRowError(row)
		}
		sets := make([]string, len(cols))
		for i := range cols {
			sets[i] = quoteIdent(cols[i]) + " = " + vals[i]
		}
		return undoWhere(row, "UPDATE "+table+" SET "+strings.Join(sets, ", "), row.After, gen)
	default:
		return undoWhere(row, "DELETE FROM "+table, row.After, gen)
	}
}

func assignColumns(columns, values []string, gen map[string]struct{}) ([]string, []string) {
	cols := make([]string, 0, len(columns))
	vals := make([]string, 0, len(values))
	for i, column := range columns {
		if isGenerated(gen, column) {
			continue
		}
		cols = append(cols, column)
		val := "NULL"
		if i < len(values) {
			val = values[i]
		}
		vals = append(vals, val)
	}
	return cols, vals
}

func isGenerated(gen map[string]struct{}, name string) bool {
	if len(gen) == 0 {
		return false
	}
	_, ok := gen[strings.ToLower(name)]
	return ok
}

func generatedRowError(row model.FlashRow) error {
	return fmt.Errorf("%s", i18n.Tf("error.flashbackGeneratedRow", map[string]any{
		"Table": flashTable(row.Schema, row.Table),
	}))
}

func undoWhere(row model.FlashRow, head string, image []string, gen map[string]struct{}) (string, error) {
	pk := !row.NoPK && len(row.PK) > 0
	indexes := row.PK
	dropped := false
	if !pk {
		indexes = make([]int, 0, len(row.Columns))
		for i, column := range row.Columns {
			if isGenerated(gen, column) {
				dropped = true
				continue
			}
			indexes = append(indexes, i)
		}
		if len(indexes) == 0 {
			indexes = make([]int, len(row.Columns))
			for i := range indexes {
				indexes[i] = i
			}
			dropped = false
		}
	}
	parts := make([]string, 0, len(indexes))
	for _, index := range indexes {
		if index < 0 || index >= len(row.Columns) || index >= len(image) {
			continue
		}
		parts = append(parts, quoteIdent(row.Columns[index])+" <=> "+image[index])
	}
	if len(parts) == 0 {
		return "", generatedRowError(row)
	}
	statement := head + " WHERE " + strings.Join(parts, " AND ")
	if !pk {
		statement += " LIMIT 1"
		note := "this matches every column and LIMIT 1"
		if dropped {
			note = "this matches every non-generated column and LIMIT 1"
		}
		return "-- no primary key on " + flashTable(row.Schema, row.Table) + "; " + note + "\n" + statement + ";", nil
	}
	return statement + ";", nil
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
