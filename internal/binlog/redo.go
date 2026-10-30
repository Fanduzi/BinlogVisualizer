// Package binlog captures row images for forward (redo) SQL output.
// input: go-mysql RowsEvent values and the binary JSON documents captured beside that decode.
// output: model.RedoRow values carrying raw cell images and per-column descriptors; names are NOT required at capture — MINIMAL-metadata rows keep @1..@N labels for the planner to resolve against a schema definition (ADR 0022).
// pos: parser helper used only when redo capture is on. It reuses the decoded row images from the same RowsEvent as display capture.
// note: if this file changes, update this header and README.md.
package binlog

import (
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"binlogviz/internal/model"
)

func captureRedoRows(ev *replication.RowsEvent, kind, schema, table string) []model.RedoRow {
	docs := recallFlashJSON(ev)
	base := model.RedoRow{Schema: schema, Table: table, Op: operationFromKind(kind)}
	if ev == nil || ev.Table == nil || len(ev.Rows) == 0 {
		base.ProblemKind = model.FlashProblemCapture
		return []model.RedoRow{base}
	}
	width := int(ev.ColumnCount)
	if width == 0 {
		width = len(ev.Rows[0])
	}
	labels, names := columnLabels(ev.Table, width)
	meta := pkMetaFrom(ev.Table)
	if len(ev.Table.PrimaryKey) > 0 && len(meta.indexes) == 0 {
		base.ProblemKind = model.FlashProblemType
		base.ProblemColumn = "PRIMARY KEY"
		base.ProblemType = "PRIMARY KEY"
		return []model.RedoRow{base}
	}
	noPK := len(meta.indexes) == 0
	update := base.Op == "UPDATE"
	logical := len(ev.Rows)
	if update {
		logical = len(ev.Rows) / 2
	}
	if logical == 0 {
		base.ProblemKind = model.FlashProblemCapture
		return []model.RedoRow{base}
	}
	cols := flashColumnMeta(ev.Table, width)
	out := make([]model.RedoRow, 0, logical)
	for i := 0; i < logical; i++ {
		row := base
		row.Columns = labels
		row.Names = names
		row.Cols = cols
		row.NoPK = noPK
		row.PK = append([]int(nil), meta.indexes...)
		var beforeRow, afterRow []any
		var beforeSkips, afterSkips []int
		beforeIndex, afterIndex := -1, -1
		switch {
		case update:
			beforeIndex, afterIndex = i*2, i*2+1
			beforeRow = ev.Rows[beforeIndex]
			afterRow = ev.Rows[afterIndex]
			beforeSkips = skipsAt(ev.SkippedColumns, beforeIndex)
			afterSkips = skipsAt(ev.SkippedColumns, afterIndex)
		case row.Op == "DELETE":
			beforeIndex = i
			beforeRow = ev.Rows[beforeIndex]
			beforeSkips = skipsAt(ev.SkippedColumns, beforeIndex)
		default:
			afterIndex = i
			afterRow = ev.Rows[afterIndex]
			afterSkips = skipsAt(ev.SkippedColumns, afterIndex)
		}
		if len(beforeSkips) > 0 || len(afterSkips) > 0 {
			row.ProblemKind = model.FlashProblemImage
			return append(out, row)
		}
		row.Before = beforeRow
		row.After = afterRow
		row.BeforeDocs = jsonDocBytes(jsonCellsAt(docs, beforeIndex))
		row.AfterDocs = jsonDocBytes(jsonCellsAt(docs, afterIndex))
		row.NonStrict = redoEnumZero(ev.Table, beforeRow) || redoEnumZero(ev.Table, afterRow)
		row.ZeroDate = redoZeroDate(ev.Table, beforeRow) || redoZeroDate(ev.Table, afterRow)
		out = append(out, row)
	}
	return out
}

// jsonDocBytes flattens one image's JSON cells to documents aligned with it.
// A nil entry means the column is not JSON or its bytes were not captured.
func jsonDocBytes(cells []jsonCell) [][]byte {
	if len(cells) == 0 {
		return nil
	}
	out := make([][]byte, len(cells))
	for i, cell := range cells {
		if cell.ok {
			out[i] = cell.doc
		}
	}
	return out
}

// redoEnumZero reports an ENUM column holding index 0 (the error member),
// which strict sql_mode rejects on apply. It reads the raw cell value.
func redoEnumZero(table *replication.TableMapEvent, image []any) bool {
	for i, value := range image {
		if value == nil || flashRealType(table, i) != mysql.MYSQL_TYPE_ENUM {
			continue
		}
		if idx, ok := int64Value(value); ok && idx == 0 {
			return true
		}
	}
	return false
}

// redoZeroDate reports a DATE, DATETIME or TIMESTAMP cell whose month or day
// is zero. It reads the raw decoded string, the same text sqlTemporal quotes.
func redoZeroDate(table *replication.TableMapEvent, image []any) bool {
	for i, value := range image {
		text, ok := value.(string)
		if !ok {
			continue
		}
		switch flashRealType(table, i) {
		case mysql.MYSQL_TYPE_NEWDATE,
			mysql.MYSQL_TYPE_DATETIME, mysql.MYSQL_TYPE_DATETIME2,
			mysql.MYSQL_TYPE_TIMESTAMP, mysql.MYSQL_TYPE_TIMESTAMP2:
			if zeroDateText(text) {
				return true
			}
		}
	}
	return false
}

// zeroDateText reads a 'YYYY-MM-DD[...]' wall-clock string and reports a
// zero month or day. The undo path checks the same shape inside its literal.
func zeroDateText(text string) bool {
	if len(text) < 10 || text[4] != '-' || text[7] != '-' {
		return false
	}
	return text[5:7] == "00" || text[8:10] == "00"
}
