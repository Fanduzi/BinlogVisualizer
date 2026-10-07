// Package binlog formats bounded ROW images from an already-decoded rows event.
// input: go-mysql RowsEvent values, optional FULL row metadata (names, the SIGNEDNESS bitmap), and a per-event image cap.
// output: model.RowImage values with NULL, integers (signed, unsigned, or both when signedness is absent), decimals, strings, datetimes, JSON, and bounded hex blobs.
// pos: parser helper used only when row-image capture is on.
// note: if this file changes, update this header and README.md.
package binlog

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/go-mysql-org/go-mysql/replication"

	"binlogviz/internal/model"
)

func captureRowImages(ev *replication.RowsEvent, kind, schema, table string) ([]model.RowImage, int) {
	if ev == nil || len(ev.Rows) == 0 {
		return nil, 0
	}
	op := operationFromKind(kind)
	update := op == "UPDATE"
	logical := len(ev.Rows)
	if update {
		logical = len(ev.Rows) / 2
	}
	if logical == 0 {
		return nil, 0
	}
	width := int(ev.ColumnCount)
	if width == 0 {
		width = len(ev.Rows[0])
	}
	labels, names := columnLabels(ev.Table, width)
	var unsigned map[int]bool
	if ev.Table != nil {
		unsigned = unsignedMap(ev.Table)
	}
	keep := logical
	if keep > model.MaxRowImagesPerTxn {
		keep = model.MaxRowImagesPerTxn
	}
	images := make([]model.RowImage, 0, keep)
	for i := 0; i < keep; i++ {
		var beforeRow, afterRow []any
		var beforeSkips, afterSkips []int
		switch {
		case update:
			beforeRow = ev.Rows[i*2]
			afterRow = ev.Rows[i*2+1]
			beforeSkips = skipsAt(ev.SkippedColumns, i*2)
			afterSkips = skipsAt(ev.SkippedColumns, i*2+1)
		case op == "DELETE":
			beforeRow = ev.Rows[i]
			beforeSkips = skipsAt(ev.SkippedColumns, i)
		default:
			afterRow = ev.Rows[i]
			afterSkips = skipsAt(ev.SkippedColumns, i)
		}
		img := model.RowImage{
			Schema:  schema,
			Table:   table,
			Op:      op,
			Columns: labels,
			Names:   names,
		}
		if beforeRow != nil {
			img.Before = formatImage(beforeRow, skipSet(beforeSkips), labels, unsigned, nil)
		}
		if afterRow != nil {
			img.After = formatImage(afterRow, skipSet(afterSkips), labels, unsigned, img.Before)
		}
		if update {
			img.Changed = changedColumns(labels, img.Before, img.After)
		}
		images = append(images, img)
	}
	return images, logical - keep
}

func operationFromKind(kind string) string {
	switch kind {
	case kindUpdateRows:
		return "UPDATE"
	case kindDeleteRows:
		return "DELETE"
	default:
		return "INSERT"
	}
}

func columnLabels(table *replication.TableMapEvent, width int) ([]string, string) {
	labels := make([]string, width)
	names := model.RowNamesPositional
	var meta []string
	if table != nil && len(table.ColumnName) == width && width > 0 {
		meta = table.ColumnNameString()
		names = model.RowNamesFull
	}
	for i := 0; i < width; i++ {
		if names == model.RowNamesFull && i < len(meta) && meta[i] != "" {
			labels[i] = meta[i]
			continue
		}
		labels[i] = "@" + strconv.Itoa(i+1)
	}
	return labels, names
}

func skipsAt(all [][]int, index int) []int {
	if index < 0 || index >= len(all) {
		return nil
	}
	return all[index]
}

func skipSet(skips []int) map[int]struct{} {
	if len(skips) == 0 {
		return nil
	}
	out := make(map[int]struct{}, len(skips))
	for _, skip := range skips {
		out[skip] = struct{}{}
	}
	return out
}

func formatImage(row []any, skips map[int]struct{}, labels []string, unsigned map[int]bool, fallback []model.RowCell) []model.RowCell {
	cells := make([]model.RowCell, len(labels))
	for i := range labels {
		if _, skipped := skips[i]; skipped {
			if i < len(fallback) {
				cells[i] = fallback[i]
			}
			continue
		}
		var value any
		if i < len(row) {
			value = row[i]
		}
		var signedness *bool
		if unsigned != nil {
			if bit, ok := unsigned[i]; ok {
				signedness = &bit
			}
		}
		cells[i] = formatCell(value, signedness)
	}
	return cells
}

func changedColumns(labels []string, before, after []model.RowCell) []string {
	var changed []string
	for i, label := range labels {
		if i >= len(before) || i >= len(after) {
			continue
		}
		if before[i].Null != after[i].Null || before[i].Text != after[i].Text {
			changed = append(changed, label)
		}
	}
	return changed
}

// formatCell renders one decoded binlog value.
// unsigned nil means the binlog did not say signedness (mysqlbinlog prints both forms when they differ).
func formatCell(value any, unsigned *bool) model.RowCell {
	if value == nil {
		return model.RowCell{Null: true}
	}
	if text, ok := formatInt(value, unsigned); ok {
		return model.RowCell{Text: text}
	}
	switch typed := value.(type) {
	case int:
		return model.RowCell{Text: strconv.Itoa(typed)}
	case float32:
		return model.RowCell{Text: strconv.FormatFloat(float64(typed), 'g', -1, 32)}
	case float64:
		return model.RowCell{Text: strconv.FormatFloat(typed, 'g', -1, 64)}
	case []byte:
		return model.RowCell{Text: formatBlob(typed)}
	case string:
		return model.RowCell{Text: boundText(typed)}
	case time.Time:
		return model.RowCell{Text: typed.UTC().Format("2006-01-02 15:04:05.000000")}
	case fmt.Stringer:
		return model.RowCell{Text: boundText(typed.String())}
	default:
		return model.RowCell{Text: boundText(fmt.Sprint(typed))}
	}
}

func formatInt(value any, unsigned *bool) (string, bool) {
	var signed int64
	var wide uint64
	switch typed := value.(type) {
	case int8:
		signed, wide = int64(typed), uint64(uint8(typed))
	case int16:
		signed, wide = int64(typed), uint64(uint16(typed))
	case int32:
		signed, wide = int64(typed), uint64(uint32(typed))
	case int64:
		signed, wide = typed, uint64(typed)
	default:
		return "", false
	}
	signedText := strconv.FormatInt(signed, 10)
	unsignedText := strconv.FormatUint(wide, 10)
	if unsigned != nil {
		if *unsigned {
			return unsignedText, true
		}
		return signedText, true
	}
	if signedText != unsignedText {
		return signedText + " (" + unsignedText + ")", true
	}
	return signedText, true
}

func formatBlob(value []byte) string {
	shown := value
	truncated := false
	if len(value) > model.MaxRowValueBytes {
		shown = value[:model.MaxRowValueBytes]
		truncated = true
	}
	hexed := hex.EncodeToString(shown)
	if truncated {
		return "0x" + hexed + model.TruncationMarker(model.MaxRowValueBytes, len(value))
	}
	return fmt.Sprintf("0x%s (%d bytes)", hexed, len(value))
}

// pkMeta is the primary-key columns copied out of one TABLE_MAP.
// Empty means the binlog did not name a primary key.
type pkMeta struct {
	indexes []int
	names   []string
	sign    []*bool
}

func pkMetaFrom(table *replication.TableMapEvent) pkMeta {
	if table == nil || len(table.PrimaryKey) == 0 || len(table.ColumnName) != int(table.ColumnCount) {
		return pkMeta{}
	}
	names := table.ColumnNameString()
	unsigned := unsignedMap(table)
	meta := pkMeta{
		indexes: make([]int, 0, len(table.PrimaryKey)),
		names:   make([]string, 0, len(table.PrimaryKey)),
		sign:    make([]*bool, 0, len(table.PrimaryKey)),
	}
	for _, col := range table.PrimaryKey {
		idx := int(col)
		if idx < 0 || idx >= len(names) || names[idx] == "" {
			return pkMeta{}
		}
		meta.indexes = append(meta.indexes, idx)
		meta.names = append(meta.names, names[idx])
		if unsigned == nil {
			meta.sign = append(meta.sign, nil)
			continue
		}
		bit, ok := unsigned[idx]
		if !ok {
			meta.sign = append(meta.sign, nil)
			continue
		}
		copied := bit
		meta.sign = append(meta.sign, &copied)
	}
	return meta
}

// primaryKeyValues returns one identity per UPDATE or DELETE image.
// An empty string means that image did not carry the primary-key columns.
// A nil slice means the table map did not name a primary key.
func primaryKeyValues(ev *replication.RowsEvent, kind string, meta pkMeta) []string {
	if ev == nil || len(meta.indexes) == 0 || len(ev.Rows) == 0 {
		return nil
	}
	if kind != kindUpdateRows && kind != kindDeleteRows {
		return nil
	}
	update := kind == kindUpdateRows
	logical := len(ev.Rows)
	if update {
		logical = len(ev.Rows) / 2
	}
	if logical == 0 {
		return nil
	}
	keys := make([]string, logical)
	for i := 0; i < logical; i++ {
		var row []any
		var skips []int
		if update {
			row = ev.Rows[i*2]
			skips = skipsAt(ev.SkippedColumns, i*2)
		} else {
			row = ev.Rows[i]
			skips = skipsAt(ev.SkippedColumns, i)
		}
		keys[i] = formatPrimaryKey(row, skips, meta)
	}
	return keys
}

func formatPrimaryKey(row []any, skips []int, meta pkMeta) string {
	skipped := skipSet(skips)
	parts := make([]string, len(meta.indexes))
	for i, idx := range meta.indexes {
		if _, omit := skipped[idx]; omit || idx < 0 || idx >= len(row) {
			return ""
		}
		cell := formatCell(row[idx], meta.sign[i])
		parts[i] = meta.names[i] + "=" + formatPKText(cell)
	}
	return strings.Join(parts, ", ")
}

func formatPKText(cell model.RowCell) string {
	if cell.Null {
		return "NULL"
	}
	text := cell.Text
	if strings.ContainsAny(text, ",=\"") || strings.TrimSpace(text) != text {
		return strconv.Quote(text)
	}
	return text
}

func boundText(value string) string {
	if len(value) <= model.MaxRowValueBytes {
		return value
	}
	cut := model.MaxRowValueBytes
	for cut > 0 && !utf8.RuneStart(value[cut]) {
		cut--
	}
	return value[:cut] + model.TruncationMarker(cut, len(value))
}
