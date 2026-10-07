// Package binlog formats exact SQL literals from an already-decoded rows event.
// input: go-mysql RowsEvent values, the binary JSON documents captured beside that decode, and FULL row metadata (names, signedness, collation, enum/set members, primary key).
// output: model.FlashRow values for undo SQL, or a problem that names why a row cannot be rendered exactly. JSON is rebuilt from the binary document. Character columns that are not utf8mb4 use a charset introducer and hex bytes. ENUM is the member index and SET is the bitmask. Each row carries TABLE_MAP column metadata for a schema-file check, and NonStrict when an ENUM value is index 0.
// pos: parser helper used only when flashback capture is on. It reuses the decoded row images from the same RowsEvent as display capture.
// note: if this file changes, update this header and README.md.
package binlog

import (
	"encoding/hex"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/shopspring/decimal"

	"binlogviz/internal/model"
)

// mysqlCollationBinary is the collation id for the binary charset.
const mysqlCollationBinary uint64 = 63

func captureFlashbackRows(ev *replication.RowsEvent, kind, schema, table string) []model.FlashRow {
	docs := recallFlashJSON(ev)
	base := model.FlashRow{Schema: schema, Table: table, Op: operationFromKind(kind)}
	if ev == nil || ev.Table == nil || len(ev.Rows) == 0 {
		base.ProblemKind = model.FlashProblemCapture
		return []model.FlashRow{base}
	}
	width := int(ev.ColumnCount)
	if width == 0 {
		width = len(ev.Rows[0])
	}
	labels, names := columnLabels(ev.Table, width)
	if names != model.RowNamesFull || !flashNamesComplete(ev.Table, width) {
		base.ProblemKind = model.FlashProblemNames
		return []model.FlashRow{base}
	}
	meta := pkMetaFrom(ev.Table)
	if len(ev.Table.PrimaryKey) > 0 && len(meta.indexes) == 0 {
		base.ProblemKind = model.FlashProblemType
		base.ProblemColumn = "PRIMARY KEY"
		base.ProblemType = "PRIMARY KEY"
		return []model.FlashRow{base}
	}
	noPK := len(meta.indexes) == 0
	update := base.Op == "UPDATE"
	logical := len(ev.Rows)
	if update {
		logical = len(ev.Rows) / 2
	}
	if logical == 0 {
		base.ProblemKind = model.FlashProblemCapture
		return []model.FlashRow{base}
	}
	unsigned := ev.Table.UnsignedMap()
	enums := ev.Table.EnumStrValueMap()
	sets := ev.Table.SetStrValueMap()
	collation := ev.Table.CollationMap()
	out := make([]model.FlashRow, 0, logical)
	for i := 0; i < logical; i++ {
		row := base
		row.Columns = labels
		row.Cols = flashColumnMeta(ev.Table, width)
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
		var problem *model.FlashRow
		if beforeRow != nil {
			row.Before, problem = flashLiterals(ev.Table, labels, beforeRow, jsonCellsAt(docs, beforeIndex), unsigned, enums, sets, collation, schema, table)
		}
		if problem == nil && afterRow != nil {
			row.After, problem = flashLiterals(ev.Table, labels, afterRow, jsonCellsAt(docs, afterIndex), unsigned, enums, sets, collation, schema, table)
		}
		if problem != nil {
			return append(out, *problem)
		}
		row.NonStrict = imageHasEnumZero(ev.Table, row.Before) || imageHasEnumZero(ev.Table, row.After)
		out = append(out, row)
	}
	return out
}

func imageHasEnumZero(table *replication.TableMapEvent, image []string) bool {
	for i, lit := range image {
		if lit == "0" && flashRealType(table, i) == mysql.MYSQL_TYPE_ENUM {
			return true
		}
	}
	return false
}

func flashColumnMeta(table *replication.TableMapEvent, width int) []model.FlashCol {
	if table == nil || width <= 0 {
		return nil
	}
	unsigned := table.UnsignedMap()
	enums := table.EnumStrValueMap()
	sets := table.SetStrValueMap()
	coll := table.CollationMap()
	enumColl := table.EnumSetCollationMap()
	out := make([]model.FlashCol, width)
	for i := 0; i < width; i++ {
		typ := flashRealType(table, i)
		var meta uint16
		if i < len(table.ColumnMeta) {
			meta = table.ColumnMeta[i]
		}
		charset := flashCharset(coll, enumColl, i)
		col := model.FlashCol{Base: flashBaseName(typ, meta, charset), Charset: charset}
		if unsigned != nil {
			if bit, ok := unsigned[i]; ok {
				col.HasSign = true
				col.Unsigned = bit
			}
		}
		if members := enums[i]; len(members) > 0 {
			col.Members = append([]string(nil), members...)
		} else if members := sets[i]; len(members) > 0 {
			col.Members = append([]string(nil), members...)
		}
		switch typ {
		case mysql.MYSQL_TYPE_NEWDECIMAL:
			col.HasPrec = true
			col.Prec = int(meta >> 8)
			col.Scale = int(meta & 0xFF)
		case mysql.MYSQL_TYPE_TIME2, mysql.MYSQL_TYPE_DATETIME2, mysql.MYSQL_TYPE_TIMESTAMP2:
			col.HasFSP = true
			col.FSP = int(meta)
		}
		out[i] = col
	}
	return out
}

func flashCharset(coll, enumColl map[int]uint64, i int) string {
	if id, ok := coll[i]; ok {
		if name, known := collationCharset[id]; known {
			return name
		}
	}
	if id, ok := enumColl[i]; ok {
		if name, known := collationCharset[id]; known {
			return name
		}
	}
	return ""
}

func flashBaseName(typ byte, meta uint16, charset string) string {
	switch typ {
	case mysql.MYSQL_TYPE_TINY:
		return "tinyint"
	case mysql.MYSQL_TYPE_SHORT:
		return "smallint"
	case mysql.MYSQL_TYPE_INT24:
		return "mediumint"
	case mysql.MYSQL_TYPE_LONG:
		return "int"
	case mysql.MYSQL_TYPE_LONGLONG:
		return "bigint"
	case mysql.MYSQL_TYPE_NEWDECIMAL:
		return "decimal"
	case mysql.MYSQL_TYPE_FLOAT:
		return "float"
	case mysql.MYSQL_TYPE_DOUBLE:
		return "double"
	case mysql.MYSQL_TYPE_BIT:
		return "bit"
	case mysql.MYSQL_TYPE_YEAR:
		return "year"
	case mysql.MYSQL_TYPE_DATE, mysql.MYSQL_TYPE_NEWDATE:
		return "date"
	case mysql.MYSQL_TYPE_TIME, mysql.MYSQL_TYPE_TIME2:
		return "time"
	case mysql.MYSQL_TYPE_DATETIME, mysql.MYSQL_TYPE_DATETIME2:
		return "datetime"
	case mysql.MYSQL_TYPE_TIMESTAMP, mysql.MYSQL_TYPE_TIMESTAMP2:
		return "timestamp"
	case mysql.MYSQL_TYPE_JSON:
		return "json"
	case mysql.MYSQL_TYPE_ENUM:
		return "enum"
	case mysql.MYSQL_TYPE_SET:
		return "set"
	case mysql.MYSQL_TYPE_VARCHAR, mysql.MYSQL_TYPE_VAR_STRING:
		if charset == "" {
			return ""
		}
		if charset == "binary" {
			return "varbinary"
		}
		return "varchar"
	case mysql.MYSQL_TYPE_STRING:
		if charset == "" {
			return ""
		}
		if charset == "binary" {
			return "binary"
		}
		return "char"
	case mysql.MYSQL_TYPE_BLOB:
		return blobBaseName(meta, charset)
	case mysql.MYSQL_TYPE_GEOMETRY:
		return "geometry"
	case mysql.MYSQL_TYPE_VECTOR:
		return "vector"
	default:
		return ""
	}
}

func blobBaseName(meta uint16, charset string) string {
	if charset == "" {
		return ""
	}
	prefix := ""
	switch meta {
	case 1:
		prefix = "tiny"
	case 2:
		prefix = ""
	case 3:
		prefix = "medium"
	case 4:
		prefix = "long"
	default:
		return ""
	}
	if charset == "binary" {
		if prefix == "" {
			return "blob"
		}
		return prefix + "blob"
	}
	if prefix == "" {
		return "text"
	}
	return prefix + "text"
}

func flashNamesComplete(table *replication.TableMapEvent, width int) bool {
	if table == nil || width == 0 {
		return false
	}
	names := table.ColumnNameString()
	if len(names) != width {
		return false
	}
	for _, name := range names {
		if name == "" {
			return false
		}
	}
	return true
}

func jsonCellsAt(docs [][]jsonCell, index int) []jsonCell {
	if index < 0 || index >= len(docs) {
		return nil
	}
	return docs[index]
}

func flashLiterals(table *replication.TableMapEvent, labels []string, values []any, cells []jsonCell, unsigned map[int]bool, enums, sets map[int][]string, collation map[int]uint64, schema, tableName string) ([]string, *model.FlashRow) {
	lits := make([]string, len(labels))
	for i := range labels {
		var value any
		if i < len(values) {
			value = values[i]
		}
		var cell jsonCell
		if i < len(cells) {
			cell = cells[i]
		}
		lit, typeProblem := flashLiteral(table, i, value, cell, unsigned, enums, sets, collation)
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

func flashLiteral(table *replication.TableMapEvent, col int, value any, cell jsonCell, unsigned map[int]bool, enums, sets map[int][]string, collation map[int]uint64) (string, string) {
	if value == nil {
		return "NULL", ""
	}
	if _, ok := value.(*replication.JsonDiff); ok {
		return "", "partial JSON"
	}
	typ := flashRealType(table, col)
	switch typ {
	case mysql.MYSQL_TYPE_TINY, mysql.MYSQL_TYPE_SHORT, mysql.MYSQL_TYPE_INT24, mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_LONGLONG:
		bit, ok := false, false
		if unsigned != nil {
			bit, ok = unsigned[col]
		}
		if !ok {
			return "", typeName(typ) + " (signedness is unavailable)"
		}
		text, ok := sqlInteger(value, bit)
		if !ok {
			return "", typeName(typ)
		}
		return text, ""
	case mysql.MYSQL_TYPE_YEAR:
		switch typed := value.(type) {
		case int:
			return strconv.Itoa(typed), ""
		case int64:
			return strconv.FormatInt(typed, 10), ""
		default:
			return "", "YEAR"
		}
	case mysql.MYSQL_TYPE_NEWDECIMAL:
		text, ok := sqlDecimal(value)
		if !ok {
			return "", "DECIMAL"
		}
		return text, ""
	case mysql.MYSQL_TYPE_FLOAT:
		return "", "FLOAT"
	case mysql.MYSQL_TYPE_DOUBLE:
		return "", "DOUBLE"
	case mysql.MYSQL_TYPE_BIT:
		return "", "BIT"
	case mysql.MYSQL_TYPE_GEOMETRY:
		return "", "GEOMETRY"
	case mysql.MYSQL_TYPE_VECTOR:
		return "", "VECTOR"
	case mysql.MYSQL_TYPE_JSON:
		if _, ok := value.(*replication.JsonDiff); ok {
			return "", "partial JSON"
		}
		text, ok := sqlJSON(value, cell)
		if !ok {
			return "", "JSON"
		}
		return text, ""
	case mysql.MYSQL_TYPE_ENUM:
		members := enums[col]
		if len(members) == 0 {
			return "", "ENUM (member list is unavailable)"
		}
		text, ok := sqlEnum(value, members)
		if !ok {
			return "", "ENUM"
		}
		return text, ""
	case mysql.MYSQL_TYPE_SET:
		members := sets[col]
		if len(members) == 0 {
			return "", "SET (member list is unavailable)"
		}
		text, ok := sqlSet(value, members)
		if !ok {
			return "", "SET"
		}
		return text, ""
	case mysql.MYSQL_TYPE_DATE, mysql.MYSQL_TYPE_NEWDATE,
		mysql.MYSQL_TYPE_TIME, mysql.MYSQL_TYPE_TIME2,
		mysql.MYSQL_TYPE_DATETIME, mysql.MYSQL_TYPE_DATETIME2,
		mysql.MYSQL_TYPE_TIMESTAMP, mysql.MYSQL_TYPE_TIMESTAMP2:
		text, ok := sqlTemporal(value)
		if !ok {
			return "", typeName(typ)
		}
		return text, ""
	case mysql.MYSQL_TYPE_VARCHAR, mysql.MYSQL_TYPE_VAR_STRING, mysql.MYSQL_TYPE_STRING, mysql.MYSQL_TYPE_BLOB:
		return sqlCharacter(typ, col, value, collation)
	default:
		return "", typeName(typ)
	}
}

func flashRealType(table *replication.TableMapEvent, i int) byte {
	if table == nil || i < 0 || i >= len(table.ColumnType) {
		return mysql.MYSQL_TYPE_NULL
	}
	typ := table.ColumnType[i]
	switch typ {
	case mysql.MYSQL_TYPE_STRING:
		if i < len(table.ColumnMeta) {
			rtyp := byte(table.ColumnMeta[i] >> 8)
			if rtyp == mysql.MYSQL_TYPE_ENUM || rtyp == mysql.MYSQL_TYPE_SET {
				return rtyp
			}
		}
	case mysql.MYSQL_TYPE_DATE:
		return mysql.MYSQL_TYPE_NEWDATE
	}
	return typ
}

func sqlInteger(value any, unsigned bool) (string, bool) {
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
	if unsigned {
		return strconv.FormatUint(wide, 10), true
	}
	return strconv.FormatInt(signed, 10), true
}

func sqlDecimal(value any) (string, bool) {
	var text string
	switch typed := value.(type) {
	case string:
		text = typed
	case decimal.Decimal:
		text = typed.String()
	default:
		return "", false
	}
	if !decimalLiteral(text) {
		return "", false
	}
	return text, true
}

func decimalLiteral(text string) bool {
	if text == "" {
		return false
	}
	i := 0
	if text[0] == '-' {
		i++
		if i >= len(text) {
			return false
		}
	}
	dot := false
	digits := 0
	for ; i < len(text); i++ {
		switch text[i] {
		case '.':
			if dot {
				return false
			}
			dot = true
		default:
			if text[i] < '0' || text[i] > '9' {
				return false
			}
			digits++
		}
	}
	return digits > 0
}

func sqlJSON(value any, cell jsonCell) (string, bool) {
	if cell.ok {
		return sqlJSONBinary(cell.doc)
	}
	// A []byte here is a binary document supplied by a test or a caller that
	// already sliced the row image. Text from go-mysql is not exact.
	raw, ok := value.([]byte)
	if !ok {
		return "", false
	}
	return sqlJSONBinary(raw)
}

// sqlEnum writes the 1-based member index (0 is the empty member).
// A quoted member name is the column charset, which is not utf8mb4 under SET NAMES utf8mb4.
func sqlEnum(value any, members []string) (string, bool) {
	idx, ok := int64Value(value)
	if !ok || idx < 0 || idx > int64(len(members)) {
		return "", false
	}
	return strconv.FormatInt(idx, 10), true
}

// sqlSet writes the member bitmask. Bit 0 is the first member.
func sqlSet(value any, members []string) (string, bool) {
	bits, ok := uint64Value(value)
	if !ok || len(members) > 64 {
		return "", false
	}
	if len(members) < 64 && bits>>uint(len(members)) != 0 {
		return "", false
	}
	return strconv.FormatUint(bits, 10), true
}

func int64Value(value any) (int64, bool) {
	switch typed := value.(type) {
	case int:
		return int64(typed), true
	case int8:
		return int64(typed), true
	case int16:
		return int64(typed), true
	case int32:
		return int64(typed), true
	case int64:
		return typed, true
	default:
		return 0, false
	}
}

func uint64Value(value any) (uint64, bool) {
	switch typed := value.(type) {
	case int64:
		return uint64(typed), true
	case int32:
		return uint64(uint32(typed)), true
	case int:
		return uint64(typed), true
	case uint64:
		return typed, true
	default:
		return 0, false
	}
}

func sqlTemporal(value any) (string, bool) {
	text, ok := value.(string)
	if !ok || text == "" || !utf8.ValidString(text) {
		return "", false
	}
	return quoteSQLString(text), true
}

func sqlCharacter(typ byte, col int, value any, collation map[int]uint64) (string, string) {
	name := typeName(typ)
	if collation == nil {
		return "", name + " (collation is unavailable)"
	}
	id, ok := collation[col]
	if !ok || id == 0 {
		return "", name + " (collation is unavailable)"
	}
	raw, ok := characterBytes(value)
	if !ok {
		return "", name
	}
	if id == mysqlCollationBinary {
		return hexLiteral(raw), ""
	}
	charset, known := collationCharset[id]
	if !known || charset == "" {
		return "", name + " (charset for collation " + strconv.FormatUint(id, 10) + " is unknown)"
	}
	if charset == "utf8mb4" {
		if !utf8.Valid(raw) {
			return "", name
		}
		return quoteSQLString(string(raw)), ""
	}
	return charsetHexLiteral(charset, raw), ""
}

func characterBytes(value any) ([]byte, bool) {
	switch typed := value.(type) {
	case []byte:
		return typed, true
	case string:
		return []byte(typed), true
	default:
		return nil, false
	}
}

func charsetHexLiteral(charset string, value []byte) string {
	if len(value) == 0 {
		return "_" + charset + " X''"
	}
	return "_" + charset + " 0x" + strings.ToUpper(hex.EncodeToString(value))
}

func hexLiteral(value []byte) string {
	if len(value) == 0 {
		return "X''"
	}
	return "X'" + strings.ToUpper(hex.EncodeToString(value)) + "'"
}

func quoteSQLString(value string) string {
	var b strings.Builder
	b.Grow(len(value) + 2)
	b.WriteByte('\'')
	for i := 0; i < len(value); i++ {
		switch value[i] {
		case 0:
			b.WriteString(`\0`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\\':
			b.WriteString(`\\`)
		case '\'':
			b.WriteString(`\'`)
		case 0x1a:
			b.WriteString(`\Z`)
		default:
			b.WriteByte(value[i])
		}
	}
	b.WriteByte('\'')
	return b.String()
}

func typeName(typ byte) string {
	switch typ {
	case mysql.MYSQL_TYPE_TINY:
		return "TINYINT"
	case mysql.MYSQL_TYPE_SHORT:
		return "SMALLINT"
	case mysql.MYSQL_TYPE_INT24:
		return "MEDIUMINT"
	case mysql.MYSQL_TYPE_LONG:
		return "INT"
	case mysql.MYSQL_TYPE_LONGLONG:
		return "BIGINT"
	case mysql.MYSQL_TYPE_NEWDECIMAL:
		return "DECIMAL"
	case mysql.MYSQL_TYPE_FLOAT:
		return "FLOAT"
	case mysql.MYSQL_TYPE_DOUBLE:
		return "DOUBLE"
	case mysql.MYSQL_TYPE_BIT:
		return "BIT"
	case mysql.MYSQL_TYPE_YEAR:
		return "YEAR"
	case mysql.MYSQL_TYPE_DATE, mysql.MYSQL_TYPE_NEWDATE:
		return "DATE"
	case mysql.MYSQL_TYPE_TIME, mysql.MYSQL_TYPE_TIME2:
		return "TIME"
	case mysql.MYSQL_TYPE_DATETIME, mysql.MYSQL_TYPE_DATETIME2:
		return "DATETIME"
	case mysql.MYSQL_TYPE_TIMESTAMP, mysql.MYSQL_TYPE_TIMESTAMP2:
		return "TIMESTAMP"
	case mysql.MYSQL_TYPE_JSON:
		return "JSON"
	case mysql.MYSQL_TYPE_ENUM:
		return "ENUM"
	case mysql.MYSQL_TYPE_SET:
		return "SET"
	case mysql.MYSQL_TYPE_VARCHAR, mysql.MYSQL_TYPE_VAR_STRING:
		return "VARCHAR"
	case mysql.MYSQL_TYPE_STRING:
		return "CHAR"
	case mysql.MYSQL_TYPE_BLOB:
		return "BLOB"
	case mysql.MYSQL_TYPE_GEOMETRY:
		return "GEOMETRY"
	case mysql.MYSQL_TYPE_VECTOR:
		return "VECTOR"
	default:
		return "type " + strconv.Itoa(int(typ))
	}
}
