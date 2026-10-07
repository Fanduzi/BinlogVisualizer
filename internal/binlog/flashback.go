// Package binlog formats exact SQL literals from an already-decoded rows event.
// input: go-mysql RowsEvent values and FULL row metadata (names, signedness, collation, enum/set members, primary key).
// output: model.FlashRow values for undo SQL, or a problem that names why a row cannot be rendered exactly.
// pos: parser helper used only when flashback capture is on. It reuses the decoded row images from the same RowsEvent as display capture.
// note: if this file changes, update this header and README.md.
package binlog

import (
	"encoding/hex"
	"encoding/json"
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
		row.NoPK = noPK
		row.PK = append([]int(nil), meta.indexes...)
		var beforeRow, afterRow []any
		var beforeSkips, afterSkips []int
		switch {
		case update:
			beforeRow = ev.Rows[i*2]
			afterRow = ev.Rows[i*2+1]
			beforeSkips = skipsAt(ev.SkippedColumns, i*2)
			afterSkips = skipsAt(ev.SkippedColumns, i*2+1)
		case row.Op == "DELETE":
			beforeRow = ev.Rows[i]
			beforeSkips = skipsAt(ev.SkippedColumns, i)
		default:
			afterRow = ev.Rows[i]
			afterSkips = skipsAt(ev.SkippedColumns, i)
		}
		if len(beforeSkips) > 0 || len(afterSkips) > 0 {
			row.ProblemKind = model.FlashProblemImage
			return append(out, row)
		}
		var problem *model.FlashRow
		if beforeRow != nil {
			row.Before, problem = flashLiterals(ev.Table, labels, beforeRow, unsigned, enums, sets, collation, schema, table)
		}
		if problem == nil && afterRow != nil {
			row.After, problem = flashLiterals(ev.Table, labels, afterRow, unsigned, enums, sets, collation, schema, table)
		}
		if problem != nil {
			return append(out, *problem)
		}
		out = append(out, row)
	}
	return out
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

func flashLiterals(table *replication.TableMapEvent, labels []string, values []any, unsigned map[int]bool, enums, sets map[int][]string, collation map[int]uint64, schema, tableName string) ([]string, *model.FlashRow) {
	lits := make([]string, len(labels))
	for i := range labels {
		var value any
		if i < len(values) {
			value = values[i]
		}
		lit, typeProblem := flashLiteral(table, i, value, unsigned, enums, sets, collation)
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

func flashLiteral(table *replication.TableMapEvent, col int, value any, unsigned map[int]bool, enums, sets map[int][]string, collation map[int]uint64) (string, string) {
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
		text, ok := sqlJSON(value)
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

func sqlJSON(value any) (string, bool) {
	switch typed := value.(type) {
	case []byte:
		if len(typed) == 0 {
			return "CAST('null' AS JSON)", true
		}
		if !json.Valid(typed) || !utf8.Valid(typed) {
			return "", false
		}
		return "CAST(" + quoteSQLString(string(typed)) + " AS JSON)", true
	case string:
		if !json.Valid([]byte(typed)) || !utf8.ValidString(typed) {
			return "", false
		}
		return "CAST(" + quoteSQLString(typed) + " AS JSON)", true
	default:
		return "", false
	}
}

func sqlEnum(value any, members []string) (string, bool) {
	idx, ok := int64Value(value)
	if !ok || idx < 0 || idx > int64(len(members)) {
		return "", false
	}
	if idx == 0 {
		return "''", true
	}
	return quoteSQLString(members[idx-1]), true
}

func sqlSet(value any, members []string) (string, bool) {
	bits, ok := uint64Value(value)
	if !ok || len(members) > 64 {
		return "", false
	}
	if len(members) < 64 && bits>>uint(len(members)) != 0 {
		return "", false
	}
	if bits == 0 {
		return "''", true
	}
	parts := make([]string, 0, len(members))
	for i, member := range members {
		if bits&(uint64(1)<<uint(i)) != 0 {
			parts = append(parts, member)
		}
	}
	return quoteSQLString(strings.Join(parts, ",")), true
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
	binary := id == mysqlCollationBinary
	switch typed := value.(type) {
	case []byte:
		if binary {
			return hexLiteral(typed), ""
		}
		if !utf8.Valid(typed) {
			return "", name
		}
		return quoteSQLString(string(typed)), ""
	case string:
		if binary {
			return hexLiteral([]byte(typed)), ""
		}
		if !utf8.ValidString(typed) {
			return "", name
		}
		return quoteSQLString(typed), ""
	default:
		return "", name
	}
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
