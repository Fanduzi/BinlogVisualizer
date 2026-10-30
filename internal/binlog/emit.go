// Package binlog renders one decoded cell value as an exact SQL literal.
// input: a per-column emit descriptor (real type, meta, signedness, members, charset) and the decoded value plus its binary JSON document.
// output: the literal, or a problem string naming what could not be rendered exactly.
// pos: the shared cell formatter. flashback.go resolves table-map facts into emitCol for undo; internal/redo resolves schema-merged facts into it for forward SQL (ADR 0022), so both directions render through one dispatch.
// note: if this file changes, update this header and README.md.
package binlog

import (
	"strconv"
	"unicode/utf8"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"binlogviz/internal/model"
)

// emitCol is one column's literal-rendering facts. The flashback adapter
// fills it from TABLE_MAP maps; the redo adapter fills it from a
// schema-merged model.FlashCol. A missing fact is a refusal, not a guess.
type emitCol struct {
	typ      byte     // real type: STRING resolved to ENUM/SET, DATE to NEWDATE
	meta     uint16   // ColumnMeta cell (BIT width, STRING width, FSP, prec/scale)
	hasSign  bool     // signedness bitmap had this column
	unsigned bool     // the column is unsigned when hasSign
	members  []string // ENUM/SET member list
	charset  string   // resolved charset name; "binary" emits hex
	collErr  string   // parenthetical for the charset problem text
}

// EmitLiteral renders one decoded value as an exact SQL literal for redo
// output. col carries the merged column facts (table map, schema-filled, or
// both); doc is the binary JSON document captured beside the row image, nil
// when the column is not JSON. A non-empty problem return is a refusal
// reason, not a renderable literal.
func EmitLiteral(col model.FlashCol, value any, doc []byte) (string, string) {
	ec := emitCol{
		typ:      col.Typ,
		meta:     col.Meta,
		hasSign:  col.HasSign,
		unsigned: col.Unsigned,
		members:  col.Members,
		charset:  col.Charset,
	}
	if col.Binary {
		ec.charset = "binary"
	}
	switch ec.typ {
	case mysql.MYSQL_TYPE_VARCHAR, mysql.MYSQL_TYPE_VAR_STRING, mysql.MYSQL_TYPE_STRING, mysql.MYSQL_TYPE_BLOB:
		if ec.charset == "" {
			ec.collErr = "collation is unavailable"
		}
	}
	return emitLiteral(ec, value, jsonCell{doc: doc, ok: doc != nil})
}

// emitLiteral is the shared per-type dispatch. Problems name what could not
// be rendered; callers wrap them in their own refusal type.
func emitLiteral(col emitCol, value any, cell jsonCell) (string, string) {
	if value == nil {
		return "NULL", ""
	}
	if _, ok := value.(*replication.JsonDiff); ok {
		return "", "partial JSON"
	}
	switch col.typ {
	case mysql.MYSQL_TYPE_TINY, mysql.MYSQL_TYPE_SHORT, mysql.MYSQL_TYPE_INT24, mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_LONGLONG:
		if !col.hasSign {
			return "", typeName(col.typ) + " (signedness is unavailable)"
		}
		text, ok := sqlInteger(value, col.typ, col.unsigned)
		if !ok {
			return "", typeName(col.typ)
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
		text, ok := sqlBit(value, col.meta)
		if !ok {
			return "", "BIT"
		}
		return text, ""
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
		if len(col.members) == 0 {
			return "", "ENUM (member list is unavailable)"
		}
		text, ok := sqlEnum(value, col.members)
		if !ok {
			return "", "ENUM"
		}
		return text, ""
	case mysql.MYSQL_TYPE_SET:
		if len(col.members) == 0 {
			return "", "SET (member list is unavailable)"
		}
		text, ok := sqlSet(value, col.members)
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
			return "", typeName(col.typ)
		}
		return text, ""
	case mysql.MYSQL_TYPE_VARCHAR, mysql.MYSQL_TYPE_VAR_STRING, mysql.MYSQL_TYPE_STRING, mysql.MYSQL_TYPE_BLOB:
		name := typeName(col.typ)
		if col.collErr != "" {
			return "", name + " (" + col.collErr + ")"
		}
		raw, ok := characterBytes(value)
		if !ok {
			return "", name
		}
		if col.charset == "binary" {
			return hexLiteral(padBinary(col.typ, col.meta, raw)), ""
		}
		if col.charset == "utf8mb4" {
			if !utf8.Valid(raw) {
				return "", name
			}
			return quoteSQLString(string(raw)), ""
		}
		return charsetHexLiteral(col.charset, raw), ""
	default:
		return "", typeName(col.typ)
	}
}
