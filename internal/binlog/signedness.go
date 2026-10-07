// Package binlog reads the TABLE_MAP SIGNEDNESS bitmap.
// input: a decoded TableMapEvent and its raw SignednessBitmap.
// output: column index to unsigned, counting YEAR and old DECIMAL the way MySQL writes the bitmap.
// pos: row-image capture, flashback literals, schema-file column metadata, and primary-key text.
// note: if this file changes, update this header and README.md.
package binlog

import (
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

// unsignedMap maps a column index to the unsigned flag in the TABLE_MAP
// SIGNEDNESS bitmap.
//
// MySQL writes one bit per column for which has_signedess_information_type
// is true (sql/field_common_properties.h). That is the historical
// is_numeric_type list: TINY, SHORT, INT24, LONG, LONGLONG, FLOAT, DOUBLE,
// DECIMAL, NEWDECIMAL, and YEAR. Bits are MSB-first in column order, the
// same walk as parse_signedness in rows_event.cpp. go-mysql v1.14 and the
// IsNumericColumn used by TableMapEvent.UnsignedMap omit YEAR and
// MYSQL_TYPE_DECIMAL, so every numeric column after a YEAR takes its
// neighbour's bit. nil means the event has no signedness bitmap.
func unsignedMap(table *replication.TableMapEvent) map[int]bool {
	if table == nil || len(table.SignednessBitmap) == 0 {
		return nil
	}
	ret := make(map[int]bool)
	i := 0
	for _, field := range table.SignednessBitmap {
		for c := byte(0x80); c != 0; {
			if i >= int(table.ColumnCount) {
				return ret
			}
			if signednessBit(table, i) {
				ret[i] = field&c != 0
				c >>= 1
			}
			i++
		}
	}
	return ret
}

// signednessBit reports whether this column consumes one SIGNEDNESS bit.
// The type is the binlog real type, so ENUM and SET stored as
// MYSQL_TYPE_STRING do not consume a bit.
func signednessBit(table *replication.TableMapEvent, i int) bool {
	switch flashRealType(table, i) {
	case mysql.MYSQL_TYPE_TINY,
		mysql.MYSQL_TYPE_SHORT,
		mysql.MYSQL_TYPE_INT24,
		mysql.MYSQL_TYPE_LONG,
		mysql.MYSQL_TYPE_LONGLONG,
		mysql.MYSQL_TYPE_FLOAT,
		mysql.MYSQL_TYPE_DOUBLE,
		mysql.MYSQL_TYPE_DECIMAL,
		mysql.MYSQL_TYPE_NEWDECIMAL,
		mysql.MYSQL_TYPE_YEAR:
		return true
	default:
		return false
	}
}
