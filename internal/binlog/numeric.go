// Package binlog turns a decoded numeric cell into the value MySQL stored.
// input: a go-mysql row value and the binlog real type.
// output: signed and unsigned integer text, a BIT integer, and a BIT SQL literal.
// pos: row-image capture and flashback literals. MEDIUMINT is 24 bits inside an int32.
// note: if this file changes, update this header and README.md.
package binlog

import (
	"strconv"
	"strings"

	"github.com/go-mysql-org/go-mysql/mysql"
)

// integerReadings splits a decoded integer into the signed text and the
// unsigned text of the column's width.
//
// go-mysql sign-extends MEDIUMINT to int32 (ParseBinaryInt24). A uint32
// conversion keeps the extra sign byte, so 9000000 becomes 4287190080.
// The unsigned reading is the low 24 bits. TINYINT and SMALLINT already
// arrive in a Go type of the right width. A typ of 0 uses that Go width,
// which is what mysqlbinlog prints when the column type is an INT.
func integerReadings(value any, typ byte) (signed, unsigned string, ok bool) {
	bits := integerBits(typ)
	var s int64
	var u uint64
	switch typed := value.(type) {
	case int8:
		s = int64(typed)
		u = uint64(uint8(typed))
		if bits == 0 {
			bits = 8
		}
	case int16:
		s = int64(typed)
		u = uint64(uint16(typed))
		if bits == 0 {
			bits = 16
		}
	case int32:
		s = int64(typed)
		u = uint64(uint32(typed))
		if bits == 0 {
			bits = 32
		}
	case int64:
		s = typed
		u = uint64(typed)
		if bits == 0 {
			bits = 64
		}
	default:
		return "", "", false
	}
	if bits > 0 && bits < 64 {
		u &= (uint64(1) << uint(bits)) - 1
	}
	return strconv.FormatInt(s, 10), strconv.FormatUint(u, 10), true
}

// integerBits is the stored width. 0 means the caller did not name a type.
func integerBits(typ byte) int {
	switch typ {
	case mysql.MYSQL_TYPE_TINY:
		return 8
	case mysql.MYSQL_TYPE_SHORT:
		return 16
	case mysql.MYSQL_TYPE_INT24:
		return 24
	case mysql.MYSQL_TYPE_LONG:
		return 32
	case mysql.MYSQL_TYPE_LONGLONG:
		return 64
	default:
		return 0
	}
}

// bitDecimal is the unsigned integer a BIT value holds.
// go-mysql returns int64. BIT(64) with the high bit set is negative, and
// uint64 of that pattern is the value MySQL stores.
func bitDecimal(value any) (string, bool) {
	bits, ok := unsignedBits(value)
	if !ok {
		return "", false
	}
	return strconv.FormatUint(bits, 10), true
}

func unsignedBits(value any) (uint64, bool) {
	switch typed := value.(type) {
	case int64:
		return uint64(typed), true
	case int32:
		return uint64(uint32(typed)), true
	case int16:
		return uint64(uint16(typed)), true
	case int8:
		return uint64(uint8(typed)), true
	case int:
		return uint64(typed), true
	case uint64:
		return typed, true
	default:
		return 0, false
	}
}

// sqlBit is a bit literal of the column width. The numeric value is the
// same one bitDecimal prints. meta is the TABLE_MAP BIT metadata.
func sqlBit(value any, meta uint16) (string, bool) {
	nbits := int(meta>>8)*8 + int(meta&0xFF)
	if nbits <= 0 || nbits > 64 {
		return "", false
	}
	bits, ok := unsignedBits(value)
	if !ok {
		return "", false
	}
	if nbits < 64 && bits>>uint(nbits) != 0 {
		return "", false
	}
	var b strings.Builder
	b.Grow(nbits + 3)
	b.WriteString("b'")
	for i := nbits - 1; i >= 0; i-- {
		if bits&(uint64(1)<<uint(i)) != 0 {
			b.WriteByte('1')
		} else {
			b.WriteByte('0')
		}
	}
	b.WriteByte('\'')
	return b.String(), true
}
