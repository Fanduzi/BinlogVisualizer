package binlog

import (
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"
)

// MySQL binary JSON type tags. See sql-common/json_binary.cc.
const (
	jsonSmallObject byte = 0x00
	jsonLargeObject byte = 0x01
	jsonSmallArray  byte = 0x02
	jsonLargeArray  byte = 0x03
	jsonLiteral     byte = 0x04
	jsonInt16       byte = 0x05
	jsonUint16      byte = 0x06
	jsonInt32       byte = 0x07
	jsonUint32      byte = 0x08
	jsonInt64       byte = 0x09
	jsonUint64      byte = 0x0a
	jsonDouble      byte = 0x0b
	jsonString      byte = 0x0c
	jsonOpaque      byte = 0x0f

	jsonNullLiteral  byte = 0x00
	jsonTrueLiteral  byte = 0x01
	jsonFalseLiteral byte = 0x02
)

// sqlJSONBinary renders one binary JSON document as SQL that stores the same bytes.
// An empty document is the JSON null MySQL writes for a zero-length value.
func sqlJSONBinary(doc []byte) (string, bool) {
	if len(doc) == 0 {
		return "CAST('null' AS JSON)", true
	}
	expr, ok := sqlJSONAt(doc[0], doc[1:], true)
	if !ok {
		return "", false
	}
	return expr, true
}

// sqlJSONAt renders the value that starts with tp. data is the payload after the type byte.
// top is true only for the document root, which must be a JSON value rather than a bare scalar.
func sqlJSONAt(tp byte, data []byte, top bool) (string, bool) {
	switch tp {
	case jsonSmallObject:
		return sqlJSONContainer(data, true, true)
	case jsonLargeObject:
		return sqlJSONContainer(data, false, true)
	case jsonSmallArray:
		return sqlJSONContainer(data, true, false)
	case jsonLargeArray:
		return sqlJSONContainer(data, false, false)
	case jsonLiteral:
		return sqlJSONLiteral(data, top)
	case jsonInt16, jsonUint16, jsonInt32, jsonUint32, jsonInt64, jsonUint64:
		return sqlJSONInt(tp, data, top)
	case jsonDouble:
		return sqlJSONDouble(data, top)
	case jsonString:
		return sqlJSONString(data, top)
	case jsonOpaque:
		return sqlJSONOpaque(data, top)
	default:
		return "", false
	}
}

func sqlJSONContainer(data []byte, small, object bool) (string, bool) {
	width := 2
	if !small {
		width = 4
	}
	if len(data) < 2*width {
		return "", false
	}
	count, ok := jsonCount(data, small)
	if !ok {
		return "", false
	}
	size, ok := jsonCount(data[width:], small)
	if !ok || size < 0 || size > len(data) || count < 0 || count > len(data) {
		return "", false
	}
	keyEntry := 2 + width
	valueEntry := 1 + width
	header := 2*width + count*valueEntry
	if object {
		header += count * keyEntry
	}
	if header > size {
		return "", false
	}
	args := make([]string, 0, count*2)
	for i := 0; i < count; i++ {
		if object {
			entry := 2*width + keyEntry*i
			keyOff, ok := jsonCount(data[entry:], small)
			if !ok || entry+width+2 > len(data) {
				return "", false
			}
			keyLen := int(binary.LittleEndian.Uint16(data[entry+width:]))
			if keyOff < header || keyOff > len(data) || keyLen < 0 || keyOff+keyLen > len(data) {
				return "", false
			}
			key := string(data[keyOff : keyOff+keyLen])
			if !utf8.ValidString(key) {
				return "", false
			}
			args = append(args, quoteSQLString(key))
		}
		entry := 2*width + valueEntry*i
		if object {
			entry += keyEntry * count
		}
		if entry >= len(data) {
			return "", false
		}
		valType := data[entry]
		var expr string
		if jsonInline(valType, small) {
			end := entry + valueEntry
			if end > len(data) {
				return "", false
			}
			expr, ok = sqlJSONNested(valType, data[entry+1:end])
		} else {
			valOff, okOff := jsonCount(data[entry+1:], small)
			if !okOff || valOff < 0 || valOff >= len(data) {
				return "", false
			}
			expr, ok = sqlJSONNested(valType, data[valOff:])
		}
		if !ok {
			return "", false
		}
		args = append(args, expr)
	}
	fn := "JSON_ARRAY"
	if object {
		fn = "JSON_OBJECT"
	}
	return fn + "(" + strings.Join(args, ", ") + ")", true
}

func sqlJSONNested(tp byte, data []byte) (string, bool) {
	return sqlJSONAt(tp, data, false)
}

func jsonInline(tp byte, small bool) bool {
	switch tp {
	case jsonInt16, jsonUint16, jsonLiteral:
		return true
	case jsonInt32, jsonUint32:
		return !small
	default:
		return false
	}
}

func jsonCount(data []byte, small bool) (int, bool) {
	if small {
		if len(data) < 2 {
			return 0, false
		}
		return int(binary.LittleEndian.Uint16(data)), true
	}
	if len(data) < 4 {
		return 0, false
	}
	return int(binary.LittleEndian.Uint32(data)), true
}

func sqlJSONLiteral(data []byte, top bool) (string, bool) {
	if len(data) < 1 {
		return "", false
	}
	var token, text string
	switch data[0] {
	case jsonNullLiteral:
		token, text = "NULL", "null"
	case jsonTrueLiteral:
		token, text = "true", "true"
	case jsonFalseLiteral:
		token, text = "false", "false"
	default:
		return "", false
	}
	if top {
		return "CAST('" + text + "' AS JSON)", true
	}
	return token, true
}

func sqlJSONInt(tp byte, data []byte, top bool) (string, bool) {
	var expr string
	switch tp {
	case jsonInt16:
		if len(data) < 2 {
			return "", false
		}
		v := int64(int16(binary.LittleEndian.Uint16(data)))
		if jsonSignedWidth(v) != tp {
			return "", false
		}
		expr = strconv.FormatInt(v, 10)
	case jsonInt32:
		if len(data) < 4 {
			return "", false
		}
		v := int64(int32(binary.LittleEndian.Uint32(data)))
		if jsonSignedWidth(v) != tp {
			return "", false
		}
		expr = strconv.FormatInt(v, 10)
	case jsonInt64:
		if len(data) < 8 {
			return "", false
		}
		v := int64(binary.LittleEndian.Uint64(data))
		if jsonSignedWidth(v) != tp {
			return "", false
		}
		expr = strconv.FormatInt(v, 10)
	case jsonUint16:
		if len(data) < 2 {
			return "", false
		}
		v := uint64(binary.LittleEndian.Uint16(data))
		if jsonUnsignedWidth(v) != tp {
			return "", false
		}
		expr = "CAST(" + strconv.FormatUint(v, 10) + " AS UNSIGNED)"
	case jsonUint32:
		if len(data) < 4 {
			return "", false
		}
		v := uint64(binary.LittleEndian.Uint32(data))
		if jsonUnsignedWidth(v) != tp {
			return "", false
		}
		expr = "CAST(" + strconv.FormatUint(v, 10) + " AS UNSIGNED)"
	case jsonUint64:
		if len(data) < 8 {
			return "", false
		}
		v := binary.LittleEndian.Uint64(data)
		if jsonUnsignedWidth(v) != tp {
			return "", false
		}
		expr = "CAST(" + strconv.FormatUint(v, 10) + " AS UNSIGNED)"
	default:
		return "", false
	}
	if top {
		return "CAST(" + expr + " AS JSON)", true
	}
	return expr, true
}

// MySQL stores a JSON integer in the narrowest signed or unsigned binary type.
func jsonSignedWidth(v int64) byte {
	if v >= math.MinInt16 && v <= math.MaxInt16 {
		return jsonInt16
	}
	if v >= math.MinInt32 && v <= math.MaxInt32 {
		return jsonInt32
	}
	return jsonInt64
}

func jsonUnsignedWidth(v uint64) byte {
	if v <= math.MaxUint16 {
		return jsonUint16
	}
	if v <= math.MaxUint32 {
		return jsonUint32
	}
	return jsonUint64
}

func sqlJSONDouble(data []byte, top bool) (string, bool) {
	if len(data) < 8 {
		return "", false
	}
	f := math.Float64frombits(binary.LittleEndian.Uint64(data[:8]))
	expr, ok := sqlDoubleLiteral(f)
	if !ok {
		return "", false
	}
	if top {
		return "CAST(" + expr + " AS JSON)", true
	}
	return expr, true
}

func sqlDoubleLiteral(f float64) (string, bool) {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return "", false
	}
	if f == 0 {
		if math.Signbit(f) {
			return "-0.0E0", true
		}
		return "0.0E0", true
	}
	return strconv.FormatFloat(f, 'e', -1, 64), true
}

func sqlJSONString(data []byte, top bool) (string, bool) {
	n, width, ok := jsonVarLen(data)
	if !ok || width+n > len(data) {
		return "", false
	}
	text := string(data[width : width+n])
	if !utf8.ValidString(text) {
		return "", false
	}
	if top {
		return "CAST(" + quoteSQLString(jsonTextString(text)) + " AS JSON)", true
	}
	return quoteSQLString(text), true
}

func jsonTextString(s string) string {
	var b strings.Builder
	b.Grow(len(s) + 2)
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		default:
			if r < 0x20 {
				fmt.Fprintf(&b, `\u%04x`, r)
			} else {
				b.WriteRune(r)
			}
		}
	}
	b.WriteByte('"')
	return b.String()
}

func jsonVarLen(data []byte) (length, width int, ok bool) {
	if len(data) == 0 {
		return 0, 0, false
	}
	maxCount := len(data)
	if maxCount > 5 {
		maxCount = 5
	}
	var n uint64
	for pos := 0; pos < maxCount; pos++ {
		v := data[pos]
		n |= uint64(v&0x7f) << (7 * pos)
		if v&0x80 == 0 {
			if n > math.MaxUint32 {
				return 0, 0, false
			}
			return int(n), pos + 1, true
		}
	}
	return 0, 0, false
}

func sqlJSONOpaque(data []byte, top bool) (string, bool) {
	if len(data) < 1 {
		return "", false
	}
	field := data[0]
	n, width, ok := jsonVarLen(data[1:])
	if !ok || 1+width+n > len(data) {
		return "", false
	}
	payload := data[1+width : 1+width+n]
	var expr string
	switch field {
	case mysqlTypeNewDecimal:
		expr, ok = sqlJSONDecimal(payload)
	case mysqlTypeDate:
		expr, ok = sqlJSONDate(payload)
	case mysqlTypeTime:
		expr, ok = sqlJSONTime(payload)
	case mysqlTypeDatetime:
		expr, ok = sqlJSONDatetime(payload)
	default:
		return "", false
	}
	if !ok {
		return "", false
	}
	if top {
		return "CAST(" + expr + " AS JSON)", true
	}
	return expr, true
}

// Opaque field types stored inside binary JSON. These match the MySQL type codes,
// which are not the binlog row-image type codes.
const (
	mysqlTypeNewDecimal byte = 0xf6
	mysqlTypeDate       byte = 0x0a
	mysqlTypeTime       byte = 0x0b
	mysqlTypeDatetime   byte = 0x0c
)

func sqlJSONDecimal(payload []byte) (string, bool) {
	if len(payload) < 2 {
		return "", false
	}
	precision := int(payload[0])
	scale := int(payload[1])
	if precision < 1 || precision > 65 || scale < 0 || scale > precision {
		return "", false
	}
	text, ok := jsonDecimalDigits(payload[2:], precision, scale)
	if !ok {
		return "", false
	}
	body := text
	if strings.HasPrefix(body, "-") {
		body = body[1:]
	}
	intPart, frac, _ := strings.Cut(body, ".")
	if intPart == "" {
		intPart = "0"
	}
	intDigits := len(intPart)
	literalP := intDigits + scale
	groups := (intDigits + 8) / 9
	if groups < 1 {
		groups = 1
	}
	castP := groups*9 + scale
	digits := intPart
	if scale > 0 {
		if len(frac) > scale {
			return "", false
		}
		if len(frac) < scale {
			frac += strings.Repeat("0", scale-len(frac))
		}
		digits = intPart + "." + frac
	}
	if strings.HasPrefix(text, "-") {
		digits = "-" + digits
	}
	switch {
	case scale > 0 && precision == literalP:
		return digits, true
	case precision == castP:
		return "CAST(" + quoteSQLString(digits) + " AS DECIMAL(" + strconv.Itoa(precision) + "," + strconv.Itoa(scale) + "))", true
	default:
		return "", false
	}
}

var jsonDecimalSizes = []int{0, 1, 1, 2, 2, 3, 3, 4, 4, 4}

func jsonDecimalDigits(data []byte, precision, scale int) (string, bool) {
	integral := precision - scale
	uncompIntegral := integral / 9
	uncompFractional := scale / 9
	compIntegral := integral - uncompIntegral*9
	compFractional := scale - uncompFractional*9
	if compIntegral < 0 || compIntegral > 9 || compFractional < 0 || compFractional > 9 {
		return "", false
	}
	binSize := uncompIntegral*4 + jsonDecimalSizes[compIntegral] + uncompFractional*4 + jsonDecimalSizes[compFractional]
	if binSize == 0 || len(data) < binSize {
		return "", false
	}
	buf := append([]byte(nil), data[:binSize]...)
	var mask uint32
	if buf[0]&0x80 == 0 {
		mask = 0xffffffff
	}
	buf[0] ^= 0x80
	var b strings.Builder
	if mask != 0 {
		b.WriteByte('-')
	}
	mask8 := uint8(mask)
	pos, value := jsonDecimalGroup(compIntegral, buf, mask8)
	leading := true
	if value != 0 {
		leading = false
		b.WriteString(strconv.FormatUint(uint64(value), 10))
	}
	for range uncompIntegral {
		if pos+4 > len(buf) {
			return "", false
		}
		value = binary.BigEndian.Uint32(buf[pos:]) ^ mask
		pos += 4
		piece := strconv.FormatUint(uint64(value), 10)
		if leading {
			if value != 0 {
				leading = false
				b.WriteString(piece)
			}
			continue
		}
		b.WriteString(strings.Repeat("0", 9-len(piece)))
		b.WriteString(piece)
	}
	if leading {
		b.WriteByte('0')
	}
	if scale == 0 {
		return b.String(), true
	}
	b.WriteByte('.')
	for range uncompFractional {
		if pos+4 > len(buf) {
			return "", false
		}
		value = binary.BigEndian.Uint32(buf[pos:]) ^ mask
		pos += 4
		piece := strconv.FormatUint(uint64(value), 10)
		b.WriteString(strings.Repeat("0", 9-len(piece)))
		b.WriteString(piece)
	}
	if compFractional > 0 {
		if pos >= len(buf) {
			return "", false
		}
		size, value := jsonDecimalGroup(compFractional, buf[pos:], mask8)
		if size == 0 {
			return "", false
		}
		piece := strconv.FormatUint(uint64(value), 10)
		if pad := compFractional - len(piece); pad > 0 {
			b.WriteString(strings.Repeat("0", pad))
		}
		b.WriteString(piece)
	}
	return b.String(), true
}

func jsonDecimalGroup(comp int, data []byte, mask uint8) (int, uint32) {
	size := jsonDecimalSizes[comp]
	if len(data) < size {
		return 0, 0
	}
	var value uint32
	switch size {
	case 1:
		value = uint32(data[0] ^ mask)
	case 2:
		value = uint32(data[1]^mask) | uint32(data[0]^mask)<<8
	case 3:
		value = uint32(data[2]^mask) | uint32(data[1]^mask)<<8 | uint32(data[0]^mask)<<16
	case 4:
		value = uint32(data[3]^mask) | uint32(data[2]^mask)<<8 | uint32(data[1]^mask)<<16 | uint32(data[0]^mask)<<24
	}
	return size, value
}

func sqlJSONDate(payload []byte) (string, bool) {
	year, month, day, _, _, _, _, ok := jsonPackedTime(payload)
	if !ok {
		return "", false
	}
	return "CAST(" + quoteSQLString(fmt.Sprintf("%04d-%02d-%02d", year, month, day)) + " AS DATE)", true
}

func sqlJSONTime(payload []byte) (string, bool) {
	if len(payload) < 8 {
		return "", false
	}
	v := int64(binary.LittleEndian.Uint64(payload[:8]))
	sign := ""
	if v < 0 {
		sign = "-"
		v = -v
	}
	intPart := v >> 24
	hour := (intPart >> 12) % (1 << 10)
	minute := (intPart >> 6) % (1 << 6)
	second := intPart % (1 << 6)
	frac := v % (1 << 24)
	text := fmt.Sprintf("%s%02d:%02d:%02d.%06d", sign, hour, minute, second, frac)
	return "CAST(" + quoteSQLString(text) + " AS TIME(6))", true
}

func sqlJSONDatetime(payload []byte) (string, bool) {
	year, month, day, hour, minute, second, frac, ok := jsonPackedTime(payload)
	if !ok {
		return "", false
	}
	text := fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d.%06d", year, month, day, hour, minute, second, frac)
	return "CAST(" + quoteSQLString(text) + " AS DATETIME(6))", true
}

func jsonPackedTime(payload []byte) (year, month, day, hour, minute, second, frac int, ok bool) {
	if len(payload) < 8 {
		return 0, 0, 0, 0, 0, 0, 0, false
	}
	v := int64(binary.LittleEndian.Uint64(payload[:8]))
	if v < 0 {
		v = -v
	}
	intPart := v >> 24
	ymd := intPart >> 17
	ym := ymd >> 5
	hms := intPart % (1 << 17)
	year = int(ym / 13)
	month = int(ym % 13)
	day = int(ymd % (1 << 5))
	hour = int(hms >> 12)
	minute = int((hms >> 6) % (1 << 6))
	second = int(hms % (1 << 6))
	frac = int(v % (1 << 24))
	return year, month, day, hour, minute, second, frac, true
}
