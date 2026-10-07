package binlog

import (
	"encoding/binary"
	"reflect"
	"sync"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

// jsonCell is the binary JSON document for one column of one row image.
// ok is false when the column is not JSON, the value is NULL, or the bytes were not captured.
type jsonCell struct {
	ok  bool
	doc []byte
}

// flashJSON keeps binary JSON documents for the RowsEvent currently being decoded.
// go-mysql replaces those bytes with text before the event is returned.
var flashJSON sync.Map

func rememberFlashJSON(ev *replication.RowsEvent, docs [][]jsonCell) {
	if ev == nil || docs == nil {
		return
	}
	flashJSON.Store(ev, docs)
}

func recallFlashJSON(ev *replication.RowsEvent) [][]jsonCell {
	if ev == nil {
		return nil
	}
	v, ok := flashJSON.LoadAndDelete(ev)
	if !ok {
		return nil
	}
	docs, _ := v.([][]jsonCell)
	return docs
}

// decodeRowsKeepJSON is the flashback rows decoder. It decodes normally, then
// keeps each JSON column's binary document for exact undo SQL.
func decodeRowsKeepJSON(re *replication.RowsEvent, data []byte) error {
	if err := re.Decode(data); err != nil {
		return err
	}
	if docs, ok := extractFlashJSON(re, data); ok {
		rememberFlashJSON(re, docs)
	}
	return nil
}

type rowsDecodeMeta struct {
	tableIDSize int
	version     int
	needBitmap2 bool
	compressed  bool
	partial     bool
}

func rowsDecodeMetaOf(re *replication.RowsEvent) (rowsDecodeMeta, bool) {
	if re == nil {
		return rowsDecodeMeta{}, false
	}
	v := reflect.ValueOf(re).Elem()
	tableID := v.FieldByName("tableIDSize")
	version := v.FieldByName("Version")
	need2 := v.FieldByName("needBitmap2")
	compressed := v.FieldByName("compressed")
	eventType := v.FieldByName("eventType")
	if !tableID.IsValid() || !version.IsValid() || !need2.IsValid() || !compressed.IsValid() || !eventType.IsValid() {
		return rowsDecodeMeta{}, false
	}
	meta := rowsDecodeMeta{
		tableIDSize: int(tableID.Int()),
		version:     int(version.Int()),
		needBitmap2: need2.Bool(),
		compressed:  compressed.Bool(),
		partial:     replication.EventType(eventType.Uint()) == replication.PARTIAL_UPDATE_ROWS_EVENT,
	}
	if meta.tableIDSize != 4 && meta.tableIDSize != 6 {
		return rowsDecodeMeta{}, false
	}
	return meta, true
}

// extractFlashJSON walks the raw row images and copies JSON documents.
// ok is false when the walk cannot be trusted; callers then refuse JSON instead of guessing.
func extractFlashJSON(re *replication.RowsEvent, data []byte) ([][]jsonCell, bool) {
	if re == nil || re.Table == nil || len(re.Rows) == 0 {
		return nil, false
	}
	meta, ok := rowsDecodeMetaOf(re)
	if !ok || meta.compressed {
		return nil, false
	}
	pos, ok := rowsImageStart(data, re, meta)
	if !ok {
		return nil, false
	}
	docs := make([][]jsonCell, len(re.Rows))
	for r := range re.Rows {
		bitmap := re.ColumnBitmap1
		partial := false
		if meta.needBitmap2 && r%2 == 1 {
			bitmap = re.ColumnBitmap2
			partial = meta.partial
		}
		n, cells, ok := walkRowImage(data[pos:], re, bitmap, partial)
		if !ok {
			return nil, false
		}
		docs[r] = cells
		pos += n
	}
	if pos != len(data) {
		return nil, false
	}
	return docs, true
}

func rowsImageStart(data []byte, re *replication.RowsEvent, meta rowsDecodeMeta) (int, bool) {
	pos := meta.tableIDSize + 2
	if meta.version == 2 {
		if pos+2 > len(data) {
			return 0, false
		}
		dataLen := int(binary.LittleEndian.Uint16(data[pos:]))
		pos += 2
		if dataLen < 2 || pos+(dataLen-2) > len(data) {
			return 0, false
		}
		pos += dataLen - 2
	}
	_, _, n := mysql.LengthEncodedInt(data[pos:])
	if n <= 0 || pos+n > len(data) {
		return 0, false
	}
	pos += n
	bits := int(re.ColumnCount+7) / 8
	if pos+bits > len(data) {
		return 0, false
	}
	pos += bits
	if meta.needBitmap2 {
		if pos+bits > len(data) {
			return 0, false
		}
		pos += bits
	}
	return pos, true
}

func walkRowImage(data []byte, re *replication.RowsEvent, bitmap []byte, partial bool) (int, []jsonCell, bool) {
	if re.Table == nil || int(re.ColumnCount) != len(re.Table.ColumnType) {
		return 0, nil, false
	}
	pos := 0
	var partialBitmap []byte
	if partial {
		opt, _, n := mysql.LengthEncodedInt(data[pos:])
		if n <= 0 || pos+n > len(data) {
			return 0, nil, false
		}
		pos += n
		if opt&uint64(replication.EnumBinlogRowValueOptionsPartialJsonUpdates) != 0 {
			byteCount := int(re.Table.JsonColumnCount()+7) / 8
			if pos+byteCount > len(data) {
				return 0, nil, false
			}
			partialBitmap = data[pos : pos+byteCount]
			pos += byteCount
		}
	}
	present := 0
	for i := 0; i < int(re.ColumnCount); i++ {
		if bitSet(bitmap, i) {
			present++
		}
	}
	nullLen := (present + 7) / 8
	if pos+nullLen > len(data) {
		return 0, nil, false
	}
	nullBitmap := data[pos : pos+nullLen]
	pos += nullLen
	cells := make([]jsonCell, re.ColumnCount)
	nullIdx := 0
	partialIdx := 0
	for i := 0; i < int(re.ColumnCount); i++ {
		isPartial := false
		if partialBitmap != nil && re.Table.ColumnType[i] == mysql.MYSQL_TYPE_JSON {
			isPartial = bitSet(partialBitmap, partialIdx)
			partialIdx++
		}
		if !bitSet(bitmap, i) {
			continue
		}
		if bitSet(nullBitmap, nullIdx) {
			nullIdx++
			continue
		}
		nullIdx++
		if re.Table.ColumnType[i] == mysql.MYSQL_TYPE_JSON && !isPartial {
			meta := 0
			if i < len(re.Table.ColumnMeta) {
				meta = int(re.Table.ColumnMeta[i])
			}
			if meta <= 0 || pos+meta > len(data) {
				return 0, nil, false
			}
			length := int(mysql.FixedLengthInt(data[pos : pos+meta]))
			pos += meta
			if length < 0 || pos+length > len(data) {
				return 0, nil, false
			}
			cells[i] = jsonCell{ok: true, doc: append([]byte(nil), data[pos:pos+length]...)}
			pos += length
			continue
		}
		n, ok := cellSize(data[pos:], re.Table.ColumnType[i], columnMeta(re.Table, i))
		if !ok {
			return 0, nil, false
		}
		pos += n
	}
	return pos, cells, true
}

func columnMeta(table *replication.TableMapEvent, i int) uint16 {
	if table == nil || i < 0 || i >= len(table.ColumnMeta) {
		return 0
	}
	return table.ColumnMeta[i]
}

func bitSet(bitmap []byte, i int) bool {
	if i < 0 || i>>3 >= len(bitmap) {
		return false
	}
	return bitmap[i>>3]&(1<<uint(i&7)) != 0
}

func cellSize(data []byte, tp byte, meta uint16) (int, bool) {
	orig := meta
	length := 0
	if tp == mysql.MYSQL_TYPE_STRING {
		if meta >= 256 {
			b0 := uint8(meta >> 8)
			b1 := uint8(meta & 0xFF)
			if b0&0x30 != 0x30 {
				length = int(uint16(b1) | (uint16((b0&0x30)^0x30) << 4))
				tp = b0 | 0x30
			} else {
				length = int(meta & 0xFF)
				tp = b0
			}
		} else {
			length = int(meta)
		}
	}
	need := func(n int) (int, bool) {
		if n < 0 || len(data) < n {
			return 0, false
		}
		return n, true
	}
	switch tp {
	case mysql.MYSQL_TYPE_NULL:
		return 0, true
	case mysql.MYSQL_TYPE_TINY, mysql.MYSQL_TYPE_YEAR:
		return need(1)
	case mysql.MYSQL_TYPE_SHORT:
		return need(2)
	case mysql.MYSQL_TYPE_INT24, mysql.MYSQL_TYPE_TIME:
		return need(3)
	case mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_FLOAT, mysql.MYSQL_TYPE_TIMESTAMP:
		return need(4)
	case mysql.MYSQL_TYPE_LONGLONG, mysql.MYSQL_TYPE_DOUBLE, mysql.MYSQL_TYPE_DATETIME:
		return need(8)
	case mysql.MYSQL_TYPE_TIMESTAMP2:
		return need(4 + int((meta+1)/2))
	case mysql.MYSQL_TYPE_DATETIME2:
		return need(5 + int((meta+1)/2))
	case mysql.MYSQL_TYPE_TIME2:
		return need(3 + int((meta+1)/2))
	case mysql.MYSQL_TYPE_DATE, mysql.MYSQL_TYPE_NEWDATE:
		return need(3)
	case mysql.MYSQL_TYPE_NEWDECIMAL:
		prec := int(meta >> 8)
		scale := int(meta & 0xFF)
		if prec < 1 || scale > prec {
			return 0, false
		}
		return need(decimalBinarySize(prec, scale))
	case mysql.MYSQL_TYPE_BIT:
		nbits := int((meta>>8)*8 + (meta & 0xFF))
		n := (nbits + 7) / 8
		if n == 0 {
			return 0, false
		}
		return need(n)
	case mysql.MYSQL_TYPE_ENUM:
		l := int(orig & 0xFF)
		if l != 1 && l != 2 {
			return 0, false
		}
		return need(l)
	case mysql.MYSQL_TYPE_SET:
		n := int(orig & 0xFF)
		if n <= 0 {
			return 0, false
		}
		return need(n)
	case mysql.MYSQL_TYPE_BLOB, mysql.MYSQL_TYPE_GEOMETRY, mysql.MYSQL_TYPE_VECTOR:
		return blobSize(data, meta)
	case mysql.MYSQL_TYPE_VARCHAR, mysql.MYSQL_TYPE_VAR_STRING:
		return stringSize(data, int(meta))
	case mysql.MYSQL_TYPE_STRING:
		return stringSize(data, length)
	case mysql.MYSQL_TYPE_JSON:
		m := int(meta)
		if m <= 0 || len(data) < m {
			return 0, false
		}
		n := int(mysql.FixedLengthInt(data[:m]))
		if n < 0 || len(data) < m+n {
			return 0, false
		}
		return m + n, true
	default:
		return 0, false
	}
}

func decimalBinarySize(precision, scale int) int {
	integral := precision - scale
	uncompIntegral := integral / 9
	uncompFractional := scale / 9
	compIntegral := integral - uncompIntegral*9
	compFractional := scale - uncompFractional*9
	return uncompIntegral*4 + jsonDecimalSizes[compIntegral] + uncompFractional*4 + jsonDecimalSizes[compFractional]
}

func blobSize(data []byte, meta uint16) (int, bool) {
	switch meta {
	case 1:
		if len(data) < 1 {
			return 0, false
		}
		n := int(data[0])
		if len(data) < 1+n {
			return 0, false
		}
		return 1 + n, true
	case 2:
		if len(data) < 2 {
			return 0, false
		}
		n := int(binary.LittleEndian.Uint16(data))
		if len(data) < 2+n {
			return 0, false
		}
		return 2 + n, true
	case 3:
		if len(data) < 3 {
			return 0, false
		}
		n := int(mysql.FixedLengthInt(data[:3]))
		if len(data) < 3+n {
			return 0, false
		}
		return 3 + n, true
	case 4:
		if len(data) < 4 {
			return 0, false
		}
		n := int(binary.LittleEndian.Uint32(data))
		if n < 0 || len(data) < 4+n {
			return 0, false
		}
		return 4 + n, true
	default:
		return 0, false
	}
}

func stringSize(data []byte, maxLen int) (int, bool) {
	if maxLen < 256 {
		if len(data) < 1 {
			return 0, false
		}
		n := int(data[0])
		if len(data) < 1+n {
			return 0, false
		}
		return 1 + n, true
	}
	if len(data) < 2 {
		return 0, false
	}
	n := int(binary.LittleEndian.Uint16(data))
	if len(data) < 2+n {
		return 0, false
	}
	return 2 + n, true
}
