// Package binlog bounds the values inside an event before go-mysql decodes them.
// input: raw rows-event bodies with their table map, and transaction payload event bodies.
// output: nil, or a corruptCause naming the first value that cannot be decoded in bounded time and memory.
// pos: second layer of the raw-byte checks in event_reader.go. go-mysql's binary JSON decoder recurses on offsets without a depth limit and its payload decoder decompresses without a size limit; a Go fatal error from either cannot be recovered.
// note: if this file changes, update this header and README.md.
package binlog

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/klauspost/compress/zstd"
)

// jsonMaxDepth is MySQL's JSON_DOCUMENT_MAX_DEPTH.
const jsonMaxDepth = 100

// maxPayloadUncompressed caps a transaction payload's uncompressed size at
// MySQL's largest max_allowed_packet (1 GiB).
const maxPayloadUncompressed = 1 << 30

// maxPayloadWindow caps the zstd window a payload frame may ask for. MySQL's
// compressor uses a few MiB at its default level.
const maxPayloadWindow = 1 << 27

// validateJSONBinary checks a MySQL binary JSON document the way MySQL's
// reader trusts it: every key and value offset lies strictly inside its
// container and past the container header, nesting is at most 100 deep, and
// the walk visits no more nodes than the document has bytes (so values shared
// through offsets cannot blow up). An empty document is the JSON null go-mysql
// returns for it.
func validateJSONBinary(doc []byte) error {
	if len(doc) == 0 {
		return nil
	}
	c := jsonCheck{budget: len(doc) + 1}
	return c.value(doc[0], doc[1:], 0)
}

type jsonCheck struct{ budget int }

func jsonBad(format string, args ...any) error {
	return corruptCause("JSON value: " + fmt.Sprintf(format, args...))
}

func (c *jsonCheck) value(tp byte, data []byte, depth int) error {
	c.budget--
	if c.budget < 0 {
		return jsonBad("offsets reference more values than the document holds")
	}
	need := func(n int) error {
		if len(data) < n {
			return jsonBad("type %#x needs %d bytes, %d left", tp, n, len(data))
		}
		return nil
	}
	switch tp {
	case replication.JSONB_SMALL_OBJECT, replication.JSONB_LARGE_OBJECT, replication.JSONB_SMALL_ARRAY, replication.JSONB_LARGE_ARRAY:
		small := tp == replication.JSONB_SMALL_OBJECT || tp == replication.JSONB_SMALL_ARRAY
		object := tp == replication.JSONB_SMALL_OBJECT || tp == replication.JSONB_LARGE_OBJECT
		return c.container(data, small, object, depth+1)
	case replication.JSONB_LITERAL:
		return need(1)
	case replication.JSONB_INT16, replication.JSONB_UINT16:
		return need(2)
	case replication.JSONB_INT32, replication.JSONB_UINT32:
		return need(4)
	case replication.JSONB_INT64, replication.JSONB_UINT64, replication.JSONB_DOUBLE:
		return need(8)
	case replication.JSONB_STRING:
		n, w, ok := jsonVarLen(data)
		if !ok || w+n > len(data) {
			return jsonBad("string length exceeds the value")
		}
		return nil
	case replication.JSONB_OPAQUE:
		if len(data) < 1 {
			return jsonBad("opaque value has no type")
		}
		n, w, ok := jsonVarLen(data[1:])
		if !ok || 1+w+n > len(data) {
			return jsonBad("opaque length exceeds the value")
		}
		return nil
	default:
		return jsonBad("unknown type %#x", tp)
	}
}

func (c *jsonCheck) container(data []byte, small, object bool, depth int) error {
	if depth > jsonMaxDepth {
		return jsonBad("nesting deeper than %d", jsonMaxDepth)
	}
	offsetSize := 4
	if small {
		offsetSize = 2
	}
	read := func(b []byte) uint64 {
		if small {
			return uint64(binary.LittleEndian.Uint16(b))
		}
		return uint64(binary.LittleEndian.Uint32(b))
	}
	if len(data) < 2*offsetSize {
		return jsonBad("container header exceeds the value")
	}
	count := read(data)
	size := read(data[offsetSize:])
	if size > uint64(len(data)) {
		return jsonBad("container size %d exceeds the %d bytes left", size, len(data))
	}
	data = data[:size]
	keyEntry := uint64(offsetSize + 2)
	valueEntry := uint64(1 + offsetSize)
	header := uint64(2 * offsetSize)
	perEntry := valueEntry
	if object {
		perEntry += keyEntry
	}
	if size < header || count > (size-header)/perEntry {
		return jsonBad("%d entries do not fit a %d-byte container", count, size)
	}
	header += count * perEntry
	if object {
		for i := uint64(0); i < count; i++ {
			at := 2*uint64(offsetSize) + keyEntry*i
			keyOffset := read(data[at:])
			keyLength := uint64(binary.LittleEndian.Uint16(data[at+uint64(offsetSize):]))
			if keyOffset < header || keyOffset+keyLength > size {
				return jsonBad("key %d offset %d is outside the container", i, keyOffset)
			}
		}
	}
	for i := uint64(0); i < count; i++ {
		at := 2*uint64(offsetSize) + valueEntry*i
		if object {
			at += keyEntry * count
		}
		tp := data[at]
		if jsonInline(tp, small) {
			c.budget--
			if c.budget < 0 {
				return jsonBad("offsets reference more values than the document holds")
			}
			continue
		}
		valueOffset := read(data[at+1:])
		if valueOffset < header || valueOffset >= size {
			return jsonBad("value %d offset %d is outside the container", i, valueOffset)
		}
		if err := c.value(tp, data[valueOffset:], depth); err != nil {
			return err
		}
	}
	return nil
}

// validateJSONDiffs checks a partial JSON update cell (Json_diff_vector):
// each diff is an operation, a path and, unless it removes, a binary value.
func validateJSONDiffs(cell []byte) error {
	for pos := 0; pos < len(cell); {
		op := replication.JsonDiffOperation(cell[pos])
		pos++
		if op != replication.JsonDiffOperationReplace && op != replication.JsonDiffOperationInsert && op != replication.JsonDiffOperationRemove {
			return jsonBad("unknown partial update operation %d", op)
		}
		pathLen, _, n := mysql.LengthEncodedInt(cell[pos:])
		if n <= 0 || pathLen > uint64(len(cell)-pos-n) {
			return jsonBad("partial update path exceeds the cell")
		}
		pos += n + int(pathLen)
		if op == replication.JsonDiffOperationRemove {
			continue
		}
		valueLen, _, n := mysql.LengthEncodedInt(cell[pos:])
		if n <= 0 || valueLen > uint64(len(cell)-pos-n) {
			return jsonBad("partial update value exceeds the cell")
		}
		pos += n
		if err := validateJSONBinary(cell[pos : pos+int(valueLen)]); err != nil {
			return err
		}
		pos += int(valueLen)
	}
	return nil
}

// checkRowsJSON walks every row image of a rows event whose table has a JSON
// column and validates each JSON value before go-mysql decodes it. A walk
// that cannot account for every byte is corrupt.
func checkRowsJSON(re *replication.RowsEvent, data []byte) error {
	if re.Table == nil || !hasJSONColumn(re.Table) {
		return nil
	}
	meta, ok := rowsDecodeMetaOf(re)
	if !ok || meta.compressed {
		return nil
	}
	pos, ok := rowsImageStart(data, re, meta)
	if !ok {
		return corruptCause("rows event header exceeds the event")
	}
	for image := 0; pos < len(data); image++ {
		bitmap, partial := re.ColumnBitmap1, false
		if meta.needBitmap2 && image%2 == 1 {
			bitmap, partial = re.ColumnBitmap2, meta.partial
		}
		n, cells, ok := walkRowImage(data[pos:], re, bitmap, partial)
		if !ok || n == 0 {
			return corruptCause("rows event image cannot be bounded")
		}
		for _, cell := range cells {
			if cell.ok {
				if err := validateJSONBinary(cell.doc); err != nil {
					return err
				}
			}
			if cell.diff != nil {
				if err := validateJSONDiffs(cell.diff); err != nil {
					return err
				}
			}
		}
		pos += n
	}
	return nil
}

func hasJSONColumn(table *replication.TableMapEvent) bool {
	for _, tp := range table.ColumnType {
		if tp == mysql.MYSQL_TYPE_JSON {
			return true
		}
	}
	return false
}

// checkPayload bounds a MySQL 8 transaction payload before go-mysql
// decompresses it: the declared uncompressed size must be at most 1 GiB and
// must equal what the zstd frame expands to (decompressed here with that size
// as the memory limit), and every inner event must fit the payload and pass
// the same count, rows and JSON checks as an outer event. fde is the outer
// Format Description event, nil if none was read.
func checkPayload(body []byte, fde []byte) error {
	var compression, declared uint64
	hasDeclared := false
	pos := 0
	for {
		if pos >= len(body) {
			return corruptCause("payload header has no end mark")
		}
		field := body[pos]
		pos++
		if field == replication.OTW_PAYLOAD_HEADER_END_MARK {
			break
		}
		if pos >= len(body) {
			return corruptCause("payload header field exceeds the event")
		}
		length := int(body[pos])
		pos++
		// net_store_length writes at most 9 bytes: 0xfe and 8 value bytes
		// for values of 2^24 and up, which a 1 GiB size needs.
		if length > 9 || pos+length > len(body) {
			return corruptCause("payload header field exceeds the event")
		}
		// Field values are net_field_length encoded (MySQL's
		// net_store_length); length is the encoded byte count.
		v, _, n := mysql.LengthEncodedInt(body[pos : pos+length])
		if n != length {
			return corruptCause("payload header field is malformed")
		}
		switch field {
		case replication.OTW_PAYLOAD_COMPRESSION_TYPE_FIELD:
			compression = v
		case replication.OTW_PAYLOAD_UNCOMPRESSED_SIZE_FIELD:
			declared, hasDeclared = v, true
		}
		pos += length
	}
	if compression != replication.ZSTD {
		return nil // go-mysql refuses other compression types without decoding.
	}
	limit := uint64(maxPayloadUncompressed)
	if hasDeclared {
		if declared > maxPayloadUncompressed {
			return corruptCause(fmt.Sprintf("payload declares %d uncompressed bytes, more than %d", declared, maxPayloadUncompressed))
		}
		limit = declared
	}
	// Stream with a read limit: the frame cannot expand past the declared
	// size, and the window it may ask for is capped.
	decoder, err := zstd.NewReader(bytes.NewReader(body[pos:]), zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxWindow(maxPayloadWindow))
	if err != nil {
		return corruptCause("payload is not a zstd frame: " + err.Error())
	}
	defer decoder.Close()
	inner, err := io.ReadAll(io.LimitReader(decoder, int64(limit)+1))
	if err != nil {
		return corruptCause("payload does not decompress: " + err.Error())
	}
	if uint64(len(inner)) > limit {
		return corruptCause(fmt.Sprintf("payload decompresses past %d bytes", limit))
	}
	if hasDeclared && uint64(len(inner)) != declared {
		return corruptCause(fmt.Sprintf("payload decompresses to %d bytes, not the declared %d", len(inner), declared))
	}
	return checkPayloadEvents(inner, fde)
}

// checkPayloadEvents parses the inner events with a parser whose rows hook
// only runs the bounds checks. Inner events carry no checksum, so the Format
// Description it is given declares none.
func checkPayloadEvents(inner []byte, fde []byte) error {
	vp := replication.NewBinlogParser()
	fault := &rowsFault{}
	vp.SetRowsEventDecodeFunc(func(re *replication.RowsEvent, data []byte) error {
		if err := checkRowsBounds(re, data); err != nil {
			if _, ok := err.(corruptCause); ok {
				fault.err = err
			}
			return err
		}
		return nil
	})
	if len(fde) > replication.EventHeaderSize+5 {
		off := append([]byte(nil), fde...)
		if replication.BinlogChecksum(off[len(off)-5]) == replication.BINLOG_CHECKSUM_ALG_CRC32 {
			off[len(off)-5] = byte(replication.BINLOG_CHECKSUM_ALG_OFF)
		}
		if _, err := vp.Parse(off); err != nil {
			return nil // the outer decode reports this file's problem
		}
	}
	for pos := 0; pos+replication.EventHeaderSize <= len(inner); {
		size := int(binary.LittleEndian.Uint32(inner[pos+eventSizeOffset:]))
		if size < replication.EventHeaderSize || size > len(inner)-pos {
			return corruptCause("payload inner event exceeds the payload")
		}
		raw := inner[pos : pos+size]
		if err := checkEventBounds(replication.EventType(raw[4]), raw[replication.EventHeaderSize:]); err != nil {
			return err
		}
		fault.err = nil
		if _, err := vp.Parse(raw); err != nil && fault.err != nil {
			return fault.err
		}
		pos += size
	}
	return nil
}
