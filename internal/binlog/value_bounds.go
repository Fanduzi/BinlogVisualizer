// Package binlog bounds the values inside an event before go-mysql decodes them.
// input: raw rows-event bodies with their table map, and transaction payload event bodies.
// output: nil, or a corruptCause naming the first value that cannot be decoded in bounded time and memory; for a payload, a checked streamedPayload that hands its inner events on one at a time.
// pos: second layer of the raw-byte checks in event_reader.go. go-mysql's binary JSON decoder recurses on offsets without a depth limit and its payload decoder decompresses without a size limit, so payloads are decoded here as a bounded stream and never handed to it; a Go fatal error from either cannot be recovered.
// note: if this file changes, update this header and README.md.
package binlog

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"

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

// payloadEventChunk is the first allocation for an inner event's bytes; the
// buffer grows only as decompressed bytes arrive, so a header that claims a
// large event costs memory only for the bytes the frame really produces.
const payloadEventChunk = 1 << 20

// maxPayloadTableMaps caps the distinct table ids one payload may map. The
// inner parser keeps every table map it has seen, so without a cap a small
// frame of distinct table maps would grow memory with the payload, not with
// its largest event. MySQL maps each table a transaction touches once.
const maxPayloadTableMaps = 1 << 16

// checkPayload runs the payload checks alone; tests use it.
func checkPayload(body []byte, fde []byte) error {
	_, err := decodePayload(body, fde)
	return err
}

// streamedPayload is a transaction payload checked by decodePayload whose
// inner events are not held: each decodes the frame again and hands the
// inner events to fn one at a time, so a consumer never holds more than the
// event it is given. Events stays nil.
type streamedPayload struct {
	replication.TransactionPayloadEvent
	each func(fn func(*replication.BinlogEvent) error) error
}

// decodePayload checks a MySQL 8 transaction payload in place of go-mysql,
// which decompresses with no size limit and keeps every inner event. The
// header fields are net_field_length values and the declared uncompressed
// size must be at most 1 GiB. Then the whole frame is walked once as a
// bounded stream (walkPayload) and every inner event decoded and dropped;
// only if all of it passes does the caller get a streamedPayload, whose each
// walks the same bytes again and hands the events on. Nothing of a payload
// is emitted before all of it is known good, so a corrupt event anywhere
// fails the payload before its first event reaches analyze or flashback.
// That includes an inner rows event with no table map before it: go-mysql
// failed the whole payload on it (an outer such event is skipped, an inner
// one never was), and so does this. The cost is decompressing twice; memory stays the largest inner
// event plus a constant. A nil event and nil error mean a compression type
// go-mysql refuses itself.
func decodePayload(body []byte, fde []byte) (*streamedPayload, error) {
	ev, limit, hasDeclared, err := parsePayloadHeader(body)
	if err != nil || ev == nil {
		return nil, err
	}
	if err := walkPayload(ev, limit, hasDeclared, fde, nil); err != nil {
		return nil, err
	}
	sp := &streamedPayload{TransactionPayloadEvent: *ev}
	sp.each = func(fn func(*replication.BinlogEvent) error) error {
		return walkPayload(ev, limit, hasDeclared, fde, fn)
	}
	return sp, nil
}

// parsePayloadHeader reads the payload header fields. The returned limit is
// the declared size, or 1 GiB when none is declared.
func parsePayloadHeader(body []byte) (*replication.TransactionPayloadEvent, uint64, bool, error) {
	ev := &replication.TransactionPayloadEvent{}
	hasDeclared := false
	pos := 0
	for {
		if pos >= len(body) {
			return nil, 0, false, corruptCause("payload header has no end mark")
		}
		field := body[pos]
		pos++
		if field == replication.OTW_PAYLOAD_HEADER_END_MARK {
			break
		}
		if pos >= len(body) {
			return nil, 0, false, corruptCause("payload header field exceeds the event")
		}
		length := int(body[pos])
		pos++
		// net_store_length writes at most 9 bytes: 0xfe and 8 value bytes
		// for values of 2^24 and up, which a 1 GiB size needs.
		if length > 9 || pos+length > len(body) {
			return nil, 0, false, corruptCause("payload header field exceeds the event")
		}
		v, ok := netFieldLength(body[pos : pos+length])
		if !ok {
			return nil, 0, false, corruptCause(fmt.Sprintf("payload header field %d is malformed: % x is not a net_field_length value", field, body[pos:pos+length]))
		}
		switch field {
		case replication.OTW_PAYLOAD_SIZE_FIELD:
			ev.Size = v
		case replication.OTW_PAYLOAD_COMPRESSION_TYPE_FIELD:
			ev.CompressionType = v
		case replication.OTW_PAYLOAD_UNCOMPRESSED_SIZE_FIELD:
			ev.UncompressedSize, hasDeclared = v, true
		}
		pos += length
	}
	if ev.CompressionType != replication.ZSTD {
		return nil, 0, false, nil // go-mysql refuses other compression types without decoding.
	}
	ev.Payload = body[pos:]
	limit := uint64(maxPayloadUncompressed)
	if hasDeclared {
		if ev.UncompressedSize > maxPayloadUncompressed {
			return nil, 0, false, corruptCause(fmt.Sprintf("payload declares %s uncompressed, more than the limit %s", humanBytes(ev.UncompressedSize), humanBytes(maxPayloadUncompressed)))
		}
		limit = ev.UncompressedSize
	}
	return ev, limit, hasDeclared, nil
}

// walkPayload reads ev's zstd frame (128 MiB window cap) as a stream, one
// inner event at a time: each inner header must name an event that fits in
// what is left of limit, its bytes go into a buffer that grows only as they
// arrive, it passes the same count, rows and JSON checks as an outer event,
// it is parsed, handed to fn when fn is not nil, and dropped. The walk stops
// with a corruptCause at the first inner event that does not fit or decode,
// or as soon as the frame yields a byte past limit; with a declared size the
// total must equal it. An error from fn is returned as is.
func walkPayload(ev *replication.TransactionPayloadEvent, limit uint64, hasDeclared bool, fde []byte, fn func(*replication.BinlogEvent) error) error {
	decoder, err := zstd.NewReader(bytes.NewReader(ev.Payload),
		zstd.WithDecoderConcurrency(1),
		zstd.WithDecoderLowmem(true),
		zstd.WithDecoderMaxWindow(maxPayloadWindow))
	if err != nil {
		return corruptCause("payload is not a zstd frame: " + err.Error())
	}
	defer decoder.Close()
	stream := &payloadStream{r: decoder, left: limit}

	vp := replication.NewBinlogParser()
	fault := &rowsFault{}
	vp.SetRowsEventDecodeFunc(boundedRowsDecode(func(re *replication.RowsEvent, data []byte) error {
		return re.Decode(data)
	}, fault))
	// Inner events carry no checksum, so the Format Description they are
	// parsed with declares none, as go-mysql's payload decoder does.
	if len(fde) > replication.EventHeaderSize+5 {
		off := append([]byte(nil), fde...)
		if replication.BinlogChecksum(off[len(off)-5]) == replication.BINLOG_CHECKSUM_ALG_CRC32 {
			off[len(off)-5] = byte(replication.BINLOG_CHECKSUM_ALG_OFF)
		}
		if _, err := vp.Parse(off); err != nil {
			return corruptCause("format description: " + err.Error())
		}
	}

	tables := make(map[uint64]struct{})
	var total uint64
	header := make([]byte, replication.EventHeaderSize)
	for {
		n, err := io.ReadFull(stream, header)
		if err == io.EOF {
			break
		}
		if err != nil {
			if err == io.ErrUnexpectedEOF {
				if got := total + uint64(n); hasDeclared && got != ev.UncompressedSize {
					return corruptCause(fmt.Sprintf("payload decompresses to %d bytes, not the declared %d", got, ev.UncompressedSize))
				}
				return corruptCause(fmt.Sprintf("payload ends inside an inner event header (%d of %d bytes)", n, len(header)))
			}
			return stream.fail(err)
		}
		size := uint64(binary.LittleEndian.Uint32(header[eventSizeOffset:]))
		if size < replication.EventHeaderSize {
			return corruptCause(fmt.Sprintf("payload inner event at %d declares %d bytes, less than its %d-byte header", total, size, replication.EventHeaderSize))
		}
		if size-replication.EventHeaderSize > stream.left {
			return corruptCause(fmt.Sprintf("payload inner event of %s at %d exceeds the payload of %s", humanBytes(size), total, humanBytes(limit)))
		}
		raw, err := readGrowing(stream, header, int(size))
		if err != nil {
			if err == io.ErrUnexpectedEOF {
				return corruptCause(fmt.Sprintf("payload inner event of %s at %d exceeds the payload", humanBytes(size), total))
			}
			return stream.fail(err)
		}
		eventType := replication.EventType(raw[4])
		if err := checkEventBounds(eventType, raw[replication.EventHeaderSize:]); err != nil {
			return err
		}
		inner, err := parseInner(vp, fault, raw)
		if err != nil {
			if _, ok := err.(corruptCause); ok {
				return err
			}
			if err == errInnerNoTableMap {
				return corruptCause(fmt.Sprintf("payload inner %s at %d has no table map before it", eventType, total))
			}
			return corruptCause(fmt.Sprintf("payload inner %s at %d does not decode", eventType, total))
		}
		if tm, ok := inner.Event.(*replication.TableMapEvent); ok {
			tables[tm.TableID] = struct{}{}
			if len(tables) > maxPayloadTableMaps {
				return corruptCause(fmt.Sprintf("payload maps more than %d tables", maxPayloadTableMaps))
			}
		}
		total += size
		if fn != nil {
			if err := fn(inner); err != nil {
				return err
			}
		}
	}
	if hasDeclared && total != ev.UncompressedSize {
		return corruptCause(fmt.Sprintf("payload decompresses to %d bytes, not the declared %d", total, ev.UncompressedSize))
	}
	return nil
}

// parseInner parses one inner event. A rows bounds failure comes back as its
// corruptCause, a rows event with no table map as errInnerNoTableMap, and
// any other go-mysql error or a panic as errInnerDecode, so neither go-mysql's dump of
// the bytes nor Go runtime text reaches the message.
func parseInner(vp *replication.BinlogParser, fault *rowsFault, raw []byte) (ev *replication.BinlogEvent, err error) {
	defer func() {
		if r := recover(); r != nil {
			ev, err = nil, errInnerDecode
		}
	}()
	fault.err = nil
	ev, err = vp.Parse(raw)
	if err == nil {
		return ev, nil
	}
	if fault.err != nil {
		return nil, fault.err
	}
	// go-mysql wraps it in an EventError with a dump of the bytes.
	if strings.HasSuffix(err.Error(), missingTableMap) {
		return nil, errInnerNoTableMap
	}
	return nil, errInnerDecode
}

var (
	errInnerDecode     = errors.New("inner event does not decode")
	errInnerNoTableMap = errors.New("inner rows event has no table map")
)

// netFieldLength reads one MySQL net_field_length value that fills b
// exactly. 0xfb (NULL) and 0xff (error marker) are not values.
func netFieldLength(b []byte) (uint64, bool) {
	if len(b) == 0 {
		return 0, false
	}
	var n int
	switch b[0] {
	case 0xfb, 0xff:
		return 0, false
	case 0xfc:
		n = 3
	case 0xfd:
		n = 4
	case 0xfe:
		n = 9
	default:
		return uint64(b[0]), len(b) == 1
	}
	if len(b) != n {
		return 0, false
	}
	var v uint64
	for i := n - 1; i >= 1; i-- {
		v = v<<8 | uint64(b[i])
	}
	return v, true
}

// errPayloadPastLimit stops a stream that yields more than its limit.
var errPayloadPastLimit = errors.New("past the limit")

// payloadStream yields at most left bytes of r and fails with
// errPayloadPastLimit as soon as r has one more.
type payloadStream struct {
	r    io.Reader
	left uint64
}

func (s *payloadStream) Read(p []byte) (int, error) {
	if s.left == 0 {
		var one [1]byte
		n, err := s.r.Read(one[:])
		if n > 0 {
			return 0, errPayloadPastLimit
		}
		return 0, err
	}
	if uint64(len(p)) > s.left {
		p = p[:s.left]
	}
	n, err := s.r.Read(p)
	s.left -= uint64(n)
	return n, err
}

func (s *payloadStream) fail(err error) error {
	if errors.Is(err, errPayloadPastLimit) {
		return corruptCause("payload decompresses past its declared size")
	}
	return corruptCause("payload does not decompress: " + err.Error())
}

// readGrowing returns header followed by the rest of a size-byte event read
// from r. The buffer starts at payloadEventChunk and doubles only when it is
// full, never past size.
func readGrowing(r io.Reader, header []byte, size int) ([]byte, error) {
	buf := make([]byte, len(header), min(size, max(payloadEventChunk, len(header))))
	copy(buf, header)
	for len(buf) < size {
		if len(buf) == cap(buf) {
			grown := make([]byte, len(buf), min(size, 2*cap(buf)))
			copy(grown, buf)
			buf = grown
		}
		n, err := io.ReadFull(r, buf[len(buf):cap(buf)])
		buf = buf[:len(buf)+n]
		if err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return nil, err
		}
	}
	return buf, nil
}

// humanBytes prints n in binary units with the exact byte count.
func humanBytes(n uint64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := uint64(unit), 0
	for m := n / unit; m >= unit && exp < 4; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB (%d bytes)", float64(n)/float64(div), "KMGTP"[exp], n)
}
