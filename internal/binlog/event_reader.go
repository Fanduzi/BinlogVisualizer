// Package binlog reads binlog events as raw bytes and checks them before any decode.
// input: a binlog file path, a start offset, and a go-mysql BinlogParser that keeps the Format Description state.
// output: each event handed to onEvent after its size, CRC32 footer and length fields were checked against the event bytes; a CorruptEventError at the event start otherwise.
// pos: the only place event bytes enter go-mysql. A Go fatal error (out of memory) or a hang inside a decoder cannot be recovered, so nothing hostile may reach one.
// note: if this file changes, update this header and README.md.
package binlog

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math/bits"
	"os"

	"github.com/go-mysql-org/go-mysql/replication"
)

// binlogFlagsOffset is where the 2-byte flags field sits in the event header.
const binlogFlagsOffset = 17

// eventSizeOffset is where the 4-byte event size sits in the event header.
const eventSizeOffset = 9

// corruptCause is a bounds failure found in event bytes whose CRC32 matched
// (or that carry none). The read loop turns it into a CorruptEventError.
type corruptCause string

func (c corruptCause) Error() string { return string(c) }

// missingTableMap is go-mysql's error for rows events before any table map;
// ParseReader skips those events, and so does readEvents.
const missingTableMap = "invalid table id, no corresponding table map event"

// readEvents replaces BinlogParser.ParseFile. It reads each event's bytes
// itself, bounded by the bytes left in the file, verifies the CRC32 footer
// declared by the Format Description (with LOG_EVENT_BINLOG_IN_USE_F masked),
// checks the length fields go-mysql would allocate from, and only then hands
// the bytes to bp.Parse. consumed is where the event being read starts.
func readEvents(bp *replication.BinlogParser, path string, offset int64, consumed *int64, fault *rowsFault, onEvent replication.OnEventFunc) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	size := info.Size()
	magic := make([]byte, binlogMagicSize)
	if _, err := io.ReadFull(f, magic); err != nil {
		return err
	}
	if !bytes.Equal(magic, replication.BinLogFileHeader) {
		return fmt.Errorf("%s is not a valid binlog file, head 4 bytes must fe'bin' ", path)
	}
	if fault == nil {
		fault = &rowsFault{}
	}
	r := &eventReader{bp: bp, f: f, size: size, path: path, consumed: consumed, fault: fault}
	if offset > binlogMagicSize {
		// The Format Description is always read first, as go-mysql does.
		if _, err := r.next(binlogMagicSize, onEvent); err != nil {
			return err
		}
	} else {
		offset = binlogMagicSize
	}
	pos := offset
	for {
		n, err := r.next(pos, onEvent)
		if err != nil {
			return err
		}
		if n == 0 {
			return nil
		}
		pos += n
	}
}

type eventReader struct {
	bp       *replication.BinlogParser
	f        *os.File
	size     int64
	path     string
	consumed *int64
	fault    *rowsFault
	alg      replication.BinlogChecksum
	fde      []byte
}

// next reads, checks and dispatches the event at pos. It returns the event
// size, or 0 at a clean end (fewer than a header's bytes left, which the
// caller's unread-tail check reports).
func (r *eventReader) next(pos int64, onEvent replication.OnEventFunc) (int64, error) {
	left := r.size - pos
	if left < replication.EventHeaderSize {
		return 0, nil
	}
	header := make([]byte, replication.EventHeaderSize)
	if _, err := r.f.ReadAt(header, pos); err != nil {
		return 0, err
	}
	eventSize := int64(binary.LittleEndian.Uint32(header[eventSizeOffset:]))
	if eventSize < replication.EventHeaderSize {
		return 0, fmt.Errorf("invalid event header, event size is %d, too small", eventSize)
	}
	if eventSize > left {
		// Never allocate more than the file still holds.
		return 0, fmt.Errorf("get event err %v, need %d but got %d", io.EOF, eventSize-replication.EventHeaderSize, left-replication.EventHeaderSize)
	}
	raw := make([]byte, eventSize)
	if _, err := r.f.ReadAt(raw, pos); err != nil {
		return 0, err
	}
	corrupt := func(cause string) error {
		return &CorruptEventError{Path: r.path, Offset: *r.consumed, Cause: cause}
	}
	eventType := replication.EventType(raw[4])
	if eventType == replication.FORMAT_DESCRIPTION_EVENT {
		fde := &replication.FormatDescriptionEvent{}
		if err := fde.Decode(raw[replication.EventHeaderSize:]); err != nil {
			return 0, corrupt("format description: " + err.Error())
		}
		r.alg = fde.ChecksumAlgorithm
		r.fde = raw
	}
	body := raw[replication.EventHeaderSize:]
	if r.alg == replication.BINLOG_CHECKSUM_ALG_CRC32 {
		if !crc32Matches(raw, eventType) {
			return 0, corrupt("CRC32 checksum mismatch")
		}
		body = body[:len(body)-replication.BinlogChecksumLength]
	}
	if err := checkEventBounds(eventType, body); err != nil {
		return 0, corrupt(err.Error())
	}
	if eventType == replication.TRANSACTION_PAYLOAD_EVENT {
		if err := checkPayload(body, r.fde); err != nil {
			return 0, corrupt(err.Error())
		}
	}
	r.fault.err = nil
	ev, err := r.bp.Parse(raw)
	if err != nil {
		// go-mysql flattens decoder errors into text, so the rows check
		// reports through fault instead.
		if r.fault.err != nil {
			return 0, corrupt(r.fault.err.Error())
		}
		if err.Error() == missingTableMap {
			return eventSize, nil
		}
		return 0, err
	}
	if err := onEvent(ev); err != nil {
		return 0, err
	}
	return eventSize, nil
}

// crc32Matches checks the CRC32 footer of raw. MySQL sets
// LOG_EVENT_BINLOG_IN_USE_F in the Format Description of an open binlog after
// the checksum is written and clears it on close, so the flag is masked
// before the check, as MySQL's own reader does. go-mysql v1.14 does not mask it.
func crc32Matches(raw []byte, eventType replication.EventType) bool {
	if len(raw) < replication.EventHeaderSize+replication.BinlogChecksumLength {
		return false
	}
	n := len(raw) - replication.BinlogChecksumLength
	want := binary.LittleEndian.Uint32(raw[n:])
	if eventType == replication.FORMAT_DESCRIPTION_EVENT && raw[binlogFlagsOffset]&byte(replication.LOG_EVENT_BINLOG_IN_USE_F) != 0 {
		masked := append([]byte(nil), raw[:n]...)
		masked[binlogFlagsOffset] &^= byte(replication.LOG_EVENT_BINLOG_IN_USE_F)
		return crc32.ChecksumIEEE(masked) == want
	}
	return crc32.ChecksumIEEE(raw[:n]) == want
}

// checkEventBounds rejects count fields that go-mysql would allocate from
// before it reads the bytes they describe. body excludes the header and the
// checksum footer.
func checkEventBounds(eventType replication.EventType, body []byte) error {
	switch eventType {
	case replication.PREVIOUS_GTIDS_EVENT:
		return checkPreviousGTIDs(body)
	case replication.MARIADB_GTID_LIST_EVENT:
		if len(body) < 4 {
			return corruptCause("GTID list event is too short")
		}
		count := uint64(binary.LittleEndian.Uint32(body) & (1<<28 - 1))
		if count > uint64(len(body)-4)/16 {
			return corruptCause("GTID list count exceeds the event size")
		}
	}
	return nil
}

// checkPreviousGTIDs walks n_sids and each interval count against the bytes
// the event holds. Every SID consumes at least 24 bytes, so the walk is
// bounded by the event size.
func checkPreviousGTIDs(body []byte) error {
	tooBig := corruptCause("Previous_GTIDs counts exceed the event size")
	if len(body) < 8 {
		return tooBig
	}
	tagged := body[7] == 1
	var sids uint64
	if tagged {
		var masked [8]byte
		copy(masked[:], body[1:7])
		sids = binary.LittleEndian.Uint64(masked[:])
	} else {
		sids = binary.LittleEndian.Uint64(body)
	}
	pos := 8
	if sids > uint64(len(body)-pos)/24 {
		return tooBig
	}
	for i := uint64(0); i < sids; i++ {
		if len(body)-pos < 16 {
			return tooBig
		}
		pos += 16
		if tagged {
			if len(body)-pos < 1 {
				return tooBig
			}
			tagLen := int(body[pos]) / 2
			pos++
			if len(body)-pos < tagLen {
				return tooBig
			}
			pos += tagLen
		}
		if len(body)-pos < 8 {
			return tooBig
		}
		intervals := binary.LittleEndian.Uint64(body[pos:])
		pos += 8
		if intervals > uint64(len(body)-pos)/16 {
			return tooBig
		}
		pos += int(intervals) * 16
	}
	return nil
}

// boundedRowsDecode wraps a rows decoder. The rows event header declares its
// own column count; go-mysql allocates a row of that width and loops over the
// row images until the event ends. A width that differs from the table map,
// or column bitmaps with no column set (each row image would consume no
// bytes, so the loop never ends), is corrupt.
func boundedRowsDecode(next func(*replication.RowsEvent, []byte) error, fault *rowsFault) func(*replication.RowsEvent, []byte) error {
	return func(re *replication.RowsEvent, data []byte) error {
		if err := checkRowsBounds(re, data); err != nil {
			if _, ok := err.(corruptCause); ok {
				fault.err = err
			}
			return err
		}
		return next(re, data)
	}
}

// rowsFault carries a rows bounds failure from the decoder hook back to the
// read loop.
type rowsFault struct{ err error }

func checkRowsBounds(re *replication.RowsEvent, data []byte) error {
	pos, err := re.DecodeHeader(data)
	if err != nil {
		return err
	}
	if re.Table != nil && re.ColumnCount != re.Table.ColumnCount {
		return corruptCause(fmt.Sprintf("rows event has %d columns but its table map has %d", re.ColumnCount, re.Table.ColumnCount))
	}
	if pos < len(data) && setBits(re.ColumnBitmap1, re.ColumnCount) == 0 && setBits(re.ColumnBitmap2, re.ColumnCount) == 0 {
		return corruptCause("rows event names no columns")
	}
	return checkRowsJSON(re, data)
}

func setBits(bitmap []byte, columns uint64) int {
	n := 0
	for i, b := range bitmap {
		if uint64(i)*8 >= columns {
			break
		}
		if rest := columns - uint64(i)*8; rest < 8 {
			b &= byte(1<<rest - 1)
		}
		n += bits.OnesCount8(b)
	}
	return n
}
