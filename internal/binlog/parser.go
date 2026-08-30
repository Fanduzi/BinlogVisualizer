// Package binlog extracts raw events and parse progress from local MySQL binlog files.
// input: binlog file paths, go-mysql replication parser callbacks, and optional progress consumers.
// output: Parser implementations that emit RawEvent values with bounded SQL, producer/transaction provenance, and physical MariaDB XA identities plus monotonic per-input ParseProgress updates.
// pos: parser adapter layer between on-disk binlog files and BinlogViz command/analyzer pipelines.
// note: if this file changes, update this header and README.md.
package binlog

import (
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
)

// parser implements Parser using go-mysql-org/go-mysql/replication.
type parser struct{}

type cachedTableName struct {
	schema string
	table  string
}

// NewParser creates a new binlog parser.
func NewParser() Parser {
	return &parser{}
}

// ParseFiles reads binlog files and calls handler for each event.
func (p *parser) ParseFiles(paths []string, handler func(RawEvent) error) error {
	return p.ParseFilesWithProgress(paths, nil, handler)
}

// ParseFilesFromOffset reads binlog files starting from the given byte offset.
// Note: TableMapEvents before the offset are lost, so RowsEvents may have empty schema/table fields.
// For timestamp-only probes this is harmless; for full event parsing, start from offset 0.
func (p *parser) ParseFilesFromOffset(paths []string, offset int64, handler func(RawEvent) error) error {
	bp := replication.NewBinlogParser()

	for _, path := range paths {
		tableNames := make(map[uint64]cachedTableName)
		serverVersion := ""
		startOffset := offset
		if startOffset < 0 {
			startOffset = 0
		}
		cursor := startOffset
		if cursor == 0 {
			cursor = binlogMagicSize
		}
		if err := bp.ParseFile(path, startOffset, func(ev *replication.BinlogEvent) error {
			if ev == nil {
				return nil
			}

			raw := RawEvent{
				Timestamp:     time.Unix(int64(ev.Header.Timestamp), 0),
				EventType:     ev.Header.EventType.String(),
				BinlogPath:    path,
				ServerID:      ev.Header.ServerID,
				ServerVersion: serverVersion,
				ServerFlavor:  serverFlavor(serverVersion),
			}
			raw.PositionStart, raw.PositionEnd, raw.BinlogBytes, cursor = deriveEventPositionRange(ev.Header, cursor)
			raw.Position = uint32(raw.PositionEnd)

			applyBinlogEventMetadata(&raw, raw.EventType, ev.Event, tableNames)
			if raw.ServerVersion != "" {
				serverVersion = raw.ServerVersion
			}

			return handler(raw)
		}); err != nil {
			return err
		}
	}
	return nil
}

// ParseFilesWithProgress reads binlog files and optionally reports file-relative offsets.
func (p *parser) ParseFilesWithProgress(paths []string, onProgress func(ParseProgress), handler func(RawEvent) error) error {
	bp := replication.NewBinlogParser()

	for index, path := range paths {
		tableNames := make(map[uint64]cachedTableName)
		serverVersion := ""
		fileSize := int64(0)
		if info, err := os.Stat(path); err == nil {
			fileSize = info.Size()
		}
		lastOffset := int64(0)
		cursor := int64(binlogMagicSize)
		if err := bp.ParseFile(path, 0, func(ev *replication.BinlogEvent) error {
			if ev == nil {
				return nil
			}

			raw := RawEvent{
				Timestamp:     time.Unix(int64(ev.Header.Timestamp), 0),
				EventType:     ev.Header.EventType.String(),
				BinlogPath:    path,
				ServerID:      ev.Header.ServerID,
				ServerVersion: serverVersion,
				ServerFlavor:  serverFlavor(serverVersion),
			}
			raw.PositionStart, raw.PositionEnd, raw.BinlogBytes, cursor = deriveEventPositionRange(ev.Header, cursor)
			raw.Position = uint32(raw.PositionEnd)

			offset := clampProgressOffset(raw.PositionEnd, fileSize)
			lastOffset = maxInt64(lastOffset, offset)
			if onProgress != nil {
				onProgress(ParseProgress{Path: path, Index: index, Offset: lastOffset})
			}

			applyBinlogEventMetadata(&raw, raw.EventType, ev.Event, tableNames)
			if raw.ServerVersion != "" {
				serverVersion = raw.ServerVersion
			}

			return handler(raw)
		}); err != nil {
			return err
		}
		if onProgress != nil && fileSize > 0 {
			onProgress(ParseProgress{Path: path, Index: index, Offset: fileSize})
		}
	}
	return nil
}

func applyBinlogEventMetadata(raw *RawEvent, eventTypeName string, event any, tableNames map[uint64]cachedTableName) {
	switch e := event.(type) {
	case *replication.QueryEvent:
		raw.Query = string(e.Query)
		raw.Schema = string(e.Schema)
		raw.ThreadID = e.SlaveProxyID
		raw.ActorUser, raw.ActorHost = queryEventActor(e.StatusVars)
	case *replication.GTIDEvent:
		if set, err := e.GTIDNext(); err == nil {
			raw.GTID = set.String()
		}
	case *replication.GtidTaggedLogEvent:
		if set, err := e.GTIDNext(); err == nil {
			raw.GTID = set.String()
		}
	case *replication.MariadbGTIDEvent:
		raw.GTID = e.GTID.String()
	case *replication.GenericEvent:
		if eventTypeName == replication.XA_PREPARE_LOG_EVENT.String() {
			raw.XAXID = mariaDBXAPrepareXID(e.Data)
		}
	case *replication.XIDEvent:
		if e.XID != 0 {
			raw.XID = strconv.FormatUint(e.XID, 10)
		}
	case *replication.RowsQueryEvent:
		raw.QuerySQL = string(e.Query)
	case *replication.MariadbAnnotateRowsEvent:
		raw.QuerySQL = string(e.Query)
	case *replication.TableMapEvent:
		name := cachedTableName{schema: string(e.Schema), table: string(e.Table)}
		raw.Schema = name.schema
		raw.Table = name.table
		if tableNames != nil {
			tableNames[e.TableID] = name
		}
	case *replication.RowsEvent:
		applyRowsEventTableName(raw, e, tableNames)
		raw.RowCount = logicalRowCount(eventTypeName, len(e.Rows))
	case *replication.FormatDescriptionEvent:
		raw.ServerVersion = e.ServerVersion
		raw.ServerFlavor = serverFlavor(e.ServerVersion)
	}
}

func mariaDBXAPrepareXID(data []byte) string {
	const headerSize = 13 // one-phase byte, format ID, gtrid length, bqual length
	if len(data) < headerSize {
		return ""
	}
	formatID := int32(binary.LittleEndian.Uint32(data[1:5]))
	gtridLen := uint64(binary.LittleEndian.Uint32(data[5:9]))
	bqualLen := uint64(binary.LittleEndian.Uint32(data[9:13]))
	payloadLen := gtridLen + bqualLen
	if payloadLen > uint64(len(data)-headerSize) {
		return ""
	}
	gtridEnd := headerSize + int(gtridLen)
	payloadEnd := headerSize + int(payloadLen)
	return fmt.Sprintf("X'%x',X'%x',%d", data[headerSize:gtridEnd], data[gtridEnd:payloadEnd], formatID)
}

func serverFlavor(version string) string {
	if version == "" {
		return ""
	}
	if strings.Contains(strings.ToLower(version), "mariadb") {
		return "mariadb"
	}
	return "mysql"
}

func queryEventActor(statusVars []byte) (string, string) {
	for pos := 0; pos < len(statusVars); {
		code := statusVars[pos]
		pos++
		switch code {
		case 0:
			pos += 4
		case 1:
			pos += 8
		case 2:
			pos = skipLengthEncodedStatusString(statusVars, pos, true)
		case 3:
			pos += 4
		case 4:
			pos += 6
		case 5, 6:
			pos = skipLengthEncodedStatusString(statusVars, pos, false)
		case 7, 8:
			pos += 2
		case 9:
			pos += 8
		case 10:
			pos += 4
		case 11:
			user, next, ok := readLengthEncodedStatusString(statusVars, pos)
			if !ok {
				return "", ""
			}
			host, _, ok := readLengthEncodedStatusString(statusVars, next)
			if !ok {
				return "", ""
			}
			return user, host
		default:
			return "", ""
		}
		if pos < 0 || pos > len(statusVars) {
			return "", ""
		}
	}
	return "", ""
}

func skipLengthEncodedStatusString(data []byte, pos int, trailingNUL bool) int {
	_, next, ok := readLengthEncodedStatusString(data, pos)
	if !ok {
		return len(data) + 1
	}
	if trailingNUL {
		next++
	}
	return next
}

func readLengthEncodedStatusString(data []byte, pos int) (string, int, bool) {
	if pos < 0 || pos >= len(data) {
		return "", pos, false
	}
	length := int(data[pos])
	pos++
	if length > len(data)-pos {
		return "", pos, false
	}
	return string(data[pos : pos+length]), pos + length, true
}

// logicalRowCount converts raw row-image counts into DBA-facing logical rows.
// UPDATE events store before/after images as consecutive rows.
func logicalRowCount(eventTypeName string, imageCount int) int {
	if isUpdateRowsEventName(eventTypeName) {
		return imageCount / 2
	}
	return imageCount
}

func isUpdateRowsEventName(eventTypeName string) bool {
	et := strings.ToUpper(eventTypeName)
	return strings.Contains(et, "UPDATE") && strings.Contains(et, "ROW")
}

func applyRowsEventTableName(raw *RawEvent, event *replication.RowsEvent, tableNames map[uint64]cachedTableName) {
	tableID := event.TableID
	if tableID == 0 && event.Table != nil {
		tableID = event.Table.TableID
	}
	if tableID != 0 {
		if name, ok := tableNames[tableID]; ok {
			raw.Schema = name.schema
			raw.Table = name.table
			return
		}
	}
	if event.Table == nil {
		return
	}

	name := cachedTableName{schema: string(event.Table.Schema), table: string(event.Table.Table)}
	raw.Schema = name.schema
	raw.Table = name.table
	if tableNames != nil && tableID != 0 {
		tableNames[tableID] = name
	}
}

// binlogMagicSize is the 4-byte BINLOG magic at the start of every file.
const binlogMagicSize = 4

// deriveEventPositionRange returns file-relative [start, end) and byte length.
// MariaDB 11.4+ leaves LogPos=0 on many events (BEGIN, TABLE_MAP, WriteRows);
// only XID typically has a real LogPos. When LogPos is 0 we reconstruct from
// the running cursor so large transactions are not recorded as a 31-byte XID.
func deriveEventPositionRange(header *replication.EventHeader, cursor int64) (start, end, size, next int64) {
	if header == nil {
		return 0, 0, 0, cursor
	}

	eventSize := int64(header.EventSize)
	if header.LogPos > 0 {
		end = int64(header.LogPos)
		start = end - eventSize
		if start < 0 {
			start = 0
		}
		if end < start {
			end = start
		}
		return start, end, end - start, end
	}

	// LogPos=0: reconstruct from the running file cursor (MariaDB 11.4+).
	if cursor <= 0 {
		cursor = binlogMagicSize
	}
	if eventSize <= 0 {
		return cursor, cursor, 0, cursor
	}
	start = cursor
	end = cursor + eventSize
	return start, end, eventSize, end
}

func clampProgressOffset(offset, fileSize int64) int64 {
	if offset < 0 {
		return 0
	}
	if fileSize > 0 && offset > fileSize {
		return fileSize
	}
	return offset
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
