// Package binlog extracts raw events and parse progress from local MySQL binlog files.
// input: binlog file paths, go-mysql replication parser callbacks, optional progress consumers, and decoded TransactionPayloadEvent inner events.
// output: Parser implementations that emit RawEvent values with canonical kinds, expanded transaction-payload inner events stamped with the wrapper's file-relative span once, bounded SQL, producer/transaction provenance, MySQL 8 GTID commit timestamps when both are non-zero, and physical MariaDB XA identities plus monotonic per-input ParseProgress updates. TIMESTAMP row-image strings are the UTC wall clock of the stored instant, not the process zone. A full-file parse that stops before the last byte returns an unread-tail error. rawEventFromHeader is the shared header projection used by the file loop and payload expand. SetCaptureFlashback attaches exact undo literals; it stays off for analyze.
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

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

// parser implements Parser using go-mysql-org/go-mysql/replication.
type parser struct {
	captureRows  bool
	captureFlash bool
}

type cachedTableName struct {
	schema    string
	table     string
	keyStatus string
	pk        pkMeta
}

func cachedFromTable(table *replication.TableMapEvent) cachedTableName {
	if table == nil {
		return cachedTableName{}
	}
	return cachedTableName{
		schema:    string(table.Schema),
		table:     string(table.Table),
		keyStatus: tableKeyStatus(table),
		pk:        pkMetaFrom(table),
	}
}

// NewParser creates a new binlog parser.
func NewParser() Parser {
	return &parser{}
}

// SetCaptureRowImages keeps bounded cell values on ROW events.
// Off by default so analyze does not retain row images.
func (p *parser) SetCaptureRowImages(on bool) {
	if p == nil {
		return
	}
	p.captureRows = on
}

// SetCaptureFlashback keeps exact cell literals on ROW events for undo SQL.
// Off by default so analyze does not retain them.
func (p *parser) SetCaptureFlashback(on bool) {
	if p == nil {
		return
	}
	p.captureFlash = on
}

// ParseFiles reads binlog files and calls handler for each event.
func (p *parser) ParseFiles(paths []string, handler func(RawEvent) error) error {
	return p.ParseFilesWithProgress(paths, nil, handler)
}

// ParseFilesFromOffset reads binlog files starting from the given byte offset.
// Note: TableMapEvents before the offset are lost, so RowsEvents may have empty schema/table fields.
// For timestamp-only probes this is harmless; for full event parsing, start from offset 0.
func (p *parser) ParseFilesFromOffset(paths []string, offset int64, handler func(RawEvent) error) error {
	return p.parseFiles(paths, offset, nil, handler)
}

// ParseFilesWithProgress reads binlog files and optionally reports file-relative offsets.
func (p *parser) ParseFilesWithProgress(paths []string, onProgress func(ParseProgress), handler func(RawEvent) error) error {
	return p.parseFiles(paths, 0, onProgress, handler)
}

func (p *parser) parseFiles(paths []string, startOffset int64, onProgress func(ParseProgress), handler func(RawEvent) error) error {
	bp := replication.NewBinlogParser()
	// TIMESTAMP is a UTC instant. With parseTime off, go-mysql prints it via time.Unix,
	// whose location is time.Local, so the string followed the process zone. DATETIME
	// is a zone-less wall clock and does not use this location.
	bp.SetTimestampStringLocation(time.UTC)
	if startOffset < 0 {
		startOffset = 0
	}

	for index, path := range paths {
		tableNames := make(map[uint64]cachedTableName)
		serverVersion := ""
		fileSize := int64(0)
		if onProgress != nil {
			if info, err := os.Stat(path); err == nil {
				fileSize = info.Size()
			}
		}
		lastOffset := int64(0)
		cursor := startOffset
		if cursor == 0 {
			cursor = binlogMagicSize
		}
		// Bytes actually handed to the callback. LogPos can run ahead of the
		// file after a fixture is edited, so the tail check uses this sum.
		consumed := cursor
		if err := bp.ParseFile(path, startOffset, func(ev *replication.BinlogEvent) error {
			if ev == nil {
				return nil
			}
			if ev.Header != nil {
				consumed += int64(ev.Header.EventSize)
			}

			raw := rawEventFromHeader(ev.Header, path, serverVersion)
			raw.PositionStart, raw.PositionEnd, raw.BinlogBytes, cursor = deriveEventPositionRange(ev.Header, cursor)
			raw.Position = uint32(raw.PositionEnd)

			if onProgress != nil {
				offset := clampProgressOffset(raw.PositionEnd, fileSize)
				lastOffset = maxInt64(lastOffset, offset)
				onProgress(ParseProgress{Path: path, Index: index, Offset: lastOffset})
			}

			if inners, ok := expandTransactionPayload(ev, path, serverVersion, tableNames, p.captureRows, p.captureFlash); ok {
				assignPayloadWrapperFileSpan(inners, raw.PositionStart, raw.PositionEnd, raw.BinlogBytes)
				for i := range inners {
					if inners[i].ServerVersion != "" {
						serverVersion = inners[i].ServerVersion
					}
					if err := handler(inners[i]); err != nil {
						return err
					}
				}
				return nil
			}

			applyBinlogEventMetadata(&raw, ev.Header.EventType, ev.Event, tableNames, p.captureRows, p.captureFlash)
			if raw.ServerVersion != "" {
				serverVersion = raw.ServerVersion
			}

			return handler(raw)
		}); err != nil {
			return err
		}
		if startOffset == 0 {
			if err := unreadBinlogTail(path, consumed); err != nil {
				return err
			}
		}
		if onProgress != nil && fileSize > 0 {
			onProgress(ParseProgress{Path: path, Index: index, Offset: fileSize})
		}
	}
	return nil
}

// unreadBinlogTail reports a short read the upstream parser treats as a clean EOF.
// go-mysql returns success when the next event header is only partly present.
func unreadBinlogTail(path string, consumed int64) error {
	info, err := os.Stat(path)
	if err != nil || consumed < 0 || info.Size() <= consumed {
		return nil
	}
	return fmt.Errorf("%s: %d unread bytes after position %d", path, info.Size()-consumed, consumed)
}

func rawEventFromHeader(header *replication.EventHeader, path, serverVersion string) RawEvent {
	raw := RawEvent{
		BinlogPath:    path,
		ServerVersion: serverVersion,
		ServerFlavor:  serverFlavor(serverVersion),
	}
	if header == nil {
		return raw
	}
	raw.Timestamp = time.Unix(int64(header.Timestamp), 0)
	raw.EventType = canonicalEventType(header.EventType)
	raw.ServerID = header.ServerID
	return raw
}

func applyBinlogEventMetadata(raw *RawEvent, et replication.EventType, event any, tableNames map[uint64]cachedTableName, captureRows ...bool) {
	capture := len(captureRows) > 0 && captureRows[0]
	flash := len(captureRows) > 1 && captureRows[1]
	switch e := event.(type) {
	case *replication.QueryEvent:
		raw.Query = string(e.Query)
		raw.Schema = string(e.Schema)
		raw.ThreadID = e.SlaveProxyID
		raw.ActorUser, raw.ActorHost = queryEventActor(e.StatusVars)
	case *replication.GTIDEvent:
		setCommitTimestamps(raw, e.OriginalCommitTimestamp, e.ImmediateCommitTimestamp)
		if et == replication.ANONYMOUS_GTID_EVENT {
			raw.GTID = ""
			return
		}
		if set, err := e.GTIDNext(); err == nil {
			raw.GTID = set.String()
		}
	case *replication.GtidTaggedLogEvent:
		setCommitTimestamps(raw, e.OriginalCommitTimestamp, e.ImmediateCommitTimestamp)
		if set, err := e.GTIDNext(); err == nil {
			raw.GTID = set.String()
		}
	case *replication.MariadbGTIDEvent:
		raw.GTID = e.GTID.String()
	case *replication.GenericEvent:
		if et == replication.XA_PREPARE_LOG_EVENT {
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
		name := cachedFromTable(e)
		raw.Schema = name.schema
		raw.Table = name.table
		raw.KeyStatus = name.keyStatus
		if tableNames != nil {
			tableNames[e.TableID] = name
		}
	case *replication.RowsEvent:
		applyRowsEventTableName(raw, e, tableNames)
		raw.KeyStatus = rowsKeyStatus(e, tableNames)
		raw.RowCount = logicalRowCount(et, len(e.Rows))
		if capture {
			raw.RowImages, raw.RowImagesOmitted = captureRowImages(e, raw.EventType, raw.Schema, raw.Table)
		}
		if flash {
			raw.FlashRows = captureFlashbackRows(e, raw.EventType, raw.Schema, raw.Table)
		}
		if raw.EventType == kindUpdateRows || raw.EventType == kindDeleteRows {
			raw.RowKeys = primaryKeyValues(e, raw.EventType, pkMetaForRows(e, tableNames))
		}
	case *replication.FormatDescriptionEvent:
		raw.ServerVersion = e.ServerVersion
		raw.ServerFlavor = serverFlavor(e.ServerVersion)
	}
}

// setCommitTimestamps keeps a GTID pair only when both microseconds are present.
// A zero on either side is an absent timestamp, not a delay of zero.
func setCommitTimestamps(raw *RawEvent, original, immediate uint64) {
	if raw == nil || original == 0 || immediate == 0 {
		return
	}
	raw.OriginalCommitTimestamp = original
	raw.ImmediateCommitTimestamp = immediate
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
func logicalRowCount(et replication.EventType, imageCount int) int {
	switch et {
	case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2,
		replication.PARTIAL_UPDATE_ROWS_EVENT, replication.MARIADB_UPDATE_ROWS_COMPRESSED_EVENT_V1:
		return imageCount / 2
	default:
		return imageCount
	}
}

// expandTransactionPayload returns inner RawEvents when a transaction payload
// decoded successfully. The wrapper is not a canonical kind and is omitted.
// Inner file positions always use the wrapper's file-relative range; inner
// LogPos is the uncompressed stream and is not a binlog offset.
func expandTransactionPayload(ev *replication.BinlogEvent, path, serverVersion string, tableNames map[uint64]cachedTableName, captureRows ...bool) ([]RawEvent, bool) {
	capture := len(captureRows) > 0 && captureRows[0]
	flash := len(captureRows) > 1 && captureRows[1]
	if ev == nil {
		return nil, false
	}
	payload, ok := ev.Event.(*replication.TransactionPayloadEvent)
	if !ok || len(payload.Events) == 0 {
		return nil, false
	}
	wrapperStart, wrapperEnd, wrapperBytes, _ := deriveEventPositionRange(ev.Header, 0)
	out := make([]RawEvent, 0, len(payload.Events))
	for _, inner := range payload.Events {
		if inner == nil || inner.Header == nil {
			continue
		}
		raw := rawEventFromHeader(inner.Header, path, serverVersion)
		// go-mysql v1.14 decodes payload inners on a fresh parser, so the UTC
		// location above never reaches them. Delete this when that parser inherits it.
		normalizePayloadTimestampCells(inner.Event, payloadTimestampLocation)
		applyBinlogEventMetadata(&raw, inner.Header.EventType, inner.Event, tableNames, capture, flash)
		out = append(out, raw)
	}
	assignPayloadWrapperFileSpan(out, wrapperStart, wrapperEnd, wrapperBytes)
	return out, len(out) > 0
}

// payloadTimestampLocation is the zone go-mysql used when it printed payload
// TIMESTAMP strings. It is the process zone. Tests replace the pointer.
var payloadTimestampLocation = time.Local

// normalizePayloadTimestampCells rewrites TIMESTAMP strings printed in loc into the
// UTC wall clock. DATETIME strings are left alone. A zero date does not parse and stays.
func normalizePayloadTimestampCells(event any, loc *time.Location) {
	rows, ok := event.(*replication.RowsEvent)
	if !ok || rows == nil || rows.Table == nil || loc == nil {
		return
	}
	for _, row := range rows.Rows {
		for i, value := range row {
			if i >= len(rows.Table.ColumnType) {
				break
			}
			switch rows.Table.ColumnType[i] {
			case mysql.MYSQL_TYPE_TIMESTAMP, mysql.MYSQL_TYPE_TIMESTAMP2:
				text, ok := value.(string)
				if !ok {
					continue
				}
				if utc, ok := timestampWallToUTC(text, loc); ok {
					row[i] = utc
				}
			}
		}
	}
}

func timestampWallToUTC(raw string, loc *time.Location) (string, bool) {
	layout := "2006-01-02 15:04:05"
	if dot := strings.IndexByte(raw, '.'); dot >= 0 {
		frac := len(raw) - dot - 1
		if frac < 1 || frac > 6 || dot != len(layout) {
			return "", false
		}
		layout += "." + strings.Repeat("0", frac)
	} else if len(raw) != len(layout) {
		return "", false
	}
	parsed, err := time.ParseInLocation(layout, raw, loc)
	if err != nil {
		return "", false
	}
	return parsed.UTC().Format(layout), true
}

// assignPayloadWrapperFileSpan stamps every expanded inner with the wrapper's
// file-relative [start, end) and charges BinlogBytes once.
func assignPayloadWrapperFileSpan(inners []RawEvent, start, end, size int64) {
	for i := range inners {
		inners[i].PositionStart = start
		inners[i].PositionEnd = end
		inners[i].BinlogBytes = 0
		inners[i].Position = uint32(end)
	}
	if len(inners) > 0 {
		inners[0].BinlogBytes = size
	}
}

func pkMetaForRows(event *replication.RowsEvent, tableNames map[uint64]cachedTableName) pkMeta {
	if event == nil {
		return pkMeta{}
	}
	if event.Table != nil {
		return pkMetaFrom(event.Table)
	}
	if tableNames == nil {
		return pkMeta{}
	}
	name, ok := tableNames[event.TableID]
	if !ok {
		return pkMeta{}
	}
	return name.pk
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

	name := cachedFromTable(event.Table)
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
