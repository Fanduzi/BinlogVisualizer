// Package binlog parses remote replication frames through the same checked decode as file bytes.
// input: a FrameSource of raw event bytes, a connect-time end boundary, and an origin label (remote://host:port).
// output: RawEvents stamped remote://host:port/<file>, bounded to the manifest's last file end; the boundary is a clean EOF, never an error.
// pos: the remote Parser implementation; the FrameSource seam keeps this file network-free and unit-testable (ADR 0021).
// note: if this file changes, update this header and README.md.
package binlog

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
)

// FrameSource is the remote seam the parser consumes. internal/remote.Source
// satisfies it; tests feed frames by hand. Each Next returns one whole event
// frame — header, body and checksum footer, the same bytes a file stores.
type FrameSource interface {
	Next(ctx context.Context) ([]byte, error)
}

// RemoteBoundary is where a remote analyze stops: the last entry of the
// SHOW BINARY LOGS manifest taken at connect (ADR 0021). Events written
// after the probe are not part of this run.
type RemoteBoundary struct {
	File string
	Size int64
}

// remoteParser implements Parser over a live frame stream.
type remoteParser struct {
	ctx          context.Context
	src          FrameSource
	boundary     RemoteBoundary
	captureRows  bool
	captureFlash bool
}

// NewRemoteParser builds a Parser that reads remote frames until the
// boundary or a clean close. ctx bounds the whole stream.
func NewRemoteParser(ctx context.Context, src FrameSource, boundary RemoteBoundary) Parser {
	return &remoteParser{ctx: ctx, src: src, boundary: boundary}
}

// SetCaptureRowImages keeps bounded cell values on ROW events.
func (p *remoteParser) SetCaptureRowImages(on bool) {
	if p == nil {
		return
	}
	p.captureRows = on
}

// SetCaptureFlashback keeps exact cell literals on ROW events for undo SQL.
func (p *remoteParser) SetCaptureFlashback(on bool) {
	if p == nil {
		return
	}
	p.captureFlash = on
}

// ParseFiles reads the stream. paths carries one synthetic origin label
// (remote://host:port) that prefixes each event's real binlog file.
func (p *remoteParser) ParseFiles(paths []string, handler func(RawEvent) error) error {
	origin := ""
	if len(paths) > 0 {
		origin = paths[0]
	}
	return p.parseRemote(origin, handler)
}

// errRemoteEnd stops the stream inside the event callback: the connect-time
// boundary was reached. It unwinds to the parse loop, which reports a clean
// end — it never reaches the caller as an error.
var errRemoteEnd = errors.New("remote read reached its connect-time end")

func (p *remoteParser) parseRemote(origin string, handler func(RawEvent) error) error {
	bp := replication.NewBinlogParser()
	bp.SetTimestampStringLocation(time.UTC)
	decodeRows := func(re *replication.RowsEvent, data []byte) error { return re.Decode(data) }
	if p.captureFlash {
		decodeRows = decodeRowsKeepJSON
	}
	fault := &rowsFault{}
	bp.SetRowsEventDecodeFunc(boundedRowsDecode(decodeRows, fault))

	tableNames := make(map[uint64]cachedTableName)
	serverVersion := ""
	currentFile := ""
	cursor := int64(binlogMagicSize)
	// consumed is stream bytes so far; CorruptEventError reports it as the
	// offset where the bad frame started.
	var consumed int64
	inHandler := false
	emittedAny := false
	consume := func(raw RawEvent) error {
		inHandler = true
		err := handler(raw)
		inHandler = false
		return err
	}
	eventPath := func() string {
		if currentFile == "" {
			return origin
		}
		return origin + "/" + currentFile
	}

	r := &eventReader{bp: bp, path: origin, consumed: &consumed, fault: fault}
	for {
		frame, err := p.src.Next(p.ctx)
		if err == io.EOF {
			// The exact sentinel is a clean close; a stream error that wraps
			// io.EOF (a dropped connection) is not an end of binlog.
			return nil
		}
		if err != nil {
			return err
		}
		if _, err := r.dispatchRemote(frame, &inHandler, func(ev *replication.BinlogEvent) error {
			if ev == nil || ev.Header == nil {
				return nil
			}
			var rot *replication.RotateEvent
			if ev.Header.EventType == replication.ROTATE_EVENT {
				if r, ok := ev.Event.(*replication.RotateEvent); ok {
					if p.rotatePastBoundary(string(r.NextLogName)) {
						return errRemoteEnd
					}
					rot = r
				}
			}
			// The rotate event lives in the file it closes: path and bounds
			// are the old file's; the tracking update follows them.
			path := eventPath()
			start, end, _, next := deriveEventPositionRange(ev.Header, cursor)
			if currentFile == p.boundary.File && p.boundary.File != "" && end > p.boundary.Size {
				return errRemoteEnd
			}
			cursor = next
			consumed += int64(ev.Header.EventSize)
			if rot != nil {
				name := string(rot.NextLogName)
				if name != currentFile {
					// Table ids are scoped to one binlog file; a rotate
					// starts a new map.
					currentFile = name
					tableNames = make(map[uint64]cachedTableName)
					cursor = int64(rot.Position)
				}
			}

			raw := rawEventFromHeader(ev.Header, path, serverVersion)
			raw.PositionStart, raw.PositionEnd, raw.BinlogBytes = start, end, end-start
			raw.Position = uint32(raw.PositionEnd)

			if payload, ok := ev.Event.(*streamedPayload); ok {
				emitted, err := streamTransactionPayload(payload, path, serverVersion, tableNames, p.captureRows, p.captureFlash, func(inner RawEvent) error {
					inner.PositionStart, inner.PositionEnd, inner.Position = raw.PositionStart, raw.PositionEnd, uint32(raw.PositionEnd)
					inner.BinlogBytes = 0
					if !emittedAny {
						inner.BinlogBytes = raw.BinlogBytes
					}
					emittedAny = true
					if inner.ServerVersion != "" {
						serverVersion = inner.ServerVersion
					}
					return consume(inner)
				})
				emittedAny = false
				if err != nil {
					if cause, ok := err.(corruptCause); ok {
						return &CorruptEventError{Path: path, Offset: raw.PositionStart, Cause: string(cause)}
					}
					return err
				}
				if emitted > 0 {
					return nil
				}
			}
			if inners, ok := expandTransactionPayload(ev, path, serverVersion, tableNames, p.captureRows, p.captureFlash); ok {
				assignPayloadWrapperFileSpan(inners, raw.PositionStart, raw.PositionEnd, raw.BinlogBytes)
				for i := range inners {
					if inners[i].ServerVersion != "" {
						serverVersion = inners[i].ServerVersion
					}
					if err := consume(inners[i]); err != nil {
						return err
					}
				}
				return nil
			}

			applyBinlogEventMetadata(&raw, ev.Header.EventType, ev.Event, tableNames, p.captureRows, p.captureFlash)
			if raw.ServerVersion != "" {
				serverVersion = raw.ServerVersion
			}
			return consume(raw)
		}); err != nil {
			if errors.Is(err, errRemoteEnd) {
				return nil
			}
			return err
		}
	}
}

// rotatePastBoundary reports whether a rotate names a binlog beyond the
// connect-time end: past the last manifest file, or absent from the server's
// retained set and sorting after it (ADR 0021).
func (p *remoteParser) rotatePastBoundary(name string) bool {
	boundary := p.boundary.File
	if boundary == "" || name == boundary {
		return false
	}
	return name > boundary
}

// dispatchRemote checks a whole network frame then hands it to dispatch.
// The header's event-size field must equal the frame length exactly: a
// mismatch means the frame cannot be trusted, so it is corrupt before decode.
// The panic guard turns a decoder fault into CorruptEventError, as
// parseFileGuarded does for file bytes.
func (r *eventReader) dispatchRemote(raw []byte, inHandler *bool, onEvent replication.OnEventFunc) (n int64, err error) {
	defer func() {
		recovered := recover()
		if recovered == nil {
			return
		}
		if *inHandler {
			panic(recovered)
		}
		err = &CorruptEventError{Path: r.path, Offset: *r.consumed, Cause: "event decoder failed on malformed event bytes"}
	}()
	if len(raw) < replication.EventHeaderSize {
		return 0, &CorruptEventError{Path: r.path, Offset: *r.consumed, Cause: "event frame shorter than the event header"}
	}
	eventSize := int64(binary.LittleEndian.Uint32(raw[eventSizeOffset:]))
	if eventSize != int64(len(raw)) {
		return 0, &CorruptEventError{Path: r.path, Offset: *r.consumed, Cause: "event size field does not match the frame length"}
	}
	return r.dispatch(raw, onEvent)
}
