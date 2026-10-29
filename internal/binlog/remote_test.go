// Package binlog tests the remote frame parser against real fixture bytes.
// input: fixture binlog frames replayed through a fake FrameSource, with crafted ROTATE frames for boundary cases.
// output: parity with the file parser on event content, boundary stop behavior, and CorruptEventError on hostile frames.
// pos: unit tests for remote.go; the FrameSource seam keeps them network-free (ADR 0021).
// note: if this file changes, update this header and README.md.
package binlog

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"testing"

	"github.com/go-mysql-org/go-mysql/replication"
)

// splitFrames cuts a fixture binlog file into the event frames a server
// would stream, in order.
func splitFrames(t *testing.T, path string) [][]byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) < binlogMagicSize {
		t.Fatalf("%s is shorter than the binlog magic", path)
	}
	data = data[binlogMagicSize:]
	var frames [][]byte
	for pos := 0; pos < len(data); {
		if len(data)-pos < replication.EventHeaderSize {
			t.Fatalf("%s: trailing %d bytes below a header", path, len(data)-pos)
		}
		size := int(binary.LittleEndian.Uint32(data[pos+eventSizeOffset:]))
		if size <= 0 || pos+size > len(data) {
			t.Fatalf("%s: event at %d declares size %d out of %d bytes", path, pos, size, len(data))
		}
		frames = append(frames, data[pos:pos+size])
		pos += size
	}
	return frames
}

// makeRotateFrame builds a ROTATE_EVENT frame pointing at name:pos, as the
// server sends at dump start and on rotation. crc says whether the trailing
// checksum field is present: frames sent before the Format Description carry
// none, frames after it carry a CRC32 the reader verifies.
func makeRotateFrame(name string, pos uint64, logPos uint32, crc bool) []byte {
	body := make([]byte, 8+len(name))
	binary.LittleEndian.PutUint64(body, pos)
	copy(body[8:], name)
	size := replication.EventHeaderSize + len(body)
	if crc {
		size += replication.BinlogChecksumLength
	}
	raw := make([]byte, size)
	raw[4] = byte(replication.ROTATE_EVENT)
	binary.LittleEndian.PutUint32(raw[5:], 1)
	binary.LittleEndian.PutUint32(raw[9:], uint32(size))
	binary.LittleEndian.PutUint32(raw[13:], logPos)
	copy(raw[replication.EventHeaderSize:], body)
	if crc {
		sum := crc32.ChecksumIEEE(raw[:len(raw)-replication.BinlogChecksumLength])
		binary.LittleEndian.PutUint32(raw[len(raw)-replication.BinlogChecksumLength:], sum)
	}
	return raw
}

// fakeSource replays frames in order; errAt returns err instead of that frame.
type fakeSource struct {
	frames [][]byte
	i      int
	errAt  int
	err    error
}

func (f *fakeSource) Next(context.Context) ([]byte, error) {
	if f.err != nil && f.i == f.errAt {
		return nil, f.err
	}
	if f.i >= len(f.frames) {
		return nil, io.EOF
	}
	frame := f.frames[f.i]
	f.i++
	return frame, nil
}

// hangingSource replays frames then panics if Next is called once more —
// that call is where a real blocking dump waits forever once the server
// goes quiet. fakeSource's io.EOF masked exactly this hang.
type hangingSource struct {
	frames [][]byte
	i      int
}

func (h *hangingSource) Next(context.Context) ([]byte, error) {
	if h.i >= len(h.frames) {
		panic("Next called after the stream's last frame: a blocking dump connection waits forever here")
	}
	frame := h.frames[h.i]
	h.i++
	return frame, nil
}

func collectRemote(t *testing.T, src FrameSource, boundary RemoteBoundary) ([]RawEvent, error) {
	t.Helper()
	var events []RawEvent
	p := NewRemoteParser(context.Background(), src, boundary)
	err := p.ParseFiles([]string{"remote://db:3306"}, func(raw RawEvent) error {
		events = append(events, raw)
		return nil
	})
	return events, err
}

// TestRemoteParserGoldenPath replays a real fixture as a stream and requires
// the same events the file parser emits, byte for byte on the fields that
// matter. The FrameSource seam makes this the remote golden path (ADR 0021).
func TestRemoteParserGoldenPath(t *testing.T) {
	fixture := "testdata/mysql-8.0.46-dml-full.binlog"
	frames := splitFrames(t, fixture)

	var fileEvents []RawEvent
	if err := NewParser().ParseFiles([]string{fixture}, func(raw RawEvent) error {
		fileEvents = append(fileEvents, raw)
		return nil
	}); err != nil {
		t.Fatalf("file parse failed: %v", err)
	}

	remoteEvents, err := collectRemote(t, &fakeSource{frames: frames}, RemoteBoundary{})
	if err != nil {
		t.Fatalf("remote parse failed: %v", err)
	}
	if len(remoteEvents) != len(fileEvents) {
		t.Fatalf("remote emitted %d events, file emitted %d", len(remoteEvents), len(fileEvents))
	}
	for i := range remoteEvents {
		got, want := remoteEvents[i], fileEvents[i]
		if got.BinlogPath != "remote://db:3306" {
			t.Fatalf("event %d path = %q, want the stream origin", i, got.BinlogPath)
		}
		if got.EventType != want.EventType ||
			got.Timestamp != want.Timestamp ||
			got.PositionStart != want.PositionStart ||
			got.PositionEnd != want.PositionEnd ||
			got.Schema != want.Schema ||
			got.Table != want.Table ||
			got.Query != want.Query ||
			got.RowCount != want.RowCount ||
			got.GTID != want.GTID ||
			got.XID != want.XID ||
			got.ServerID != want.ServerID {
			t.Fatalf("event %d diverged: remote %+v vs file %+v", i, got, want)
		}
	}
}

// TestRemoteParserRotateStampsPath: after a ROTATE the events carry
// remote://host:port/<file> so coverage and reports name the real binlog.
func TestRemoteParserRotateStampsPath(t *testing.T) {
	frames := splitFrames(t, "testdata/mysql-8.0.46-dml-full.binlog")
	stream := append([][]byte{makeRotateFrame("binlog.000001", 4, 0, false)}, frames...)

	events, err := collectRemote(t, &fakeSource{frames: stream}, RemoteBoundary{})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) < 2 {
		t.Fatalf("only %d events", len(events))
	}
	if events[0].BinlogPath != "remote://db:3306" {
		t.Fatalf("the rotate itself stays on the bare origin, got %q", events[0].BinlogPath)
	}
	for i, ev := range events[1:] {
		if ev.BinlogPath != "remote://db:3306/binlog.000001" {
			t.Fatalf("event %d path = %q", i+1, ev.BinlogPath)
		}
	}
}

// TestRemoteParserBoundaryStopsAtSnapshotEnd: events whose end position runs
// past the connect-time manifest end are excluded — data written after the
// probe is not part of this run (ADR 0021).
func TestRemoteParserBoundaryStopsAtSnapshotEnd(t *testing.T) {
	fixture := "testdata/mysql-8.0.46-dml-full.binlog"
	frames := splitFrames(t, fixture)
	stream := append([][]byte{makeRotateFrame("binlog.000001", 4, 0, false)}, frames...)

	var fileEvents []RawEvent
	if err := NewParser().ParseFiles([]string{fixture}, func(raw RawEvent) error {
		fileEvents = append(fileEvents, raw)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// Stop after the first two events: the boundary is their end position.
	cut := fileEvents[1].PositionEnd
	events, err := collectRemote(t, &fakeSource{frames: stream}, RemoteBoundary{File: "binlog.000001", Size: cut})
	if err != nil {
		t.Fatalf("boundary stop must be a clean end, got %v", err)
	}
	// rotate + the two events at/below the boundary.
	if len(events) != 3 {
		t.Fatalf("expected rotate + 2 events, got %d", len(events))
	}
	if events[2].PositionEnd != cut {
		t.Fatalf("last emitted event ends at %d, want %d", events[2].PositionEnd, cut)
	}
}

// TestRemoteParserStopsWhenBoundaryReached: when the last retained event
// ends exactly at the manifest end, the parser must emit it and stop on
// its own — a blocking dump sends nothing after that point, so calling
// Next again hangs forever (the live e2e hit this and timed out). The
// stream is truncated at the boundary frame and the source panics on one
// more Next, so passing proves the parser never reads past the boundary.
func TestRemoteParserStopsWhenBoundaryReached(t *testing.T) {
	fixture := "testdata/mysql-8.0.46-dml-full.binlog"
	frames := splitFrames(t, fixture)

	var fileEvents []RawEvent
	if err := NewParser().ParseFiles([]string{fixture}, func(raw RawEvent) error {
		fileEvents = append(fileEvents, raw)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	cut := fileEvents[1].PositionEnd

	// Rotate + the two events at/below the boundary, then nothing: any
	// further Next is the blocking read that hung the e2e test.
	stream := append([][]byte{makeRotateFrame("binlog.000001", 4, 0, false)}, frames[:2]...)
	events, err := collectRemote(t, &hangingSource{frames: stream}, RemoteBoundary{File: "binlog.000001", Size: cut})
	if err != nil {
		t.Fatalf("reaching the boundary must be a clean end, got %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("expected rotate + 2 events, got %d", len(events))
	}
	if events[2].PositionEnd != cut {
		t.Fatalf("last emitted event ends at %d, want the boundary %d", events[2].PositionEnd, cut)
	}
}

// TestRemoteParserRotatePastBoundary: a rotate naming a file beyond the
// manifest's last entry means the server rotated after the probe — stop.
// The fixture's own trailing rotate names mysql-bin.000004, so it is the
// boundary file here; a crafted rotate past it must not be emitted.
func TestRemoteParserRotatePastBoundary(t *testing.T) {
	frames := splitFrames(t, "testdata/mysql-8.0.46-dml-full.binlog")
	stream := append(
		[][]byte{makeRotateFrame("mysql-bin.000003", 4, 0, false)},
		frames...,
	)
	stream = append(stream, makeRotateFrame("mysql-bin.000005", 4, 0, true))

	events, err := collectRemote(t, &fakeSource{frames: stream}, RemoteBoundary{File: "mysql-bin.000004", Size: 1 << 30})
	if err != nil {
		t.Fatalf("rotate past the boundary must be a clean end, got %v", err)
	}
	// The stream held the prepended rotate, the fixture (including its own
	// rotate onto the boundary file), and a trailing rotate past it: only
	// the last one is excluded.
	if len(events) != len(frames)+1 {
		t.Fatalf("expected the rotate beyond the boundary to be excluded: %d events", len(events))
	}
}

func TestRemoteParserCorruptCRC(t *testing.T) {
	frames := splitFrames(t, "testdata/mysql-8.0.46-dml-full.binlog")
	bad := append([]byte(nil), frames[2]...)
	bad[len(bad)-1] ^= 0xff
	frames[2] = bad

	_, err := collectRemote(t, &fakeSource{frames: frames}, RemoteBoundary{})
	var corrupt *CorruptEventError
	if !errors.As(err, &corrupt) {
		t.Fatalf("a flipped checksum byte must be corrupt, got %v", err)
	}
}

func TestRemoteParserSizeMismatch(t *testing.T) {
	frames := splitFrames(t, "testdata/mysql-8.0.46-dml-full.binlog")
	bad := append([]byte(nil), frames[1]...)
	binary.LittleEndian.PutUint32(bad[eventSizeOffset:], uint32(len(bad)+4))
	frames[1] = bad

	_, err := collectRemote(t, &fakeSource{frames: frames}, RemoteBoundary{})
	var corrupt *CorruptEventError
	if !errors.As(err, &corrupt) {
		t.Fatalf("a size field lying about the frame must be corrupt, got %v", err)
	}
}

func TestRemoteParserShortFrame(t *testing.T) {
	_, err := collectRemote(t, &fakeSource{frames: [][]byte{{1, 2, 3}}}, RemoteBoundary{})
	var corrupt *CorruptEventError
	if !errors.As(err, &corrupt) {
		t.Fatalf("a frame shorter than a header must be corrupt, got %v", err)
	}
}

// TestRemoteParserStreamError: a mid-stream failure propagates — a truncated
// read is an input failure at the command layer, never a silent short report.
func TestRemoteParserStreamError(t *testing.T) {
	frames := splitFrames(t, "testdata/mysql-8.0.46-dml-full.binlog")
	boom := fmt.Errorf("connection reset")
	_, err := collectRemote(t, &fakeSource{frames: frames, errAt: 3, err: boom}, RemoteBoundary{})
	if !errors.Is(err, boom) {
		t.Fatalf("stream error = %v, want the injected failure", err)
	}
}
