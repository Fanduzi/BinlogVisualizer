// Package binlog verifies raw event metadata extraction and reusable file probing helpers.
// input: real binlog fixtures parsed through the package parser and probe APIs.
// output: regression coverage for source metadata passthrough and per-file timestamp/size probing.
// pos: focused test layer for DBA-oriented metadata without changing analyzer or planner semantics.
// note: if this file changes, update this header and README.md.
package binlog

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
)

type stubProbeParser struct {
	events     map[string][]RawEvent
	err        error
	parseCalls int
	lastPaths  []string
}

func (s *stubProbeParser) ParseFiles(paths []string, handler func(RawEvent) error) error {
	s.parseCalls++
	s.lastPaths = append([]string(nil), paths...)
	if s.err != nil {
		return s.err
	}

	for _, path := range paths {
		for _, event := range s.events[path] {
			if err := handler(event); err != nil {
				return err
			}
		}
	}

	return nil
}

// stubOffsetProbeParser extends stubProbeParser with OffsetParser support.
type stubOffsetProbeParser struct {
	stubProbeParser
	offsetEvents map[string][]RawEvent
	offsetErr    error
}

func (s *stubOffsetProbeParser) ParseFilesFromOffset(paths []string, offset int64, handler func(RawEvent) error) error {
	if s.offsetErr != nil {
		return s.offsetErr
	}
	for _, path := range paths {
		for _, event := range s.offsetEvents[path] {
			if err := handler(event); err != nil {
				return err
			}
		}
	}
	return nil
}

func TestParseFilesWithProgressCapturesRawEventMetadata(t *testing.T) {
	path := filepath.Join("testdata", "minimal.binlog")

	parser := NewParser()
	progressParser, ok := parser.(ProgressParser)
	if !ok {
		t.Fatal("expected parser to support progress")
	}

	var events []RawEvent
	err := progressParser.ParseFilesWithProgress([]string{path}, nil, func(raw RawEvent) error {
		events = append(events, raw)
		return nil
	})
	if err != nil {
		t.Fatalf("parse fixture: %v", err)
	}
	if len(events) == 0 {
		t.Fatal("expected parsed events")
	}
	for _, event := range events {
		if got, want := event.Position, uint32(event.PositionEnd); got != want {
			t.Fatalf("expected legacy Position=%d to match PositionEnd=%d for %s", got, want, event.EventType)
		}
		if event.PositionEnd < event.PositionStart {
			t.Fatalf("expected PositionEnd >= PositionStart for %s, got start=%d end=%d", event.EventType, event.PositionStart, event.PositionEnd)
		}
		if got, want := event.BinlogBytes, event.PositionEnd-event.PositionStart; got != want {
			t.Fatalf("expected BinlogBytes=%d to equal end-start=%d for %s", got, want, event.EventType)
		}
	}

	first := events[0]
	if first.EventType != "FormatDescriptionEvent" {
		t.Fatalf("expected first event to be FormatDescriptionEvent, got %q", first.EventType)
	}
	if first.BinlogPath != path {
		t.Fatalf("expected BinlogPath %q, got %q", path, first.BinlogPath)
	}
	if first.PositionStart != 4 {
		t.Fatalf("expected first PositionStart=4, got %d", first.PositionStart)
	}
	if first.PositionEnd != 123 {
		t.Fatalf("expected first PositionEnd=123, got %d", first.PositionEnd)
	}
	if first.BinlogBytes != 119 {
		t.Fatalf("expected first BinlogBytes=119, got %d", first.BinlogBytes)
	}

	var writeRows *RawEvent
	for index := range events {
		if events[index].EventType == "WriteRowsEventV2" {
			writeRows = &events[index]
			break
		}
	}
	if writeRows == nil {
		t.Fatal("expected WriteRowsEventV2 in fixture")
	}
	if writeRows.PositionStart != 553 {
		t.Fatalf("expected write rows PositionStart=553, got %d", writeRows.PositionStart)
	}
	if writeRows.PositionEnd != 599 {
		t.Fatalf("expected write rows PositionEnd=599, got %d", writeRows.PositionEnd)
	}
	if writeRows.BinlogBytes != 46 {
		t.Fatalf("expected write rows BinlogBytes=46, got %d", writeRows.BinlogBytes)
	}
}

func TestProbeFilesReportsFixtureMetadata(t *testing.T) {
	path := filepath.Join("testdata", "minimal.binlog")

	probes, err := ProbeFiles([]string{path})
	if err != nil {
		t.Fatalf("probe files: %v", err)
	}
	if len(probes) != 1 {
		t.Fatalf("expected one probe result, got %d", len(probes))
	}

	probe := probes[0]
	if probe.BinlogPath != path {
		t.Fatalf("expected BinlogPath %q, got %q", path, probe.BinlogPath)
	}
	if probe.SizeBytes != 1500 {
		t.Fatalf("expected SizeBytes=1500, got %d", probe.SizeBytes)
	}

	wantFirst := time.Date(2026, 3, 15, 14, 10, 14, 0, time.UTC)
	if !probe.FirstEventAt.Equal(wantFirst) {
		t.Fatalf("expected FirstEventAt=%s, got %s", wantFirst, probe.FirstEventAt)
	}

	wantLast := time.Date(2026, 3, 15, 14, 10, 26, 0, time.UTC)
	if !probe.LastEventAt.Equal(wantLast) {
		t.Fatalf("expected LastEventAt=%s, got %s", wantLast, probe.LastEventAt)
	}
}

func TestProbeFilesWithParserUsesChronologicalTimestampBounds(t *testing.T) {
	dir := t.TempDir()
	pathA := filepath.Join(dir, "mysql-bin.000001")
	pathB := filepath.Join(dir, "mysql-bin.000002")
	if err := os.WriteFile(pathA, []byte("abc"), 0o644); err != nil {
		t.Fatalf("write %s: %v", pathA, err)
	}
	if err := os.WriteFile(pathB, []byte("12345"), 0o644); err != nil {
		t.Fatalf("write %s: %v", pathB, err)
	}

	parser := &stubProbeParser{
		events: map[string][]RawEvent{
			pathA: {
				{BinlogPath: pathA, Timestamp: time.Date(2026, 3, 15, 15, 0, 0, 0, time.UTC)},
				{BinlogPath: pathA},
				{BinlogPath: pathA, Timestamp: time.Date(2026, 3, 15, 14, 0, 0, 0, time.UTC)},
			},
			pathB: {
				{BinlogPath: pathB, Timestamp: time.Date(2026, 3, 15, 17, 0, 0, 0, time.UTC)},
				{BinlogPath: pathB, Timestamp: time.Date(2026, 3, 15, 16, 0, 0, 0, time.UTC)},
			},
		},
	}

	probes, err := probeFilesWithParser([]string{pathA, pathB}, parser)
	if err != nil {
		t.Fatalf("probe files with parser: %v", err)
	}
	if parser.parseCalls != 1 {
		t.Fatalf("expected a single parser pass, got %d", parser.parseCalls)
	}
	if len(parser.lastPaths) != 2 || parser.lastPaths[0] != pathA || parser.lastPaths[1] != pathB {
		t.Fatalf("expected parser paths [%s %s], got %v", pathA, pathB, parser.lastPaths)
	}
	if len(probes) != 2 {
		t.Fatalf("expected 2 probes, got %d", len(probes))
	}

	if !probes[0].FirstEventAt.Equal(time.Date(2026, 3, 15, 14, 0, 0, 0, time.UTC)) {
		t.Fatalf("expected earliest timestamp for first file, got %s", probes[0].FirstEventAt)
	}
	if !probes[0].LastEventAt.Equal(time.Date(2026, 3, 15, 15, 0, 0, 0, time.UTC)) {
		t.Fatalf("expected latest timestamp for first file, got %s", probes[0].LastEventAt)
	}
	if probes[0].SizeBytes != 3 {
		t.Fatalf("expected SizeBytes=3 for first file, got %d", probes[0].SizeBytes)
	}

	if !probes[1].FirstEventAt.Equal(time.Date(2026, 3, 15, 16, 0, 0, 0, time.UTC)) {
		t.Fatalf("expected earliest timestamp for second file, got %s", probes[1].FirstEventAt)
	}
	if !probes[1].LastEventAt.Equal(time.Date(2026, 3, 15, 17, 0, 0, 0, time.UTC)) {
		t.Fatalf("expected latest timestamp for second file, got %s", probes[1].LastEventAt)
	}
	if probes[1].SizeBytes != 5 {
		t.Fatalf("expected SizeBytes=5 for second file, got %d", probes[1].SizeBytes)
	}
}

func TestProbeFilesWithParserPropagatesParserErrors(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mysql-bin.000001")
	if err := os.WriteFile(path, []byte("abc"), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}

	wantErr := errors.New("boom")
	_, err := probeFilesWithParser([]string{path}, &stubProbeParser{err: wantErr})
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected parser error %v, got %v", wantErr, err)
	}
}

func TestDeriveEventPositionRange(t *testing.T) {
	tests := []struct {
		name      string
		header    *replication.EventHeader
		wantStart int64
		wantEnd   int64
		wantBytes int64
	}{
		{
			name:      "nil header",
			header:    nil,
			wantStart: 0,
			wantEnd:   0,
			wantBytes: 0,
		},
		{
			name:      "standard event",
			header:    &replication.EventHeader{LogPos: 123, EventSize: 19},
			wantStart: 104,
			wantEnd:   123,
			wantBytes: 19,
		},
		{
			name:      "underflow clamps start",
			header:    &replication.EventHeader{LogPos: 10, EventSize: 40},
			wantStart: 0,
			wantEnd:   10,
			wantBytes: 10,
		},
		{
			name:      "zero sized event",
			header:    &replication.EventHeader{LogPos: 20, EventSize: 0},
			wantStart: 20,
			wantEnd:   20,
			wantBytes: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotStart, gotEnd, gotBytes := deriveEventPositionRange(tt.header)
			if gotStart != tt.wantStart || gotEnd != tt.wantEnd || gotBytes != tt.wantBytes {
				t.Fatalf("expected (%d,%d,%d), got (%d,%d,%d)", tt.wantStart, tt.wantEnd, tt.wantBytes, gotStart, gotEnd, gotBytes)
			}
		})
	}
}

func TestProbeFilesWithOffsetParserUsesOffsetForLastTimestamp(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mysql-bin.000001")
	if err := os.WriteFile(path, make([]byte, 300), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}

	parser := &stubOffsetProbeParser{
		stubProbeParser: stubProbeParser{
			events: map[string][]RawEvent{
				path: {
					{BinlogPath: path, Timestamp: time.Date(2026, 3, 15, 10, 0, 0, 0, time.UTC)},
					{BinlogPath: path, Timestamp: time.Date(2026, 3, 15, 10, 5, 0, 0, time.UTC)},
				},
			},
		},
		offsetEvents: map[string][]RawEvent{
			path: {
				{BinlogPath: path, Timestamp: time.Date(2026, 3, 15, 10, 5, 0, 0, time.UTC)},
				{BinlogPath: path, Timestamp: time.Date(2026, 3, 15, 10, 30, 0, 0, time.UTC)},
			},
		},
	}

	probes, err := probeFilesWithParser([]string{path}, parser)
	if err != nil {
		t.Fatalf("probe files: %v", err)
	}
	if len(probes) != 1 {
		t.Fatalf("expected 1 probe, got %d", len(probes))
	}

	wantFirst := time.Date(2026, 3, 15, 10, 0, 0, 0, time.UTC)
	if !probes[0].FirstEventAt.Equal(wantFirst) {
		t.Fatalf("expected FirstEventAt=%s, got %s", wantFirst, probes[0].FirstEventAt)
	}

	// Last timestamp should come from offset parse (10:30), not full parse (10:05).
	wantLast := time.Date(2026, 3, 15, 10, 30, 0, 0, time.UTC)
	if !probes[0].LastEventAt.Equal(wantLast) {
		t.Fatalf("expected LastEventAt=%s (from offset), got %s", wantLast, probes[0].LastEventAt)
	}
}

func TestProbeFilesWithOffsetParserLeavesLastZeroWhenOffsetErrors(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mysql-bin.000001")
	if err := os.WriteFile(path, make([]byte, 300), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}

	parser := &stubOffsetProbeParser{
		stubProbeParser: stubProbeParser{
			events: map[string][]RawEvent{
				path: {
					{BinlogPath: path, Timestamp: time.Date(2026, 3, 15, 10, 0, 0, 0, time.UTC)},
					{BinlogPath: path, Timestamp: time.Date(2026, 3, 15, 10, 5, 0, 0, time.UTC)},
				},
			},
		},
		offsetErr: errors.New("get event err EOF, need 1967665253 but got 262125"),
	}

	probes, err := probeFilesWithParser([]string{path}, parser)
	if err != nil {
		t.Fatalf("expected graceful handling, got error: %v", err)
	}
	if len(probes) != 1 {
		t.Fatalf("expected 1 probe, got %d", len(probes))
	}

	wantFirst := time.Date(2026, 3, 15, 10, 0, 0, 0, time.UTC)
	if !probes[0].FirstEventAt.Equal(wantFirst) {
		t.Fatalf("expected FirstEventAt=%s, got %s", wantFirst, probes[0].FirstEventAt)
	}

	// When offset probe errors, LastEventAt must stay zero so the planner
	// treats the file's end time as unknown and includes it conservatively.
	if !probes[0].LastEventAt.IsZero() {
		t.Fatalf("expected zero LastEventAt when offset probe errors, got %s", probes[0].LastEventAt)
	}
}

func TestProbeFilesWithOffsetParserReturnsZeroWhenNoTimestamps(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mysql-bin.000001")
	if err := os.WriteFile(path, make([]byte, 300), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}

	parser := &stubOffsetProbeParser{
		stubProbeParser: stubProbeParser{
			events: map[string][]RawEvent{
				path: {{BinlogPath: path}}, // zero timestamp
			},
		},
		offsetErr: errors.New("corrupt event"),
	}

	probes, err := probeFilesWithParser([]string{path}, parser)
	if err != nil {
		t.Fatalf("probe files: %v", err)
	}
	if len(probes) != 1 {
		t.Fatalf("expected 1 probe, got %d", len(probes))
	}

	if !probes[0].FirstEventAt.IsZero() {
		t.Fatalf("expected zero FirstEventAt, got %s", probes[0].FirstEventAt)
	}
	if !probes[0].LastEventAt.IsZero() {
		t.Fatalf("expected zero LastEventAt, got %s", probes[0].LastEventAt)
	}
}
