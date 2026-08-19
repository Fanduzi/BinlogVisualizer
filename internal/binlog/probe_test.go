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

	// Single file: no successor to infer LastEventAt from, stays zero.
	if !probe.LastEventAt.IsZero() {
		t.Fatalf("expected zero LastEventAt for single-file probe, got %s", probe.LastEventAt)
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
		cursor    int64
		wantStart int64
		wantEnd   int64
		wantBytes int64
		wantNext  int64
	}{
		{
			name:      "nil header",
			header:    nil,
			cursor:    4,
			wantStart: 0,
			wantEnd:   0,
			wantBytes: 0,
			wantNext:  4,
		},
		{
			name:      "standard event",
			header:    &replication.EventHeader{LogPos: 123, EventSize: 19},
			cursor:    4,
			wantStart: 104,
			wantEnd:   123,
			wantBytes: 19,
			wantNext:  123,
		},
		{
			name:      "underflow clamps start",
			header:    &replication.EventHeader{LogPos: 10, EventSize: 40},
			cursor:    4,
			wantStart: 0,
			wantEnd:   10,
			wantBytes: 10,
			wantNext:  10,
		},
		{
			name:      "zero sized event with LogPos",
			header:    &replication.EventHeader{LogPos: 20, EventSize: 0},
			cursor:    20,
			wantStart: 20,
			wantEnd:   20,
			wantBytes: 0,
			wantNext:  20,
		},
		{
			name:      "MariaDB 11.4 zero LogPos reconstructs from cursor",
			header:    &replication.EventHeader{LogPos: 0, EventSize: 8000},
			cursor:    385,
			wantStart: 385,
			wantEnd:   8385,
			wantBytes: 8000,
			wantNext:  8385,
		},
		{
			name:      "MariaDB 11.4 XID still uses real LogPos",
			header:    &replication.EventHeader{LogPos: 77914948, EventSize: 31},
			cursor:    77914917,
			wantStart: 77914917,
			wantEnd:   77914948,
			wantBytes: 31,
			wantNext:  77914948,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotStart, gotEnd, gotBytes, gotNext := deriveEventPositionRange(tt.header, tt.cursor)
			if gotStart != tt.wantStart || gotEnd != tt.wantEnd || gotBytes != tt.wantBytes || gotNext != tt.wantNext {
				t.Fatalf("expected (%d,%d,%d,next=%d), got (%d,%d,%d,next=%d)",
					tt.wantStart, tt.wantEnd, tt.wantBytes, tt.wantNext, gotStart, gotEnd, gotBytes, gotNext)
			}
		})
	}
}

func TestMariaDBZeroLogPosRowEventsKeepNonZeroBytes(t *testing.T) {
	// Dogfood #18: MariaDB 11.8 WRITE_ROWS often have LogPos=0 and EventSize>0.
	// Reconstructing from the running cursor keeps minute binlog_bytes off zero.
	cursor := int64(625)
	start, end, bytes, next := deriveEventPositionRange(&replication.EventHeader{LogPos: 0, EventSize: 8192}, cursor)
	if start != 625 || end != 8817 || bytes != 8192 || next != 8817 {
		t.Fatalf("row event reconstruction = start=%d end=%d bytes=%d next=%d", start, end, bytes, next)
	}
	xidStart, xidEnd, xidBytes, _ := deriveEventPositionRange(&replication.EventHeader{LogPos: 77914948, EventSize: 31}, next)
	if xidStart != 77914917 || xidEnd != 77914948 || xidBytes != 31 {
		t.Fatalf("XID range = start=%d end=%d bytes=%d", xidStart, xidEnd, xidBytes)
	}
}

func TestProbeFilesFastPathInfersLastFromNextFile(t *testing.T) {
	dir := t.TempDir()
	pathA := filepath.Join(dir, "mysql-bin.000001")
	pathB := filepath.Join(dir, "mysql-bin.000002")
	if err := os.WriteFile(pathA, make([]byte, 300), 0o644); err != nil {
		t.Fatalf("write %s: %v", pathA, err)
	}
	if err := os.WriteFile(pathB, make([]byte, 300), 0o644); err != nil {
		t.Fatalf("write %s: %v", pathB, err)
	}

	parser := &stubOffsetProbeParser{
		stubProbeParser: stubProbeParser{
			events: map[string][]RawEvent{
				pathA: {
					{BinlogPath: pathA, Timestamp: time.Date(2026, 3, 15, 10, 0, 0, 0, time.UTC)},
					{BinlogPath: pathA, Timestamp: time.Date(2026, 3, 15, 10, 5, 0, 0, time.UTC)},
				},
				pathB: {
					{BinlogPath: pathB, Timestamp: time.Date(2026, 3, 15, 10, 30, 0, 0, time.UTC)},
				},
			},
		},
	}

	probes, err := probeFilesWithParser([]string{pathA, pathB}, parser)
	if err != nil {
		t.Fatalf("probe files: %v", err)
	}
	if len(probes) != 2 {
		t.Fatalf("expected 2 probes, got %d", len(probes))
	}

	wantFirstA := time.Date(2026, 3, 15, 10, 0, 0, 0, time.UTC)
	if !probes[0].FirstEventAt.Equal(wantFirstA) {
		t.Fatalf("expected FirstEventAt[0]=%s, got %s", wantFirstA, probes[0].FirstEventAt)
	}

	// LastEventAt[0] should be inferred from FirstEventAt[1].
	wantLastA := time.Date(2026, 3, 15, 10, 30, 0, 0, time.UTC)
	if !probes[0].LastEventAt.Equal(wantLastA) {
		t.Fatalf("expected LastEventAt[0]=%s (inferred from next file), got %s", wantLastA, probes[0].LastEventAt)
	}

	// Last file has no successor, LastEventAt stays zero.
	if !probes[1].LastEventAt.IsZero() {
		t.Fatalf("expected zero LastEventAt[1] (last file), got %s", probes[1].LastEventAt)
	}
}

func TestProbeFilesFastPathLeavesLastZeroForSingleFile(t *testing.T) {
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

	// Single file has no successor, so LastEventAt stays zero.
	if !probes[0].LastEventAt.IsZero() {
		t.Fatalf("expected zero LastEventAt for single file (no successor), got %s", probes[0].LastEventAt)
	}
}

func TestProbeFilesFastPathSkipsInferenceWhenNextFileHasZeroFirst(t *testing.T) {
	dir := t.TempDir()
	pathA := filepath.Join(dir, "mysql-bin.000001")
	pathB := filepath.Join(dir, "mysql-bin.000002")
	if err := os.WriteFile(pathA, make([]byte, 300), 0o644); err != nil {
		t.Fatalf("write %s: %v", pathA, err)
	}
	if err := os.WriteFile(pathB, make([]byte, 100), 0o644); err != nil {
		t.Fatalf("write %s: %v", pathB, err)
	}

	parser := &stubOffsetProbeParser{
		stubProbeParser: stubProbeParser{
			events: map[string][]RawEvent{
				pathA: {
					{BinlogPath: pathA, Timestamp: time.Date(2026, 3, 15, 10, 0, 0, 0, time.UTC)},
				},
				pathB: {
					{BinlogPath: pathB}, // zero timestamp — freshly rotated binlog
				},
			},
		},
	}

	probes, err := probeFilesWithParser([]string{pathA, pathB}, parser)
	if err != nil {
		t.Fatalf("probe files: %v", err)
	}
	if len(probes) != 2 {
		t.Fatalf("expected 2 probes, got %d", len(probes))
	}

	// File A should NOT infer LastEventAt from file B (zero FirstEventAt).
	if !probes[0].LastEventAt.IsZero() {
		t.Fatalf("expected zero LastEventAt[0] (successor has zero FirstEventAt), got %s", probes[0].LastEventAt)
	}
	if !probes[0].FirstEventAt.Equal(time.Date(2026, 3, 15, 10, 0, 0, 0, time.UTC)) {
		t.Fatalf("expected FirstEventAt[0]=10:00, got %s", probes[0].FirstEventAt)
	}
}

func TestProbeFilesFastPathInferenceWithThreeFiles(t *testing.T) {
	dir := t.TempDir()
	pathA := filepath.Join(dir, "mysql-bin.000001")
	pathB := filepath.Join(dir, "mysql-bin.000002")
	pathC := filepath.Join(dir, "mysql-bin.000003")
	for _, p := range []string{pathA, pathB, pathC} {
		if err := os.WriteFile(p, make([]byte, 300), 0o644); err != nil {
			t.Fatalf("write %s: %v", p, err)
		}
	}

	parser := &stubOffsetProbeParser{
		stubProbeParser: stubProbeParser{
			events: map[string][]RawEvent{
				pathA: {{BinlogPath: pathA, Timestamp: time.Date(2026, 3, 15, 10, 0, 0, 0, time.UTC)}},
				pathB: {{BinlogPath: pathB, Timestamp: time.Date(2026, 3, 15, 11, 0, 0, 0, time.UTC)}},
				pathC: {{BinlogPath: pathC, Timestamp: time.Date(2026, 3, 15, 12, 0, 0, 0, time.UTC)}},
			},
		},
	}

	probes, err := probeFilesWithParser([]string{pathA, pathB, pathC}, parser)
	if err != nil {
		t.Fatalf("probe files: %v", err)
	}
	if len(probes) != 3 {
		t.Fatalf("expected 3 probes, got %d", len(probes))
	}

	wantLastA := time.Date(2026, 3, 15, 11, 0, 0, 0, time.UTC)
	if !probes[0].LastEventAt.Equal(wantLastA) {
		t.Fatalf("expected LastEventAt[0]=%s (from file B), got %s", wantLastA, probes[0].LastEventAt)
	}

	wantLastB := time.Date(2026, 3, 15, 12, 0, 0, 0, time.UTC)
	if !probes[1].LastEventAt.Equal(wantLastB) {
		t.Fatalf("expected LastEventAt[1]=%s (from file C), got %s", wantLastB, probes[1].LastEventAt)
	}

	// Last file: no successor, LastEventAt stays zero.
	if !probes[2].LastEventAt.IsZero() {
		t.Fatalf("expected zero LastEventAt[2] (last file), got %s", probes[2].LastEventAt)
	}
}

func TestProbeFilesFastPathReturnsZeroWhenNoTimestamps(t *testing.T) {
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
