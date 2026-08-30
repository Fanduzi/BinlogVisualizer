// Package binlog defines raw binlog event types and parser contracts used by the command layer.
// input: timestamps, producer/transaction provenance, table/query metadata, row counts, file-relative offsets, and parser callback expectations.
// output: stable RawEvent values with optional provenance and XA identity plus Parser and ProgressParser interfaces shared across parsing and analysis code.
// pos: contract boundary isolating analyzer and CLI orchestration from concrete binlog parser implementations.
// note: if this file changes, update this header and README.md.
package binlog

import "time"

// RawEvent represents a raw binlog event before normalization.
// This type isolates the analyzer from parser-specific types.
type RawEvent struct {
	Timestamp     time.Time
	BinlogPath    string
	PositionStart int64
	PositionEnd   int64
	BinlogBytes   int64
	EventType     string
	Schema        string
	Table         string
	Query         string // SQL query for QUERY_EVENT (e.g., BEGIN, COMMIT)
	QuerySQL      string // Original SQL from Rows_query_log_event (when binlog_rows_query_log_events=ON)
	ServerID      uint32 // Event-header server ID; zero means unavailable.
	ServerVersion string // Format Description server version, when the event carries one.
	ServerFlavor  string // mysql or mariadb, derived from ServerVersion when available.
	GTID          string
	ThreadID      uint32
	XID           string // Decimal transaction XID; empty means unavailable.
	XAXID         string // SQL-form XA identifier; empty means unavailable.
	ActorUser     string
	ActorHost     string
	RowCount      int
	Position      uint32 // Legacy next-event position retained for existing callers and error messages.
}

// Parser defines the interface for parsing binlog files.
// Implementations should wrap parser libraries and emit RawEvents with source metadata populated.
type Parser interface {
	// ParseFiles reads one or more binlog files and calls handler for each event.
	ParseFiles(paths []string, handler func(RawEvent) error) error
}

// ParseProgress reports file-relative parse progress for a specific input path occurrence.
type ParseProgress struct {
	Path   string
	Index  int
	Offset int64
}

// ProgressParser optionally reports file-relative parse progress during parsing.
type ProgressParser interface {
	ParseFilesWithProgress(paths []string, onProgress func(ParseProgress), handler func(RawEvent) error) error
}

// OffsetParser parses binlog files starting from a byte offset.
type OffsetParser interface {
	ParseFilesFromOffset(paths []string, offset int64, handler func(RawEvent) error) error
}
