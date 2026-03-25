// Package binlog defines raw binlog event types and parser contracts used by the command layer.
// input: timestamps, table/query metadata, row counts, file-relative offsets, and parser callback expectations.
// output: stable RawEvent values plus Parser and ProgressParser interfaces shared across parsing and analysis code.
// pos: contract boundary isolating analyzer and CLI orchestration from concrete binlog parser implementations.
// note: if this file changes, update this header and README.md.
package binlog

import "time"

// RawEvent represents a raw binlog event before normalization.
// This type isolates the analyzer from parser-specific types.
type RawEvent struct {
	Timestamp time.Time
	EventType string
	Schema    string
	Table     string
	Query     string // SQL query for QUERY_EVENT (e.g., BEGIN, COMMIT)
	QuerySQL  string // Original SQL from Rows_query_log_event (when binlog_rows_query_log_events=ON)
	RowCount  int
	Position  uint32
}

// Parser defines the interface for parsing binlog files.
// Implementations should wrap parser libraries and emit normalized RawEvents.
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
