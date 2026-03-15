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
