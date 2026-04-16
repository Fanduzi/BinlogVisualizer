// Package binlog normalizes raw parser events into analyzer-facing events.
// input: RawEvent values emitted by the binlog parser layer.
// output: model.NormalizedEvent values with bounded SQL context and stable event kinds.
// pos: normalization boundary between parser extraction and analyzer consumption.
// note: if this file changes, keep internal/binlog/README.md synchronized.
package binlog

import (
	"strings"
	"unicode/utf8"

	"binlogviz/internal/model"
)

// NormalizeRawEvent converts a RawEvent into a NormalizedEvent for analysis.
// Returns nil for events that should be skipped (e.g., FORMAT_DESCRIPTION).
func NormalizeRawEvent(raw RawEvent) (*model.NormalizedEvent, error) {
	switch {
	case raw.EventType == "QUERY_EVENT" || raw.EventType == "QueryEvent":
		return normalizeQueryEvent(raw)
	case raw.EventType == "RowsQueryEvent" || raw.EventType == "ROWS_QUERY_EVENT":
		return normalizeRowsQueryEvent(raw)
	case strings.HasPrefix(raw.EventType, "WriteRows") || strings.HasPrefix(raw.EventType, "WRITE_ROWS"):
		ev := newNormalizedEvent(raw)
		ev.EventType = "ROWS"
		ev.Operation = "INSERT"
		return ev, nil
	case strings.HasPrefix(raw.EventType, "UpdateRows") || strings.HasPrefix(raw.EventType, "UPDATE_ROWS"):
		ev := newNormalizedEvent(raw)
		ev.EventType = "ROWS"
		ev.Operation = "UPDATE"
		return ev, nil
	case strings.HasPrefix(raw.EventType, "DeleteRows") || strings.HasPrefix(raw.EventType, "DELETE_ROWS"):
		ev := newNormalizedEvent(raw)
		ev.EventType = "ROWS"
		ev.Operation = "DELETE"
		return ev, nil
	case raw.EventType == "XID_EVENT" || raw.EventType == "XIDEvent":
		ev := newNormalizedEvent(raw)
		ev.EventType = "XID"
		return ev, nil
	case raw.EventType == "TABLE_MAP_EVENT" || raw.EventType == "TableMapEvent":
		ev := newNormalizedEvent(raw)
		ev.EventType = "TABLE_MAP"
		return ev, nil
	default:
		// Skip unsupported events
		return nil, nil
	}
}

func newNormalizedEvent(raw RawEvent) *model.NormalizedEvent {
	return &model.NormalizedEvent{
		Timestamp:     raw.Timestamp,
		BinlogPath:    raw.BinlogPath,
		PositionStart: raw.PositionStart,
		PositionEnd:   raw.PositionEnd,
		BinlogBytes:   raw.BinlogBytes,
		Schema:        raw.Schema,
		Table:         raw.Table,
		RowCount:      raw.RowCount,
	}
}

func normalizeQueryEvent(raw RawEvent) (*model.NormalizedEvent, error) {
	query := strings.TrimSpace(raw.Query)
	switch {
	case strings.EqualFold(query, "BEGIN"):
		ev := newNormalizedEvent(raw)
		ev.EventType = "BEGIN"
		return ev, nil
	case strings.EqualFold(query, "COMMIT"):
		ev := newNormalizedEvent(raw)
		ev.EventType = "COMMIT"
		return ev, nil
	default:
		// Skip other QUERY events (DDL, etc.)
		return nil, nil
	}
}

// normalizeRowsQueryEvent handles Rows_query_log_event which contains the original SQL.
// The SQL is bounded at model.MaxStoredSQLBytes to prevent memory issues with huge queries.
// OriginalBytes is preserved for accurate reporting even when SQL is truncated.
func normalizeRowsQueryEvent(raw RawEvent) (*model.NormalizedEvent, error) {
	ev := newNormalizedEvent(raw)
	ev.EventType = "ROWS_QUERY"

	sql := raw.QuerySQL
	if sql == "" {
		return ev, nil
	}

	// Preserve original byte count before any truncation
	originalBytes := len(sql)
	ev.QueryOriginalBytes = originalBytes

	// Bound the SQL to prevent memory bloat
	if originalBytes > model.MaxStoredSQLBytes {
		sql = safeTruncateBytes(sql, model.MaxStoredSQLBytes)
		ev.QueryTruncated = true
	}
	ev.QuerySQL = sql

	return ev, nil
}

// safeTruncateBytes truncates to maxBytes without cutting UTF-8 characters.
func safeTruncateBytes(s string, maxBytes int) string {
	if len(s) <= maxBytes {
		return s
	}
	if maxBytes <= 0 {
		return ""
	}

	truncated := s[:maxBytes]
	for len(truncated) > 0 && !utf8.ValidString(truncated) {
		_, size := utf8.DecodeLastRuneInString(truncated)
		if size <= 0 {
			return ""
		}
		truncated = truncated[:len(truncated)-size]
	}
	return truncated
}
