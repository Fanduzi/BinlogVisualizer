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
	if raw.EventType == "QUERY_EVENT" || raw.EventType == "QueryEvent" {
		query := strings.TrimSpace(raw.Query)
		if !strings.EqualFold(query, "BEGIN") && !strings.EqualFold(query, "COMMIT") {
			return nil, nil
		}
	} else if !isSupportedNormalizedEvent(raw.EventType) {
		return nil, nil
	}

	var ev model.NormalizedEvent
	ok, err := NormalizeRawEventInto(raw, &ev)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	return &ev, nil
}

func isSupportedNormalizedEvent(eventType string) bool {
	return eventType == "RowsQueryEvent" ||
		eventType == "ROWS_QUERY_EVENT" ||
		eventType == "XID_EVENT" ||
		eventType == "XIDEvent" ||
		eventType == "TABLE_MAP_EVENT" ||
		eventType == "TableMapEvent" ||
		strings.HasPrefix(eventType, "WriteRows") ||
		strings.HasPrefix(eventType, "WRITE_ROWS") ||
		strings.HasPrefix(eventType, "UpdateRows") ||
		strings.HasPrefix(eventType, "UPDATE_ROWS") ||
		strings.HasPrefix(eventType, "DeleteRows") ||
		strings.HasPrefix(eventType, "DELETE_ROWS")
}

// NormalizeRawEventInto converts a RawEvent into dst and reports whether the event should be kept.
func NormalizeRawEventInto(raw RawEvent, dst *model.NormalizedEvent) (bool, error) {
	if dst == nil {
		return false, nil
	}
	switch {
	case raw.EventType == "QUERY_EVENT" || raw.EventType == "QueryEvent":
		return normalizeQueryEventInto(raw, dst)
	case raw.EventType == "RowsQueryEvent" || raw.EventType == "ROWS_QUERY_EVENT":
		return normalizeRowsQueryEventInto(raw, dst)
	case strings.HasPrefix(raw.EventType, "WriteRows") || strings.HasPrefix(raw.EventType, "WRITE_ROWS"):
		fillNormalizedEvent(dst, raw)
		dst.EventType = "ROWS"
		dst.Operation = "INSERT"
		return true, nil
	case strings.HasPrefix(raw.EventType, "UpdateRows") || strings.HasPrefix(raw.EventType, "UPDATE_ROWS"):
		fillNormalizedEvent(dst, raw)
		dst.EventType = "ROWS"
		dst.Operation = "UPDATE"
		return true, nil
	case strings.HasPrefix(raw.EventType, "DeleteRows") || strings.HasPrefix(raw.EventType, "DELETE_ROWS"):
		fillNormalizedEvent(dst, raw)
		dst.EventType = "ROWS"
		dst.Operation = "DELETE"
		return true, nil
	case raw.EventType == "XID_EVENT" || raw.EventType == "XIDEvent":
		fillNormalizedEvent(dst, raw)
		dst.EventType = "XID"
		return true, nil
	case raw.EventType == "TABLE_MAP_EVENT" || raw.EventType == "TableMapEvent":
		fillNormalizedEvent(dst, raw)
		dst.EventType = "TABLE_MAP"
		return true, nil
	default:
		// Skip unsupported events
		return false, nil
	}
}

func fillNormalizedEvent(dst *model.NormalizedEvent, raw RawEvent) {
	*dst = model.NormalizedEvent{
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

func normalizeQueryEventInto(raw RawEvent, dst *model.NormalizedEvent) (bool, error) {
	query := strings.TrimSpace(raw.Query)
	switch {
	case strings.EqualFold(query, "BEGIN"):
		fillNormalizedEvent(dst, raw)
		dst.EventType = "BEGIN"
		return true, nil
	case strings.EqualFold(query, "COMMIT"):
		fillNormalizedEvent(dst, raw)
		dst.EventType = "COMMIT"
		return true, nil
	default:
		// Skip other QUERY events (DDL, etc.)
		return false, nil
	}
}

// normalizeRowsQueryEvent handles Rows_query_log_event which contains the original SQL.
// The SQL is bounded at model.MaxStoredSQLBytes to prevent memory issues with huge queries.
// OriginalBytes is preserved for accurate reporting even when SQL is truncated.
func normalizeRowsQueryEventInto(raw RawEvent, dst *model.NormalizedEvent) (bool, error) {
	fillNormalizedEvent(dst, raw)
	dst.EventType = "ROWS_QUERY"

	sql := raw.QuerySQL
	if sql == "" {
		return true, nil
	}

	// Preserve original byte count before any truncation
	originalBytes := len(sql)
	dst.QueryOriginalBytes = originalBytes

	// Bound the SQL to prevent memory bloat
	if originalBytes > model.MaxStoredSQLBytes {
		sql = safeTruncateBytes(sql, model.MaxStoredSQLBytes)
		dst.QueryTruncated = true
	}
	dst.QuerySQL = sql

	return true, nil
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
