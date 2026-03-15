package binlog

import (
	"strings"

	"binlogviz/internal/model"
)

// NormalizeRawEvent converts a RawEvent into a NormalizedEvent for analysis.
// Returns nil for events that should be skipped (e.g., FORMAT_DESCRIPTION).
func NormalizeRawEvent(raw RawEvent) (*model.NormalizedEvent, error) {
	ev := &model.NormalizedEvent{
		Timestamp: raw.Timestamp,
		Schema:    raw.Schema,
		Table:     raw.Table,
		RowCount:  raw.RowCount,
	}

	switch {
	case raw.EventType == "QUERY_EVENT" || raw.EventType == "QueryEvent":
		return normalizeQueryEvent(raw, ev)
	case raw.EventType == "RowsQueryEvent" || raw.EventType == "ROWS_QUERY_EVENT":
		return normalizeRowsQueryEvent(raw, ev)
	case strings.HasPrefix(raw.EventType, "WriteRows") || strings.HasPrefix(raw.EventType, "WRITE_ROWS"):
		ev.EventType = "ROWS"
		ev.Operation = "INSERT"
		return ev, nil
	case strings.HasPrefix(raw.EventType, "UpdateRows") || strings.HasPrefix(raw.EventType, "UPDATE_ROWS"):
		ev.EventType = "ROWS"
		ev.Operation = "UPDATE"
		return ev, nil
	case strings.HasPrefix(raw.EventType, "DeleteRows") || strings.HasPrefix(raw.EventType, "DELETE_ROWS"):
		ev.EventType = "ROWS"
		ev.Operation = "DELETE"
		return ev, nil
	case raw.EventType == "XID_EVENT" || raw.EventType == "XIDEvent":
		ev.EventType = "XID"
		return ev, nil
	case raw.EventType == "TABLE_MAP_EVENT" || raw.EventType == "TableMapEvent":
		ev.EventType = "TABLE_MAP"
		return ev, nil
	default:
		// Skip unsupported events
		return nil, nil
	}
}

func normalizeQueryEvent(raw RawEvent, ev *model.NormalizedEvent) (*model.NormalizedEvent, error) {
	query := strings.ToUpper(strings.TrimSpace(raw.Query))
	switch {
	case query == "BEGIN":
		ev.EventType = "BEGIN"
		return ev, nil
	case query == "COMMIT":
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
func normalizeRowsQueryEvent(raw RawEvent, ev *model.NormalizedEvent) (*model.NormalizedEvent, error) {
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

	// Find the last valid UTF-8 boundary at or before maxBytes
	for maxBytes > 0 {
		if s[maxBytes-1] < 0x80 || (s[maxBytes-1] >= 0xC0) {
			// Valid boundary: ASCII byte or start of multi-byte sequence
			break
		}
		maxBytes--
	}

	if maxBytes <= 0 {
		return ""
	}
	return s[:maxBytes]
}
