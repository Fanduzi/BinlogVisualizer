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
	case raw.EventType == "QUERY_EVENT":
		return normalizeQueryEvent(raw, ev)
	case strings.HasPrefix(raw.EventType, "WRITE_ROWS"):
		ev.EventType = "ROWS"
		ev.Operation = "INSERT"
		return ev, nil
	case strings.HasPrefix(raw.EventType, "UPDATE_ROWS"):
		ev.EventType = "ROWS"
		ev.Operation = "UPDATE"
		return ev, nil
	case strings.HasPrefix(raw.EventType, "DELETE_ROWS"):
		ev.EventType = "ROWS"
		ev.Operation = "DELETE"
		return ev, nil
	case raw.EventType == "XID_EVENT":
		ev.EventType = "XID"
		return ev, nil
	case raw.EventType == "TABLE_MAP_EVENT":
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
