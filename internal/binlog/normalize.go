// Package binlog normalizes raw parser events into analyzer-facing events.
// input: RawEvent values with canonical kinds, optional producer/transaction provenance, and Query SQL.
// output: model.NormalizedEvent values with preserved provenance, bounded SQL context, XA identity including END/ROLLBACK/BEGIN, Query DDL including GRANT/REVOKE, and stable event/operation kinds.
// pos: Query classifier between the parser adapter and analyzer consumption.
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
	if raw.EventType == kindQuery {
		query := strings.TrimSpace(raw.Query)
		_, _, isXA := parseXAQuery(query)
		if !strings.EqualFold(query, "BEGIN") && !strings.EqualFold(query, "COMMIT") && !hasQueryDDLPrefix(query) && !hasLoadDataPrefix(query) && !isXA {
			return nil, nil
		}
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

// NormalizeRawEventInto converts a RawEvent into dst and reports whether the event should be kept.
func NormalizeRawEventInto(raw RawEvent, dst *model.NormalizedEvent) (bool, error) {
	if dst == nil {
		return false, nil
	}
	switch raw.EventType {
	case kindQuery:
		return normalizeQueryEventInto(raw, dst)
	case kindWriteRows:
		fillNormalizedEvent(dst, raw)
		dst.EventType = "ROWS"
		dst.Operation = "INSERT"
		return true, nil
	case kindUpdateRows:
		fillNormalizedEvent(dst, raw)
		dst.EventType = "ROWS"
		dst.Operation = "UPDATE"
		return true, nil
	case kindDeleteRows:
		fillNormalizedEvent(dst, raw)
		dst.EventType = "ROWS"
		dst.Operation = "DELETE"
		return true, nil
	case kindRowsQuery:
		return normalizeRowsQueryEventInto(raw, dst)
	case kindGTID:
		fillNormalizedEvent(dst, raw)
		dst.EventType = "GTID"
		return true, nil
	case kindXID:
		fillNormalizedEvent(dst, raw)
		dst.EventType = "XID"
		return true, nil
	case kindXAPrepare:
		fillNormalizedEvent(dst, raw)
		dst.EventType = "XA_PREPARE"
		return true, nil
	case kindTableMap:
		fillNormalizedEvent(dst, raw)
		dst.EventType = "TABLE_MAP"
		return true, nil
	default:
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
		ServerID:      raw.ServerID,
		ServerVersion: raw.ServerVersion,
		ServerFlavor:  raw.ServerFlavor,
		GTID:          raw.GTID,
		ThreadID:      raw.ThreadID,
		XID:           raw.XID,
		XAXID:         raw.XAXID,
		ActorUser:     raw.ActorUser,
		ActorHost:     raw.ActorHost,
		Schema:        raw.Schema,
		Table:         raw.Table,
		RowCount:      raw.RowCount,
	}
}

func normalizeQueryEventInto(raw RawEvent, dst *model.NormalizedEvent) (bool, error) {
	query := strings.TrimSpace(raw.Query)
	xaEventType, xaXID, isXA := parseXAQuery(query)
	switch {
	case strings.EqualFold(query, "BEGIN"):
		fillNormalizedEvent(dst, raw)
		dst.EventType = "BEGIN"
		return true, nil
	case strings.EqualFold(query, "COMMIT"):
		fillNormalizedEvent(dst, raw)
		dst.EventType = "COMMIT"
		return true, nil
	case isXA:
		fillNormalizedEvent(dst, raw)
		dst.EventType = xaEventType
		dst.XAXID = xaXID
		return true, nil
	case hasLoadDataPrefix(query):
		raw.QuerySQL = raw.Query
		return normalizeRowsQueryEventInto(raw, dst)
	case hasQueryDDLPrefix(query):
		fillNormalizedEvent(dst, raw)
		dst.EventType = "DDL"
		dst.QuerySQL = query
		return true, nil
	default:
		return false, nil
	}
}

func parseXAQuery(query string) (eventType, xid string, ok bool) {
	query = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(query), ";"))
	if !hasWordPrefixFold(query, "XA") {
		return "", "", false
	}
	remainder := strings.TrimSpace(query[len("XA"):])
	for _, boundary := range []struct {
		verb      string
		eventType string
	}{
		{verb: "START", eventType: "XA_START"},
		{verb: "BEGIN", eventType: "XA_START"},
		{verb: "END", eventType: "XA_END"},
		{verb: "PREPARE", eventType: "XA_PREPARE"},
		{verb: "COMMIT", eventType: "XA_COMMIT"},
		{verb: "ROLLBACK", eventType: "XA_ROLLBACK"},
	} {
		if !hasWordPrefixFold(remainder, boundary.verb) {
			continue
		}
		xid = strings.TrimSpace(remainder[len(boundary.verb):])
		return boundary.eventType, xid, xid != ""
	}
	return "", "", false
}

func hasQueryDDLPrefix(sql string) bool {
	return hasWordPrefixFold(sql, "ALTER") ||
		hasWordPrefixFold(sql, "CREATE") ||
		hasWordPrefixFold(sql, "DROP") ||
		hasWordPrefixFold(sql, "TRUNCATE") ||
		hasWordPrefixFold(sql, "RENAME") ||
		hasWordPrefixFold(sql, "GRANT") ||
		hasWordPrefixFold(sql, "REVOKE")
}

func hasWordPrefixFold(sql, word string) bool {
	if len(sql) < len(word) || !strings.EqualFold(sql[:len(word)], word) {
		return false
	}
	if len(sql) == len(word) {
		return true
	}
	next := sql[len(word)]
	return next == ' ' || next == '\t' || next == '\n' || next == '\r'
}

func normalizeRowsQueryEventInto(raw RawEvent, dst *model.NormalizedEvent) (bool, error) {
	fillNormalizedEvent(dst, raw)
	dst.EventType = "ROWS_QUERY"

	sql := raw.QuerySQL
	if sql == "" {
		return true, nil
	}

	originalBytes := len(sql)
	dst.QueryOriginalBytes = originalBytes

	if originalBytes > model.MaxStoredSQLBytes {
		sql = safeTruncateBytes(sql, model.MaxStoredSQLBytes)
		dst.QueryTruncated = true
	}
	dst.QuerySQL = sql
	if hasLoadDataPrefix(sql) {
		dst.Operation = "LOAD_DATA"
	}

	return true, nil
}

func hasLoadDataPrefix(sql string) bool {
	sql = strings.TrimSpace(sql)
	if !hasWordPrefixFold(sql, "LOAD") {
		return false
	}
	return hasWordPrefixFold(strings.TrimSpace(sql[len("LOAD"):]), "DATA")
}

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
