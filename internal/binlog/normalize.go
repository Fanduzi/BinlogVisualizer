// Package binlog normalizes raw parser events into analyzer-facing events.
// input: RawEvent values with canonical kinds, optional producer/transaction provenance, and Query SQL.
// output: model.NormalizedEvent values with preserved provenance, MySQL 8 commit timestamps, bounded SQL context, optional flashback rows, XA identity including END/ROLLBACK/BEGIN, plain ROLLBACK (not ROLLBACK TO SAVEPOINT), Query DDL including GRANT/REVOKE and SET PASSWORD (and that query's sql_mode), independent ADMIN including exact FLUSH TABLES, CHECK TABLE prefix, and SET ROLE prefix, Unclassified QUERY with bounded SQL, dropped Ignored QUERY / Query-DML, and stable event/operation kinds.
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
	if raw.EventType == kindQuery && !keepsNormalizedQuery(raw.Query) {
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
		Timestamp:         raw.Timestamp,
		BinlogPath:        raw.BinlogPath,
		PositionStart:     raw.PositionStart,
		PositionEnd:       raw.PositionEnd,
		BinlogBytes:       raw.BinlogBytes,
		ServerID:          raw.ServerID,
		ServerVersion:     raw.ServerVersion,
		ServerFlavor:      raw.ServerFlavor,
		SQLMode:           raw.SQLMode,
		GTID:              raw.GTID,
		OriginalCommitUs:  raw.OriginalCommitTimestamp,
		ImmediateCommitUs: raw.ImmediateCommitTimestamp,
		ThreadID:          raw.ThreadID,
		XID:               raw.XID,
		XAXID:             raw.XAXID,
		ActorUser:         raw.ActorUser,
		ActorHost:         raw.ActorHost,
		Schema:            raw.Schema,
		Table:             raw.Table,
		RowCount:          raw.RowCount,
		KeyStatus:         raw.KeyStatus,
		RowImages:         raw.RowImages,
		RowImagesOmitted:  raw.RowImagesOmitted,
		RowKeys:           raw.RowKeys,
		FlashRows:         raw.FlashRows,
		RedoRows:          raw.RedoRows,
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
	case isPlainRollback(query):
		fillNormalizedEvent(dst, raw)
		dst.EventType = "ROLLBACK"
		return true, nil
	case isXA:
		fillNormalizedEvent(dst, raw)
		dst.EventType = xaEventType
		dst.XAXID = xaXID
		dst.QuerySQL = boundQuerySQL(query)
		return true, nil
	case hasLoadDataPrefix(query):
		raw.QuerySQL = raw.Query
		return normalizeRowsQueryEventInto(raw, dst)
	case hasQueryDDLPrefix(query):
		fillNormalizedEvent(dst, raw)
		dst.EventType = "DDL"
		dst.QuerySQL = query
		return true, nil
	case hasIndependentAdminQueryPrefix(query):
		fillNormalizedEvent(dst, raw)
		dst.EventType = "ADMIN"
		dst.QuerySQL = query
		return true, nil
	case isIgnoredQuery(query) || IsQueryDML(query) || query == "":
		return false, nil
	default:
		fillNormalizedEvent(dst, raw)
		dst.EventType = "UNCLASSIFIED_QUERY"
		dst.QueryTruncated = len(query) > model.MaxStoredSQLBytes
		dst.QuerySQL = boundQuerySQL(query)
		return true, nil
	}
}

func keepsNormalizedQuery(query string) bool {
	query = strings.TrimSpace(query)
	return query != "" && !isIgnoredQuery(query) && !IsQueryDML(query)
}

// IsIgnoredQuery reports session-prefix SET that is dropped on purpose.
// SET ROLE, SET DEFAULT ROLE, and SET PASSWORD are not Ignored QUERY.
func IsIgnoredQuery(query string) bool {
	return isIgnoredQuery(query)
}

// IsIgnoredQueryEvent reports a QUERY event analyze drops and never uses as a close.
func IsIgnoredQueryEvent(raw RawEvent) bool {
	return isQueryEventType(raw.EventType) && isIgnoredQuery(raw.Query)
}

func isIgnoredQuery(sql string) bool {
	sql = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(sql), ";"))
	if !hasWordPrefixFold(sql, "SET") {
		return false
	}
	if isSetPasswordQuery(sql) || hasThreeWordPrefixFold(sql, "SET", "DEFAULT", "ROLE") {
		return false
	}
	return !hasTwoWordPrefixFold(sql, "SET", "ROLE")
}

func isSetPasswordQuery(sql string) bool {
	return hasTwoWordPrefixFold(sql, "SET", "PASSWORD")
}

func isPlainRollback(query string) bool {
	query = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(query), ";"))
	return strings.EqualFold(query, "ROLLBACK") || strings.EqualFold(query, "ROLLBACK WORK")
}

func boundQuerySQL(query string) string {
	if len(query) <= model.MaxStoredSQLBytes {
		return query
	}
	return safeTruncateBytes(query, model.MaxStoredSQLBytes)
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
		hasWordPrefixFold(sql, "REVOKE") ||
		isSetPasswordQuery(sql)
}

// hasIndependentAdminQueryPrefix matches the independent
// GTID-started non-explicit group statements only. SET DEFAULT ROLE is that phrase, not a generic SET prefix.
// CHECK TABLE and SET ROLE are two-word prefixes (CHECK TABLE t, SET ROLE ALL, SET ROLE name).
// SET ROLE does not match SET DEFAULT ROLE. FLUSH TABLES is exact so FLUSH TABLES WITH READ LOCK
// and FLUSH TABLES tbl stay Unclassified QUERY.
func hasIndependentAdminQueryPrefix(sql string) bool {
	sql = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(sql), ";"))
	return hasTwoWordPrefixFold(sql, "ANALYZE", "TABLE") ||
		hasTwoWordPrefixFold(sql, "OPTIMIZE", "TABLE") ||
		hasTwoWordPrefixFold(sql, "FLUSH", "PRIVILEGES") ||
		hasThreeWordPrefixFold(sql, "SET", "DEFAULT", "ROLE") ||
		hasTwoWordPrefixFold(sql, "CHECK", "TABLE") || hasTwoWordPrefixFold(sql, "SET", "ROLE") ||
		strings.EqualFold(sql, "FLUSH TABLES")
}

func hasTwoWordPrefixFold(sql, first, second string) bool {
	if !hasWordPrefixFold(sql, first) {
		return false
	}
	return hasWordPrefixFold(strings.TrimSpace(sql[len(first):]), second)
}

func hasThreeWordPrefixFold(sql, first, second, third string) bool {
	if !hasWordPrefixFold(sql, first) {
		return false
	}
	rest := strings.TrimSpace(sql[len(first):])
	if !hasWordPrefixFold(rest, second) {
		return false
	}
	return hasWordPrefixFold(strings.TrimSpace(rest[len(second):]), third)
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

// stripLeadingSQLComments drops the comments before a statement's first
// keyword. MariaDB logs "/* c */ CREATE USER ..." verbatim where MySQL 8.0
// logs "CREATE USER ...", and every classifier here matches on the first
// keyword (#269). Executable comments (/*! and /*M!) run their body, so they
// stay. Text that is only comments, or an unterminated comment, is returned
// unchanged.
func stripLeadingSQLComments(query string) string {
	rest := strings.TrimLeft(query, " \t\r\n")
	for {
		switch {
		case strings.HasPrefix(rest, "/*!") || strings.HasPrefix(rest, "/*M!"):
			return finishStrip(query, rest)
		case strings.HasPrefix(rest, "/*"):
			end := strings.Index(rest[2:], "*/")
			if end < 0 {
				return query
			}
			rest = rest[2+end+2:]
		case strings.HasPrefix(rest, "#") || hasLineCommentDashes(rest):
			end := strings.IndexByte(rest, '\n')
			if end < 0 {
				return query
			}
			rest = rest[end+1:]
		default:
			return finishStrip(query, rest)
		}
		rest = strings.TrimLeft(rest, " \t\r\n")
	}
}

func finishStrip(query, rest string) string {
	if rest == "" {
		return query
	}
	return rest
}

// hasLineCommentDashes matches "-- " style comments: two dashes followed by
// whitespace or the end of the text, as MySQL requires.
func hasLineCommentDashes(s string) bool {
	if !strings.HasPrefix(s, "--") {
		return false
	}
	if len(s) == 2 {
		return true
	}
	switch s[2] {
	case ' ', '\t', '\r', '\n':
		return true
	}
	return false
}
