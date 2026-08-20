package binlog

import "strings"

// StatementOrMixedWarning is printed on stderr when Query-DML exists without
// matching row images. The wording is part of the operator-facing contract.
const StatementOrMixedWarning = "binlog appears STATEMENT or MIXED; only ROW images are counted"

const (
	InputFormatROW       = "ROW"
	InputFormatStatement = "STATEMENT"
	InputFormatMixed     = "MIXED"
)

// FormatObserver counts Query-DML versus ROW images so analyze can warn
// when a STATEMENT or MIXED file would otherwise look like a healthy ROW report.
type FormatObserver struct {
	QueryDMLEvents int
	RowImageEvents int
}

// Observe records one raw parser event.
func (o *FormatObserver) Observe(raw RawEvent) {
	if o == nil {
		return
	}
	if isQueryEventType(raw.EventType) && IsQueryDML(raw.Query) {
		o.QueryDMLEvents++
		return
	}
	if IsRowImageEvent(raw.EventType) {
		o.RowImageEvents++
	}
}

// Guess returns ROW, STATEMENT, MIXED, or empty when there is not enough signal.
func (o FormatObserver) Guess() string {
	return GuessInputFormat(o.QueryDMLEvents, o.RowImageEvents)
}

// GuessInputFormat classifies a file from cheap Query-DML vs row-image counts.
func GuessInputFormat(queryDML, rowImages int) string {
	switch {
	case queryDML > 0 && rowImages == 0:
		return InputFormatStatement
	case queryDML > 0 && rowImages > 0:
		return InputFormatMixed
	case rowImages > 0:
		return InputFormatROW
	default:
		return ""
	}
}

// IsQueryDML reports whether a QUERY_EVENT body is DML rather than BEGIN/COMMIT/DDL.
func IsQueryDML(query string) bool {
	sql := strings.TrimSpace(query)
	if sql == "" {
		return false
	}
	return hasSQLKeywordPrefix(sql, "INSERT") ||
		hasSQLKeywordPrefix(sql, "UPDATE") ||
		hasSQLKeywordPrefix(sql, "DELETE") ||
		hasSQLKeywordPrefix(sql, "REPLACE")
}

// IsRowImageEvent reports whether the parser event type carries ROW before/after images.
func IsRowImageEvent(eventType string) bool {
	et := strings.ToUpper(eventType)
	return strings.Contains(et, "WRITEROWS") || strings.Contains(et, "WRITE_ROWS") ||
		strings.Contains(et, "UPDATEROWS") || strings.Contains(et, "UPDATE_ROWS") ||
		strings.Contains(et, "DELETEROWS") || strings.Contains(et, "DELETE_ROWS")
}

func isQueryEventType(eventType string) bool {
	return eventType == "QUERY_EVENT" || eventType == "QueryEvent"
}

func hasSQLKeywordPrefix(sql, word string) bool {
	if len(sql) < len(word) || !strings.EqualFold(sql[:len(word)], word) {
		return false
	}
	if len(sql) == len(word) {
		return true
	}
	next := sql[len(word)]
	return next == ' ' || next == '\t' || next == '\n' || next == '\r' || next == '/' || next == '('
}
