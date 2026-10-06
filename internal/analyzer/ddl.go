// Package analyzer builds DDL diagnostics and timeline metadata from normalized events.
// input: normalized query events, explicit SQL statements, and binlog source metadata.
// output: deterministic model.DDLEvent slices plus lightweight DDL statement parsing helpers for schema/table/user/privilege/view/trigger/routine/event statements. Index DDL and CREATE TRIGGER use the table after ON. Identifiers are cut at the first parenthesis outside backticks, so the no-space form name(col) stays the object name. Unrecognized DDL is kept as generic DDL. Credential literals are replaced with <secret>.
// pos: analyzer-side DDL extraction layer that feeds later diagnostics and report assembly.
// note: if this file changes, update this header and README.md.
package analyzer

import (
	"sort"
	"strings"
	"time"

	"binlogviz/internal/model"
)

// DDLStatement is the normalized parse result of a supported DDL statement.
type DDLStatement struct {
	Operation string
	Object    string
	Schema    string
	Table     string
	Statement string
}

// DDLAggregator collects DDL events for later diagnostics and reporting.
type DDLAggregator struct {
	events []model.DDLEvent
}

// NewDDLAggregator creates an empty DDLAggregator.
func NewDDLAggregator() *DDLAggregator {
	return &DDLAggregator{}
}

// ParseDDLStatement normalizes a supported DDL statement and extracts object metadata.
func ParseDDLStatement(sql string) (DDLStatement, bool) {
	trimmed := strings.TrimSpace(sql)
	if trimmed == "" || !hasSupportedDDLPrefix(trimmed) {
		return DDLStatement{}, false
	}

	normalized := strings.Join(strings.Fields(sanitizeDisplaySQL(redactCredentials(trimmed))), " ")
	tokens := strings.Fields(normalized)
	if len(tokens) < 2 {
		return DDLStatement{}, false
	}

	operation, object, startIndex, ok := classifyDDL(tokens)
	if !ok {
		return DDLStatement{
			Operation: "DDL",
			Object:    "ddl",
			Statement: normalized,
		}, true
	}

	var identifier string
	if object == "index" || (object == "trigger" && hasKeyword(tokens, "ON")) {
		// The name before ON is the index or trigger. The table is the next identifier.
		identifier, _ = identifierAfterKeyword(tokens, "ON")
	} else {
		identifier = findDDLIdentifier(tokens[skipOptionalIfClause(tokens, startIndex):])
	}
	schema, table := splitQualifiedIdentifier(identifier)
	if object == "database" && schema == "" && table != "" {
		schema, table = table, ""
	}

	return DDLStatement{
		Operation: operation,
		Object:    object,
		Schema:    schema,
		Table:     table,
		Statement: normalized,
	}, true
}

func hasSupportedDDLPrefix(sql string) bool {
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

// ConsumeEvent extracts a DDL event from a normalized event when possible.
func (a *DDLAggregator) ConsumeEvent(ev model.NormalizedEvent) {
	ddlEvent, ok := DDLEventFromNormalizedEvent(ev)
	if !ok {
		return
	}
	a.events = append(a.events, ddlEvent)
}

// ConsumeStatement appends a parsed DDL statement directly.
func (a *DDLAggregator) ConsumeStatement(ts time.Time, binlogPath string, positionStart, positionEnd, binlogBytes int64, sql string) {
	stmt, ok := ParseDDLStatement(sql)
	if !ok {
		return
	}
	a.events = append(a.events, model.DDLEvent{
		BinlogPath:    binlogPath,
		Timestamp:     ts.UTC(),
		Schema:        stmt.Schema,
		Table:         stmt.Table,
		Operation:     stmt.Operation,
		Object:        stmt.Object,
		Statement:     stmt.Statement,
		PositionStart: positionStart,
		PositionEnd:   positionEnd,
		BinlogBytes:   binlogBytes,
	})
}

// Snapshot returns collected DDL events sorted by timestamp and source position.
func (a *DDLAggregator) Snapshot() []model.DDLEvent {
	if len(a.events) == 0 {
		return nil
	}

	out := append([]model.DDLEvent(nil), a.events...)
	sort.Slice(out, func(i, j int) bool {
		if !out[i].Timestamp.Equal(out[j].Timestamp) {
			return out[i].Timestamp.Before(out[j].Timestamp)
		}
		if out[i].BinlogPath != out[j].BinlogPath {
			return out[i].BinlogPath < out[j].BinlogPath
		}
		if out[i].PositionStart != out[j].PositionStart {
			return out[i].PositionStart < out[j].PositionStart
		}
		return out[i].PositionEnd < out[j].PositionEnd
	})
	return out
}

// DDLEventFromNormalizedEvent converts a normalized query event into a model.DDLEvent when the SQL is DDL.
func DDLEventFromNormalizedEvent(ev model.NormalizedEvent) (model.DDLEvent, bool) {
	stmt, ok := ParseDDLStatement(ev.QuerySQL)
	if !ok {
		return model.DDLEvent{}, false
	}

	schema := stmt.Schema
	if schema == "" {
		schema = ev.Schema
	}
	table := stmt.Table
	if table == "" && stmt.Object == "table" {
		table = ev.Table
	}

	return model.DDLEvent{
		BinlogPath:    ev.BinlogPath,
		Timestamp:     ev.Timestamp.UTC(),
		Schema:        schema,
		Table:         table,
		Operation:     stmt.Operation,
		Object:        stmt.Object,
		Statement:     stmt.Statement,
		PositionStart: ev.PositionStart,
		PositionEnd:   ev.PositionEnd,
		BinlogBytes:   ev.BinlogBytes,
	}, true
}

func classifyDDL(tokens []string) (operation string, object string, identifierIndex int, ok bool) {
	if op, ok := createIndexOperation(tokens); ok {
		return op, "index", 0, true
	}

	first := strings.ToUpper(tokens[0])
	second := ""
	if len(tokens) > 1 {
		second = strings.ToUpper(tokens[1])
	}
	switch first {
	case "GRANT":
		return "GRANT", "privilege", 1, true
	case "REVOKE":
		return "REVOKE", "privilege", 1, true
	case "TRUNCATE":
		if second == "TABLE" {
			return "TRUNCATE TABLE", "table", 2, true
		}
		return "TRUNCATE TABLE", "table", 1, true
	case "RENAME":
		if second == "TABLE" {
			return "RENAME TABLE", "table", 2, true
		}
		return "", "", 0, false
	case "CREATE", "ALTER", "DROP":
		return classifyObjectDDL(tokens)
	default:
		return "", "", 0, false
	}
}

func classifyObjectDDL(tokens []string) (operation string, object string, identifierIndex int, ok bool) {
	verb := strings.ToUpper(tokens[0])
	index := skipDDLModifiers(tokens, 1)
	if index >= len(tokens) {
		return "", "", 0, false
	}
	keyword := strings.ToUpper(tokens[index])
	nameAt := index + 1
	switch keyword {
	case "TABLE":
		return verb + " TABLE", "table", nameAt, true
	case "DATABASE", "SCHEMA":
		return verb + " DATABASE", "database", nameAt, true
	case "USER":
		return verb + " USER", "user", nameAt, true
	case "INDEX":
		return verb + " INDEX", "index", 0, true
	case "VIEW":
		return verb + " VIEW", "view", nameAt, true
	case "TRIGGER":
		return verb + " TRIGGER", "trigger", nameAt, true
	case "PROCEDURE", "FUNCTION":
		return verb + " " + keyword, "routine", nameAt, true
	case "EVENT":
		return verb + " EVENT", "event", nameAt, true
	default:
		return "", "", 0, false
	}
}

func skipDDLModifiers(tokens []string, i int) int {
	for i < len(tokens) {
		tok := strings.ToUpper(tokens[i])
		switch {
		case tok == "OR" || tok == "REPLACE" || tok == "TEMPORARY" || tok == "SQL" || tok == "SECURITY" || tok == "INVOKER":
			i++
		case tok == "DEFINER":
			i++
			if i < len(tokens) && tokens[i] == "=" {
				i++
				if i < len(tokens) && !isDDLObjectKeyword(tokens[i]) {
					i++
				}
			}
		case strings.HasPrefix(tok, "DEFINER=") || strings.HasPrefix(tok, "ALGORITHM="):
			i++
		case tok == "ALGORITHM":
			i++
			if i < len(tokens) && tokens[i] == "=" {
				i++
			}
			if i < len(tokens) && !isDDLObjectKeyword(tokens[i]) {
				i++
			}
		default:
			return i
		}
	}
	return i
}

func isDDLObjectKeyword(tok string) bool {
	switch strings.ToUpper(tok) {
	case "TABLE", "DATABASE", "SCHEMA", "USER", "INDEX", "VIEW", "TRIGGER", "PROCEDURE", "FUNCTION", "EVENT":
		return true
	default:
		return false
	}
}

func hasKeyword(tokens []string, keyword string) bool {
	for _, token := range tokens {
		if strings.EqualFold(token, keyword) {
			return true
		}
	}
	return false
}

func createIndexOperation(tokens []string) (string, bool) {
	if len(tokens) < 2 || !strings.EqualFold(tokens[0], "CREATE") {
		return "", false
	}
	if strings.EqualFold(tokens[1], "INDEX") {
		return "CREATE INDEX", true
	}
	if len(tokens) >= 3 && strings.EqualFold(tokens[2], "INDEX") {
		switch strings.ToUpper(tokens[1]) {
		case "UNIQUE", "FULLTEXT", "SPATIAL":
			return "CREATE " + strings.ToUpper(tokens[1]) + " INDEX", true
		}
	}
	return "", false
}

func identifierAfterKeyword(tokens []string, keyword string) (string, bool) {
	for i := 0; i < len(tokens)-1; i++ {
		if !strings.EqualFold(tokens[i], keyword) {
			continue
		}
		clean := trimDDLIdentifier(tokens[i+1])
		if clean == "" {
			return "", false
		}
		return clean, true
	}
	return "", false
}

func skipOptionalIfClause(tokens []string, start int) int {
	index := start
	if len(tokens) > index+1 && strings.EqualFold(tokens[index], "IF") {
		index++
		if len(tokens) > index && strings.EqualFold(tokens[index], "NOT") {
			index++
		}
		if len(tokens) > index && strings.EqualFold(tokens[index], "EXISTS") {
			index++
		}
	}
	return index
}

func findDDLIdentifier(tokens []string) string {
	for _, token := range tokens {
		clean := strings.TrimSpace(token)
		if clean == "" {
			continue
		}
		return trimDDLIdentifier(clean)
	}
	return ""
}

// trimDDLIdentifier keeps the object name. A column list glued on as name(col)
// ends at the first '(' outside backticks. A doubled backtick is an escaped quote.
// A trailing comma is still dropped.
func trimDDLIdentifier(token string) string {
	return strings.TrimRight(cutAtParenOutsideBackticks(strings.TrimSpace(token)), ",")
}

func cutAtParenOutsideBackticks(token string) string {
	inBacktick := false
	for i := 0; i < len(token); i++ {
		switch token[i] {
		case '`':
			if inBacktick && i+1 < len(token) && token[i+1] == '`' {
				i++
				continue
			}
			inBacktick = !inBacktick
		case '(':
			if !inBacktick {
				return token[:i]
			}
		}
	}
	return token
}

func splitQualifiedIdentifier(identifier string) (string, string) {
	identifier = strings.TrimSpace(identifier)
	if identifier == "" {
		return "", ""
	}

	identifier = strings.Trim(identifier, "`")
	parts := strings.Split(identifier, ".")
	for index := range parts {
		parts[index] = strings.Trim(parts[index], "`")
	}

	if len(parts) >= 2 {
		return parts[len(parts)-2], parts[len(parts)-1]
	}
	return "", parts[0]
}
