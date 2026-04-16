// Package analyzer builds DDL diagnostics and timeline metadata from normalized events.
// input: normalized query events, explicit SQL statements, and binlog source metadata.
// output: deterministic model.DDLEvent slices plus lightweight DDL statement parsing helpers.
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
	normalized := strings.Join(strings.Fields(strings.TrimSpace(sql)), " ")
	if normalized == "" {
		return DDLStatement{}, false
	}

	tokens := strings.Fields(normalized)
	if len(tokens) < 3 {
		return DDLStatement{}, false
	}

	operation, object, startIndex, ok := classifyDDL(tokens)
	if !ok {
		return DDLStatement{}, false
	}

	identifier := findDDLIdentifier(tokens[startIndex:])
	schema, table := splitQualifiedIdentifier(identifier)

	return DDLStatement{
		Operation: operation,
		Object:    object,
		Schema:    schema,
		Table:     table,
		Statement: normalized,
	}, true
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

	return model.DDLEvent{
		BinlogPath:    ev.BinlogPath,
		Timestamp:     ev.Timestamp.UTC(),
		Schema:        stmt.Schema,
		Table:         stmt.Table,
		Operation:     stmt.Operation,
		Object:        stmt.Object,
		Statement:     stmt.Statement,
		PositionStart: ev.PositionStart,
		PositionEnd:   ev.PositionEnd,
		BinlogBytes:   ev.BinlogBytes,
	}, true
}

func classifyDDL(tokens []string) (operation string, object string, identifierIndex int, ok bool) {
	first := strings.ToUpper(tokens[0])
	second := strings.ToUpper(tokens[1])

	switch {
	case first == "ALTER" && second == "TABLE":
		return "ALTER TABLE", "table", 2, true
	case first == "CREATE" && second == "TABLE":
		return "CREATE TABLE", "table", skipOptionalIfClause(tokens, 2), true
	case first == "DROP" && second == "TABLE":
		return "DROP TABLE", "table", skipOptionalIfClause(tokens, 2), true
	case first == "TRUNCATE" && second == "TABLE":
		return "TRUNCATE TABLE", "table", 2, true
	default:
		return "", "", 0, false
	}
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
		return strings.TrimRight(clean, ",(")
	}
	return ""
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
