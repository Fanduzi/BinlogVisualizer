// Package analyzer builds DDL diagnostics and timeline metadata from normalized events.
// input: normalized query events, explicit SQL statements, and binlog source metadata.
// output: deterministic model.DDLEvent slices plus lightweight DDL statement parsing helpers for schema/table/user/privilege/view/trigger/routine/event statements. Every name is read by mysqlexpr's identifier reader (backticks undoubled the same way, a quoted name is not a keyword, a digit-leading name is a name, versioned comments and ANSI_QUOTES follow the event). CREATE/ALTER/DROP USER keep the account spelling. A multi-account DROP USER is one event per account; RENAME USER is one event per "old TO new" pair. A multi-table DROP TABLE is one event per table; a multi-pair RENAME TABLE is one event per pair, named by the old table. Index DDL uses the table after ON. CREATE and DROP TRIGGER use the trigger name. An unquoted name stops at '(', so the no-space form name(col) stays the object name. Unrecognized DDL is kept as generic DDL. Statement text is stored at the 4096-byte cap. Credential literals, including MariaDB IDENTIFIED VIA … USING PASSWORD, are replaced with <secret>. SET PASSWORD is account DDL. A DDL event keeps the holding group's GTID when the analyzer recorded one, and the file offset of that group's first event.
// pos: analyzer-side DDL extraction layer that feeds later diagnostics and report assembly.
// note: if this file changes, update this header and README.md.
package analyzer

import (
	"sort"
	"strings"
	"time"

	"binlogviz/internal/model"
	"binlogviz/internal/mysqlexpr"
	"binlogviz/internal/redact"
)

// DDLStatement is the normalized parse result of a supported DDL statement.
type DDLStatement struct {
	Operation     string
	Object        string
	Schema        string
	Table         string
	Statement     string
	Truncated     bool
	OriginalBytes int
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
	return parseDDLStatement(sql, mysqlexpr.Dialect{})
}

func parseDDLStatement(sql string, d mysqlexpr.Dialect) (DDLStatement, bool) {
	trimmed := strings.TrimSpace(sql)
	if trimmed == "" || !hasSupportedDDLPrefix(trimmed) {
		return DDLStatement{}, false
	}

	normalized := strings.Join(strings.Fields(redact.Display(redact.Credentials(trimmed))), " ")
	stored, truncated, originalBytes := boundDDLStatement(normalized)
	// A readable DROP TABLE is classified here. strings.Fields splits a
	// versioned comment into tokens, so /*!40005 TEMPORARY */ never reached
	// the name reader.
	if names, isDrop, ok := mysqlexpr.ParseDropTable(trimmed, d); isDrop && ok && len(names) > 0 {
		return DDLStatement{
			Operation:     "DROP TABLE",
			Object:        "table",
			Schema:        names[0].Schema,
			Table:         names[0].Table,
			Statement:     stored,
			Truncated:     truncated,
			OriginalBytes: originalBytes,
		}, true
	}
	tokens := strings.Fields(normalized)
	if len(tokens) < 2 {
		return DDLStatement{}, false
	}

	operation, object, _, ok := classifyDDL(tokens)
	if !ok {
		return DDLStatement{
			Operation:     "DDL",
			Object:        "ddl",
			Statement:     stored,
			Truncated:     truncated,
			OriginalBytes: originalBytes,
		}, true
	}

	schema, table := ddlObjectName(trimmed, operation, object, d)
	if object == "database" && schema == "" && table != "" {
		schema, table = table, ""
	}

	return DDLStatement{
		Operation:     operation,
		Object:        object,
		Schema:        schema,
		Table:         table,
		Statement:     stored,
		Truncated:     truncated,
		OriginalBytes: originalBytes,
	}, true
}

func boundDDLStatement(normalized string) (stored string, truncated bool, originalBytes int) {
	qc := model.NewQueryContext(normalized)
	if qc == nil {
		return "", false, 0
	}
	return qc.SQL, qc.Truncated, qc.OriginalBytes
}

func hasSupportedDDLPrefix(sql string) bool {
	return hasWordPrefixFold(sql, "ALTER") ||
		hasWordPrefixFold(sql, "CREATE") ||
		hasWordPrefixFold(sql, "DROP") ||
		hasWordPrefixFold(sql, "TRUNCATE") ||
		hasWordPrefixFold(sql, "RENAME") ||
		hasWordPrefixFold(sql, "GRANT") ||
		hasWordPrefixFold(sql, "REVOKE") ||
		isSetPassword(sql)
}

func isSetPassword(sql string) bool {
	sql = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(sql), ";"))
	if !hasWordPrefixFold(sql, "SET") {
		return false
	}
	return hasWordPrefixFold(strings.TrimSpace(sql[len("SET"):]), "PASSWORD")
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
	for _, ddlEvent := range ddlEventsFor(ev) {
		a.events = append(a.events, ddlEvent)
	}
}

// ddlEventsFor is one timeline row per DROP TABLE target, one row per
// RENAME TABLE pair (named by its old table, so --table keeps matching),
// one row per DROP USER account or RENAME USER pair, and one row for every
// other statement. Statement order is kept.
func ddlEventsFor(ev model.NormalizedEvent) []model.DDLEvent {
	one, ok := DDLEventFromNormalizedEvent(ev)
	if !ok {
		return nil
	}
	ids := parsedDropTables(ev)
	if len(ids) > 1 {
		out := make([]model.DDLEvent, len(ids))
		for i, id := range ids {
			out[i] = one
			out[i].Schema = id.schema
			out[i].Table = id.table
		}
		return out
	}
	pairs := parsedRenameTables(ev)
	if len(pairs) > 1 {
		out := make([]model.DDLEvent, len(pairs))
		for i, pair := range pairs {
			out[i] = one
			out[i].Schema = pair.schema
			out[i].Table = pair.table
		}
		return out
	}
	if one.Object != "user" {
		return []model.DDLEvent{one}
	}
	targets := parsedUserTargets(ev)
	if len(targets) == 0 {
		return []model.DDLEvent{one}
	}
	out := make([]model.DDLEvent, len(targets))
	for i, target := range targets {
		out[i] = one
		out[i].Table = target
	}
	return out
}

// parsedDropTables reads a DROP TABLE list. Schema is the name's schema, or
// the session schema when the name is unqualified. Nil when sql is not a
// readable DROP TABLE.
func parsedDropTables(ev model.NormalizedEvent) []tableIdentity {
	d := mysqlexpr.EventDialect(ev.ServerVersion, ev.ServerFlavor, ev.SQLMode)
	names, isDrop, ok := mysqlexpr.ParseDropTable(ev.QuerySQL, d)
	if !isDrop || !ok || len(names) == 0 {
		return nil
	}
	names = mysqlexpr.WithSession(names, ev.QuerySchema, ev.Schema)
	out := make([]tableIdentity, 0, len(names))
	for _, name := range names {
		out = append(out, newTableIdentity(name.Schema, name.Table))
	}
	return out
}

// parsedRenameTables reads a RENAME TABLE pair list like parsedDropTables
// reads a DROP TABLE list, keeping each pair's old name so the row names
// the table that existed before the statement ran. Nil when sql is not a
// readable RENAME TABLE.
func parsedRenameTables(ev model.NormalizedEvent) []tableIdentity {
	d := mysqlexpr.EventDialect(ev.ServerVersion, ev.ServerFlavor, ev.SQLMode)
	pairs, isRename, ok := mysqlexpr.ParseRenameTable(ev.QuerySQL, d)
	if !isRename || !ok || len(pairs) == 0 {
		return nil
	}
	oldNames := make([]mysqlexpr.QualifiedIdent, len(pairs))
	for i, pair := range pairs {
		oldNames[i] = pair.Old
	}
	oldNames = mysqlexpr.WithSession(oldNames, ev.QuerySchema, ev.Schema)
	out := make([]tableIdentity, 0, len(oldNames))
	for _, name := range oldNames {
		out = append(out, newTableIdentity(name.Schema, name.Table))
	}
	return out
}

// parsedUserTargets expands a USER statement's account list the way
// parsedDropTables expands a DROP TABLE list: DROP USER names each dropped
// account, RENAME USER names each "old TO new" pair. A CREATE or ALTER USER
// list stays one row because auth options sit between the accounts. Nil when
// the statement is not a readable DROP or RENAME USER.
func parsedUserTargets(ev model.NormalizedEvent) []string {
	d := mysqlexpr.EventDialect(ev.ServerVersion, ev.ServerFlavor, ev.SQLMode)
	if accounts, isDrop, ok := mysqlexpr.ParseDropUser(ev.QuerySQL, d); isDrop {
		if !ok {
			return nil
		}
		return accounts
	}
	targets, isRename, ok := mysqlexpr.ParseRenameUser(ev.QuerySQL, d)
	if !isRename || !ok {
		return nil
	}
	return targets
}

// ConsumeStatement appends a parsed DDL statement directly.
func (a *DDLAggregator) ConsumeStatement(ts time.Time, binlogPath string, positionStart, positionEnd, binlogBytes int64, sql string) {
	for _, event := range ddlEventsFor(model.NormalizedEvent{
		Timestamp:     ts,
		BinlogPath:    binlogPath,
		PositionStart: positionStart,
		PositionEnd:   positionEnd,
		BinlogBytes:   binlogBytes,
		QuerySQL:      sql,
	}) {
		a.events = append(a.events, event)
	}
}

// Snapshot returns collected DDL events sorted by timestamp and source position.
func (a *DDLAggregator) Snapshot() []model.DDLEvent {
	if len(a.events) == 0 {
		return nil
	}

	out := append([]model.DDLEvent(nil), a.events...)
	sort.SliceStable(out, func(i, j int) bool {
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
	stmt, ok := parseDDLStatement(ev.QuerySQL, mysqlexpr.EventDialect(ev.ServerVersion, ev.ServerFlavor, ev.SQLMode))
	if !ok {
		return model.DDLEvent{}, false
	}

	schema := stmt.Schema
	// An account is global, not a session-schema object.
	if schema == "" && stmt.Object != "user" {
		schema = ev.Schema
	}
	table := stmt.Table
	if table == "" && stmt.Object == "table" {
		table = ev.Table
	}

	txnPath := ev.HoldingStartPath
	txnPos := ev.HoldingStartPos
	if txnPos <= 0 {
		txnPath = ev.BinlogPath
		txnPos = ev.PositionStart
	}
	return model.DDLEvent{
		BinlogPath:             ev.BinlogPath,
		Timestamp:              ev.Timestamp.UTC(),
		Schema:                 schema,
		Table:                  table,
		Operation:              stmt.Operation,
		Object:                 stmt.Object,
		Statement:              stmt.Statement,
		StatementTruncated:     stmt.Truncated,
		StatementOriginalBytes: stmt.OriginalBytes,
		PositionStart:          ev.PositionStart,
		PositionEnd:            ev.PositionEnd,
		BinlogBytes:            ev.BinlogBytes,
		GTID:                   ev.HoldingGTID,
		TxnStartPath:           txnPath,
		TxnStartPos:            txnPos,
		ServerID:               ev.ServerID,
		ThreadID:               ev.ThreadID,
		ActorUser:              ev.ActorUser,
		ActorHost:              ev.ActorHost,
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
	case "SET":
		if second == "PASSWORD" {
			return "SET PASSWORD", "user", 2, true
		}
		return "", "", 0, false
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
		if second == "USER" {
			return "RENAME USER", "user", 2, true
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

// ddlObjectName reads one object name with the shared identifier reader.
// DROP TABLE uses the list reader so a quoted keyword stays a name.
func ddlObjectName(sql, operation, object string, d mysqlexpr.Dialect) (string, string) {
	switch {
	case operation == "SET PASSWORD":
		return "", mysqlexpr.RestAfter(sql, "FOR", d)
	case operation == "DROP TABLE":
		names, isDrop, ok := mysqlexpr.ParseDropTable(sql, d)
		if !isDrop || !ok || len(names) == 0 {
			return "", ""
		}
		return names[0].Schema, names[0].Table
	case object == "index":
		name, ok := mysqlexpr.ReadNameAfter(sql, "ON", d, mysqlexpr.NameOpt{})
		if !ok {
			return "", ""
		}
		return name.Schema, name.Table
	case operation == "TRUNCATE TABLE":
		name, ok := mysqlexpr.ReadNameAfter(sql, "TRUNCATE", d, mysqlexpr.NameOpt{OptionalTable: true})
		if !ok {
			return "", ""
		}
		return name.Schema, name.Table
	case object == "user":
		return "", mysqlexpr.AccountAfter(sql, "USER", d)
	case object == "database":
		name, ok := mysqlexpr.ReadNameAfter(sql, "DATABASE", d, mysqlexpr.NameOpt{})
		if !ok {
			name, ok = mysqlexpr.ReadNameAfter(sql, "SCHEMA", d, mysqlexpr.NameOpt{})
		}
		if !ok {
			return "", ""
		}
		return name.Schema, name.Table
	default:
		keyword := operation
		if i := strings.LastIndex(operation, " "); i >= 0 {
			keyword = operation[i+1:]
		}
		name, ok := mysqlexpr.ReadNameAfter(sql, keyword, d, mysqlexpr.NameOpt{})
		if !ok {
			return "", ""
		}
		return name.Schema, name.Table
	}
}
