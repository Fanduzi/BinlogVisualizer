// Package binlog verifies raw binlog normalization into analyzer-facing events.
// input: synthetic RawEvent values covering MySQL/MariaDB provenance, query, XA, rows, and row-annotation variants.
// output: assertions for normalized provenance, XA identity, LOAD_DATA SQL, UTF-8-safe truncation, and skip behavior.
// pos: regression coverage for the normalize layer between parser output and analyzer input.
// note: if this file changes, keep internal/binlog/README.md synchronized.
package binlog

import (
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"binlogviz/internal/model"
)

func TestNormalizeWriteRowsEvent(t *testing.T) {
	ev, err := NormalizeRawEvent(RawEvent{
		EventType: "WRITE_ROWS_EVENTv2",
		Schema:    "shop",
		Table:     "orders",
		RowCount:  3,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ev.Operation != "INSERT" || ev.RowCount != 3 {
		t.Fatalf("unexpected normalized event: %+v", ev)
	}
}

func TestNormalizeUpdateRowsEventCorrectsRowCount(t *testing.T) {
	// UPDATE_ROWS already has correct RowCount from parser (rows/2)
	ev, err := NormalizeRawEvent(RawEvent{
		EventType: "UPDATE_ROWS_EVENTv2",
		Schema:    "shop",
		Table:     "orders",
		RowCount:  5, // already corrected by parser
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ev.Operation != "UPDATE" || ev.RowCount != 5 {
		t.Fatalf("unexpected normalized event: %+v", ev)
	}
}

func TestNormalizeDeleteRowsEvent(t *testing.T) {
	ev, err := NormalizeRawEvent(RawEvent{
		EventType: "DELETE_ROWS_EVENTv2",
		Schema:    "shop",
		Table:     "orders",
		RowCount:  2,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ev.Operation != "DELETE" || ev.RowCount != 2 {
		t.Fatalf("unexpected normalized event: %+v", ev)
	}
}

func TestNormalizeQueryBeginEvent(t *testing.T) {
	ts := time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC)
	ev, err := NormalizeRawEvent(RawEvent{
		Timestamp: ts,
		EventType: "QUERY_EVENT",
		Query:     "BEGIN",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ev.EventType != "BEGIN" {
		t.Fatalf("expected BEGIN event type, got: %s", ev.EventType)
	}
}

func TestNormalizeQueryCommitEvent(t *testing.T) {
	ts := time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC)
	ev, err := NormalizeRawEvent(RawEvent{
		Timestamp: ts,
		EventType: "QUERY_EVENT",
		Query:     "COMMIT",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ev.EventType != "COMMIT" {
		t.Fatalf("expected COMMIT event type, got: %s", ev.EventType)
	}
}

func TestNormalizeMariaDBXAQueries(t *testing.T) {
	tests := []struct {
		query     string
		eventType string
	}{
		{query: "XA START 'batch-57'", eventType: "XA_START"},
		{query: "XA BEGIN 'batch-57'", eventType: "XA_START"},
		{query: "XA END 'batch-57'", eventType: "XA_END"},
		{query: "XA PREPARE 'batch-57'", eventType: "XA_PREPARE"},
		{query: "XA COMMIT 'batch-57'", eventType: "XA_COMMIT"},
		{query: "XA ROLLBACK 'batch-57'", eventType: "XA_ROLLBACK"},
	}

	for _, tt := range tests {
		t.Run(tt.eventType, func(t *testing.T) {
			ev, err := NormalizeRawEvent(RawEvent{EventType: "QueryEvent", Query: tt.query})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if ev == nil || ev.EventType != tt.eventType {
				t.Fatalf("expected %s event, got %+v", tt.eventType, ev)
			}
			if ev.XAXID != "'batch-57'" {
				t.Fatalf("expected XA identifier to survive normalization, got %q", ev.XAXID)
			}
		})
	}
}

func TestNormalizeMariaDBXAPrepareLogEvent(t *testing.T) {
	ev, err := NormalizeRawEvent(RawEvent{EventType: "XAPrepareLogEvent"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ev == nil || ev.EventType != "XA_PREPARE" {
		t.Fatalf("expected XA_PREPARE boundary, got %+v", ev)
	}
}

func TestNormalizeXIDEvent(t *testing.T) {
	ts := time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC)
	ev, err := NormalizeRawEvent(RawEvent{
		Timestamp: ts,
		EventType: "XID_EVENT",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ev.EventType != "XID" {
		t.Fatalf("expected XID event type, got: %s", ev.EventType)
	}
}

func TestNormalizeTableMapEvent(t *testing.T) {
	ts := time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC)
	ev, err := NormalizeRawEvent(RawEvent{
		Timestamp: ts,
		EventType: "TABLE_MAP_EVENT",
		Schema:    "shop",
		Table:     "orders",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ev.EventType != "TABLE_MAP" || ev.Schema != "shop" || ev.Table != "orders" {
		t.Fatalf("unexpected normalized event: %+v", ev)
	}
}

func TestNormalizeSkipUnsupportedEvent(t *testing.T) {
	ev, err := NormalizeRawEvent(RawEvent{
		EventType: "FORMAT_DESCRIPTION_EVENT",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ev != nil {
		t.Fatalf("expected nil for unsupported event, got: %+v", ev)
	}
}

func TestNormalizeSkipEmptyEventType(t *testing.T) {
	ev, err := NormalizeRawEvent(RawEvent{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ev != nil {
		t.Fatalf("expected nil for empty event type, got: %+v", ev)
	}
}

func TestNormalizeRawEventIntoSkipsEmptyEventType(t *testing.T) {
	var dst model.NormalizedEvent
	ok, err := NormalizeRawEventInto(RawEvent{}, &dst)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Fatalf("expected empty event type to be skipped")
	}
}

func TestNormalizeRawEventIntoResetsReusableDestination(t *testing.T) {
	dst := model.NormalizedEvent{
		EventType:      "ROWS_QUERY",
		Operation:      "INSERT",
		QuerySQL:       "stale",
		QueryTruncated: true,
	}

	ok, err := NormalizeRawEventInto(RawEvent{
		EventType: "WRITE_ROWS_EVENTv2",
		Schema:    "shop",
		Table:     "orders",
		RowCount:  3,
	}, &dst)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatal("expected event to be kept")
	}
	if dst.EventType != "ROWS" || dst.Operation != "INSERT" {
		t.Fatalf("unexpected normalized event: %+v", dst)
	}
	if dst.QuerySQL != "" || dst.QueryTruncated {
		t.Fatalf("expected reusable destination to be reset, got %+v", dst)
	}
}

func TestNormalizeRawEventPreservesBinlogMetadata(t *testing.T) {
	ts := time.Date(2026, 3, 15, 14, 10, 26, 0, time.UTC)
	ev, err := NormalizeRawEvent(RawEvent{
		Timestamp:     ts,
		EventType:     "WRITE_ROWS_EVENTv2",
		Schema:        "shop",
		Table:         "orders",
		RowCount:      3,
		BinlogPath:    "mysql-bin.000123",
		PositionStart: 553,
		PositionEnd:   599,
		BinlogBytes:   46,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ev == nil {
		t.Fatal("expected normalized event")
	}
	if ev.BinlogPath != "mysql-bin.000123" {
		t.Fatalf("expected BinlogPath to be preserved, got %q", ev.BinlogPath)
	}
	if ev.PositionStart != 553 {
		t.Fatalf("expected PositionStart=553, got %d", ev.PositionStart)
	}
	if ev.PositionEnd != 599 {
		t.Fatalf("expected PositionEnd=599, got %d", ev.PositionEnd)
	}
	if ev.BinlogBytes != 46 {
		t.Fatalf("expected BinlogBytes=46, got %d", ev.BinlogBytes)
	}
}

func TestNormalizeRawEventPreservesTransactionProvenance(t *testing.T) {
	ev, err := NormalizeRawEvent(RawEvent{
		EventType:     "MariadbGTIDEvent",
		ServerID:      7,
		ServerVersion: "11.8.3-MariaDB-log",
		ServerFlavor:  "mariadb",
		GTID:          "0-7-1848",
		ThreadID:      1875,
		XID:           "3928",
		ActorUser:     "alice",
		ActorHost:     "db.local",
	})
	if err != nil {
		t.Fatalf("NormalizeRawEvent: %v", err)
	}
	if ev == nil || ev.EventType != "GTID" {
		t.Fatalf("expected normalized GTID event, got %+v", ev)
	}
	if ev.ServerID != 7 || ev.ServerVersion != "11.8.3-MariaDB-log" || ev.ServerFlavor != "mariadb" ||
		ev.GTID != "0-7-1848" || ev.ThreadID != 1875 || ev.XID != "3928" ||
		ev.ActorUser != "alice" || ev.ActorHost != "db.local" {
		t.Fatalf("provenance was not preserved: %+v", ev)
	}
}

// Tests for go-mysql CamelCase event types (real parser output)

func TestNormalizeQueryEventBegin(t *testing.T) {
	// go-mysql returns "QueryEvent" not "QUERY_EVENT"
	ts := time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC)
	ev, err := NormalizeRawEvent(RawEvent{
		Timestamp: ts,
		EventType: "QueryEvent",
		Query:     "BEGIN",
		Schema:    "testdb",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ev.EventType != "BEGIN" {
		t.Fatalf("expected BEGIN event type, got: %s", ev.EventType)
	}
	if ev.Schema != "testdb" {
		t.Fatalf("expected schema testdb, got: %s", ev.Schema)
	}
}

func TestNormalizeQueryEventCommit(t *testing.T) {
	ts := time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC)
	ev, err := NormalizeRawEvent(RawEvent{
		Timestamp: ts,
		EventType: "QueryEvent",
		Query:     "COMMIT",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ev.EventType != "COMMIT" {
		t.Fatalf("expected COMMIT event type, got: %s", ev.EventType)
	}
}

func TestNormalizeXIDEventCamelCase(t *testing.T) {
	ts := time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC)
	ev, err := NormalizeRawEvent(RawEvent{
		Timestamp: ts,
		EventType: "XIDEvent",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ev.EventType != "XID" {
		t.Fatalf("expected XID event type, got: %s", ev.EventType)
	}
}

func TestNormalizeTableMapEventCamelCase(t *testing.T) {
	ts := time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC)
	ev, err := NormalizeRawEvent(RawEvent{
		Timestamp: ts,
		EventType: "TableMapEvent",
		Schema:    "testdb",
		Table:     "users",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ev.EventType != "TABLE_MAP" || ev.Schema != "testdb" || ev.Table != "users" {
		t.Fatalf("unexpected normalized event: %+v", ev)
	}
}

func TestNormalizeWriteRowsEventV2(t *testing.T) {
	ev, err := NormalizeRawEvent(RawEvent{
		EventType: "WriteRowsEventV2",
		Schema:    "testdb",
		Table:     "users",
		RowCount:  1,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ev.Operation != "INSERT" || ev.RowCount != 1 {
		t.Fatalf("unexpected normalized event: %+v", ev)
	}
}

func TestNormalizeUpdateRowsEventV2(t *testing.T) {
	ev, err := NormalizeRawEvent(RawEvent{
		EventType: "UpdateRowsEventV2",
		Schema:    "testdb",
		Table:     "users",
		RowCount:  2,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ev.Operation != "UPDATE" || ev.RowCount != 2 {
		t.Fatalf("unexpected normalized event: %+v", ev)
	}
}

func TestNormalizeDeleteRowsEventV2(t *testing.T) {
	ev, err := NormalizeRawEvent(RawEvent{
		EventType: "DeleteRowsEventV2",
		Schema:    "testdb",
		Table:     "users",
		RowCount:  1,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ev.Operation != "DELETE" || ev.RowCount != 1 {
		t.Fatalf("unexpected normalized event: %+v", ev)
	}
}

// Tests for RowsQueryEvent (original SQL capture)

func TestNormalizeRowsQueryEvent(t *testing.T) {
	ts := time.Date(2026, 3, 15, 10, 0, 0, 0, time.UTC)
	sql := "INSERT INTO users (id, name) VALUES (1, 'Alice')"
	ev, err := NormalizeRawEvent(RawEvent{
		Timestamp: ts,
		EventType: "RowsQueryEvent",
		QuerySQL:  sql,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ev.EventType != "ROWS_QUERY" {
		t.Fatalf("expected ROWS_QUERY event type, got: %s", ev.EventType)
	}
	if ev.QuerySQL != sql {
		t.Fatalf("unexpected QuerySQL: %s", ev.QuerySQL)
	}
	if ev.QueryTruncated {
		t.Fatalf("expected QueryTruncated=false for short query")
	}
	if ev.QueryOriginalBytes != len(sql) {
		t.Fatalf("expected QueryOriginalBytes=%d, got %d", len(sql), ev.QueryOriginalBytes)
	}
}

func TestNormalizeMariaDBLoadDataAnnotation(t *testing.T) {
	query := "LOAD DATA INFILE '/tmp/slow.csv' INTO TABLE dogfood_cut.slow"
	ev, err := NormalizeRawEvent(RawEvent{
		EventType: "MariadbAnnotateRowsEvent",
		QuerySQL:  query,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ev == nil || ev.EventType != "ROWS_QUERY" || ev.Operation != "LOAD_DATA" {
		t.Fatalf("expected LOAD_DATA row context, got %+v", ev)
	}
	if ev.QuerySQL != query || ev.QueryOriginalBytes != len(query) {
		t.Fatalf("expected annotation SQL to survive normalization, got %+v", ev)
	}
}

func TestNormalizeQueryEventLoadDataUsesRowsQueryPipeline(t *testing.T) {
	query := "  LOAD DATA INFILE '/tmp/slow.csv' INTO TABLE dogfood_cut.slow  "
	ev, err := NormalizeRawEvent(RawEvent{
		EventType: "QueryEvent",
		Query:     query,
		ThreadID:  1875,
	})
	if err != nil {
		t.Fatalf("NormalizeRawEvent: %v", err)
	}
	if ev == nil || ev.EventType != "ROWS_QUERY" || ev.Operation != "LOAD_DATA" {
		t.Fatalf("expected LOAD DATA query context, got %+v", ev)
	}
	if ev.QuerySQL != query || ev.QueryOriginalBytes != len(query) || ev.ThreadID != 1875 {
		t.Fatalf("expected QueryEvent SQL and provenance to enter existing context pipeline, got %+v", ev)
	}
}

func TestNormalizeRowsQueryEventCamelCase(t *testing.T) {
	ts := time.Date(2026, 3, 15, 10, 0, 0, 0, time.UTC)
	ev, err := NormalizeRawEvent(RawEvent{
		Timestamp: ts,
		EventType: "ROWS_QUERY_EVENT",
		QuerySQL:  "UPDATE products SET price = 99.99 WHERE id = 42",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ev.EventType != "ROWS_QUERY" {
		t.Fatalf("expected ROWS_QUERY event type, got: %s", ev.EventType)
	}
}

func TestNormalizeRowsQueryEventEmptySQL(t *testing.T) {
	ev, err := NormalizeRawEvent(RawEvent{
		EventType: "RowsQueryEvent",
		QuerySQL:  "",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ev.EventType != "ROWS_QUERY" {
		t.Fatalf("expected ROWS_QUERY event type, got: %s", ev.EventType)
	}
	if ev.QuerySQL != "" {
		t.Fatalf("expected empty QuerySQL, got: %s", ev.QuerySQL)
	}
}

func TestNormalizeRowsQueryEventTruncation(t *testing.T) {
	// Create a SQL string longer than 4096 bytes
	longSQL := "INSERT INTO big_table VALUES (" + makeLongString(5000) + ")"
	originalBytes := len(longSQL)
	if originalBytes <= 4096 {
		t.Fatalf("test SQL not long enough: %d bytes", originalBytes)
	}

	ev, err := NormalizeRawEvent(RawEvent{
		EventType: "RowsQueryEvent",
		QuerySQL:  longSQL,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ev.EventType != "ROWS_QUERY" {
		t.Fatalf("expected ROWS_QUERY event type, got: %s", ev.EventType)
	}
	if len(ev.QuerySQL) > 4096 {
		t.Fatalf("expected QuerySQL to be truncated to <=4096 bytes, got: %d", len(ev.QuerySQL))
	}
	if !ev.QueryTruncated {
		t.Fatalf("expected QueryTruncated=true for truncated query")
	}
	// KEY TEST: OriginalBytes must be the ORIGINAL length, not the truncated length
	if ev.QueryOriginalBytes != originalBytes {
		t.Fatalf("expected QueryOriginalBytes=%d (original), got %d", originalBytes, ev.QueryOriginalBytes)
	}
	if ev.QueryOriginalBytes == len(ev.QuerySQL) {
		t.Fatalf("QueryOriginalBytes should NOT equal truncated SQL length %d, but it does", len(ev.QuerySQL))
	}
}

func TestNormalizeRowsQueryEventTruncationUTF8Boundary(t *testing.T) {
	prefix := strings.Repeat("a", model.MaxStoredSQLBytes-1)
	longSQL := prefix + "界suffix"

	ev, err := NormalizeRawEvent(RawEvent{
		EventType: "RowsQueryEvent",
		QuerySQL:  longSQL,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(ev.QuerySQL) > model.MaxStoredSQLBytes {
		t.Fatalf("expected QuerySQL to be truncated to <=%d bytes, got: %d", model.MaxStoredSQLBytes, len(ev.QuerySQL))
	}
	if !utf8.ValidString(ev.QuerySQL) {
		t.Fatalf("expected truncated QuerySQL to remain valid UTF-8, got %q", ev.QuerySQL)
	}
	if strings.Contains(ev.QuerySQL, "界") {
		t.Fatalf("expected partial rune to be removed from truncated QuerySQL, got %q", ev.QuerySQL)
	}
}

func TestNormalizeQueryDDLCreateTableAndDatabase(t *testing.T) {
	createTable, err := NormalizeRawEvent(RawEvent{
		EventType:     "QueryEvent",
		Schema:        "testdb",
		Query:         "CREATE TABLE users (\n  id INT PRIMARY KEY,\n  name VARCHAR(100)\n)",
		PositionStart: 219,
		PositionEnd:   361,
		BinlogBytes:   142,
		BinlogPath:    "mysql-bin.000004",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if createTable == nil {
		t.Fatal("expected CREATE TABLE Query event to be kept")
	}
	if createTable.EventType != "DDL" {
		t.Fatalf("expected EventType=DDL, got %q", createTable.EventType)
	}
	if !strings.Contains(createTable.QuerySQL, "CREATE TABLE users") {
		t.Fatalf("expected QuerySQL to keep CREATE TABLE text, got %q", createTable.QuerySQL)
	}
	if createTable.Schema != "testdb" || createTable.PositionStart != 219 || createTable.BinlogBytes != 142 {
		t.Fatalf("expected schema/positions preserved, got %+v", createTable)
	}

	createDB, err := NormalizeRawEvent(RawEvent{
		EventType: "QUERY_EVENT",
		Query:     "CREATE DATABASE IF NOT EXISTS dogfood",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if createDB == nil || createDB.EventType != "DDL" {
		t.Fatalf("expected CREATE DATABASE to be kept as DDL, got %+v", createDB)
	}
}

func TestNormalizeQueryDDLGrantAndCreateUser(t *testing.T) {
	grant, err := NormalizeRawEvent(RawEvent{
		EventType: "QUERY_EVENT",
		Query:     "GRANT REPLICATION SLAVE ON *.* TO 'repl'@'127.0.0.1'",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if grant == nil || grant.EventType != "DDL" {
		t.Fatalf("expected GRANT to be kept as DDL, got %+v", grant)
	}

	createUser, err := NormalizeRawEvent(RawEvent{
		EventType: "QueryEvent",
		Query:     "CREATE USER 'repl'@'%'",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if createUser == nil || createUser.EventType != "DDL" {
		t.Fatalf("expected CREATE USER to be kept as DDL, got %+v", createUser)
	}

	revoke, err := NormalizeRawEvent(RawEvent{
		EventType: "QUERY_EVENT",
		Query:     "REVOKE ALL PRIVILEGES ON *.* FROM 'repl'@'%'",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if revoke == nil || revoke.EventType != "DDL" {
		t.Fatalf("expected REVOKE to be kept as DDL, got %+v", revoke)
	}
}

func TestNormalizeSkipsNonTransactionalQueryWithoutAllocation(t *testing.T) {
	raw := RawEvent{
		EventType: "QUERY_EVENT",
		Schema:    "shop",
		Query:     "SET timestamp=1710000000",
	}

	allocs := testing.AllocsPerRun(1000, func() {
		ev, err := NormalizeRawEvent(raw)
		if err != nil {
			panic(err)
		}
		if ev != nil {
			panic("expected skipped query event to return nil")
		}
	})

	if allocs != 0 {
		t.Fatalf("expected zero allocations for skipped non-transactional query, got %.2f", allocs)
	}
}

// makeLongString creates a string of the specified byte length (approximate)
func makeLongString(targetLen int) string {
	result := "'"
	for len(result) < targetLen {
		result += "x"
	}
	return result + "'"
}
