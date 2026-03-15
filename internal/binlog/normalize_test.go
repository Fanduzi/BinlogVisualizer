package binlog

import (
	"testing"
	"time"
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
	// Create a string with multi-byte UTF-8 characters that exceeds 4096 bytes
	base := "SELECT '日本語テスト' " // Each Japanese char is 3 bytes
	var longSQL string
	for len(longSQL) < 4100 {
		longSQL += base
	}

	ev, err := NormalizeRawEvent(RawEvent{
		EventType: "RowsQueryEvent",
		QuerySQL:  longSQL,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(ev.QuerySQL) > 4096 {
		t.Fatalf("expected QuerySQL to be truncated to <=4096 bytes, got: %d", len(ev.QuerySQL))
	}
	// Verify it's valid UTF-8 (should not panic if we check)
	_ = []rune(ev.QuerySQL)
}

// makeLongString creates a string of the specified byte length (approximate)
func makeLongString(targetLen int) string {
	result := "'"
	for len(result) < targetLen {
		result += "x"
	}
	return result + "'"
}
