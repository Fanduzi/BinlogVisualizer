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
