// Package binlog verifies transaction-payload expand and related physical-kind mapping.
// input: decoded TransactionPayloadEvent values and the committed MySQL 8.0.36 compressed dialect fixture.
// output: assertions that inner ROW images keep INSERT/UPDATE/DELETE kinds, and a successful expand does not emit the wrapper as unmapped.
// pos: parser admission seam for #75 transaction-payload completeness (ParseFiles fixture plus decoded-payload expand).
// note: if this file changes, update this header and README.md.
package binlog

import (
	"path/filepath"
	"testing"

	"github.com/go-mysql-org/go-mysql/replication"
)

func TestExpandTransactionPayloadEmitsInnerKindsNotWrapper(t *testing.T) {
	raws, ok := expandTransactionPayload(decodedPayloadWrapper(), "mysql-bin.000001", "8.0.36", map[uint64]cachedTableName{})
	if !ok {
		t.Fatal("expected successful expand")
	}
	want := []string{kindQuery, kindTableMap, kindWriteRows, kindUpdateRows, kindDeleteRows, kindXID}
	if len(raws) != len(want) {
		t.Fatalf("expanded events=%d, want %d kinds %v", len(raws), len(want), eventTypes(raws))
	}
	for i, kind := range want {
		if raws[i].EventType != kind {
			t.Fatalf("inner[%d] kind=%q, want %q (got %v)", i, raws[i].EventType, kind, eventTypes(raws))
		}
		if raws[i].BinlogPath != "mysql-bin.000001" {
			t.Fatalf("inner[%d] path=%q, want wrapper path", i, raws[i].BinlogPath)
		}
	}
}

func TestExpandTransactionPayloadInnerRowsNormalizeAsUnwrapped(t *testing.T) {
	raws, ok := expandTransactionPayload(decodedPayloadWrapper(), "mysql-bin.000001", "8.0.36", map[uint64]cachedTableName{})
	if !ok {
		t.Fatal("expected successful expand")
	}
	inserts, updates, deletes := countNormalizedRowOps(t, raws)
	if inserts != 1 || updates != 1 || deletes != 1 {
		t.Fatalf("inner operations INSERT/UPDATE/DELETE=%d/%d/%d, want 1/1/1", inserts, updates, deletes)
	}
}

func TestSuccessfulPayloadExpandDoesNotIncrementUnmappedEvents(t *testing.T) {
	raws, ok := expandTransactionPayload(decodedPayloadWrapper(), "mysql-bin.000001", "8.0.36", nil)
	if !ok {
		t.Fatal("expected successful expand")
	}
	var observer FormatObserver
	for _, raw := range raws {
		observer.Observe(raw)
	}
	if observer.UnmappedEvents != 0 {
		t.Fatalf("UnmappedEvents=%d after successful expand, want 0", observer.UnmappedEvents)
	}
	if observer.RowImageEvents != 3 {
		t.Fatalf("RowImageEvents=%d, want 3 inner ROW images", observer.RowImageEvents)
	}
}

func TestFailedPayloadExpandLeavesWrapperUnmapped(t *testing.T) {
	raws, ok := expandTransactionPayload(&replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.TRANSACTION_PAYLOAD_EVENT, EventSize: 40, LogPos: 120},
		Event:  &replication.TransactionPayloadEvent{},
	}, "mysql-bin.000001", "8.0.36", nil)
	if ok {
		t.Fatalf("empty payload must not count as a successful expand, got %v", eventTypes(raws))
	}
}

func TestParseFilesExpandsMySQL80CompressedTransactionPayload(t *testing.T) {
	fixture := filepath.Join("testdata", "mysql80_transaction_payload.binlog")
	var raws []RawEvent
	var observer FormatObserver
	if err := NewParser().ParseFiles([]string{fixture}, func(raw RawEvent) error {
		observer.Observe(raw)
		raws = append(raws, raw)
		return nil
	}); err != nil {
		t.Fatalf("ParseFiles: %v", err)
	}

	kinds := map[string]int{}
	for _, raw := range raws {
		kinds[raw.EventType]++
	}
	inserts, updates, deletes := countNormalizedRowOps(t, raws)
	if kinds[kindWriteRows] < 1 || kinds[kindUpdateRows] < 1 || kinds[kindDeleteRows] < 1 {
		t.Fatalf("ParseFiles missing expanded ROW images: %v", kinds)
	}
	if inserts != 1 || updates != 1 || deletes != 1 {
		t.Fatalf("inner operations INSERT/UPDATE/DELETE=%d/%d/%d, want 1/1/1", inserts, updates, deletes)
	}
	if kinds[""] != 2 {
		t.Fatalf("unmapped kinds=%d (%v), want Previous GTIDs and Rotate only so the wrapper did not leak", kinds[""], kinds)
	}
	if observer.UnmappedEvents != 2 {
		t.Fatalf("UnmappedEvents=%d, want 2 after successful expand", observer.UnmappedEvents)
	}
	if observer.RowImageEvents != 3 {
		t.Fatalf("RowImageEvents=%d, want 3 inner ROW images", observer.RowImageEvents)
	}
}

func decodedPayloadWrapper() *replication.BinlogEvent {
	return &replication.BinlogEvent{
		Header: &replication.EventHeader{
			Timestamp: 1,
			EventType: replication.TRANSACTION_PAYLOAD_EVENT,
			ServerID:  1,
			EventSize: 180,
			LogPos:    400,
		},
		Event: &replication.TransactionPayloadEvent{
			Events: []*replication.BinlogEvent{
				{
					Header: &replication.EventHeader{EventType: replication.QUERY_EVENT, EventSize: 40, LogPos: 80},
					Event:  &replication.QueryEvent{Query: []byte("BEGIN")},
				},
				{
					Header: &replication.EventHeader{EventType: replication.TABLE_MAP_EVENT, EventSize: 30, LogPos: 110},
					Event:  &replication.TableMapEvent{TableID: 42, Schema: []byte("shop"), Table: []byte("orders")},
				},
				{
					Header: &replication.EventHeader{EventType: replication.WRITE_ROWS_EVENTv2, EventSize: 40, LogPos: 150},
					Event:  &replication.RowsEvent{TableID: 42, Rows: [][]any{{1, "alice"}}},
				},
				{
					Header: &replication.EventHeader{EventType: replication.UPDATE_ROWS_EVENTv2, EventSize: 50, LogPos: 200},
					Event:  &replication.RowsEvent{TableID: 42, Rows: [][]any{{1, "alice"}, {1, "alice_updated"}}},
				},
				{
					Header: &replication.EventHeader{EventType: replication.DELETE_ROWS_EVENTv2, EventSize: 40, LogPos: 240},
					Event:  &replication.RowsEvent{TableID: 42, Rows: [][]any{{1, "alice_updated"}}},
				},
				{
					Header: &replication.EventHeader{EventType: replication.XID_EVENT, EventSize: 20, LogPos: 260},
					Event:  &replication.XIDEvent{XID: 9},
				},
			},
		},
	}
}

func eventTypes(raws []RawEvent) []string {
	kinds := make([]string, len(raws))
	for i, raw := range raws {
		kinds[i] = raw.EventType
	}
	return kinds
}

func countNormalizedRowOps(t *testing.T, raws []RawEvent) (inserts, updates, deletes int) {
	t.Helper()
	for _, raw := range raws {
		ev, err := NormalizeRawEvent(raw)
		if err != nil {
			t.Fatalf("normalize %s: %v", raw.EventType, err)
		}
		if ev == nil {
			continue
		}
		switch ev.Operation {
		case "INSERT":
			inserts += ev.RowCount
		case "UPDATE":
			updates += ev.RowCount
		case "DELETE":
			deletes += ev.RowCount
		}
	}
	return inserts, updates, deletes
}
