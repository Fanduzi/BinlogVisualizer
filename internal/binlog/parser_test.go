// Package binlog verifies parser construction, progress helpers, and table-name reuse behavior.
// input: parser constructors, progress helpers, and synthetic go-mysql replication events.
// output: regression coverage for parser setup, progress math, event projection, and micro-benchmarks.
// pos: focused unit-test layer for parser helpers that support command-level parsing and progress reporting.
// note: if this file changes, update this header and README.md.
package binlog

import (
	"testing"

	"github.com/go-mysql-org/go-mysql/replication"
)

func TestNewParserReturnsImplementation(t *testing.T) {
	if NewParser() == nil {
		t.Fatal("expected parser")
	}
}

func TestClampProgressOffsetClampsToFileSize(t *testing.T) {
	got := clampProgressOffset(120, 100)
	if got != 100 {
		t.Fatalf("expected 100, got %d", got)
	}
}

func TestClampProgressOffsetPreservesValidOffset(t *testing.T) {
	got := clampProgressOffset(40, 100)
	if got != 40 {
		t.Fatalf("expected 40, got %d", got)
	}
}

func TestMaxInt64IgnoresRegression(t *testing.T) {
	got := maxInt64(40, 30)
	if got != 40 {
		t.Fatalf("expected 40, got %d", got)
	}
}

func TestApplyBinlogEventMetadataReusesCachedTableNameForRowsEvents(t *testing.T) {
	tableNames := map[uint64]cachedTableName{}

	var tableMapRaw RawEvent
	applyBinlogEventMetadata(&tableMapRaw, replication.TABLE_MAP_EVENT.String(), &replication.TableMapEvent{
		TableID: 42,
		Schema:  []byte("app"),
		Table:   []byte("orders"),
	}, tableNames)

	if tableMapRaw.Schema != "app" || tableMapRaw.Table != "orders" {
		t.Fatalf("expected table map raw event to expose app.orders, got %s.%s", tableMapRaw.Schema, tableMapRaw.Table)
	}
	if got := tableNames[42]; got.schema != "app" || got.table != "orders" {
		t.Fatalf("expected cached table name app.orders, got %#v", got)
	}

	var rowsRaw RawEvent
	applyBinlogEventMetadata(&rowsRaw, replication.WRITE_ROWS_EVENTv2.String(), &replication.RowsEvent{
		TableID: 42,
		Rows: [][]any{
			{1, "a"},
			{2, "b"},
		},
	}, tableNames)

	if rowsRaw.Schema != "app" || rowsRaw.Table != "orders" {
		t.Fatalf("expected rows event to reuse cached app.orders, got %s.%s", rowsRaw.Schema, rowsRaw.Table)
	}
	if rowsRaw.RowCount != 2 {
		t.Fatalf("expected RowCount=2, got %d", rowsRaw.RowCount)
	}
}

func TestApplyBinlogEventMetadataPreservesParserUpdateRowCountSemantics(t *testing.T) {
	var raw RawEvent
	applyBinlogEventMetadata(&raw, replication.UPDATE_ROWS_EVENTv2.String(), &replication.RowsEvent{
		Rows: [][]any{
			{1, "before"},
			{1, "after"},
			{2, "before"},
			{2, "after"},
		},
	}, nil)

	if raw.RowCount != 4 {
		t.Fatalf("expected parser row count to preserve raw row images, got %d", raw.RowCount)
	}
}

func BenchmarkApplyBinlogEventMetadataCachedRowsEvent(b *testing.B) {
	tableNames := map[uint64]cachedTableName{
		42: {schema: "app", table: "orders"},
	}
	rows := &replication.RowsEvent{
		TableID: 42,
		Rows: [][]any{
			{1, "a"},
			{2, "b"},
			{3, "c"},
			{4, "d"},
		},
	}

	b.ReportAllocs()
	var raw RawEvent
	for i := 0; i < b.N; i++ {
		raw = RawEvent{}
		applyBinlogEventMetadata(&raw, replication.WRITE_ROWS_EVENTv2.String(), rows, tableNames)
	}
	_ = raw
}
