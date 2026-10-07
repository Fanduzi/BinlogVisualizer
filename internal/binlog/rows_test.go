package binlog

import (
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"binlogviz/internal/model"
)

func boolPtr(v bool) *bool { return &v }

func wideID() uint32 { return 3000000000 }

func TestFormatCellTypesAndNull(t *testing.T) {
	if cell := formatCell(nil, nil); !cell.Null || cell.Text != "" {
		t.Fatalf("NULL cell = %+v", cell)
	}
	wide := uint32(3000000000)
	unsignedID := int32(wide)
	signedQty := int32(-4)
	cases := []struct {
		name     string
		value    any
		unsigned *bool
		want     string
	}{
		{name: "unknown signedness both forms", value: unsignedID, want: "-1294967296 (3000000000)"},
		{name: "known unsigned", value: unsignedID, unsigned: boolPtr(true), want: "3000000000"},
		{name: "known signed", value: signedQty, unsigned: boolPtr(false), want: "-4"},
		{name: "unknown signedness when equal", value: int32(2), want: "2"},
		{name: "int64", value: int64(7), want: "7"},
		{name: "decimal string", value: "19.99", want: "19.99"},
		{name: "float", value: float64(1.5), want: "1.5"},
		{name: "string", value: `it's "bad"`, want: `it's "bad"`},
		{name: "datetime", value: time.Date(2026, 10, 6, 14, 5, 1, 123456000, time.UTC), want: "2026-10-06 14:05:01.123456"},
		{name: "json string", value: `{"n":1,"sku":"Z"}`, want: `{"n":1,"sku":"Z"}`},
		{name: "blob", value: []byte{0xde, 0xad, 0xbe, 0xef, 0x01, 0x02}, want: "0xdeadbeef0102 (6 bytes)"},
		{name: "empty blob", value: []byte{}, want: "0x (0 bytes)"},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			cell := formatCell(tt.value, tt.unsigned)
			if cell.Null || cell.Text != tt.want {
				t.Fatalf("got %+v, want %q", cell, tt.want)
			}
		})
	}
	long := strings.Repeat("x", model.MaxRowValueBytes+8)
	cell := formatCell(long, nil)
	if !strings.Contains(cell.Text, "truncated") || strings.Contains(cell.Text, long) {
		t.Fatalf("string was not bounded: %q", cell.Text)
	}
	blob := bytesRepeat(0x78, model.MaxRowValueBytes+8)
	cell = formatCell(blob, nil)
	if !strings.HasPrefix(cell.Text, "0x") || !strings.Contains(cell.Text, "truncated") {
		t.Fatalf("blob was not bounded: %q", cell.Text)
	}
}

func TestCaptureRowImagesInsertUpdateDelete(t *testing.T) {
	inserts, omitted := captureRowImages(&replication.RowsEvent{
		ColumnCount: 2,
		Rows:        [][]any{{int32(1), nil}},
	}, kindWriteRows, "shop", "orders")
	if omitted != 0 || len(inserts) != 1 || inserts[0].Op != "INSERT" || inserts[0].Names != model.RowNamesPositional {
		t.Fatalf("insert = %+v omitted %d", inserts, omitted)
	}
	if inserts[0].Columns[0] != "@1" || !inserts[0].After[1].Null || inserts[0].Before != nil {
		t.Fatalf("insert cells = %+v", inserts[0])
	}

	updates, _ := captureRowImages(&replication.RowsEvent{
		ColumnCount: 2,
		Rows: [][]any{
			{int32(1), "alpha"},
			{int32(1), "changed"},
		},
		SkippedColumns: [][]int{nil, {0}},
		Table: &replication.TableMapEvent{
			ColumnCount: 2,
			ColumnName:  [][]byte{[]byte("id"), []byte("note")},
			ColumnType:  []byte{mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_VARCHAR},
		},
	}, kindUpdateRows, "shop", "orders")
	if len(updates) != 1 || updates[0].Names != model.RowNamesFull || updates[0].Columns[0] != "id" {
		t.Fatalf("update names = %+v", updates[0])
	}
	if len(updates[0].Changed) != 1 || updates[0].Changed[0] != "note" {
		t.Fatalf("changed = %v", updates[0].Changed)
	}
	if updates[0].After[0].Text != "1" || updates[0].After[1].Text != "changed" {
		t.Fatalf("after = %+v", updates[0].After)
	}

	deletes, _ := captureRowImages(&replication.RowsEvent{
		ColumnCount: 1,
		Rows:        [][]any{{int32(wideID())}},
		Table:       unsignedLongTable(),
	}, kindDeleteRows, "shop", "orders")
	if deletes[0].Op != "DELETE" || deletes[0].After != nil || deletes[0].Before[0].Text != "3000000000" {
		t.Fatalf("delete = %+v", deletes[0])
	}
}

func TestCaptureRowImagesCapsPerEvent(t *testing.T) {
	rows := make([][]any, model.MaxRowImagesPerTxn+1)
	for i := range rows {
		rows[i] = []any{int32(i)}
	}
	images, omitted := captureRowImages(&replication.RowsEvent{
		ColumnCount: 1,
		Rows:        rows,
	}, kindWriteRows, "shop", "orders")
	if len(images) != model.MaxRowImagesPerTxn || omitted != 1 {
		t.Fatalf("kept %d omitted %d", len(images), omitted)
	}
}

func TestPrimaryKeyValuesUseNamedKeyNotFirstColumn(t *testing.T) {
	table := &replication.TableMapEvent{
		ColumnCount: 3,
		ColumnName:  [][]byte{[]byte("label"), []byte("id"), []byte("n")},
		PrimaryKey:  []uint64{1},
	}
	keys := primaryKeyValues(&replication.RowsEvent{
		ColumnCount: 3,
		Table:       table,
		Rows: [][]any{
			{"hot", int32(7), int32(0)},
			{"hot", int32(7), int32(1)},
		},
	}, kindUpdateRows, pkMetaFrom(table))
	if len(keys) != 1 || keys[0] != "id=7" {
		t.Fatalf("keys=%v", keys)
	}
	if strings.Contains(keys[0], "label") || strings.Contains(keys[0], "@") {
		t.Fatalf("guessed a non-key column: %v", keys)
	}

	composite := &replication.TableMapEvent{
		ColumnCount: 3,
		ColumnName:  [][]byte{[]byte("sku"), []byte("wh"), []byte("qty")},
		PrimaryKey:  []uint64{0, 1},
	}
	got := primaryKeyValues(&replication.RowsEvent{
		ColumnCount: 3,
		Table:       composite,
		Rows:        [][]any{{"BOLT", int32(1), int32(11)}},
	}, kindDeleteRows, pkMetaFrom(composite))
	if len(got) != 1 || got[0] != "sku=BOLT, wh=1" {
		t.Fatalf("composite=%v", got)
	}

	noKey := &replication.TableMapEvent{
		ColumnCount: 2,
		ColumnName:  [][]byte{[]byte("id"), []byte("note")},
	}
	if primaryKeyValues(&replication.RowsEvent{
		ColumnCount: 2,
		Table:       noKey,
		Rows:        [][]any{{int32(1), "a"}},
	}, kindUpdateRows, pkMetaFrom(noKey)) != nil {
		t.Fatal("missing primary key must not fall back to @1")
	}
}

func unsignedLongTable() *replication.TableMapEvent {
	return &replication.TableMapEvent{
		ColumnCount:      1,
		ColumnName:       [][]byte{[]byte("id")},
		ColumnType:       []byte{mysql.MYSQL_TYPE_LONG},
		SignednessBitmap: []byte{0x80},
	}
}

func bytesRepeat(b byte, n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = b
	}
	return out
}
