package binlog

import (
	"math"
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

func storeMediumint(v uint32) int32 {
	u := v & 0xFFFFFF
	if u&0x00800000 != 0 {
		u |= 0xFF000000
	}
	return int32(u)
}

func TestMediumintUnsignedWidth(t *testing.T) {
	cases := []struct {
		stored uint32
		want   string
	}{
		{stored: 0, want: "0"},
		{stored: 1, want: "1"},
		{stored: 8388607, want: "8388607"},
		{stored: 8388608, want: "8388608"},
		{stored: 9000000, want: "9000000"},
		{stored: 12000000, want: "12000000"},
		{stored: 16777214, want: "16777214"},
		{stored: 16777215, want: "16777215"},
	}
	for _, tc := range cases {
		got, ok := formatInt(storeMediumint(tc.stored), mysql.MYSQL_TYPE_INT24, boolPtr(true))
		if !ok || got != tc.want {
			t.Fatalf("unsigned %d: got %q ok=%v", tc.stored, got, ok)
		}
		literal, ok := sqlInteger(storeMediumint(tc.stored), mysql.MYSQL_TYPE_INT24, true)
		if !ok || literal != tc.want {
			t.Fatalf("sql unsigned %d: got %q ok=%v", tc.stored, literal, ok)
		}
	}
	// mysqlbinlog prints the sign-extended int32 and the 24-bit unsigned value.
	both, ok := formatInt(storeMediumint(9000000), mysql.MYSQL_TYPE_INT24, nil)
	if !ok || both != "-7777216 (9000000)" {
		t.Fatalf("both forms: %q", both)
	}
	both, ok = formatInt(storeMediumint(16777215), mysql.MYSQL_TYPE_INT24, nil)
	if !ok || both != "-1 (16777215)" {
		t.Fatalf("max both forms: %q", both)
	}
	// An INT still uses 32 bits when the type is LONG or unknown.
	wide := asInt32(3000000000)
	for _, typ := range []byte{0, mysql.MYSQL_TYPE_LONG} {
		got, ok := formatInt(wide, typ, boolPtr(true))
		if !ok || got != "3000000000" {
			t.Fatalf("int typ %d: %q", typ, got)
		}
	}
	signed, ok := formatInt(int32(-8388608), mysql.MYSQL_TYPE_INT24, boolPtr(false))
	if !ok || signed != "-8388608" {
		t.Fatalf("signed min: %q", signed)
	}
	signed, ok = formatInt(int32(8388607), mysql.MYSQL_TYPE_INT24, boolPtr(false))
	if !ok || signed != "8388607" {
		t.Fatalf("signed max: %q", signed)
	}
}

func TestMediumintUnsignedPrimaryKeyAndFlashback(t *testing.T) {
	types := []byte{mysql.MYSQL_TYPE_INT24, mysql.MYSQL_TYPE_VARCHAR}
	names := [][]byte{[]byte("id"), []byte("note")}
	table := yearTable(types, names, []uint64{0}, []byte{0x80})
	row := []any{storeMediumint(9000000), "original"}
	keys := primaryKeyValues(&replication.RowsEvent{
		ColumnCount: 2,
		Table:       table,
		Rows:        [][]any{row},
	}, kindDeleteRows, pkMetaFrom(table))
	if len(keys) != 1 || keys[0] != "id=9000000" {
		t.Fatalf("pk: %#v", keys)
	}
	deleted := captureFlashbackRows(&replication.RowsEvent{
		ColumnCount: 2,
		Table:       table,
		Rows:        [][]any{row},
	}, kindDeleteRows, "p169", "mpk")
	if len(deleted) != 1 || deleted[0].ProblemKind != "" || deleted[0].Before[0] != "9000000" {
		t.Fatalf("flash: %+v", deleted)
	}
	images, omitted := captureRowImages(&replication.RowsEvent{
		ColumnCount: 2,
		Table:       table,
		Rows:        [][]any{row},
	}, kindDeleteRows, "p169", "mpk")
	if omitted != 0 || len(images) != 1 || images[0].Before[0].Text != "9000000" {
		t.Fatalf("show-rows: %+v omitted %d", images, omitted)
	}
}

func TestMediumintUnsignedInsideTransactionPayload(t *testing.T) {
	types := []byte{mysql.MYSQL_TYPE_YEAR, mysql.MYSQL_TYPE_INT24, mysql.MYSQL_TYPE_VARCHAR}
	names := [][]byte{[]byte("y"), []byte("id"), []byte("note")}
	// y=0, id unsigned=1. 01 = 0x40 with the rest unused.
	table := yearTable(types, names, []uint64{1}, []byte{0x40})
	table.TableID = 4
	row := []any{2026, storeMediumint(16777215), "max"}
	ev := &replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.TRANSACTION_PAYLOAD_EVENT, EventSize: 80, LogPos: 100},
		Event: &replication.TransactionPayloadEvent{
			Events: []*replication.BinlogEvent{
				{
					Header: &replication.EventHeader{EventType: replication.TABLE_MAP_EVENT, EventSize: 20, LogPos: 30},
					Event:  table,
				},
				{
					Header: &replication.EventHeader{EventType: replication.DELETE_ROWS_EVENTv2, EventSize: 40, LogPos: 70},
					Event: &replication.RowsEvent{
						TableID:     4,
						Table:       table,
						ColumnCount: 3,
						Rows:        [][]any{row},
					},
				},
			},
		},
	}
	raws, ok := expandTransactionPayload(ev, "mysql-bin.000001", "8.0.46", map[uint64]cachedTableName{}, true, true)
	if !ok || len(raws) != 2 {
		t.Fatalf("payload ok=%v n=%d", ok, len(raws))
	}
	rows := raws[1]
	if len(rows.FlashRows) != 1 || rows.FlashRows[0].ProblemKind != "" || rows.FlashRows[0].Before[1] != "16777215" {
		t.Fatalf("flash %#v", rows.FlashRows)
	}
	if len(rows.RowImages) != 1 || rows.RowImages[0].Before[1].Text != "16777215" {
		t.Fatalf("show-rows %#v", rows.RowImages)
	}
	if len(rows.RowKeys) != 1 || rows.RowKeys[0] != "id=16777215" {
		t.Fatalf("pk %#v", rows.RowKeys)
	}
}

func TestBitValueAndLiteral(t *testing.T) {
	cell := formatCell(int64(-1), mysql.MYSQL_TYPE_BIT, nil)
	if cell.Text != "18446744073709551615" {
		t.Fatalf("bit64: %q", cell.Text)
	}
	// BIT(64) metadata: 8 bytes and no leftover bits.
	lit, ok := sqlBit(int64(-1), 0x0800)
	if !ok || lit != "b'"+strings.Repeat("1", 64)+"'" {
		t.Fatalf("bit64 lit: %q", lit)
	}
	lit, ok = sqlBit(int64(5), 3) // BIT(3)
	if !ok || lit != "b'101'" {
		t.Fatalf("bit3 lit: %q", lit)
	}
	if _, ok := sqlBit(int64(5), 1); ok { // BIT(1) cannot hold 5
		t.Fatal("bit1 accepted a value past its width")
	}
}

func TestDecimalCellIsNotTruncated(t *testing.T) {
	text := strings.Repeat("9", 65)
	cell := formatCell(text, mysql.MYSQL_TYPE_NEWDECIMAL, nil)
	if cell.Text != text || strings.Contains(cell.Text, "truncated") {
		t.Fatalf("decimal cell: %q", cell.Text)
	}
}

func TestBitDoesNotConsumeSignednessBit(t *testing.T) {
	// YEAR, BIT(8), MEDIUMINT UNSIGNED. BIT is not numeric for the bitmap.
	// bits: year=0, mediumint=1. 01 = 0x40.
	types := []byte{mysql.MYSQL_TYPE_YEAR, mysql.MYSQL_TYPE_BIT, mysql.MYSQL_TYPE_INT24}
	names := [][]byte{[]byte("y"), []byte("b"), []byte("mi")}
	table := yearTable(types, names, nil, []byte{0x40})
	table.ColumnMeta = []uint16{0, 8, 0} // BIT(8): 1 byte, 0 leftover
	got := unsignedMap(table)
	if got[0] || got[1] || !got[2] {
		t.Fatalf("bitmap %#v", got)
	}
	row := []any{2026, int64(255), storeMediumint(9000000)}
	images, _ := captureRowImages(&replication.RowsEvent{
		ColumnCount: 3,
		Table:       table,
		Rows:        [][]any{row},
	}, kindWriteRows, "numx", "bits")
	if images[0].After[1].Text != "255" || images[0].After[2].Text != "9000000" {
		t.Fatalf("cells %#v", cells(images[0].After))
	}
}

func TestFloatOutOfRangePrintsMarker(t *testing.T) {
	// The shortest IEEE decimal of the largest float32 is above the value
	// MySQL will cast to FLOAT. Printing it would be a number the server rejects.
	maxCell := formatCell(float32(math.MaxFloat32), mysql.MYSQL_TYPE_FLOAT, nil)
	if maxCell.Text != "<FLOAT>" {
		t.Fatalf("max float32: %q", maxCell.Text)
	}
	neg := formatCell(float32(-math.MaxFloat32), mysql.MYSQL_TYPE_FLOAT, nil)
	if neg.Text != "<FLOAT>" {
		t.Fatalf("min float32: %q", neg.Text)
	}
	one := formatCell(float32(1), mysql.MYSQL_TYPE_FLOAT, nil)
	if one.Text != "1" {
		t.Fatalf("float 1: %q", one.Text)
	}
	tenth := formatCell(float32(0.1), mysql.MYSQL_TYPE_FLOAT, nil)
	if tenth.Text != "0.1" {
		t.Fatalf("float 0.1: %q", tenth.Text)
	}
	// A type-less float64 still prints its Go decimal. MYSQL_TYPE_DECIMAL is 0.
	plain := formatCell(float64(1.5), mysql.MYSQL_TYPE_NULL, nil)
	if plain.Text != "1.5" {
		t.Fatalf("plain float64: %q", plain.Text)
	}
	wide := formatCell(math.MaxFloat64, mysql.MYSQL_TYPE_DOUBLE, nil)
	if wide.Text == "<DOUBLE>" || wide.Text == "" {
		t.Fatalf("max float64: %q", wide.Text)
	}
}
