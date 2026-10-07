package binlog

import (
	"math"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"binlogviz/internal/model"
)

func TestUnsignedMapMatchesGoMySQLWithoutYear(t *testing.T) {
	// No YEAR and no old DECIMAL: the local walk and go-mysql v1.14 agree,
	// so analyze output for existing fixtures stays the same.
	types := []byte{
		mysql.MYSQL_TYPE_TINY,
		mysql.MYSQL_TYPE_VARCHAR,
		mysql.MYSQL_TYPE_SHORT,
		mysql.MYSQL_TYPE_LONG,
		mysql.MYSQL_TYPE_LONGLONG,
		mysql.MYSQL_TYPE_NEWDECIMAL,
		mysql.MYSQL_TYPE_FLOAT,
		mysql.MYSQL_TYPE_DOUBLE,
		mysql.MYSQL_TYPE_INT24,
		mysql.MYSQL_TYPE_STRING, // ENUM
		mysql.MYSQL_TYPE_BIT,
	}
	meta := make([]uint16, len(types))
	meta[9] = uint16(mysql.MYSQL_TYPE_ENUM) << 8
	table := &replication.TableMapEvent{
		ColumnCount:      uint64(len(types)),
		ColumnType:       types,
		ColumnMeta:       meta,
		SignednessBitmap: []byte{0xAA}, // 1,0,1,0,1,0,1,0 over the eight numeric columns
	}
	got := unsignedMap(table)
	want := table.UnsignedMap()
	if len(got) != len(want) {
		t.Fatalf("len %d != upstream %d\nlocal %#v\nupstream %#v", len(got), len(want), got, want)
	}
	for i, bit := range want {
		if got[i] != bit {
			t.Fatalf("col %d local %v upstream %v", i, got[i], bit)
		}
	}
}

func TestYearBeforeUnsignedIntegers(t *testing.T) {
	// QA layout from #165: YEAR, then signed and unsigned numerics.
	// Bitmap is hand-packed MSB-first, one bit per has_signedess_information_type
	// column. YEAR's own bit is 0, the way MySQL 8.0 writes it.
	//
	//	id INT signed, y YEAR, n DECIMAL(6,1) signed, si INT signed,
	//	ui BIGINT UNSIGNED, sb BIGINT signed, tu TINYINT UNSIGNED, ss SMALLINT signed
	//
	// bits: 0 0 0 0 1 0 1 0 = 0x0A
	types := []byte{
		mysql.MYSQL_TYPE_LONG,
		mysql.MYSQL_TYPE_YEAR,
		mysql.MYSQL_TYPE_NEWDECIMAL,
		mysql.MYSQL_TYPE_LONG,
		mysql.MYSQL_TYPE_LONGLONG,
		mysql.MYSQL_TYPE_LONGLONG,
		mysql.MYSQL_TYPE_TINY,
		mysql.MYSQL_TYPE_SHORT,
	}
	names := [][]byte{
		[]byte("id"), []byte("y"), []byte("n"), []byte("si"),
		[]byte("ui"), []byte("sb"), []byte("tu"), []byte("ss"),
	}
	table := yearTable(types, names, []uint64{0}, []byte{0x0A})
	row := []any{
		int32(1),
		2026,
		"-12.5",
		int32(-5),
		int64(-1), // 18446744073709551615
		int64(math.MinInt64),
		asInt8(255),
		int16(-32768),
	}
	deleted := captureFlashbackRows(&replication.RowsEvent{
		ColumnCount: uint64(len(types)),
		Table:       table,
		Rows:        [][]any{row},
	}, kindDeleteRows, "p164", "sg")
	if len(deleted) != 1 || deleted[0].ProblemKind != "" {
		t.Fatalf("sg: %+v", deleted)
	}
	got := deleted[0].Before
	want := []string{"1", "2026", "-12.5", "-5", "18446744073709551615", "-9223372036854775808", "255", "-32768"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("sg col %s = %s, want %s\nall %#v", names[i], got[i], want[i], got)
		}
	}
	if !deleted[0].Cols[4].HasSign || !deleted[0].Cols[4].Unsigned {
		t.Fatalf("ui signedness: %+v", deleted[0].Cols[4])
	}
	if !deleted[0].Cols[1].HasSign || deleted[0].Cols[1].Unsigned {
		t.Fatalf("year bit: %+v", deleted[0].Cols[1])
	}
	if deleted[0].Cols[5].Unsigned || !deleted[0].Cols[6].Unsigned || deleted[0].Cols[7].Unsigned {
		t.Fatalf("shifted cols: sb %+v tu %+v ss %+v", deleted[0].Cols[5], deleted[0].Cols[6], deleted[0].Cols[7])
	}

	images, omitted := captureRowImages(&replication.RowsEvent{
		ColumnCount: uint64(len(types)),
		Table:       table,
		Rows:        [][]any{row},
	}, kindDeleteRows, "p164", "sg")
	if omitted != 0 || len(images) != 1 {
		t.Fatalf("images omitted=%d %#v", omitted, images)
	}
	before := images[0].Before
	for i, text := range want {
		if before[i].Text != text {
			t.Fatalf("show-rows %s = %s, want %s", names[i], before[i].Text, text)
		}
	}
}

func TestYearPrimaryKeyUnsignedAndTwoYears(t *testing.T) {
	// evpk2, plus a second YEAR so two YEAR bits are consumed before the INT.
	// bits: yr=0, yr2=0, region signed=0, id unsigned=1 = 0x10
	types := []byte{
		mysql.MYSQL_TYPE_YEAR,
		mysql.MYSQL_TYPE_YEAR,
		mysql.MYSQL_TYPE_SHORT,
		mysql.MYSQL_TYPE_LONG,
		mysql.MYSQL_TYPE_VARCHAR,
	}
	names := [][]byte{[]byte("yr"), []byte("yr2"), []byte("region"), []byte("id"), []byte("note")}
	table := yearTable(types, names, []uint64{0, 1, 2, 3}, []byte{0x10})
	original := []any{2026, 1999, int16(1), asInt32(4000000000), "original"}
	junk := []any{2026, 1999, int16(1), asInt32(3000000000), "junk"}
	updated := []any{2026, 1999, int16(1), asInt32(4000000000), "oops"}

	upd := captureFlashbackRows(&replication.RowsEvent{
		ColumnCount: uint64(len(types)),
		Table:       table,
		Rows:        [][]any{original, updated},
	}, kindUpdateRows, "p164", "evpk2")
	if upd[0].ProblemKind != "" {
		t.Fatalf("update: %+v", upd[0])
	}
	if upd[0].Before[3] != "4000000000" || upd[0].After[3] != "4000000000" || upd[0].Before[4] != "'original'" || upd[0].After[4] != "'oops'" {
		t.Fatalf("update literals before %#v after %#v", upd[0].Before, upd[0].After)
	}
	if !upd[0].Cols[3].Unsigned {
		t.Fatalf("id should be unsigned in the schema check: %+v", upd[0].Cols[3])
	}

	ins := captureFlashbackRows(&replication.RowsEvent{
		ColumnCount: uint64(len(types)),
		Table:       table,
		Rows:        [][]any{junk},
	}, kindWriteRows, "p164", "evpk2")
	if ins[0].ProblemKind != "" || ins[0].After[3] != "3000000000" {
		t.Fatalf("insert: %+v after %#v", ins[0], ins[0].After)
	}

	keys := primaryKeyValues(&replication.RowsEvent{
		ColumnCount: uint64(len(types)),
		Table:       table,
		Rows:        [][]any{original},
	}, kindDeleteRows, pkMetaFrom(table))
	if len(keys) != 1 || keys[0] != "yr=2026, yr2=1999, region=1, id=4000000000" {
		t.Fatalf("pk: %#v", keys)
	}
}

func TestSignednessBitsForFloatDoubleDecimalAndMediumint(t *testing.T) {
	// YEAR, FLOAT, DOUBLE, old DECIMAL, MEDIUMINT UNSIGNED, INT UNSIGNED, BIGINT UNSIGNED.
	// FLOAT and DOUBLE are refused by flashback, but they still consume a bit.
	// bits: 0,0,0,0,1,1,1 = 0x0E
	types := []byte{
		mysql.MYSQL_TYPE_YEAR,
		mysql.MYSQL_TYPE_FLOAT,
		mysql.MYSQL_TYPE_DOUBLE,
		mysql.MYSQL_TYPE_DECIMAL,
		mysql.MYSQL_TYPE_INT24,
		mysql.MYSQL_TYPE_LONG,
		mysql.MYSQL_TYPE_LONGLONG,
	}
	names := [][]byte{
		[]byte("y"), []byte("f"), []byte("d"), []byte("old"),
		[]byte("mi"), []byte("id"), []byte("ui"),
	}
	table := yearTable(types, names, []uint64{5}, []byte{0x0E})
	got := unsignedMap(table)
	wantBits := map[int]bool{0: false, 1: false, 2: false, 3: false, 4: true, 5: true, 6: true}
	for i, bit := range wantBits {
		if got[i] != bit {
			t.Fatalf("col %d = %v, want %v\nmap %#v", i, got[i], bit, got)
		}
	}
	// go-mysql v1.14 skips YEAR and MYSQL_TYPE_DECIMAL, so this map must not
	// be that walk. The assertion is the expected bits above.

	row := []any{
		2026,
		float32(1.25),
		float64(2.5),
		"1",
		int32(1000000),
		asInt32(4000000000),
		int64(-1),
	}
	images, _ := captureRowImages(&replication.RowsEvent{
		ColumnCount: uint64(len(types)),
		Table:       table,
		Rows:        [][]any{row},
	}, kindWriteRows, "p164", "wide")
	if images[0].After[4].Text != "1000000" || images[0].After[5].Text != "4000000000" || images[0].After[6].Text != "18446744073709551615" {
		t.Fatalf("show-rows: %#v", cells(images[0].After))
	}
	refused := captureFlashbackRows(&replication.RowsEvent{
		ColumnCount: uint64(len(types)),
		Table:       table,
		Rows:        [][]any{row},
	}, kindWriteRows, "p164", "wide")
	if refused[0].ProblemKind != model.FlashProblemType || refused[0].ProblemType != "FLOAT" {
		t.Fatalf("float still refused: %+v", refused[0])
	}
}

func TestYearSignednessInsideTransactionPayload(t *testing.T) {
	types := []byte{
		mysql.MYSQL_TYPE_YEAR,
		mysql.MYSQL_TYPE_YEAR,
		mysql.MYSQL_TYPE_SHORT,
		mysql.MYSQL_TYPE_LONG,
		mysql.MYSQL_TYPE_LONGLONG,
		mysql.MYSQL_TYPE_TINY,
		mysql.MYSQL_TYPE_INT24,
		mysql.MYSQL_TYPE_NEWDECIMAL,
		mysql.MYSQL_TYPE_VARCHAR,
	}
	names := [][]byte{
		[]byte("yr"), []byte("yr2"), []byte("region"), []byte("id"),
		[]byte("ui"), []byte("tu"), []byte("mi"), []byte("n"), []byte("note"),
	}
	// yr=0 yr2=0 region=0 id=1 ui=1 tu=1 mi=1 n=0
	// 0001 1110 = 0x1E
	table := yearTable(types, names, []uint64{0, 1, 2, 3}, []byte{0x1E})
	table.TableID = 9
	row := []any{
		2026,
		1999,
		int16(1),
		asInt32(4000000000),
		int64(-1),
		asInt8(255),
		int32(1000000),
		"-12.5",
		"original",
	}
	ev := &replication.BinlogEvent{
		Header: &replication.EventHeader{
			Timestamp: 1,
			EventType: replication.TRANSACTION_PAYLOAD_EVENT,
			EventSize: 100,
			LogPos:    200,
		},
		Event: &replication.TransactionPayloadEvent{
			Events: []*replication.BinlogEvent{
				{
					Header: &replication.EventHeader{EventType: replication.TABLE_MAP_EVENT, EventSize: 30, LogPos: 40},
					Event:  table,
				},
				{
					Header: &replication.EventHeader{EventType: replication.DELETE_ROWS_EVENTv2, EventSize: 40, LogPos: 80},
					Event: &replication.RowsEvent{
						TableID:     9,
						Table:       table,
						ColumnCount: uint64(len(types)),
						Rows:        [][]any{row},
					},
				},
			},
		},
	}
	raws, ok := expandTransactionPayload(ev, "mysql-bin.000001", "8.0.46", map[uint64]cachedTableName{}, true, true)
	if !ok || len(raws) != 2 {
		t.Fatalf("payload: ok=%v n=%d", ok, len(raws))
	}
	rows := raws[1]
	if rows.EventType != kindDeleteRows || len(rows.FlashRows) != 1 || rows.FlashRows[0].ProblemKind != "" {
		t.Fatalf("flash: type %s %+v", rows.EventType, rows.FlashRows)
	}
	got := rows.FlashRows[0].Before
	if got[3] != "4000000000" || got[4] != "18446744073709551615" || got[5] != "255" || got[6] != "1000000" || got[7] != "-12.5" {
		t.Fatalf("payload literals %#v", got)
	}
	if len(rows.RowImages) != 1 || rows.RowImages[0].Before[3].Text != "4000000000" || rows.RowImages[0].Before[4].Text != "18446744073709551615" {
		t.Fatalf("payload show-rows %#v", rows.RowImages)
	}
	if len(rows.RowKeys) != 1 || rows.RowKeys[0] != "yr=2026, yr2=1999, region=1, id=4000000000" {
		t.Fatalf("payload pk %#v", rows.RowKeys)
	}
}

func TestUnsignedMapNilWithoutBitmap(t *testing.T) {
	table := &replication.TableMapEvent{
		ColumnCount: 1,
		ColumnType:  []byte{mysql.MYSQL_TYPE_LONG},
	}
	if unsignedMap(table) != nil || unsignedMap(nil) != nil {
		t.Fatal("missing bitmap should be nil")
	}
}

func yearTable(types []byte, names [][]byte, pk []uint64, bitmap []byte) *replication.TableMapEvent {
	meta := make([]uint16, len(types))
	for i, typ := range types {
		if typ == mysql.MYSQL_TYPE_NEWDECIMAL {
			meta[i] = (6 << 8) | 1
		}
	}
	return &replication.TableMapEvent{
		ColumnCount:      uint64(len(types)),
		ColumnType:       types,
		ColumnMeta:       meta,
		ColumnName:       names,
		PrimaryKey:       pk,
		SignednessBitmap: bitmap,
		DefaultCharset:   []uint64{255},
		Schema:           []byte("shop"),
		Table:            []byte("yearnum"),
	}
}

func asInt32(n uint32) int32 { return int32(n) }

func asInt8(n uint8) int8 { return int8(n) }

func cells(row []model.RowCell) []string {
	out := make([]string, len(row))
	for i, cell := range row {
		out[i] = cell.Text
	}
	return out
}
