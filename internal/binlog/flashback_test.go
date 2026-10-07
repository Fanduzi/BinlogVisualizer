package binlog

import (
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"binlogviz/internal/model"
)

func TestCaptureFlashbackDeleteInsertAndUnsigned(t *testing.T) {
	table := flashTable(t, []byte{mysql.MYSQL_TYPE_LONGLONG, mysql.MYSQL_TYPE_VARCHAR}, [][]byte{[]byte("id"), []byte("note")}, []uint64{0}, 0x80, 255)
	deleted := captureFlashbackRows(&replication.RowsEvent{
		ColumnCount: 2,
		Table:       table,
		Rows:        [][]any{{int64(-1), `it's "bad" \ 雪`}},
	}, kindDeleteRows, "shop", "wide")
	if len(deleted) != 1 || deleted[0].ProblemKind != "" {
		t.Fatalf("delete image: %+v", deleted)
	}
	if deleted[0].Before[0] != "18446744073709551615" || deleted[0].Before[1] != `'it\'s "bad" \\ 雪'` {
		t.Fatalf("delete literals: %#v", deleted[0].Before)
	}
	if len(deleted[0].PK) != 1 || deleted[0].PK[0] != 0 || deleted[0].NoPK {
		t.Fatalf("pk: %+v", deleted[0].PK)
	}

	signed := flashTable(t, []byte{mysql.MYSQL_TYPE_LONGLONG, mysql.MYSQL_TYPE_VARCHAR}, [][]byte{[]byte("id"), []byte("note")}, []uint64{0}, 0x00, 255)
	inserted := captureFlashbackRows(&replication.RowsEvent{
		ColumnCount: 2,
		Table:       signed,
		Rows:        [][]any{{int64(-9223372036854775808), "x"}},
	}, kindWriteRows, "shop", "wide")
	if inserted[0].ProblemKind != "" || inserted[0].After[0] != "-9223372036854775808" {
		t.Fatalf("signed min: %+v", inserted[0])
	}
}

func TestCaptureFlashbackRefusesNamesImageAndFloat(t *testing.T) {
	unnamed := &replication.TableMapEvent{
		ColumnCount: 1,
		ColumnType:  []byte{mysql.MYSQL_TYPE_LONG},
	}
	rows := captureFlashbackRows(&replication.RowsEvent{
		ColumnCount: 1,
		Table:       unnamed,
		Rows:        [][]any{{int32(1)}},
	}, kindWriteRows, "shop", "orders")
	if len(rows) != 1 || rows[0].ProblemKind != model.FlashProblemNames {
		t.Fatalf("names: %+v", rows)
	}

	table := flashTable(t, []byte{mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_VARCHAR}, [][]byte{[]byte("id"), []byte("note")}, []uint64{0}, 0x00, 255)
	partial := captureFlashbackRows(&replication.RowsEvent{
		ColumnCount:    2,
		Table:          table,
		Rows:           [][]any{{int32(1), "a"}, {int32(1), "b"}},
		SkippedColumns: [][]int{nil, {1}},
	}, kindUpdateRows, "shop", "orders")
	if partial[0].ProblemKind != model.FlashProblemImage {
		t.Fatalf("image: %+v", partial)
	}

	floated := flashTable(t, []byte{mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_FLOAT}, [][]byte{[]byte("id"), []byte("ratio")}, []uint64{0}, 0x00, 255)
	bad := captureFlashbackRows(&replication.RowsEvent{
		ColumnCount: 2,
		Table:       floated,
		Rows:        [][]any{{int32(1), float32(1.5)}},
	}, kindWriteRows, "shop", "orders")
	if bad[0].ProblemKind != model.FlashProblemType || bad[0].ProblemColumn != "ratio" || bad[0].ProblemType != "FLOAT" {
		t.Fatalf("float: %+v", bad[0])
	}
}

func TestCaptureFlashbackEnumSetBlobAndNoPK(t *testing.T) {
	table := &replication.TableMapEvent{
		ColumnCount:      4,
		ColumnType:       []byte{mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_ENUM, mysql.MYSQL_TYPE_SET, mysql.MYSQL_TYPE_BLOB},
		ColumnName:       [][]byte{[]byte("id"), []byte("color"), []byte("flags"), []byte("raw")},
		PrimaryKey:       []uint64{0},
		EnumStrValue:     [][][]byte{{[]byte("red"), []byte("blue")}},
		SetStrValue:      [][][]byte{{[]byte("a"), []byte("b"), []byte("c")}},
		SignednessBitmap: []byte{0x00},
		DefaultCharset:   []uint64{63},
	}
	rows := captureFlashbackRows(&replication.RowsEvent{
		ColumnCount: 4,
		Table:       table,
		Rows:        [][]any{{int32(7), int64(2), int64(5), []byte{0xde, 0xad, 0xff}}},
	}, kindDeleteRows, "shop", "wide")
	if rows[0].ProblemKind != "" {
		t.Fatalf("problem: %+v", rows[0])
	}
	if rows[0].NoPK {
		t.Fatal("id is a primary key")
	}
	got := rows[0].Before
	if got[1] != "'blue'" || got[2] != "'a,c'" || got[3] != "X'DEADFF'" {
		t.Fatalf("literals: %#v", got)
	}

	heap := flashTable(t, []byte{mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_VARCHAR}, [][]byte{[]byte("id"), []byte("note")}, nil, 0x00, 255)
	heapRows := captureFlashbackRows(&replication.RowsEvent{
		ColumnCount: 2,
		Table:       heap,
		Rows:        [][]any{{nil, nil}},
	}, kindWriteRows, "shop", "heap")
	if !heapRows[0].NoPK || heapRows[0].After[0] != "NULL" || heapRows[0].After[1] != "NULL" {
		t.Fatalf("heap: %+v", heapRows[0])
	}
}

func flashTable(t *testing.T, types []byte, names [][]byte, pk []uint64, sign byte, collation uint64) *replication.TableMapEvent {
	t.Helper()
	return &replication.TableMapEvent{
		ColumnCount:      uint64(len(types)),
		ColumnType:       types,
		ColumnName:       names,
		PrimaryKey:       pk,
		SignednessBitmap: []byte{sign},
		DefaultCharset:   []uint64{collation},
	}
}
