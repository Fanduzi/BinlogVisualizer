package binlog

import (
	"encoding/binary"
	"encoding/hex"
	"math"
	"strings"
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

func TestSQLJSONExactAndRefused(t *testing.T) {
	cases := []struct {
		hex  string
		want string
	}{
		{"0001000c000b0001000501006e", "JSON_OBJECT('n', 1)"},
		{"0001000c000b0001000601006e", "JSON_OBJECT('n', CAST(1 AS UNSIGNED))"},
		{"00010012000b0001000f0c0064f60403028963", "JSON_OBJECT('d', 9.99)"},
		{"00010015000b0001000f0c0064f6070b028000000963", "JSON_OBJECT('d', CAST('9.99' AS DECIMAL(11,2)))"},
		{"00010014000b0001000b0c00640000000000000080", "JSON_OBJECT('d', -0.0E0)"},
		{"00010014000b0001000b0c00640000000000002440", "JSON_OBJECT('d', 1e+01)"},
		{"00010016000b0001000f0c00740c0820a107053144a519", "JSON_OBJECT('t', CAST('2020-01-02 03:04:05.500000' AS DATETIME(6)))"},
		{"0400", "CAST('null' AS JSON)"},
		{"050100", "CAST(1 AS JSON)"},
		{"0b0000000000000080", "CAST(-0.0E0 AS JSON)"},
	}
	for _, tc := range cases {
		doc, err := hex.DecodeString(tc.hex)
		if err != nil {
			t.Fatal(err)
		}
		got, ok := sqlJSONBinary(doc)
		if !ok || got != tc.want {
			t.Fatalf("%s: ok=%v got %s", tc.hex, ok, got)
		}
	}

	if _, ok := sqlJSONBinary([]byte{0x07, 0x01, 0x00, 0x00, 0x00}); ok {
		t.Fatal("non-canonical int width was accepted")
	}
	nan := make([]byte, 9)
	nan[0] = jsonDouble
	binary.LittleEndian.PutUint64(nan[1:], math.Float64bits(math.NaN()))
	if _, ok := sqlJSONBinary(nan); ok {
		t.Fatal("NaN was accepted")
	}
	if _, ok := sqlJSONBinary([]byte{jsonOpaque, 0x07, 0x01, 0x00}); ok {
		t.Fatal("opaque timestamp was accepted")
	}
	if _, ok := sqlJSON("{}", jsonCell{}); ok {
		t.Fatal("text JSON was accepted")
	}
	table := &replication.TableMapEvent{ColumnType: []byte{mysql.MYSQL_TYPE_JSON}}
	if _, problem := flashLiteral(table, 0, &replication.JsonDiff{}, jsonCell{}, nil, nil, nil, nil); problem != "partial JSON" {
		t.Fatalf("partial: %q", problem)
	}
}

func TestSQLCharacterCharsets(t *testing.T) {
	lit, problem := sqlCharacter(mysql.MYSQL_TYPE_VARCHAR, 0, []byte{0xE9}, map[int]uint64{0: 8})
	if problem != "" || lit != "_latin1 0xE9" {
		t.Fatalf("latin1 e9: %s %s", lit, problem)
	}
	lit, problem = sqlCharacter(mysql.MYSQL_TYPE_VARCHAR, 0, []byte{0xC3, 0xA9}, map[int]uint64{0: 8})
	if problem != "" || lit != "_latin1 0xC3A9" {
		t.Fatalf("latin1 c3a9: %s %s", lit, problem)
	}
	lit, problem = sqlCharacter(mysql.MYSQL_TYPE_VARCHAR, 0, []byte{0x00, 0x41, 0x00, 0x42}, map[int]uint64{0: 54})
	if problem != "" || lit != "_utf16 0x00410042" {
		t.Fatalf("utf16: %s %s", lit, problem)
	}
	lit, problem = sqlCharacter(mysql.MYSQL_TYPE_VARCHAR, 0, []byte{}, map[int]uint64{0: 8})
	if problem != "" || lit != "_latin1 X''" {
		t.Fatalf("empty: %s %s", lit, problem)
	}
	lit, problem = sqlCharacter(mysql.MYSQL_TYPE_VARCHAR, 0, []byte("雪"), map[int]uint64{0: 255})
	if problem != "" || lit != "'雪'" {
		t.Fatalf("utf8mb4: %s %s", lit, problem)
	}
	lit, problem = sqlCharacter(mysql.MYSQL_TYPE_BLOB, 0, []byte{0xE9}, map[int]uint64{0: 63})
	if problem != "" || lit != "X'E9'" {
		t.Fatalf("binary: %s %s", lit, problem)
	}
	_, problem = sqlCharacter(mysql.MYSQL_TYPE_VARCHAR, 0, []byte{0x61}, map[int]uint64{0: 99999})
	if !strings.Contains(problem, "charset for collation 99999 is unknown") {
		t.Fatalf("unknown: %s", problem)
	}
	_, problem = sqlCharacter(mysql.MYSQL_TYPE_VARCHAR, 0, []byte{0xE9}, map[int]uint64{0: 255})
	if problem != "VARCHAR" {
		t.Fatalf("invalid utf8: %s", problem)
	}
}

func TestFlashbackFixtureJSONAndCharsetsRoundTripLiterals(t *testing.T) {
	parser := NewParser()
	parser.(FlashbackParser).SetCaptureFlashback(true)
	var literals []string
	var problems []model.FlashRow
	err := parser.ParseFiles([]string{"testdata/mysql-8.0.46-flashback-full.binlog"}, func(ev RawEvent) error {
		for _, row := range ev.FlashRows {
			if row.ProblemKind != "" {
				problems = append(problems, row)
				continue
			}
			literals = append(literals, row.Before...)
			literals = append(literals, row.After...)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(problems) != 0 {
		t.Fatalf("problems: %+v", problems)
	}
	joined := strings.Join(literals, "\n")
	for _, want := range []string{
		"JSON_OBJECT('n', 1, 'ok', true, 'sku', 'Z')",
		"JSON_OBJECT('sku', 'B')",
		"JSON_ARRAY(1, 2)",
		"JSON_OBJECT('n', 10)",
		`JSON_OBJECT('s', 'a\\b')`,
		`'it\'s "bad" \\ 雪'`,
		"X'DEADBEEFFF00'",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("missing %s\n%s", want, joined)
		}
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
