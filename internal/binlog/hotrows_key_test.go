package binlog

import (
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"binlogviz/internal/mysqlexpr"
)

// fakeKeyResolver answers from one definition, like the catalog does.
type fakeKeyResolver struct {
	cols []mysqlexpr.SchemaCol
	pk   []int
	ok   bool
}

func (f fakeKeyResolver) FlashColumns(string, string) ([]mysqlexpr.SchemaCol, []int, bool) {
	return f.cols, f.pk, f.ok
}

// A table map without a key leaves RowKeys empty; a definition supplies it
// (ADR 0032), and the binlog's own key always wins.
func TestRowKeysFallBackToDefinition(t *testing.T) {
	table := flashTable(t, []byte{mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_VARCHAR}, nil, nil, 0x00, 0)
	table.Schema = []byte("shop")
	table.Table = []byte("t")
	ev := &replication.RowsEvent{ColumnCount: 2, Table: table, Rows: [][]any{{int64(7), "x"}}}

	if keys, from := rowKeys(nil, nil, ev, kindDeleteRows, nil); keys != nil || from {
		t.Fatalf("without a definition the keys stay unknown: %v %v", keys, from)
	}

	cols, ok := mysqlexpr.ParseSchemaColumns("`id` int NOT NULL, `s` varchar(8), PRIMARY KEY (`id`)")
	if !ok {
		t.Fatal("parse the definition")
	}
	resolver := fakeKeyResolver{cols: cols, pk: []int{0}, ok: true}
	keys, from := rowKeys(resolver, nil, ev, kindDeleteRows, nil)
	if !from || len(keys) != 1 || keys[0] != "id=7" {
		t.Fatalf("the definition key must become the identity: %v from=%v", keys, from)
	}

	// The binlog's own key wins: the same event with FULL metadata.
	full := flashTable(t, []byte{mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_VARCHAR}, [][]byte{[]byte("id"), []byte("s")}, []uint64{0}, 0x00, 255)
	full.Schema = []byte("shop")
	full.Table = []byte("t")
	fullEv := &replication.RowsEvent{ColumnCount: 2, Table: full, Rows: [][]any{{int64(7), "x"}}}
	keys, from = rowKeys(resolver, nil, fullEv, kindDeleteRows, nil)
	if from || len(keys) != 1 || keys[0] != "id=7" {
		t.Fatalf("binlog metadata must win: %v from=%v", keys, from)
	}

	// A definition that does not describe this row is not used.
	wide, _ := mysqlexpr.ParseSchemaColumns("`a` int, `b` int, `c` int, PRIMARY KEY (`a`)")
	if keys, from := rowKeys(fakeKeyResolver{cols: wide, pk: []int{0}, ok: true}, nil, ev, kindDeleteRows, nil); keys != nil || from {
		t.Fatalf("a definition of the wrong width must not supply keys: %v", keys)
	}
}
