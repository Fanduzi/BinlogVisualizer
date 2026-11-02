package schemacatalog

import (
	"reflect"
	"strings"
	"testing"

	"binlogviz/internal/mysqlexpr"
)

// FlashColumns is what the undo capture asks when the binlog has no names
// (ADR 0030). It must also report the definition's primary key, because that
// is what addresses the row being reversed.
func colNames(cols []mysqlexpr.SchemaCol) []string {
	names := make([]string, len(cols))
	for i := range cols {
		names[i] = cols[i].Name
	}
	return names
}

func TestFlashColumnsReportsPrimaryKey(t *testing.T) {
	cases := []struct {
		name string
		ddl  string
		want []int
	}{
		{"table-level single key", "CREATE TABLE shop.t (id INT NOT NULL, note VARCHAR(8), PRIMARY KEY (id))", []int{0}},
		{"inline key", "CREATE TABLE shop.t (id INT PRIMARY KEY, note VARCHAR(8))", []int{0}},
		{"composite key", "CREATE TABLE shop.t (a INT, b INT, note VARCHAR(8), PRIMARY KEY (a, b))", []int{0, 1}},
		{"key not the first column", "CREATE TABLE shop.t (note VARCHAR(8), id INT, PRIMARY KEY (id))", []int{1}},
		{"no key", "CREATE TABLE shop.t (id INT, note VARCHAR(8))", nil},
		{"quoted key column", "CREATE TABLE shop.t (`id` INT, `note` VARCHAR(8), PRIMARY KEY (`id`))", []int{0}},
		{"named constraint", "CREATE TABLE shop.t (id INT, PRIMARY KEY pk (id))", []int{0}},
		{"constraint prefix", "CREATE TABLE shop.t (id INT, CONSTRAINT pk PRIMARY KEY (id))", []int{0}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := New("")
			c.LoadFile(tc.ddl + " DEFAULT CHARSET=utf8mb4;")
			cols, pk, ok := c.FlashColumns("shop", "t")
			if !ok || len(cols) == 0 {
				t.Fatalf("definition must resolve, got ok=%v cols=%d", ok, len(cols))
			}
			if !reflect.DeepEqual(pk, tc.want) {
				t.Fatalf("pk = %v, want %v (cols=%v)", pk, tc.want, colNames(cols))
			}
		})
	}
}

// An ALTER that changes the key is not modelled: the columns stay usable, but
// the key is dropped rather than kept stale, so undo degrades to matching every
// column instead of addressing a row by a key the definition no longer owns.
func TestFlashColumnsDropsKeyAfterUnmodelledKeyAlter(t *testing.T) {
	c := New("")
	c.LoadFile("CREATE TABLE shop.t (id INT, b INT, PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4;")
	if cols, pk, ok := c.FlashColumns("shop", "t"); !ok || len(pk) != 1 || len(cols) != 2 {
		t.Fatalf("precondition: want the declared key, ok=%v pk=%v", ok, pk)
	}

	c.ObserveDDL("shop", "ALTER TABLE t DROP PRIMARY KEY")
	cols, pk, ok := c.FlashColumns("shop", "t")
	if !ok || len(cols) != 2 {
		t.Fatalf("the columns stay usable after a key-only ALTER, ok=%v cols=%d", ok, len(cols))
	}
	if len(pk) != 0 {
		t.Fatalf("the key must be dropped, got pk=%v", pk)
	}
	if got := warningText(c.Warnings()); !strings.Contains(got, "PRIMARY KEY was changed") {
		t.Fatalf("the drop must be announced, warnings=%q", got)
	}
}

// A key addition is the same rule in the other direction.
func TestFlashColumnsDropsKeyAfterAddedKeyAlter(t *testing.T) {
	c := New("")
	c.LoadFile("CREATE TABLE shop.t (id INT, b INT, PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4;")
	c.ObserveDDL("shop", "ALTER TABLE t ADD PRIMARY KEY (b)")
	if _, pk, ok := c.FlashColumns("shop", "t"); !ok || len(pk) != 0 {
		t.Fatalf("a changed key must not stay addressable, ok=%v pk=%v", ok, pk)
	}
}
