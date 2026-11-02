package binlog

import (
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"binlogviz/internal/model"
	"binlogviz/internal/schemacatalog"
)

// A MINIMAL table map carries no column names; a schema definition supplies
// them (ADR 0030). Without one the row stays unusable, exactly as before.
func TestCaptureFlashbackRowsSchemaDefinitionNamesMinimalColumns(t *testing.T) {
	table := flashTable(t,
		[]byte{mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_VARCHAR},
		nil, nil, 0x00, 0)
	event := &replication.RowsEvent{
		ColumnCount: 2,
		Table:       table,
		Rows:        [][]any{{int64(7), "paid"}},
	}

	before := captureFlashbackRows(event, kindDeleteRows, "shop", "orders", nil)
	if len(before) != 1 || before[0].ProblemKind != model.FlashProblemNames {
		t.Fatalf("no definition must keep the row_metadata refusal, got %+v", before)
	}

	catalog := schemacatalog.New("")
	catalog.LoadFile("CREATE TABLE shop.orders (id INT NOT NULL, status VARCHAR(16), PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4;")
	rows := captureFlashbackRows(event, kindDeleteRows, "shop", "orders", catalog)
	if len(rows) != 1 {
		t.Fatalf("rows: %+v", rows)
	}
	row := rows[0]
	if row.ProblemKind != "" {
		t.Fatalf("a definition must resolve the row, got %s: %s", row.ProblemKind, row.ProblemType)
	}
	if len(row.Columns) != 2 || row.Columns[0] != "id" || row.Columns[1] != "status" {
		t.Fatalf("columns must come from the definition, got %#v", row.Columns)
	}
	if len(row.Before) != 2 || row.Before[0] != "7" || row.Before[1] != "'paid'" {
		t.Fatalf("literals: %#v", row.Before)
	}
	// The definition declares PRIMARY KEY (id), so undo addresses the row by it.
	if row.NoPK || len(row.PK) != 1 || row.PK[0] != 0 {
		t.Fatalf("the definition's key must become the undo address, got PK=%v NoPK=%v", row.PK, row.NoPK)
	}
}

// A definition that disagrees on column count cannot name the row: it is
// unresolved, not stretched.
func TestCaptureFlashbackRowsRejectsMismatchedDefinitionWidth(t *testing.T) {
	table := flashTable(t,
		[]byte{mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_VARCHAR},
		nil, nil, 0x00, 0)
	catalog := schemacatalog.New("")
	catalog.LoadFile("CREATE TABLE shop.orders (id INT NOT NULL, status VARCHAR(16), note VARCHAR(8)) DEFAULT CHARSET=utf8mb4;")

	rows := captureFlashbackRows(&replication.RowsEvent{
		ColumnCount: 2,
		Table:       table,
		Rows:        [][]any{{int64(7), "paid"}},
	}, kindDeleteRows, "shop", "orders", catalog)
	if len(rows) != 1 || rows[0].ProblemKind != model.FlashProblemNames {
		t.Fatalf("a wider definition must not be stretched, got %+v", rows)
	}
}

// --show-rows prints the same labels the undo uses: a definition names the
// columns when the table map has none, and the image records where they came
// from so the report can say so (ADR 0030).
func TestCaptureRowImagesUsesDefinitionNames(t *testing.T) {
	table := flashTable(t, []byte{mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_VARCHAR}, nil, nil, 0x00, 0)
	event := &replication.RowsEvent{ColumnCount: 2, Table: table, Rows: [][]any{{int64(7), "paid"}}}

	plain, _ := captureRowImages(event, kindDeleteRows, "shop", "orders", nil)
	if len(plain) != 1 || plain[0].Columns[0] != "@1" || plain[0].NamesFrom != "" {
		t.Fatalf("without a definition the labels stay positional: %+v", plain)
	}

	catalog := schemacatalog.New("")
	catalog.LoadFile("CREATE TABLE shop.orders (id INT NOT NULL, status VARCHAR(16), PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4;")
	named, _ := captureRowImages(event, kindDeleteRows, "shop", "orders", catalog)
	if len(named) != 1 {
		t.Fatalf("images: %+v", named)
	}
	if named[0].Columns[0] != "id" || named[0].Columns[1] != "status" {
		t.Fatalf("columns must come from the definition: %#v", named[0].Columns)
	}
	if named[0].NamesFrom != model.RowNamesFromSchema {
		t.Fatalf("the image must record where the names came from, got %q", named[0].NamesFrom)
	}
}
