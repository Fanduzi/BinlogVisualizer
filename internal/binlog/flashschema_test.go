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
	// Key capture is the next step of the goal; until it lands the undo takes
	// its documented no-key path, so this asserts the current contract only.
	if !row.NoPK || len(row.PK) != 0 {
		t.Fatalf("key capture is not wired yet, want the no-key path, got PK=%v NoPK=%v", row.PK, row.NoPK)
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
