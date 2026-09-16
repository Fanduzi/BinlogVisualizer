// Package analyzer verifies transaction-payload ROW images through ParseFiles.
// input: the committed MySQL 8.0.36 compressed dialect fixture.
// output: assertions that expanded inner INSERT/UPDATE/DELETE ROW images become one report transaction and the CREATE TABLE DDL group stays off that list.
// pos: ParseFiles admission seam for #75 transaction-payload completeness.
// note: if this file changes, update this header and module README.md.
package analyzer

import (
	"path/filepath"
	"testing"

	"binlogviz/internal/binlog"
)

func TestParseFilesCountsMySQL80CompressedPayloadRowImages(t *testing.T) {
	path := filepath.Join("..", "binlog", "testdata", "mysql80_transaction_payload.binlog")
	a := New(DefaultOptions())
	if err := binlog.NewParser().ParseFiles([]string{path}, func(raw binlog.RawEvent) error {
		ev, err := binlog.NormalizeRawEvent(raw)
		if err != nil {
			return err
		}
		if ev == nil {
			return nil
		}
		return a.Consume(*ev)
	}); err != nil {
		t.Fatal(err)
	}
	result, err := a.Finalize()
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Transactions) != 1 {
		t.Fatalf("report transactions=%d, want 1 compressed DML group (DDL-only CREATE must not be retained)", len(result.Transactions))
	}
	txn := result.Transactions[0]
	if txn.TotalRows != 3 || txn.Operations["INSERT"] != 1 || txn.Operations["UPDATE"] != 1 || txn.Operations["DELETE"] != 1 {
		t.Fatalf("compressed payload transaction = %+v, want INSERT/UPDATE/DELETE 1/1/1", txn)
	}
	if txn.GTID == "" {
		t.Fatalf("compressed payload group lost its selectable GTID: %+v", txn)
	}
	if len(result.Diagnostics.DDLEvents) != 1 || result.Diagnostics.DDLEvents[0].Operation != "CREATE TABLE" {
		t.Fatalf("DDL timeline = %+v, want the CREATE TABLE group only", result.Diagnostics.DDLEvents)
	}
}
