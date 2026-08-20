package analyzer

import (
	"path/filepath"
	"strings"
	"testing"

	"binlogviz/internal/binlog"
)

func TestOfficialFixtureRecordsQueryDDLAndRealTxnSpans(t *testing.T) {
	path := filepath.Join("..", "binlog", "testdata", "minimal.binlog")
	p := binlog.NewParser()
	a := New(DefaultOptions())

	if err := p.ParseFiles([]string{path}, func(raw binlog.RawEvent) error {
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

	if len(result.Diagnostics.DDLEvents) == 0 {
		t.Fatal("expected CREATE TABLE Query event on the official ROW fixture")
	}
	ddl := result.Diagnostics.DDLEvents[0]
	if ddl.Operation != "CREATE TABLE" {
		t.Fatalf("expected CREATE TABLE, got %q", ddl.Operation)
	}
	if ddl.Table != "users" {
		t.Fatalf("expected table users, got %q", ddl.Table)
	}

	if len(result.Transactions) == 0 {
		t.Fatal("expected transactions")
	}
	for _, txn := range result.Transactions {
		span := txn.PositionEnd - txn.PositionStart
		if txn.TotalRows > 0 && span <= 64 && txn.BinlogBytes <= 64 {
			t.Fatalf("txn %s still looks XID-only (bytes=%d start=%d end=%d rows=%d)",
				txn.TxnKey, txn.BinlogBytes, txn.PositionStart, txn.PositionEnd, txn.TotalRows)
		}
		if txn.BinlogBytes != span {
			t.Fatalf("txn %s binlog_bytes=%d want pos_end-pos_start=%d", txn.TxnKey, txn.BinlogBytes, span)
		}
	}

	if len(result.Timeseries.BinlogBytesSeries) == 0 || result.Timeseries.BinlogBytesSeries[0].Value == 0 {
		t.Fatalf("expected minute binlog_bytes_series to accumulate row-event lengths, got %+v", result.Timeseries.BinlogBytesSeries)
	}

	var tableDDL int
	for _, table := range result.Tables {
		if table.Schema == "testdb" && table.Table == "users" {
			tableDDL = table.DDLCount
		}
	}
	if tableDDL == 0 {
		t.Fatalf("expected testdb.users DDL Events > 0, tables=%+v", result.Tables)
	}

	if !strings.Contains(ddl.Statement, "CREATE TABLE") {
		t.Fatalf("unexpected statement %q", ddl.Statement)
	}
}
