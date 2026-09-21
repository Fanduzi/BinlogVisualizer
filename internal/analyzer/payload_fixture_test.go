// Package analyzer verifies transaction-payload ROW images through ParseFiles.
// input: the committed MySQL 8.0.36 compressed dialect fixture.
// output: assertions that expanded inner INSERT/UPDATE/DELETE ROW images become one report transaction whose file span and BinlogBytes use the wrapper's file-relative range once, and that position selectors include or omit those images with that span.
// pos: ParseFiles admission seam for #80 wrapper file-span accounting.
// note: if this file changes, update this header and module README.md.
package analyzer

import (
	"path/filepath"
	"testing"

	"binlogviz/internal/binlog"
	"binlogviz/internal/model"
)

// On-disk TRANSACTION_PAYLOAD header in testdata/mysql80_transaction_payload.binlog
// (LogPos 725, EventSize 216).
const (
	mysql80CompressedPayloadWrapperStart = 509
	mysql80CompressedPayloadWrapperEnd   = 725
	mysql80CompressedPayloadWrapperBytes = 216
)

func TestParseFilesCountsMySQL80CompressedPayloadRowImages(t *testing.T) {
	result := analyzeCompressedPayloadFixture(t, DefaultOptions())
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

func TestParseFilesCompressedPayloadTransactionUsesWrapperFileSpanOnce(t *testing.T) {
	result := analyzeCompressedPayloadFixture(t, DefaultOptions())
	if len(result.Transactions) != 1 {
		t.Fatalf("report transactions=%d, want 1", len(result.Transactions))
	}
	txn := result.Transactions[0]
	if txn.PositionStart != mysql80CompressedPayloadWrapperStart || txn.PositionEnd != mysql80CompressedPayloadWrapperEnd {
		t.Fatalf("transaction span [%d,%d), want wrapper [%d,%d) once, not uncompressed LogPos and not GTID-through-wrapper", txn.PositionStart, txn.PositionEnd, mysql80CompressedPayloadWrapperStart, mysql80CompressedPayloadWrapperEnd)
	}
	if txn.BinlogBytes != mysql80CompressedPayloadWrapperBytes {
		t.Fatalf("transaction BinlogBytes=%d, want wrapper size %d once (not times inner count)", txn.BinlogBytes, mysql80CompressedPayloadWrapperBytes)
	}
	if txn.FullReplaySpan != nil && (txn.FullReplaySpan.PositionStart != mysql80CompressedPayloadWrapperStart || txn.FullReplaySpan.PositionEnd != mysql80CompressedPayloadWrapperEnd || txn.FullReplaySpan.BinlogBytes != mysql80CompressedPayloadWrapperBytes) {
		t.Fatalf("FullReplaySpan=%+v, want wrapper [%d,%d) bytes=%d once", txn.FullReplaySpan, mysql80CompressedPayloadWrapperStart, mysql80CompressedPayloadWrapperEnd, mysql80CompressedPayloadWrapperBytes)
	}
}

func TestParseFilesPositionSelectorIncludesCompressedPayloadWrapperSpan(t *testing.T) {
	start := int64(mysql80CompressedPayloadWrapperStart)
	stop := int64(mysql80CompressedPayloadWrapperEnd)
	opts := DefaultOptions()
	opts.StartPosition = &start
	opts.StopPosition = &stop
	result := analyzeCompressedPayloadFixture(t, opts)
	if len(result.Transactions) != 1 {
		t.Fatalf("include wrapper span: transactions=%d, want 1", len(result.Transactions))
	}
	txn := result.Transactions[0]
	if txn.TotalRows != 3 || txn.Operations["INSERT"] != 1 || txn.Operations["UPDATE"] != 1 || txn.Operations["DELETE"] != 1 {
		t.Fatalf("include wrapper span: transaction = %+v, want INSERT/UPDATE/DELETE 1/1/1", txn)
	}
}

func TestParseFilesPositionSelectorExcludesCompressedPayloadWrapperSpan(t *testing.T) {
	stop := int64(mysql80CompressedPayloadWrapperStart)
	opts := DefaultOptions()
	opts.StopPosition = &stop
	result := analyzeCompressedPayloadFixture(t, opts)
	if len(result.Transactions) != 0 {
		t.Fatalf("exclude wrapper span: transactions=%d (%+v), want 0", len(result.Transactions), result.Transactions)
	}
}

func analyzeCompressedPayloadFixture(t *testing.T, opts Options) *model.AnalysisResult {
	t.Helper()
	path := filepath.Join("..", "binlog", "testdata", "mysql80_transaction_payload.binlog")
	a := New(opts)
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
	return result
}
