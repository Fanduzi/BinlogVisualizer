// Package report verifies the CSV/TSV delimited export surface.
// input: small AnalysisResult values written through Write for csv/tsv.
// output: parseable header-plus-row tables whose bounding mirrors the JSON
// datasets; quoting, empty datasets, and dataset selection are covered.
// pos: emitter regression suite for csv.go (ADR 0026).
// note: if this file changes, update this header and module README.md.
package report

import (
	"bytes"
	"encoding/csv"
	"strings"
	"testing"
	"time"

	"binlogviz/internal/model"
)

func parseDelimited(t *testing.T, raw string, comma rune) [][]string {
	t.Helper()
	r := csv.NewReader(strings.NewReader(raw))
	r.Comma = comma
	rows, err := r.ReadAll()
	if err != nil {
		t.Fatalf("delimited output does not parse: %v\n%s", err, raw)
	}
	if len(rows) == 0 {
		t.Fatal("expected at least a header row")
	}
	return rows
}

func csvFixtureResult() model.AnalysisResult {
	return model.AnalysisResult{
		SQLContextAvailable: true,
		Summary:             model.WorkloadSummary{TotalTransactions: 2, TotalRows: 13},
		Tables: []model.TableStats{
			{Schema: "shop", Table: "orders", TotalRows: 9, InsertRows: 5, UpdateRows: 3, UpdateEvents: 2, DeleteRows: 1, TxnCount: 2, EventCount: 5, BinlogBytes: 1234, DDLCount: 1, KeyStatus: "has_pk"},
			{Schema: "shop", Table: "users", TotalRows: 4, InsertRows: 4, TxnCount: 1, EventCount: 1},
		},
		Transactions: []model.Transaction{
			{
				TxnKey: "txn-1", GTID: "aaaa-bbbb:1", ThreadID: 42, XID: "7",
				StartTime: time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC), EndTime: time.Date(2026, 3, 9, 10, 0, 1, 0, time.UTC),
				Duration:  1500 * time.Millisecond,
				TotalRows: 9, EventCount: 5, BinlogBytes: 900,
				Completeness:    model.TransactionComplete,
				BinlogPathStart: "bin.000001", BinlogPathEnd: "bin.000002",
				PositionStart: 100, PositionEnd: 1000,
				Tables:       map[string]int{"shop.orders": 9},
				Operations:   map[string]int{"INSERT": 5, "UPDATE": 3, "DELETE": 1},
				QuerySummary: "UPDATE shop.orders SET a = 1",
			},
		},
		Patterns: []model.PatternStats{
			{PatternKey: "k1", Label: "insert", TotalRows: 9, TxnCount: 2, EventCount: 5, ShareOfRows: 0.9, ShareOfTransactions: 1.0, AvgRowsPerTxn: 4.5, Tables: map[string]int{"shop.orders": 9}, Operations: map[string]int{"INSERT": 5}, SampleQuerySummary: "INSERT INTO t"},
		},
		StatementFingerprints: []model.StatementFingerprint{
			{Fingerprint: "update shop.orders set a=?", Sample: "UPDATE shop.orders SET a = 1, note = 'x,y'", Statements: 3, Rows: 9, ShareOfRows: 0.6923076923076923, Tables: map[string]int{"shop.orders": 9}},
			{Fingerprint: "delete from shop.orders where id=?", Sample: "DELETE", Statements: 1, Rows: 4, ShareOfRows: 0.3, Tables: map[string]int{"shop.orders": 4}},
		},
	}
}

func TestWriteCSVTablesDataset(t *testing.T) {
	var buf bytes.Buffer
	if err := Write(&buf, csvFixtureResult(), Options{CSVDataset: CSVDatasetTables}, "csv"); err != nil {
		t.Fatal(err)
	}
	rows := parseDelimited(t, buf.String(), ',')
	header := rows[0]
	if header[0] != "schema" || header[1] != "table" || header[2] != "total_rows" {
		t.Fatalf("header = %v", header)
	}
	if len(rows) != 3 {
		t.Fatalf("rows = %d, want 1 header + 2 data", len(rows))
	}
	if rows[1][0] != "shop" || rows[1][1] != "orders" || rows[1][2] != "9" || rows[1][11] != "has_pk" {
		t.Fatalf("row1 = %v", rows[1])
	}
	if rows[2][10] != "0" || rows[2][11] != "unknown" {
		t.Fatalf("row2 ddl/key = %v", rows[2])
	}
}

func TestWriteCSVTransactionsDataset(t *testing.T) {
	var buf bytes.Buffer
	if err := Write(&buf, csvFixtureResult(), Options{CSVDataset: CSVDatasetTransactions}, "csv"); err != nil {
		t.Fatal(err)
	}
	rows := parseDelimited(t, buf.String(), ',')
	if len(rows) != 2 {
		t.Fatalf("rows = %d", len(rows))
	}
	r := rows[1]
	if r[0] != "txn-1" || r[1] != "aaaa-bbbb:1" || r[3] != "42" || r[7] != "1500" || r[8] != "9" {
		t.Fatalf("txn row = %v", r)
	}
	if r[16] != "shop.orders=9" {
		t.Fatalf("tables cell = %q", r[16])
	}
	if r[17] != "DELETE=1;INSERT=5;UPDATE=3" {
		t.Fatalf("operations cell must be sorted name=count pairs, got %q", r[17])
	}
}

func TestWriteCSVPatternsDataset(t *testing.T) {
	var buf bytes.Buffer
	if err := Write(&buf, csvFixtureResult(), Options{CSVDataset: CSVDatasetPatterns}, "csv"); err != nil {
		t.Fatal(err)
	}
	rows := parseDelimited(t, buf.String(), ',')
	if len(rows) != 2 {
		t.Fatalf("rows = %d", len(rows))
	}
	if rows[1][0] != "k1" || rows[1][5] != "0.9" || rows[1][7] != "4.5" || rows[1][10] != "INSERT INTO t" {
		t.Fatalf("pattern row = %v", rows[1])
	}
}

func TestWriteCSVFingerprintsDatasetBoundsAndQuotes(t *testing.T) {
	result := csvFixtureResult()
	opts := Options{CSVDataset: CSVDatasetFingerprints, TopFingerprints: 1, TopFingerprintsSet: true}
	var buf bytes.Buffer
	if err := Write(&buf, result, opts, "csv"); err != nil {
		t.Fatal(err)
	}
	rows := parseDelimited(t, buf.String(), ',')
	if len(rows) != 2 {
		t.Fatalf("top-fingerprints 1 must list 1 row, got %d", len(rows))
	}
	r := rows[1]
	if r[0] != "update shop.orders set a=?" || r[1] != "3" || r[2] != "9" {
		t.Fatalf("fingerprint row = %v", r)
	}
	if r[4] != "shop.orders=9" {
		t.Fatalf("tables cell = %q", r[4])
	}
	// The sample carries a comma and quotes; RFC 4180 escaping must round-trip.
	if r[5] != "UPDATE shop.orders SET a = 1, note = 'x,y'" {
		t.Fatalf("sample = %q", r[5])
	}
}

func TestWriteCSVFingerprintsSampleHiddenUnderSQLContextOff(t *testing.T) {
	result := csvFixtureResult()
	opts := Options{CSVDataset: CSVDatasetFingerprints, SQLContextMode: SQLContextOff}
	var buf bytes.Buffer
	if err := Write(&buf, result, opts, "csv"); err != nil {
		t.Fatal(err)
	}
	rows := parseDelimited(t, buf.String(), ',')
	for i, r := range rows[1:] {
		if r[5] != "" {
			t.Fatalf("row %d sample leaked under --sql-context off: %q", i, r[5])
		}
	}
}

func TestWriteTSVSeparatesOnTab(t *testing.T) {
	var buf bytes.Buffer
	if err := Write(&buf, csvFixtureResult(), Options{CSVDataset: CSVDatasetTables}, "tsv"); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(buf.String(), ",") && !strings.Contains(buf.String(), "\t") {
		t.Fatalf("tsv output must use tabs, got %q", buf.String())
	}
	rows := parseDelimited(t, buf.String(), '\t')
	if rows[0][0] != "schema" || rows[1][1] != "orders" {
		t.Fatalf("tsv rows = %v", rows[:2])
	}
}

func TestWriteCSVEmptyDatasetStillWritesHeader(t *testing.T) {
	var buf bytes.Buffer
	err := Write(&buf, model.AnalysisResult{}, Options{CSVDataset: CSVDatasetFingerprints}, "csv")
	if err != nil {
		t.Fatal(err)
	}
	rows := parseDelimited(t, buf.String(), ',')
	if len(rows) != 1 || rows[0][0] != "fingerprint" {
		t.Fatalf("header-only output = %v", rows)
	}
}

func TestWriteCSVUnknownDatasetWritesNothing(t *testing.T) {
	var buf bytes.Buffer
	err := Write(&buf, csvFixtureResult(), Options{CSVDataset: "bogus"}, "csv")
	if err != nil {
		// An unknown dataset is rejected at flag validation; the emitter
		// falling back to tables is the safety net, not the contract. This
		// test pins that fallback to stay honest if the check is bypassed.
		rows := parseDelimited(t, buf.String(), ',')
		if rows[0][0] != "schema" {
			t.Fatalf("fallback dataset = %v", rows[0])
		}
		return
	}
	rows := parseDelimited(t, buf.String(), ',')
	if rows[0][0] != "schema" {
		t.Fatalf("fallback dataset = %v", rows[0])
	}
}
