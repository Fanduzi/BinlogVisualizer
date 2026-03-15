package binlogviz

import (
	"bytes"
	"io"
	"os"
	"testing"
	"time"

	"binlogviz/internal/analyzer"
	"binlogviz/internal/binlog"
	"binlogviz/internal/model"
)

// mockParser implements binlog.Parser for testing.
type mockParser struct {
	events []binlog.RawEvent
	err    error
}

func (m *mockParser) ParseFiles(paths []string, handler func(binlog.RawEvent) error) error {
	if m.err != nil {
		return m.err
	}
	for _, ev := range m.events {
		if err := handler(ev); err != nil {
			return err
		}
	}
	return nil
}

func TestAnalyzeCommandRejectsMissingFiles(t *testing.T) {
	cmd := newAnalyzeCommand()
	cmd.SetArgs([]string{"missing-binlog.000001"})

	// Silence usage output during test
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	if err := cmd.Execute(); err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestRunAnalysisHappyPath(t *testing.T) {
	// Create mock parser with sample events
	mock := &mockParser{
		events: []binlog.RawEvent{
			{Timestamp: time.Date(2026, 3, 14, 10, 0, 0, 0, time.UTC), EventType: "QUERY_EVENT", Query: "BEGIN"},
			{Timestamp: time.Date(2026, 3, 14, 10, 0, 1, 0, time.UTC), EventType: "TABLE_MAP_EVENT", Schema: "shop", Table: "orders"},
			{Timestamp: time.Date(2026, 3, 14, 10, 0, 1, 0, time.UTC), EventType: "WRITE_ROWS_EVENT", Schema: "shop", Table: "orders", RowCount: 5},
			{Timestamp: time.Date(2026, 3, 14, 10, 0, 2, 0, time.UTC), EventType: "XID_EVENT"},
		},
	}

	opts := analyzer.Options{
		TopTables:       10,
		TopTransactions: 10,
	}

	// Capture stdout
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	err := runAnalysisWithParser([]string{"dummy.binlog"}, opts, false, mock)

	// Restore stdout
	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	io.Copy(&buf, r)
	output := buf.String()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify output contains expected sections
	if !bytes.Contains([]byte(output), []byte("=== Workload Summary ===")) {
		t.Error("expected output to contain Workload Summary section")
	}
	if !bytes.Contains([]byte(output), []byte("=== Top Tables ===")) {
		t.Error("expected output to contain Top Tables section")
	}
	if !bytes.Contains([]byte(output), []byte("shop.orders")) {
		t.Error("expected output to contain shop.orders table")
	}
	if !bytes.Contains([]byte(output), []byte("Total Transactions: 1")) {
		t.Error("expected output to show 1 transaction")
	}
}

func TestRunAnalysisJSONOutput(t *testing.T) {
	mock := &mockParser{
		events: []binlog.RawEvent{
			{Timestamp: time.Date(2026, 3, 14, 10, 0, 0, 0, time.UTC), EventType: "QUERY_EVENT", Query: "BEGIN"},
			{Timestamp: time.Date(2026, 3, 14, 10, 0, 1, 0, time.UTC), EventType: "WRITE_ROWS_EVENT", Schema: "test", Table: "users", RowCount: 3},
			{Timestamp: time.Date(2026, 3, 14, 10, 0, 2, 0, time.UTC), EventType: "XID_EVENT"},
		},
	}

	opts := analyzer.Options{TopTables: 10, TopTransactions: 10}

	// Capture stdout
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	err := runAnalysisWithParser([]string{"dummy.binlog"}, opts, true, mock)

	// Restore stdout
	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	io.Copy(&buf, r)
	output := buf.String()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify JSON output starts with { and contains expected fields
	if output[0] != '{' {
		t.Error("expected JSON output to start with '{'")
	}
	if !bytes.Contains([]byte(output), []byte(`"total_transactions": 1`)) {
		t.Error("expected JSON to contain total_transactions")
	}
	if !bytes.Contains([]byte(output), []byte(`"schema": "test"`)) {
		t.Error("expected JSON to contain test schema")
	}
}

func TestApplyTopLimitsTruncatesTables(t *testing.T) {
	result := &model.AnalysisResult{
		Tables: createTestTableStats(5),
	}
	opts := analyzer.Options{TopTables: 2}

	applyTopLimits(result, opts)

	if len(result.Tables) != 2 {
		t.Errorf("expected 2 tables, got %d", len(result.Tables))
	}
}

func TestApplyTopLimitsTruncatesTransactions(t *testing.T) {
	result := &model.AnalysisResult{
		Transactions: createTestTransactions(5),
	}
	opts := analyzer.Options{TopTransactions: 2}

	applyTopLimits(result, opts)

	if len(result.Transactions) != 2 {
		t.Errorf("expected 2 transactions, got %d", len(result.Transactions))
	}

	// Verify top transactions by TotalRows are kept
	if result.Transactions[0].TotalRows != 5 {
		t.Errorf("expected first transaction to have 5 rows, got %d", result.Transactions[0].TotalRows)
	}
	if result.Transactions[1].TotalRows != 4 {
		t.Errorf("expected second transaction to have 4 rows, got %d", result.Transactions[1].TotalRows)
	}
}

func TestApplyTopLimitsNoTruncationWhenLimitExceedsCount(t *testing.T) {
	result := &model.AnalysisResult{
		Tables:       createTestTableStats(3),
		Transactions: createTestTransactions(3),
	}
	opts := analyzer.Options{TopTables: 10, TopTransactions: 10}

	applyTopLimits(result, opts)

	if len(result.Tables) != 3 {
		t.Errorf("expected 3 tables, got %d", len(result.Tables))
	}
	if len(result.Transactions) != 3 {
		t.Errorf("expected 3 transactions, got %d", len(result.Transactions))
	}
}

func TestApplyTopLimitsDeterministicOrderByTxnKey(t *testing.T) {
	// Create transactions with same TotalRows but different TxnKeys
	// Order should be deterministic: TotalRows DESC, TxnKey ASC
	result := &model.AnalysisResult{
		Transactions: []model.Transaction{
			{TxnKey: "txn-c", TotalRows: 10},
			{TxnKey: "txn-a", TotalRows: 10},
			{TxnKey: "txn-b", TotalRows: 10},
			{TxnKey: "txn-d", TotalRows: 5},
		},
	}
	opts := analyzer.Options{TopTransactions: 3}

	applyTopLimits(result, opts)

	if len(result.Transactions) != 3 {
		t.Fatalf("expected 3 transactions, got %d", len(result.Transactions))
	}

	// All 3 with TotalRows=10 should be kept, sorted by TxnKey ASC
	// txn-a, txn-b, txn-c (all have 10 rows)
	if result.Transactions[0].TxnKey != "txn-a" {
		t.Errorf("expected first transaction to be txn-a, got %s", result.Transactions[0].TxnKey)
	}
	if result.Transactions[1].TxnKey != "txn-b" {
		t.Errorf("expected second transaction to be txn-b, got %s", result.Transactions[1].TxnKey)
	}
	if result.Transactions[2].TxnKey != "txn-c" {
		t.Errorf("expected third transaction to be txn-c, got %s", result.Transactions[2].TxnKey)
	}

	// txn-d with 5 rows should be excluded
	for _, txn := range result.Transactions {
		if txn.TxnKey == "txn-d" {
			t.Error("txn-d with lower TotalRows should not be in top 3")
		}
	}
}

func TestApplyTopLimitsMixedRowssWithTieBreaker(t *testing.T) {
	// Test with mixed TotalRows where tie-breaker matters for middle items
	result := &model.AnalysisResult{
		Transactions: []model.Transaction{
			{TxnKey: "z-large", TotalRows: 100},
			{TxnKey: "a-medium", TotalRows: 50},
			{TxnKey: "c-medium", TotalRows: 50},
			{TxnKey: "b-medium", TotalRows: 50},
			{TxnKey: "x-small", TotalRows: 10},
		},
	}
	opts := analyzer.Options{TopTransactions: 3}

	applyTopLimits(result, opts)

	if len(result.Transactions) != 3 {
		t.Fatalf("expected 3 transactions, got %d", len(result.Transactions))
	}

	// Expected order: z-large (100), a-medium (50), b-medium (50)
	// The 3 with 50 rows should be sorted by TxnKey, and we take top 2 of them
	if result.Transactions[0].TxnKey != "z-large" {
		t.Errorf("expected first to be z-large, got %s", result.Transactions[0].TxnKey)
	}
	if result.Transactions[1].TxnKey != "a-medium" {
		t.Errorf("expected second to be a-medium, got %s", result.Transactions[1].TxnKey)
	}
	if result.Transactions[2].TxnKey != "b-medium" {
		t.Errorf("expected third to be b-medium, got %s", result.Transactions[2].TxnKey)
	}
}

// Helper functions to create test data

func createTestTableStats(count int) []model.TableStats {
	stats := make([]model.TableStats, count)
	for i := 0; i < count; i++ {
		stats[i] = model.TableStats{
			Schema:    "test",
			Table:     string(rune('a' + i)),
			TotalRows: i + 1,
		}
	}
	return stats
}

func createTestTransactions(count int) []model.Transaction {
	txns := make([]model.Transaction, count)
	for i := 0; i < count; i++ {
		txns[i] = model.Transaction{
			TxnKey:    string(rune('a' + i)),
			TotalRows: i + 1,
		}
	}
	return txns
}
