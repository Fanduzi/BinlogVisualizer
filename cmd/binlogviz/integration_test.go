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
