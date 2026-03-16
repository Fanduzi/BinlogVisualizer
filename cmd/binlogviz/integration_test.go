// Package binlogviz validates end-to-end analyze command behavior and DuckDB temp-store lifecycle.
// input: mock parsers, fixture binlog files, CLI-derived analyzer options, and temporary directories for command resources.
// output: regression coverage for rendered reports, temp DuckDB cleanup, and command/analyzer integration semantics.
// pos: command-layer integration test suite covering parse-normalize-analyze-render execution paths.
// note: if this file changes, update this header and module README.md.
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

func TestCreateDuckDBTempStoreCreatesAndCleansFiles(t *testing.T) {
	root := t.TempDir()

	store, cleanup, path, err := createDuckDBTempStore(root)
	if err != nil {
		t.Fatalf("createDuckDBTempStore returned error: %v", err)
	}
	if store == nil {
		t.Fatal("expected non-nil store")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected DuckDB file at %s: %v", path, err)
	}

	if err := cleanup(); err != nil {
		t.Fatalf("cleanup returned error: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("expected DuckDB file to be removed, got err=%v", err)
	}
}

func TestRunAnalysisWithParserCleansDuckDBTempStoreOnFailure(t *testing.T) {
	root := t.TempDir()
	var createdPath string

	err := runAnalysisWithParserAndTempDir([]string{"dummy.binlog"}, analyzer.Options{}, false, &mockParser{
		events: []binlog.RawEvent{
			{Timestamp: time.Date(2026, 3, 14, 10, 0, 0, 0, time.UTC), EventType: "QUERY_EVENT", Query: "BEGIN"},
			{Timestamp: time.Date(2026, 3, 14, 10, 0, 1, 0, time.UTC), EventType: "WRITE_ROWS_EVENT", Schema: "shop", Table: "orders", RowCount: 5},
			{Timestamp: time.Date(2026, 3, 14, 10, 0, 2, 0, time.UTC), EventType: "QUERY_EVENT", Query: "BEGIN"},
		},
	}, root, func(path string) {
		createdPath = path
	})
	if err == nil {
		t.Fatal("expected analysis error")
	}
	if createdPath == "" {
		t.Fatal("expected created DuckDB path to be captured")
	}
	if _, statErr := os.Stat(createdPath); !os.IsNotExist(statErr) {
		t.Fatalf("expected cleanup to remove DuckDB path on failure, got err=%v", statErr)
	}
}

func TestBuildAnalyzerOptionsIncludesSpikeDefaults(t *testing.T) {
	// When detectSpikes is enabled, spike detection parameters should have defaults
	cliOpts := &analyzeOptions{
		detectSpikes: true,
	}

	result := buildAnalyzerOptions(cliOpts, time.Time{}, time.Time{})

	// Verify spike detection is enabled
	if !result.DetectSpikes {
		t.Error("expected DetectSpikes to be true")
	}

	// Verify spike detection has valid defaults (not zero values)
	if result.SpikeWindow <= 0 {
		t.Errorf("expected SpikeWindow > 0, got %d", result.SpikeWindow)
	}
	if result.SpikeFactor <= 0 {
		t.Errorf("expected SpikeFactor > 0, got %f", result.SpikeFactor)
	}
	if result.SpikeMinRows <= 0 {
		t.Errorf("expected SpikeMinRows > 0, got %d", result.SpikeMinRows)
	}
}

func TestBuildAnalyzerOptionsUsesDefaultOptions(t *testing.T) {
	// Verify that buildAnalyzerOptions uses DefaultOptions as base
	cliOpts := &analyzeOptions{
		topTables:        10,
		topTransactions:  10,
		largeTrxRows:     1000,
		largeTrxDuration: 30 * time.Second,
	}

	result := buildAnalyzerOptions(cliOpts, time.Time{}, time.Time{})

	// Check that we got the defaults from analyzer.DefaultOptions()
	defaults := analyzer.DefaultOptions()

	// These should match defaults when not overridden by CLI
	if result.SpikeWindow != defaults.SpikeWindow {
		t.Errorf("SpikeWindow: expected %d, got %d", defaults.SpikeWindow, result.SpikeWindow)
	}
	if result.SpikeFactor != defaults.SpikeFactor {
		t.Errorf("SpikeFactor: expected %f, got %f", defaults.SpikeFactor, result.SpikeFactor)
	}
	if result.SpikeMinRows != defaults.SpikeMinRows {
		t.Errorf("SpikeMinRows: expected %d, got %d", defaults.SpikeMinRows, result.SpikeMinRows)
	}
}

func TestSpikeDetectionWithDefaultsProducesAlert(t *testing.T) {
	// Create events spanning 10 minutes with a spike at minute 7
	base := time.Date(2026, 3, 15, 10, 0, 0, 0, time.UTC)
	mock := &mockParser{}

	// Generate events: 6 minutes of baseline, then a spike
	for minute := 0; minute < 10; minute++ {
		rowCount := 100 // baseline
		if minute >= 7 {
			rowCount = 600 // spike (6x baseline)
		}
		for i := 0; i < rowCount; i++ {
			mock.events = append(mock.events, binlog.RawEvent{
				Timestamp: base.Add(time.Duration(minute)*time.Minute + time.Duration(i)*time.Millisecond),
				EventType: "WRITE_ROWS_EVENT",
				Schema:    "shop",
				Table:     "orders",
				RowCount:  1,
			})
		}
	}

	// Use DefaultOptions which includes spike detection defaults
	opts := analyzer.DefaultOptions()
	opts.DetectSpikes = true

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

	// Verify spike alert is produced
	if !bytes.Contains([]byte(output), []byte(`"type": "spike"`)) {
		t.Error("expected spike alert in output")
	}
}

// TestRealBinlogFixtureEndToEnd verifies the complete parsing pipeline with a real MySQL binlog file.
// This test uses internal/binlog/testdata/minimal.binlog which was generated from MySQL 5.7 with ROW binlog format.
// See internal/binlog/testdata/README.md for fixture generation instructions.
func TestRealBinlogFixtureEndToEnd(t *testing.T) {
	// Fixture is in internal/binlog/testdata/, relative to project root
	fixturePath := "../../internal/binlog/testdata/minimal.binlog"

	// Verify fixture exists
	if _, err := os.Stat(fixturePath); os.IsNotExist(err) {
		t.Fatalf("fixture not found: %s - see internal/binlog/testdata/README.md for generation instructions", fixturePath)
	}

	// Run the full pipeline with real parser
	opts := analyzer.DefaultOptions()

	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	err := runAnalysis([]string{fixturePath}, opts, false)

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
	// The fixture contains INSERT/UPDATE/DELETE on testdb.users
	if !bytes.Contains([]byte(output), []byte("testdb.users")) {
		t.Error("expected output to contain testdb.users table")
	}
	// Verify we have row activity (the fixture has 5 total rows)
	if !bytes.Contains([]byte(output), []byte("5 rows")) {
		t.Error("expected output to contain '5 rows'")
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
