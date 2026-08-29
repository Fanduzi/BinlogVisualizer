// Package binlogviz validates analyze command flag parsing and CLI option translation.
// input: synthetic CLI args, parsed flag values, and analyzer/report option expectations.
// output: regression coverage for stable flag defaults, explicit selected-file coverage, validation, and option builders.
// pos: command-layer unit test suite for analyze command configuration behavior.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"

	"binlogviz/internal/analyzer"
	"binlogviz/internal/binlog"
	"binlogviz/internal/model"
	"binlogviz/internal/report"
)

func TestParseTimeRange(t *testing.T) {
	tests := []struct {
		name      string
		start     string
		end       string
		expectErr bool
	}{
		{
			name:      "valid range",
			start:     "2026-03-09T10:00:00Z",
			end:       "2026-03-09T11:00:00Z",
			expectErr: false,
		},
		{
			name:      "empty strings",
			start:     "",
			end:       "",
			expectErr: false,
		},
		{
			name:      "invalid start time",
			start:     "invalid",
			end:       "2026-03-09T11:00:00Z",
			expectErr: true,
		},
		{
			name:      "invalid end time",
			start:     "2026-03-09T10:00:00Z",
			end:       "not-a-time",
			expectErr: true,
		},
		{
			name:      "end before start",
			start:     "2026-03-09T11:00:00Z",
			end:       "2026-03-09T10:00:00Z",
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start, end, err := parseTimeRange(tt.start, tt.end)
			if tt.expectErr {
				if err == nil {
					t.Errorf("expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				// If input was provided, output should be non-zero
				if tt.start != "" && start.IsZero() {
					t.Error("expected non-zero start time when input provided")
				}
				if tt.end != "" && end.IsZero() {
					t.Error("expected non-zero end time when input provided")
				}
			}
		})
	}
}

func TestBuildAnalyzerOptions(t *testing.T) {
	tests := []struct {
		name     string
		input    analyzeOptions
		expected analyzer.Options
	}{
		{
			name: "default values",
			input: analyzeOptions{
				topTables:        10,
				topTransactions:  10,
				detectSpikes:     false,
				largeTrxRows:     1000,
				largeTrxDuration: 30 * time.Second,
			},
			expected: analyzer.Options{
				TopTables:        10,
				TopTransactions:  10,
				TopMinutes:       60,
				DetectSpikes:     false,
				LargeTxnRows:     1000,
				LargeTxnDuration: 30 * time.Second,
				SpikeWindow:      5,
				SpikeFactor:      5.0,
				SpikeMinRows:     100,
				Start:            nil,
				End:              nil,
			},
		},
		{
			name: "with json enabled",
			input: analyzeOptions{
				format:           "json",
				topTables:        10,
				topTransactions:  10,
				detectSpikes:     false,
				largeTrxRows:     1000,
				largeTrxDuration: 30 * time.Second,
			},
			expected: analyzer.Options{
				TopTables:        10,
				TopTransactions:  10,
				TopMinutes:       60,
				DetectSpikes:     false,
				LargeTxnRows:     1000,
				LargeTxnDuration: 30 * time.Second,
				SpikeWindow:      5,
				SpikeFactor:      5.0,
				SpikeMinRows:     100,
				Start:            nil,
				End:              nil,
			},
		},
		{
			name: "with spike detection",
			input: analyzeOptions{
				topTables:        25,
				topTransactions:  50,
				detectSpikes:     true,
				largeTrxRows:     1000,
				largeTrxDuration: time.Minute,
			},
			expected: analyzer.Options{
				TopTables:        25,
				TopTransactions:  50,
				TopMinutes:       60,
				DetectSpikes:     true,
				LargeTxnRows:     1000,
				LargeTxnDuration: time.Minute,
				SpikeWindow:      5,
				SpikeFactor:      5.0,
				SpikeMinRows:     100,
				Start:            nil,
				End:              nil,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			startTime, endTime, err := parseTimeRange(tt.input.startTime, tt.input.endTime)
			if err != nil {
				t.Fatalf("parseTimeRange returned error: %v", err)
			}
			analyzerOpts := buildAnalyzerOptions(&tt.input, startTime, endTime)
			if !compareAnalyzerOptions(analyzerOpts, tt.expected) {
				t.Errorf("buildAnalyzerOptions = %+v, expected %+v", analyzerOpts, tt.expected)
			}
		})
	}
}

func compareAnalyzerOptions(a, b analyzer.Options) bool {
	if a.TopTables != b.TopTables {
		return false
	}
	if a.TopTransactions != b.TopTransactions {
		return false
	}
	if a.TopMinutes != b.TopMinutes {
		return false
	}
	if a.DetectSpikes != b.DetectSpikes {
		return false
	}
	if a.LargeTxnRows != b.LargeTxnRows {
		return false
	}
	if a.LargeTxnDuration != b.LargeTxnDuration {
		return false
	}
	if a.SpikeWindow != b.SpikeWindow {
		return false
	}
	if a.SpikeFactor != b.SpikeFactor {
		return false
	}
	if a.SpikeMinRows != b.SpikeMinRows {
		return false
	}
	// Compare Start pointers
	if (a.Start == nil) != (b.Start == nil) {
		return false
	}
	if a.Start != nil && !a.Start.Equal(*b.Start) {
		return false
	}
	// Compare End pointers
	if (a.End == nil) != (b.End == nil) {
		return false
	}
	if a.End != nil && !a.End.Equal(*b.End) {
		return false
	}
	return true
}

func TestLargeTrxDurationParsing(t *testing.T) {
	tests := []struct {
		name     string
		args     []string
		expected time.Duration
	}{
		{
			name:     "default duration",
			args:     []string{},
			expected: 30 * time.Second,
		},
		{
			name:     "one minute",
			args:     []string{"--large-trx-duration=1m"},
			expected: time.Minute,
		},
		{
			name:     "five seconds",
			args:     []string{"--large-trx-duration=5s"},
			expected: 5 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := newAnalyzeCommand()
			cmd.SetArgs(append([]string{"dummy.binlog"}, tt.args...))

			// Execute with a NOP RunE to capture parsed values
			var capturedOpts *analyzeOptions
			originalRunE := cmd.RunE
			cmd.RunE = func(cmd *cobra.Command, args []string) error {
				// Get the opts from the closure in newAnalyzeCommand
				// We can access the flag values directly
				duration, err := cmd.Flags().GetDuration("large-trx-duration")
				if err != nil {
					return err
				}
				capturedOpts = &analyzeOptions{largeTrxDuration: duration}
				return nil
			}

			// Silence usage output during test
			cmd.SilenceUsage = true
			cmd.SilenceErrors = true

			// Execute the command
			_ = cmd.Execute()

			if capturedOpts == nil {
				t.Fatal("failed to capture options")
			}
			if capturedOpts.largeTrxDuration != tt.expected {
				t.Errorf("largeTrxDuration = %v, expected %v", capturedOpts.largeTrxDuration, tt.expected)
			}

			// Restore original RunE (not used but good practice)
			_ = originalRunE
		})
	}
}

func TestAnalyzeCommandDefinesFlags(t *testing.T) {
	requiredFlags := []string{
		"start",
		"end",
		"format",
		"snapshot-name",
		"snapshot-dir",
		"sql-context",
		"top",
		"top-tables",
		"top-transactions",
		"show-minutes",
		"show-patterns",
		"details",
		"detect-spikes",
		"large-trx-rows",
		"large-trx-duration",
	}

	cmd := newAnalyzeCommand()

	for _, name := range requiredFlags {
		if cmd.Flags().Lookup(name) == nil {
			t.Fatalf("missing flag %q", name)
		}
	}
}

func TestAnalyzeCommandDocumentsUnlimitedTransactions(t *testing.T) {
	flag := newAnalyzeCommand().Flags().Lookup("top-transactions")
	if flag == nil {
		t.Fatal("expected top-transactions flag")
	}
	if !strings.Contains(flag.Usage, "0") || !strings.Contains(strings.ToLower(flag.Usage), "unlimited") {
		t.Fatalf("expected top-transactions help to document zero as unlimited, got %q", flag.Usage)
	}
}

func TestAnalyzeCommandSQLContextDefaultIsSummary(t *testing.T) {
	cmd := newAnalyzeCommand()
	flag := cmd.Flags().Lookup("sql-context")
	if flag == nil {
		t.Fatal("expected sql-context flag")
	}
	if flag.DefValue != string(report.SQLContextSummary) {
		t.Fatalf("expected default sql-context %q, got %q", report.SQLContextSummary, flag.DefValue)
	}
}

func TestAnalyzeCommandRejectsInvalidSQLContext(t *testing.T) {
	cmd := newAnalyzeCommand()
	cmd.SetArgs([]string{"dummy.binlog", "--sql-context=invalid"})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected invalid sql-context error")
	}
	if !strings.Contains(err.Error(), "invalid --sql-context") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestAnalyzeCommandRejectsSnapshotNameWithoutJSONFormat(t *testing.T) {
	cmd := newAnalyzeCommand()
	cmd.SetArgs([]string{"dummy.binlog", "--snapshot-name", "incident_window"})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected snapshot-name validation error")
	}
	if !strings.Contains(err.Error(), "--snapshot-name requires --format json") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBuildReportOptionsMapsPresentationFlags(t *testing.T) {
	opts := &analyzeOptions{
		sqlContext:   string(report.SQLContextFull),
		top:          7,
		details:      true,
		showMinutes:  true,
		showPatterns: true,
	}

	got, err := buildReportOptions(opts)
	if err != nil {
		t.Fatalf("buildReportOptions returned error: %v", err)
	}
	if got.SQLContextMode != report.SQLContextFull {
		t.Fatalf("expected sql context %q, got %q", report.SQLContextFull, got.SQLContextMode)
	}
	if got.TopN != 7 {
		t.Fatalf("expected TopN 7, got %d", got.TopN)
	}
	if !got.Details {
		t.Fatal("expected Details to be true")
	}
	if !got.ShowMinutes {
		t.Fatal("expected ShowMinutes to be true")
	}
	if !got.ShowPatterns {
		t.Fatal("expected ShowPatterns to be true")
	}
}

func TestBuildReportOptionsKeepsExplicitTopTablesIndependent(t *testing.T) {
	opts := &analyzeOptions{
		top:              7,
		topTables:        2,
		topTablesChanged: true,
	}

	got, err := buildReportOptions(opts)
	if err != nil {
		t.Fatalf("buildReportOptions returned error: %v", err)
	}
	if got.TopN != 7 {
		t.Fatalf("expected TopN 7, got %d", got.TopN)
	}
	if got.TopTables != 2 {
		t.Fatalf("expected TopTables 2, got %d", got.TopTables)
	}
	if !got.TopTablesSet {
		t.Fatal("expected explicit TopTables setting to be preserved")
	}
}

func TestBuildReportOptionsPreservesUnlimitedTopTables(t *testing.T) {
	opts := &analyzeOptions{top: 10, topTablesChanged: true}

	got, err := buildReportOptions(opts)
	if err != nil {
		t.Fatalf("buildReportOptions returned error: %v", err)
	}
	if got.TopTables != 0 || !got.TopTablesSet {
		t.Fatalf("expected explicit TopTables=0 to remain unlimited, got %+v", got)
	}
}

func TestBuildAnalyzerOptionsUsesTopForLegacyDefaultsWhenUntouched(t *testing.T) {
	cliOpts := &analyzeOptions{
		top:              6,
		largeTrxRows:     1000,
		largeTrxDuration: 30 * time.Second,
	}

	result := buildAnalyzerOptions(cliOpts, time.Time{}, time.Time{})

	if result.TopTables != 6 {
		t.Fatalf("expected TopTables to inherit --top=6, got %d", result.TopTables)
	}
	if result.TopTransactions != 6 {
		t.Fatalf("expected TopTransactions to inherit --top=6, got %d", result.TopTransactions)
	}
}

func TestBuildAnalyzerOptionsPreservesExplicitLegacyTopFlags(t *testing.T) {
	cliOpts := &analyzeOptions{
		top:                    6,
		topTables:              3,
		topTransactions:        4,
		topTablesChanged:       true,
		topTransactionsChanged: true,
		largeTrxRows:           1000,
		largeTrxDuration:       30 * time.Second,
	}

	result := buildAnalyzerOptions(cliOpts, time.Time{}, time.Time{})

	if result.TopTables != 3 {
		t.Fatalf("expected explicit TopTables=3, got %d", result.TopTables)
	}
	if result.TopTransactions != 4 {
		t.Fatalf("expected explicit TopTransactions=4, got %d", result.TopTransactions)
	}
}

func TestBuildAnalyzerOptionsPreservesUnlimitedTransactions(t *testing.T) {
	cliOpts := &analyzeOptions{
		top:                    10,
		topTransactions:        0,
		topTransactionsChanged: true,
		largeTrxRows:           1000,
		largeTrxDuration:       30 * time.Second,
	}

	result := buildAnalyzerOptions(cliOpts, time.Time{}, time.Time{})
	if result.TopTransactions != 0 {
		t.Fatalf("expected explicit TopTransactions=0 to remain unlimited, got %d", result.TopTransactions)
	}
}

func TestAnalyzeTransactionOutputReportsBoundedAndUnlimitedLists(t *testing.T) {
	base := time.Date(2026, 8, 30, 10, 0, 0, 0, time.UTC)
	events := make([]model.NormalizedEvent, 0, 36)
	for i := 1; i <= 12; i++ {
		key := fmt.Sprintf("txn-%d", i)
		start := base.Add(time.Duration(i) * time.Second)
		events = append(events,
			model.NormalizedEvent{Timestamp: start, EventType: "BEGIN", TxnKey: key},
			model.NormalizedEvent{Timestamp: start.Add(time.Second), EventType: "ROWS", TxnKey: key, Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 10},
			model.NormalizedEvent{Timestamp: start.Add(2 * time.Second), EventType: "XID", TxnKey: key},
		)
	}

	for _, tc := range []struct {
		name            string
		top             int
		listed, omitted int
	}{
		{name: "default bounded", top: 10, listed: 10, omitted: 2},
		{name: "explicit unlimited", top: 0, listed: 12, omitted: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := analyzer.DefaultOptions()
			opts.TopTransactions = tc.top
			result, err := analyzer.New(opts).Analyze(events)
			if err != nil {
				t.Fatalf("Analyze returned error: %v", err)
			}

			out, err := report.RenderJSON(*result)
			if err != nil {
				t.Fatalf("RenderJSON returned error: %v", err)
			}
			var decoded struct {
				Transactions []struct {
					TxnKey string `json:"txn_key"`
				} `json:"transactions"`
				TransactionsListed  int `json:"transactions_listed"`
				TransactionsOmitted int `json:"transactions_omitted"`
			}
			if err := json.Unmarshal([]byte(out), &decoded); err != nil {
				t.Fatalf("decode JSON: %v", err)
			}
			if len(decoded.Transactions) != tc.listed || decoded.TransactionsListed != tc.listed || decoded.TransactionsOmitted != tc.omitted {
				t.Fatalf("transaction list = %d, listed=%d, omitted=%d; want %d, %d, %d", len(decoded.Transactions), decoded.TransactionsListed, decoded.TransactionsOmitted, tc.listed, tc.listed, tc.omitted)
			}
			for i, txn := range decoded.Transactions {
				want := fmt.Sprintf("txn-%d", i+1)
				if txn.TxnKey != want {
					t.Fatalf("transaction %d = %q, want %q", i, txn.TxnKey, want)
				}
			}

			html, err := report.RenderHTML(*result)
			if err != nil {
				t.Fatalf("RenderHTML returned error: %v", err)
			}
			if !strings.Contains(html, `data-transaction-key="txn-5"`) {
				t.Fatal("expected present txn-5 to be available in the HTML transaction lookup")
			}
		})
	}
}

func TestValidateFilesAllowsEmptyPaths(t *testing.T) {
	// Empty paths should not error in validateFiles itself,
	// but the caller (RunE) must check len(paths)==0 separately.
	if err := validateFiles(nil); err != nil {
		t.Fatalf("expected no error for empty paths, got %v", err)
	}
}

func TestResolveAnalyzePathsExplicitFilesIncludesSelectedFileSizes(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mysql-bin.000001")
	if err := os.WriteFile(path, make([]byte, 3000), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}

	paths, discovered, coverage, err := resolveAnalyzePaths([]string{path}, &analyzeOptions{})
	if err != nil {
		t.Fatalf("resolve explicit paths: %v", err)
	}
	if discovered {
		t.Fatal("explicit paths must not be marked as discovery")
	}
	if len(paths) != 1 || paths[0] != path {
		t.Fatalf("unexpected paths: %v", paths)
	}
	if len(coverage.Selected) != 1 || coverage.Selected[0].Size != 3000 {
		t.Fatalf("expected selected file size 3000, got %+v", coverage)
	}
}

func TestDiscoverBinlogPlanInWindowReturnsEmptyPathsForNoMatch(t *testing.T) {
	dir := t.TempDir()
	// Create one binlog file
	path := createTestBinlogPath(t, dir, "mysql-bin.000001")

	// Stub probe to return a file that starts after the requested window.
	origProbe := probeAnalyzePaths
	probeAnalyzePaths = func(paths []string, workerCount int) ([]binlog.FileProbe, error) {
		return []binlog.FileProbe{
			{
				BinlogPath:   path,
				FirstEventAt: time.Date(2026, 4, 5, 16, 0, 0, 0, time.UTC),
				LastEventAt:  time.Date(2026, 4, 5, 17, 0, 0, 0, time.UTC),
			},
		}, nil
	}
	defer func() { probeAnalyzePaths = origProbe }()

	start := time.Date(2026, 4, 5, 14, 0, 0, 0, time.UTC)
	end := time.Date(2026, 4, 5, 15, 0, 0, 0, time.UTC)
	plan, err := discoverBinlogPlanInWindow(dir, "mysql-bin.", start, end)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(plan.Paths) != 0 {
		t.Fatalf("expected 0 paths for non-overlapping window, got %d", len(plan.Paths))
	}
}

func createTestBinlogPath(t *testing.T, dir, name string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, make([]byte, 100), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	return path
}
