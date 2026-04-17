// Package binlogviz validates analyze command flag parsing and CLI option translation.
// input: synthetic CLI args, parsed flag values, and analyzer/report option expectations.
// output: regression coverage for stable flag defaults, validation, and option builders.
// pos: command-layer unit test suite for analyze command configuration behavior.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"

	"binlogviz/internal/analyzer"
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
