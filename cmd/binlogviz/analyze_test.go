package binlogviz

import (
	"testing"
	"time"

	"github.com/spf13/cobra"

	"binlogviz/internal/analyzer"
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
				DetectSpikes:     false,
				LargeTxnRows:     1000,
				LargeTxnDuration: 30 * time.Second,
				Start:            nil,
				End:              nil,
			},
		},
		{
			name: "with json enabled",
			input: analyzeOptions{
				json:             true,
				topTables:        10,
				topTransactions:  10,
				detectSpikes:     false,
				largeTrxRows:     1000,
				largeTrxDuration: 30 * time.Second,
			},
			expected: analyzer.Options{
				TopTables:        10,
				TopTransactions:  10,
				DetectSpikes:     false,
				LargeTxnRows:     1000,
				LargeTxnDuration: 30 * time.Second,
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
				DetectSpikes:     true,
				LargeTxnRows:     1000,
				LargeTxnDuration: time.Minute,
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
	if a.DetectSpikes != b.DetectSpikes {
		return false
	}
	if a.LargeTxnRows != b.LargeTxnRows {
		return false
	}
	if a.LargeTxnDuration != b.LargeTxnDuration {
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
		"json",
		"top-tables",
		"top-transactions",
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
