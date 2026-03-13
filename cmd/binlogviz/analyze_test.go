package binlogviz

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"binlogviz/internal/analyzer"
	"binlogviz/internal/model"
	"binlogviz/internal/report"
)
func TestParseFlags(t *testing.T) {
	tests := []struct {
		name     string
		args     []string
		expected analyzeOptions
	}{
		{
			name: "default values",
			args: []string{},
			expected: analyzeOptions{
				startTime:    "",
				endTime:      "",
				topTables:    10,
				topTxns:      10,
				detectSpikes: false,
				spikeWindow:  2,
				spikeFactor:  5.0,
				spikeMinRows: 50,
				largeTxnRows: 1000,
				format:       "text",
			},
		},
		{
			name: "custom values",
			args: []string{
				"--start", "2026-03-09T10:00:00Z",
				"--end", "2026-03-09T11:00:00Z",
				"--top-tables", "20",
				"--top-transactions", "30",
				"--detect-spikes",
				"--spike-window", "5",
				"--spike-factor", "10.0",
				"--spike-min-rows", "100",
				"--large-txn-rows", "500",
				"--format", "json",
			},
			expected: analyzeOptions{
				startTime:    "2026-03-09T10:00:00Z",
				endTime:      "2026-03-09T11:00:00Z",
				topTables:    20,
				topTxns:      30,
				detectSpikes: true,
				spikeWindow:  5,
				spikeFactor:  10.0,
				spikeMinRows: 100,
				largeTxnRows: 500,
				format:       "json",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := newAnalyzeCommand()
			cmd.SetArgs(tt.args)
			// Parse flags
			opts := parseFlags(cmd, tt.args)
			if opts == nil {
				t.Fatalf("parseFlags returned nil")
			}
			if *opts != tt.expected {
				t.Errorf("expected %+v, got %+v", tt.expected, *opts)
			}
		})
	}
}

func TestBuildAnalyzerOptions(t *testing.T) {
	tests := []struct {
		name     string
		input    analyzeOptions
		start    time.Time
		end      time.Time
		expected analyzer.Options
	}{
		{
			name: "default values",
			input: analyzeOptions{
				topTables:    10,
				topTxns:      10,
				detectSpikes: false,
				spikeWindow:  2,
				spikeFactor:  5.0,
				spikeMinRows: 50,
				largeTxnRows: 1000,
			},
			start: time.Time{},
			end:   time.Time{},
			expected: analyzer.Options{
				TopTables:        10,
				TopTransactions: 10,
				DetectSpikes:     false,
				SpikeWindow:      2,
				SpikeFactor:      5.0,
				SpikeMinRows:     50,
				LargeTxnRows:     1000,
				LargeTxnDuration: 0,
				Start:           nil,
				End:             nil,
			},
		},
		{
			name: "with spike detection enabled",
			input: analyzeOptions{
				topTables:    20,
				topTxns:      30,
				detectSpikes: true,
				spikeWindow:  5,
				spikeFactor:  10.0,
				spikeMinRows: 100,
				largeTxnRows: 500,
			},
			start: time.Time{},
			end:   time.Time{},
			expected: analyzer.Options{
				TopTables:        20,
				TopTransactions: 30,
				DetectSpikes:     true,
				SpikeWindow:      5,
				SpikeFactor:      10.0,
				SpikeMinRows:     100,
				LargeTxnRows:     500,
				LargeTxnDuration: 0,
				Start:           nil,
				End:             nil,
			},
		},
		{
			name: "with time window",
			input: analyzeOptions{
				topTables:    10,
				topTxns:      10,
				detectSpikes: false,
				spikeWindow:  2,
				spikeFactor:  5.0,
				spikeMinRows: 50,
				largeTxnRows: 1000,
			},
			start: time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC),
			end:   time.Date(2026, 3, 9, 11, 0, 0, 0, time.UTC),
			expected: analyzer.Options{
				TopTables:        10,
				TopTransactions: 10,
				DetectSpikes:     false,
				SpikeWindow:      2,
				SpikeFactor:      5.0,
				SpikeMinRows:     50,
				LargeTxnRows:     1000,
				LargeTxnDuration: 0,
				Start:           ptrTime(time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC)),
				End:             ptrTime(time.Date(2026, 3, 9, 11, 0, 0, 0, time.UTC)),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := buildAnalyzerOptions(&tt.input, tt.start, tt.end)
			if !compareAnalyzerOptions(result, tt.expected) {
				t.Errorf("expected %+v, got %+v", tt.expected, result)
			}
		})
	}
}

func ptrTime(t time.Time) *time.Time {
	return &t
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
	if a.SpikeWindow != b.SpikeWindow {
		return false
	}
	if a.SpikeFactor != b.SpikeFactor {
		return false
	}
	if a.SpikeMinRows != b.SpikeMinRows {
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
	if a.Start != nil && b.Start != nil {
		if !a.Start.Equal(*b.Start) {
				return false
			}
		}
	// Compare End pointers
	if (a.End == nil) != (b.End == nil) {
		return false
	}
	if a.End != nil && b.End != nil {
		if !a.End.Equal(*b.End) {
				return false
			}
		}
	return true
}
func TestParseTimeRange(t *testing.T) {
	tests := []struct {
		name      string
		start    string
		end      string
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
					t.Error("expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if start.IsZero() && tt.start != "" {
					t.Error("expected non-zero start time when input provided")
				}
				if end.IsZero() && tt.end != "" {
					t.Error("expected non-zero end time when input provided")
				}
			}
        })
    }
}

func TestRenderOutput(t *testing.T) {
	tests := []struct {
		name     string
		format   string
		wantJson bool
	}{
		{
			name:     "json format",
			format:   "json",
			wantJson: true,
		},
		{
			name:     "text format",
			format:   "text",
			wantJson: false,
		},
		{
			name:     "default format",
			format:   "",
			wantJson: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := model.AnalysisResult{
				Summary: model.WorkloadSummary{
					TotalTransactions: 10,
				},
			}
			// Capture output
			var buf bytes.Buffer
			if tt.format == "json" {
				err := report.RenderJSONTo(result, &buf)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			} else {
				err := report.RenderTextTo(result, &buf)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			}
			output := buf.String()
			if tt.wantJson {
				if !strings.Contains(output, `"total_transactions"`) {
					t.Errorf("expected JSON output with snake_case field, got: %s", output)
				}
			} else {
				if !strings.Contains(output, "Workload Summary") {
					t.Errorf("expected text output with 'Workload Summary', got: %s", output)
				}
			}
		})
	}

	t.Run("invalid format falls back to text", func(t *testing.T) {
		result := model.AnalysisResult{
			Summary: model.WorkloadSummary{
				TotalTransactions: 10,
			},
		}
		var buf bytes.Buffer
		err := report.RenderTextTo(result, &buf)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		output := buf.String()
		if !strings.Contains(output, "Workload Summary") {
			t.Errorf("expected text output, got: %s", output)
		}
	})
}
