package binlogviz

import (
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/spf13/cobra"

	"binlogviz/internal/analyzer"
	"binlogviz/internal/binlog"
	"binlogviz/internal/model"
	"binlogviz/internal/report"
)

// analyzeOptions holds the parsed CLI flags for the analyze command.
type analyzeOptions struct {
	startTime        string
	endTime          string
	json             bool
	topTables        int
	topTransactions  int
	detectSpikes     bool
	largeTrxRows     int
	largeTrxDuration time.Duration
}

func newAnalyzeCommand() *cobra.Command {
	opts := &analyzeOptions{}

	cmd := &cobra.Command{
		Use:   "analyze <binlog files...>",
		Short: "Analyze binlog files",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			// Parse time range
			startTime, endTime, err := parseTimeRange(opts.startTime, opts.endTime)
			if err != nil {
				return err
			}

			// Validate input files exist
			if err := validateFiles(args); err != nil {
				return err
			}

			// Build analyzer options
			analyzerOpts := buildAnalyzerOptions(opts, startTime, endTime)

			// Execute the analysis pipeline
			return runAnalysis(args, analyzerOpts, opts.json)
		},
	}

	// Register flags
	cmd.Flags().StringVar(&opts.startTime, "start", "", "Start time (inclusive, RFC3339 format)")
	cmd.Flags().StringVar(&opts.endTime, "end", "", "End time (inclusive, RFC3339 format)")
	cmd.Flags().BoolVar(&opts.json, "json", false, "Output in JSON format")
	cmd.Flags().IntVar(&opts.topTables, "top-tables", 10, "Number of top tables to show")
	cmd.Flags().IntVar(&opts.topTransactions, "top-transactions", 10, "Number of top transactions to show")
	cmd.Flags().BoolVar(&opts.detectSpikes, "detect-spikes", false, "Enable spike detection")
	cmd.Flags().IntVar(&opts.largeTrxRows, "large-trx-rows", 1000, "Rows threshold for large transaction alert")
	cmd.Flags().DurationVar(&opts.largeTrxDuration, "large-trx-duration", 30*time.Second, "Duration threshold for large transaction alert")

	return cmd
}

// validateFiles checks that all input files exist.
func validateFiles(paths []string) error {
	for _, path := range paths {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			return fmt.Errorf("file not found: %s", path)
		}
	}
	return nil
}

// runAnalysis executes the complete analysis pipeline.
func runAnalysis(paths []string, opts analyzer.Options, jsonOutput bool) error {
	return runAnalysisWithParser(paths, opts, jsonOutput, binlog.NewParser())
}

// runAnalysisWithParser executes the analysis pipeline with an injected parser.
// This allows testing with mock parsers without requiring real binlog files.
func runAnalysisWithParser(paths []string, opts analyzer.Options, jsonOutput bool, parser binlog.Parser) error {
	// Step 1: Parse binlog files and collect raw events
	var events []model.NormalizedEvent

	if err := parser.ParseFiles(paths, func(raw binlog.RawEvent) error {
		normalized, err := binlog.NormalizeRawEvent(raw)
		if err != nil {
			return fmt.Errorf("normalize error at position %d: %w", raw.Position, err)
		}
		if normalized != nil {
			events = append(events, *normalized)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("parse error: %w", err)
	}

	// Step 2: Run analyzer
	a := analyzer.New(opts)
	result, err := a.Analyze(events)
	if err != nil {
		return fmt.Errorf("analysis error: %w", err)
	}

	// Step 3: Apply top limits before rendering
	applyTopLimits(result, opts)

	// Step 4: Render output
	if jsonOutput {
		return report.RenderJSONToStdout(*result)
	}
	return report.RenderTextToStdout(*result)
}

// applyTopLimits truncates tables and transactions to the configured limits.
// This is done in the command layer to keep analyzer returning complete results.
func applyTopLimits(result *model.AnalysisResult, opts analyzer.Options) {
	if opts.TopTables > 0 && len(result.Tables) > opts.TopTables {
		result.Tables = result.Tables[:opts.TopTables]
	}
	if opts.TopTransactions > 0 && len(result.Transactions) > opts.TopTransactions {
		// Sort by TotalRows descending, with TxnKey ascending as tie-breaker for determinism
		sorted := make([]model.Transaction, len(result.Transactions))
		copy(sorted, result.Transactions)
		sort.Slice(sorted, func(i, j int) bool {
			if sorted[i].TotalRows != sorted[j].TotalRows {
				return sorted[i].TotalRows > sorted[j].TotalRows
			}
			return sorted[i].TxnKey < sorted[j].TxnKey
		})
		result.Transactions = sorted[:opts.TopTransactions]
	}
}

// parseTimeRange parses start and end time strings into time.Time values.
func parseTimeRange(startStr, endStr string) (time.Time, time.Time, error) {
	var startTime, endTime time.Time
	var err error
	if startStr != "" {
		startTime, err = time.Parse(time.RFC3339, startStr)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("invalid start time format: %w (use RFC3339)", err)
		}
	}
	if endStr != "" {
		endTime, err = time.Parse(time.RFC3339, endStr)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("invalid end time format: %w (use RFC3339)", err)
		}
	}
	// Validate that end is after start if both are specified
	if !startTime.IsZero() && !endTime.IsZero() && endTime.Before(startTime) {
		return time.Time{}, time.Time{}, fmt.Errorf("end time must be after start time")
	}
	return startTime, endTime, nil
}

// buildAnalyzerOptions converts CLI options to analyzer.Options.
// It starts from DefaultOptions() to ensure spike detection parameters
// have sensible defaults even when only --detect-spikes is passed.
func buildAnalyzerOptions(opts *analyzeOptions, startTime, endTime time.Time) analyzer.Options {
	// Start with defaults to get spike detection defaults
	result := analyzer.DefaultOptions()

	// Override with CLI-specific values
	result.TopTables = opts.topTables
	result.TopTransactions = opts.topTransactions
	result.DetectSpikes = opts.detectSpikes
	result.LargeTxnRows = opts.largeTrxRows
	result.LargeTxnDuration = opts.largeTrxDuration

	// Set time window if specified
	if !startTime.IsZero() {
		result.Start = &startTime
	}
	if !endTime.IsZero() {
		result.End = &endTime
	}

	return result
}
