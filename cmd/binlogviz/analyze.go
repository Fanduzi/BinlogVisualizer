package binlogviz

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"binlogviz/internal/analyzer"
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

			// Build analyzer options (validation only - actual execution in Task 15)
			_ = buildAnalyzerOptions(opts, startTime, endTime)
			_ = args // placeholder - actual processing in Task 15

			return nil
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

// buildAnalyzerOptions converts CLI options to analyzer.Options
func buildAnalyzerOptions(opts *analyzeOptions, startTime, endTime time.Time) analyzer.Options {
	var start, end *time.Time
	if !startTime.IsZero() {
		start = &startTime
	}
	if !endTime.IsZero() {
		end = &endTime
	 }
	return analyzer.Options{
		TopTables:        opts.topTables,
		TopTransactions:  opts.topTransactions,
		DetectSpikes:     opts.detectSpikes,
		LargeTxnRows:     opts.largeTrxRows,
		LargeTxnDuration: opts.largeTrxDuration,
		Start:            start,
		End:              end,
	}
}
