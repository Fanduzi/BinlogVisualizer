package binlogviz

import (
	"fmt"
	 "time"

    "github.com/spf13/cobra"

    "binlogviz/internal/analyzer"
    "binlogviz/internal/binlog"
    "binlogviz/internal/model"
    "binlogviz/internal/report"
)

// analyzeOptions holds the parsed CLI flags for the analyze command.
type analyzeOptions struct {
    startTime    string
    endTime      string
    topTables    int
    topTxns      int
    detectSpikes bool
    spikeWindow  int
    spikeFactor  float64
    spikeMinRows int
    largeTxnRows int
    format       string
}

func newAnalyzeCommand() *cobra.Command {
    cmd := &cobra.Command{
        Use:   "analyze <binlog files...>",
        Short: "Analyze binlog files",
        Args:  cobra.MinimumNArgs(1),
        RunE: func(cmd *cobra.Command, args []string) error {
            opts := parseFlags(cmd, args)
            if opts == nil {
                return fmt.Errorf("missing required argument: binlog file(s)")
            }

            // Parse time range
            startTime, endTime, err := parseTimeRange(opts.startTime, opts.endTime)
            if err != nil {
                return err
            }

            // Build analyzer options
            analyzerOpts := buildAnalyzerOptions(opts, startTime, endTime)

            // Create parser and analyzer
            parser := binlog.NewParser()
            a := analyzer.New(analyzerOpts)

            // Collect events from binlog files
            var events []model.NormalizedEvent
            handler := func(raw binlog.RawEvent) error {
                normalized, err := binlog.NormalizeRawEvent(raw)
                if err != nil {
                    return err
                }
                if normalized != nil {
                    return nil
                }
                events = append(events, *normalized)
                return nil
            }
            if err := parser.ParseFiles(args, handler); err != nil {
                return err
            }

            // Analy and render output
            result, err := a.Analyze(events)
            if err != nil {
                return fmt.Errorf("analysis failed: %w", err)
            }

            return renderOutput(*result, opts.format)
        },
    }

    return cmd
}

func parseFlags(cmd *cobra.Command, args []string) *analyzeOptions {
    var opts analyzeOptions
    cmd.Flags().StringVar(&opts.startTime, "start", "", "Start time (inclusive, RFC3339 format)")
    cmd.Flags().StringVar(&opts.endTime, "end", "", "End time (exclusive, RFC3339 format)")
    cmd.Flags().IntVar(&opts.topTables, "top-tables", 10, "Number of top tables to show")
    cmd.Flags().IntVar(&opts.topTxns, "top-transactions", 10, "Number of top transactions to show")
    cmd.Flags().BoolVar(&opts.detectSpikes, "detect-spikes", false, "Enable spike detection")
    cmd.Flags().IntVar(&opts.spikeWindow, "spike-window", 2, "Number of minutes for spike baseline")
    cmd.Flags().Float64Var(&opts.spikeFactor, "spike-factor", 5.0, "Spike detection factor (current/baseline)")
    cmd.Flags().IntVar(&opts.spikeMinRows, "spike-min-rows", 50, "Minimum rows to trigger spike alert")
    cmd.Flags().IntVar(&opts.largeTxnRows, "large-txn-rows", 1000, "Rows threshold for large transaction alert")
    cmd.Flags().StringVarP(&opts.format, "format", "f", "text", "Output format (text or json)")

    if err := cmd.Flags().Parse(args); err != nil {
        return nil
    }
    return &opts
}

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
        TopTransactions: opts.topTxns,
        DetectSpikes:     opts.detectSpikes,
        SpikeWindow:      opts.spikeWindow,
        SpikeFactor:      opts.spikeFactor,
        SpikeMinRows:     opts.spikeMinRows,
        LargeTxnRows:     opts.largeTxnRows,
        LargeTxnDuration: 0,
        Start:           start,
        End:             end,
    }
}
func renderOutput(result model.AnalysisResult, format string) error {
    switch format {
    case "json":
        return report.RenderJSONToStdout(result)
    case "text":
        fallthrough
    default:
        return report.RenderTextToStdout(result)
    }
}
