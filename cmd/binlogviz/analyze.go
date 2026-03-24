// Package binlogviz defines the analyze CLI command and manages command-scoped DuckDB temp-store lifecycle.
// input: CLI flags, binlog file paths, parser callbacks, and command-owned temporary directory roots.
// output: rendered text/JSON analysis reports plus command-level creation and cleanup of temporary DuckDB stores.
// pos: CLI orchestration layer between parser normalization, analyzer execution, and final report rendering.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"

	"binlogviz/internal/analyzer"
	"binlogviz/internal/binlog"
	"binlogviz/internal/model"
	"binlogviz/internal/report"
)

type commandAnalyzer interface {
	Consume(model.NormalizedEvent) error
	Finalize() (*model.AnalysisResult, error)
}

type normalizeRawEventFunc func(binlog.RawEvent) (*model.NormalizedEvent, error)
type commandAnalyzerFactory func(opts analyzer.Options, store *analyzer.DuckDBStore) commandAnalyzer
type tempStoreFactory func(root string) (*analyzer.DuckDBStore, func() error, string, error)

type aggregateProgress struct {
	bar          *progressbar.ProgressBar
	fileSizes    []int64
	offsets      []int64
	statusWriter io.Writer
}

// analyzeOptions holds the parsed CLI flags for the analyze command.
type analyzeOptions struct {
	startTime        string
	endTime          string
	json             bool
	sqlContext       string
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

			reportOpts, err := buildReportOptions(opts)
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
			return runAnalysisWithReportOptions(args, analyzerOpts, reportOpts, opts.json)
		},
	}

	// Register flags
	cmd.Flags().StringVar(&opts.startTime, "start", "", "Start time (inclusive, RFC3339 format)")
	cmd.Flags().StringVar(&opts.endTime, "end", "", "End time (inclusive, RFC3339 format)")
	cmd.Flags().BoolVar(&opts.json, "json", false, "Output in JSON format")
	cmd.Flags().StringVar(&opts.sqlContext, "sql-context", string(report.SQLContextSummary), "SQL context mode: summary, off, or full")
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
	return runAnalysisWithReportOptions(paths, opts, report.DefaultOptions(), jsonOutput)
}

func runAnalysisWithReportOptions(paths []string, opts analyzer.Options, reportOpts report.Options, jsonOutput bool) error {
	return runAnalysisWithParserAndTempDirAndReportOptions(paths, opts, reportOpts, jsonOutput, binlog.NewParser(), "", nil)
}

// runAnalysisWithParser executes the analysis pipeline with an injected parser.
// This allows testing with mock parsers without requiring real binlog files.
func runAnalysisWithParser(paths []string, opts analyzer.Options, jsonOutput bool, parser binlog.Parser) error {
	return runAnalysisWithParserAndTempDirAndReportOptions(paths, opts, report.DefaultOptions(), jsonOutput, parser, "", nil)
}

func runAnalysisWithParserAndTempDir(paths []string, opts analyzer.Options, jsonOutput bool, parser binlog.Parser, tempRoot string, onStoreCreated func(string)) error {
	return runAnalysisWithParserAndTempDirAndReportOptions(paths, opts, report.DefaultOptions(), jsonOutput, parser, tempRoot, onStoreCreated)
}

func runAnalysisWithParserAndTempDirAndReportOptions(paths []string, opts analyzer.Options, reportOpts report.Options, jsonOutput bool, parser binlog.Parser, tempRoot string, onStoreCreated func(string)) error {
	return runAnalysisStreamingWithDeps(paths, opts, reportOpts, jsonOutput, parser, binlog.NormalizeRawEvent, func(opts analyzer.Options, store *analyzer.DuckDBStore) commandAnalyzer {
		return analyzer.NewWithStore(opts, store)
	}, func(root string) (*analyzer.DuckDBStore, func() error, string, error) {
		store, cleanup, path, err := createDuckDBTempStore(root)
		if err == nil && onStoreCreated != nil {
			onStoreCreated(path)
		}
		return store, cleanup, path, err
	}, tempRoot)
}

func totalInputBytes(paths []string) (int64, []int64) {
	fileSizes := make([]int64, len(paths))
	total := int64(0)
	for index, path := range paths {
		info, err := os.Stat(path)
		if err != nil {
			return 0, nil
		}
		size := info.Size()
		fileSizes[index] = size
		total += size
	}
	return total, fileSizes
}

func newAggregateProgress(paths []string, out io.Writer) (*aggregateProgress, error) {
	totalBytes, fileSizes := totalInputBytes(paths)
	if totalBytes <= 0 || len(fileSizes) == 0 {
		return &aggregateProgress{fileSizes: fileSizes, offsets: make([]int64, len(paths)), statusWriter: out}, nil
	}
	bar := progressbar.NewOptions64(
		totalBytes,
		progressbar.OptionSetWriter(out),
		progressbar.OptionSetDescription("Parsing binlogs"),
		progressbar.OptionSetWidth(20),
		progressbar.OptionShowBytes(true),
		progressbar.OptionSetRenderBlankState(true),
		progressbar.OptionThrottle(65*time.Millisecond),
	)
	return &aggregateProgress{bar: bar, fileSizes: fileSizes, offsets: make([]int64, len(paths)), statusWriter: out}, nil
}

func (p *aggregateProgress) Advance(progress binlog.ParseProgress) {
	if p == nil {
		return
	}
	if progress.Index < 0 || progress.Index >= len(p.fileSizes) {
		return
	}
	fileSize := p.fileSizes[progress.Index]
	offset := progress.Offset
	if offset < 0 {
		offset = 0
	}
	if fileSize > 0 && offset > fileSize {
		offset = fileSize
	}
	if offset < p.offsets[progress.Index] {
		offset = p.offsets[progress.Index]
	}
	p.offsets[progress.Index] = offset
	if p.bar != nil {
		_ = p.bar.Set64(p.currentTotal())
	}
}

func (p *aggregateProgress) currentTotal() int64 {
	total := int64(0)
	for _, offset := range p.offsets {
		total += offset
	}
	return total
}

func (p *aggregateProgress) FinishFile(index int) {
	if p == nil || index < 0 || index >= len(p.fileSizes) {
		return
	}
	p.Advance(binlog.ParseProgress{Index: index, Offset: p.fileSizes[index]})
}

func (p *aggregateProgress) FinishParse() {
	if p == nil || p.bar == nil {
		return
	}
	_ = p.bar.Finish()
}

func (p *aggregateProgress) Finalizing() {
	if p == nil || p.statusWriter == nil {
		return
	}
	_, _ = fmt.Fprintln(p.statusWriter, "Finalizing analysis...")
}

func runAnalysisStreamingWithDeps(
	paths []string,
	opts analyzer.Options,
	reportOpts report.Options,
	jsonOutput bool,
	parser binlog.Parser,
	normalize normalizeRawEventFunc,
	newAnalyzer commandAnalyzerFactory,
	newTempStore tempStoreFactory,
	tempRoot string,
) error {
	progress, err := newAggregateProgress(paths, os.Stderr)
	if err != nil {
		return fmt.Errorf("build parse progress: %w", err)
	}

	store, cleanup, _, err := newTempStore(tempRoot)
	if err != nil {
		return fmt.Errorf("create temp DuckDB store: %w", err)
	}
	defer cleanup()

	streamAnalyzer := newAnalyzer(opts, store)

	handler := func(raw binlog.RawEvent) error {
		normalized, err := normalize(raw)
		if err != nil {
			return fmt.Errorf("normalize error at position %d: %w", raw.Position, err)
		}
		if normalized == nil {
			return nil
		}
		if err := streamAnalyzer.Consume(*normalized); err != nil {
			return fmt.Errorf("analysis consume error: %w", err)
		}
		return nil
	}

	if progressParser, ok := parser.(binlog.ProgressParser); ok {
		if err := progressParser.ParseFilesWithProgress(paths, func(progressEvent binlog.ParseProgress) {
			progress.Advance(progressEvent)
		}, handler); err != nil {
			return fmt.Errorf("parse error: %w", err)
		}
		for index := range paths {
			progress.FinishFile(index)
		}
	} else {
		if err := parser.ParseFiles(paths, handler); err != nil {
			return fmt.Errorf("parse error: %w", err)
		}
	}
	progress.FinishParse()
	progress.Finalizing()

	result, err := streamAnalyzer.Finalize()
	if err != nil {
		return fmt.Errorf("analysis finalize error: %w", err)
	}

	if jsonOutput {
		return report.RenderJSONToStdoutWithOptions(*result, reportOpts)
	}
	return report.RenderTextToStdoutWithOptions(*result, reportOpts)
}

func createDuckDBTempStore(root string) (*analyzer.DuckDBStore, func() error, string, error) {
	tempDir, err := os.MkdirTemp(root, "binlogviz-duckdb-*")
	if err != nil {
		return nil, nil, "", err
	}
	path := filepath.Join(tempDir, "analysis.duckdb")
	store, err := analyzer.NewDuckDBStore(path, analyzer.DefaultBatchFlushRows)
	if err != nil {
		_ = os.RemoveAll(tempDir)
		return nil, nil, "", err
	}
	cleanup := func() error {
		closeErr := store.Close()
		removeErr := os.RemoveAll(tempDir)
		if closeErr != nil {
			return closeErr
		}
		return removeErr
	}
	return store, cleanup, path, nil
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

func buildReportOptions(opts *analyzeOptions) (report.Options, error) {
	mode, err := report.ParseSQLContextMode(opts.sqlContext)
	if err != nil {
		return report.Options{}, err
	}
	return report.Options{SQLContextMode: mode}, nil
}
