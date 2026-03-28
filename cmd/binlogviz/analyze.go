// Package binlogviz defines the analyze CLI command and manages command-scoped DuckDB temp-store lifecycle.
// input: CLI flags, explicit binlog file paths or discovery flags, parser callbacks, and command-owned temporary directory roots.
// output: rendered text/JSON analysis reports plus command-level creation and cleanup of temporary DuckDB stores.
// pos: CLI orchestration layer between input resolution, parser normalization, analyzer execution, and final report rendering.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"

	"binlogviz/internal/analyzer"
	"binlogviz/internal/binlog"
	"binlogviz/internal/i18n"
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
	fromDir          string
	prefix           string
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
		Use:   i18n.T("cmd.analyze.use"),
		Short: i18n.T("cmd.analyze.short"),
		Args: func(cmd *cobra.Command, args []string) error {
			hasArgs := len(args) > 0
			hasFromDir := opts.fromDir != ""
			hasPrefix := opts.prefix != ""
			if hasArgs && (hasFromDir || hasPrefix) {
				return fmt.Errorf("%s", i18n.T("error.combineArgsWithDir"))
			}
			if hasFromDir != hasPrefix {
				return fmt.Errorf("%s", i18n.T("error.fromDirAndPrefixRequired"))
			}
			if hasArgs || hasFromDir {
				return nil
			}
			return cobra.MinimumNArgs(1)(cmd, args)
		},
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

			paths, discovered, err := resolveAnalyzePaths(args, opts)
			if err != nil {
				return err
			}
			if discovered {
				printResolvedPaths(os.Stderr, paths)
			}

			// Validate input files exist
			if err := validateFiles(paths); err != nil {
				return err
			}

			// Build analyzer options
			analyzerOpts := buildAnalyzerOptions(opts, startTime, endTime)

			// Execute the analysis pipeline
			return runAnalysisWithReportOptions(paths, analyzerOpts, reportOpts, opts.json)
		},
	}

	// Register flags
	cmd.Flags().StringVar(&opts.startTime, "start", "", i18n.T("cmd.analyze.flag.start"))
	cmd.Flags().StringVar(&opts.endTime, "end", "", i18n.T("cmd.analyze.flag.end"))
	cmd.Flags().StringVar(&opts.fromDir, "from-dir", "", i18n.T("cmd.analyze.flag.fromDir"))
	cmd.Flags().StringVar(&opts.prefix, "prefix", "", i18n.T("cmd.analyze.flag.prefix"))
	cmd.Flags().BoolVar(&opts.json, "json", false, i18n.T("cmd.analyze.flag.json"))
	cmd.Flags().StringVar(&opts.sqlContext, "sql-context", string(report.SQLContextSummary), i18n.T("cmd.analyze.flag.sqlContext"))
	cmd.Flags().IntVar(&opts.topTables, "top-tables", 10, i18n.T("cmd.analyze.flag.topTables"))
	cmd.Flags().IntVar(&opts.topTransactions, "top-transactions", 10, i18n.T("cmd.analyze.flag.topTransactions"))
	cmd.Flags().BoolVar(&opts.detectSpikes, "detect-spikes", false, i18n.T("cmd.analyze.flag.detectSpikes"))
	cmd.Flags().IntVar(&opts.largeTrxRows, "large-trx-rows", 1000, i18n.T("cmd.analyze.flag.largeTrxRows"))
	cmd.Flags().DurationVar(&opts.largeTrxDuration, "large-trx-duration", 30*time.Second, i18n.T("cmd.analyze.flag.largeTrxDuration"))

	return cmd
}

func resolveAnalyzePaths(args []string, opts *analyzeOptions) ([]string, bool, error) {
	hasArgs := len(args) > 0
	hasFromDir := opts.fromDir != ""
	hasPrefix := opts.prefix != ""

	if hasArgs && (hasFromDir || hasPrefix) {
		return nil, false, fmt.Errorf("%s", i18n.T("error.combineArgsWithDir"))
	}
	if hasFromDir != hasPrefix {
		return nil, false, fmt.Errorf("%s", i18n.T("error.fromDirAndPrefixRequired"))
	}
	if hasArgs {
		return args, false, nil
	}
	if hasFromDir {
		paths, err := discoverBinlogPaths(opts.fromDir, opts.prefix)
		if err != nil {
			return nil, false, err
		}
		return paths, true, nil
	}
	return nil, false, fmt.Errorf("%s", i18n.T("error.requiresBinlogOrDir"))
}

func discoverBinlogPaths(dir, prefix string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("%s", i18n.Tf("error.readBinlogDir", map[string]any{"Error": err.Error()}))
	}

	candidates := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || entry.Type()&os.ModeSymlink != 0 {
			continue
		}
		name := entry.Name()
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		suffix := strings.TrimPrefix(name, prefix)
		if suffix == "" || !isDigits(suffix) {
			continue
		}
		candidates = append(candidates, filepath.Join(dir, name))
	}

	sortBinlogPaths(candidates, prefix)
	if len(candidates) == 0 {
		return nil, fmt.Errorf("%s", i18n.Tf("error.noMatchingFiles", map[string]any{"Dir": dir, "Prefix": prefix}))
	}
	return candidates, nil
}

func sortBinlogPaths(paths []string, prefix string) {
	sort.SliceStable(paths, func(i, j int) bool {
		leftBase := filepath.Base(paths[i])
		rightBase := filepath.Base(paths[j])
		leftSuffix := strings.TrimPrefix(leftBase, prefix)
		rightSuffix := strings.TrimPrefix(rightBase, prefix)
		leftValue, leftErr := strconv.ParseInt(leftSuffix, 10, 64)
		rightValue, rightErr := strconv.ParseInt(rightSuffix, 10, 64)
		if leftErr == nil && rightErr == nil && leftValue != rightValue {
			return leftValue < rightValue
		}
		return leftBase < rightBase
	})
}

func isDigits(value string) bool {
	for _, ch := range value {
		if ch < '0' || ch > '9' {
			return false
		}
	}
	return value != ""
}

func printResolvedPaths(out io.Writer, paths []string) {
	_, _ = fmt.Fprintln(out, i18n.T("progress.resolvedFiles"))
	for _, path := range paths {
		_, _ = fmt.Fprintf(out, "- %s\n", path)
	}
}

// validateFiles checks that all input files are accessible.
func validateFiles(paths []string) error {
	for _, path := range paths {
		if _, err := os.Stat(path); err != nil {
			if os.IsNotExist(err) {
				return fmt.Errorf("%s", i18n.Tf("error.fileNotFound", map[string]any{"Path": path}))
			}
			return fmt.Errorf("%s", i18n.Tf("error.cannotAccessFile", map[string]any{"Path": path, "Error": err.Error()}))
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
		progressbar.OptionSetDescription(i18n.T("progress.parsingBinlogs")),
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
	_, _ = fmt.Fprintln(p.statusWriter, i18n.T("progress.finalizingAnalysis"))
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
		return fmt.Errorf("%s", i18n.Tf("error.buildParseProgress", map[string]any{"Error": err.Error()}))
	}

	store, cleanup, _, err := newTempStore(tempRoot)
	if err != nil {
		return fmt.Errorf("%s", i18n.Tf("error.createTempStore", map[string]any{"Error": err.Error()}))
	}
	defer cleanup()

	streamAnalyzer := newAnalyzer(opts, store)

	handler := func(raw binlog.RawEvent) error {
		normalized, err := normalize(raw)
		if err != nil {
			return fmt.Errorf("%s", i18n.Tf("error.normalizeError", map[string]any{"Position": raw.Position, "Error": err.Error()}))
		}
		if normalized == nil {
			return nil
		}
		if err := streamAnalyzer.Consume(*normalized); err != nil {
			return fmt.Errorf("%s", i18n.Tf("error.analysisConsumeError", map[string]any{"Error": err.Error()}))
		}
		return nil
	}

	if progressParser, ok := parser.(binlog.ProgressParser); ok {
		if err := progressParser.ParseFilesWithProgress(paths, func(progressEvent binlog.ParseProgress) {
			progress.Advance(progressEvent)
		}, handler); err != nil {
			return fmt.Errorf("%s", i18n.Tf("error.parseError", map[string]any{"Error": err.Error()}))
		}
		for index := range paths {
			progress.FinishFile(index)
		}
	} else {
		if err := parser.ParseFiles(paths, handler); err != nil {
			return fmt.Errorf("%s", i18n.Tf("error.parseError", map[string]any{"Error": err.Error()}))
		}
	}
	progress.FinishParse()
	progress.Finalizing()

	result, err := streamAnalyzer.Finalize()
	if err != nil {
		return fmt.Errorf("%s", i18n.Tf("error.analysisFinalizeError", map[string]any{"Error": err.Error()}))
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
			return time.Time{}, time.Time{}, fmt.Errorf("%s", i18n.Tf("error.invalidStartTime", map[string]any{"Error": err.Error()}))
		}
	}
	if endStr != "" {
		endTime, err = time.Parse(time.RFC3339, endStr)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("%s", i18n.Tf("error.invalidEndTime", map[string]any{"Error": err.Error()}))
		}
	}
	// Validate that end is after start if both are specified
	if !startTime.IsZero() && !endTime.IsZero() && endTime.Before(startTime) {
		return time.Time{}, time.Time{}, fmt.Errorf("%s", i18n.T("error.endTimeBeforeStart"))
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
