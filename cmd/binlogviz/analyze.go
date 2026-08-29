// Package binlogviz defines the analyze CLI command and manages command-scoped DuckDB temp-store lifecycle.
// input: CLI flags, explicit binlog file paths or discovery flags, parser callbacks (including Format Description server version), and command-owned temporary directory roots.
// output: rendered text/JSON/HTML analysis reports on success; STATEMENT (Query-DML, zero ROW images) returns an error with no report; stderr-only operator status and DuckDB temp-store cleanup.
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
	"binlogviz/internal/snapshot"
	"binlogviz/internal/version"
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

var inspectFirstBinlogTimestamp = readFirstBinlogTimestamp

// analyzeOptions holds the parsed CLI flags for the analyze command.
type analyzeOptions struct {
	startTime              string
	endTime                string
	fromDir                string
	prefix                 string
	format                 string
	output                 string
	snapshotName           string
	snapshotDir            string
	sqlContext             string
	top                    int
	topTables              int
	topTransactions        int
	details                bool
	showMinutes            bool
	showPatterns           bool
	detectSpikes           bool
	largeTrxRows           int
	largeTrxDuration       time.Duration
	topMinutes             int
	spikeWindow            int
	spikeFactor            float64
	spikeMinRows           int
	includeSchemas         []string
	excludeSchemas         []string
	includeTables          []string
	excludeTables          []string
	topTablesChanged       bool
	topTransactionsChanged bool
	detailStore            string
}

func newAnalyzeCommand() *cobra.Command {
	opts := &analyzeOptions{}

	cmd := &cobra.Command{
		Use:           i18n.T("cmd.analyze.use"),
		Short:         i18n.T("cmd.analyze.short"),
		SilenceUsage:  true,
		SilenceErrors: true,
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
			return fmt.Errorf("%s", i18n.T("error.requiresBinlogOrDir"))
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			// Parse time range
			startTime, endTime, err := parseTimeRange(opts.startTime, opts.endTime)
			if err != nil {
				return err
			}

			opts.topTablesChanged = cmd.Flags().Changed("top-tables")
			opts.topTransactionsChanged = cmd.Flags().Changed("top-transactions")

			reportOpts, err := buildReportOptions(opts)
			if err != nil {
				return err
			}
			if err := validateAnalyzeOptions(opts); err != nil {
				return err
			}

			paths, discovered, fileCoverage, err := resolveAnalyzePaths(args, opts)
			if err != nil {
				return err
			}
			if discovered {
				printResolvedPaths(os.Stderr, paths)
			}
			if len(paths) == 0 {
				return fmt.Errorf("%s", i18n.T("error.noResolvedFiles"))
			}

			// Validate input files exist
			if err := validateFiles(paths); err != nil {
				return err
			}

			// Resolve output destination
			dest, err := resolveOutputDestination(paths, discovered, opts.output, opts.format)
			if err != nil {
				return err
			}

			// Build analyzer options
			analyzerOpts := buildAnalyzerOptions(opts, startTime, endTime)
			snapshotMeta := buildSnapshotMetadata(paths, opts, startTime, endTime, discovered)

			// Execute the analysis pipeline
			return runAnalysisWithOutput(paths, analyzerOpts, reportOpts, opts.format, snapshotMeta, fileCoverage, opts.snapshotName, opts.snapshotDir, dest)
		},
	}

	// Register flags
	cmd.Flags().StringVar(&opts.startTime, "start", "", i18n.T("cmd.analyze.flag.start"))
	cmd.Flags().StringVar(&opts.endTime, "end", "", i18n.T("cmd.analyze.flag.end"))
	cmd.Flags().StringVar(&opts.fromDir, "from-dir", "", i18n.T("cmd.analyze.flag.fromDir"))
	cmd.Flags().StringVar(&opts.prefix, "prefix", "", i18n.T("cmd.analyze.flag.prefix"))
	cmd.Flags().StringVar(&opts.format, "format", "text", i18n.T("cmd.analyze.flag.format"))
	cmd.Flags().StringVarP(&opts.output, "output", "o", "", i18n.T("cmd.analyze.flag.output"))
	cmd.Flags().StringVar(&opts.snapshotName, "snapshot-name", "", "Save JSON analyze output as a named snapshot")
	cmd.Flags().StringVar(&opts.snapshotDir, "snapshot-dir", "", "Directory to save analyze snapshots")
	cmd.Flags().StringVar(&opts.sqlContext, "sql-context", string(report.SQLContextSummary), i18n.T("cmd.analyze.flag.sqlContext"))
	cmd.Flags().IntVar(&opts.top, "top", report.DefaultTopN, i18n.T("cmd.analyze.flag.top"))
	cmd.Flags().IntVar(&opts.topTables, "top-tables", 10, i18n.T("cmd.analyze.flag.topTables"))
	cmd.Flags().IntVar(&opts.topTransactions, "top-transactions", 10, i18n.T("cmd.analyze.flag.topTransactions"))
	cmd.Flags().BoolVar(&opts.details, "details", false, i18n.T("cmd.analyze.flag.details"))
	cmd.Flags().BoolVar(&opts.showMinutes, "show-minutes", false, i18n.T("cmd.analyze.flag.showMinutes"))
	cmd.Flags().BoolVar(&opts.showPatterns, "show-patterns", false, i18n.T("cmd.analyze.flag.showPatterns"))
	cmd.Flags().BoolVar(&opts.detectSpikes, "detect-spikes", false, i18n.T("cmd.analyze.flag.detectSpikes"))
	cmd.Flags().IntVar(&opts.largeTrxRows, "large-trx-rows", 1000, i18n.T("cmd.analyze.flag.largeTrxRows"))
	cmd.Flags().DurationVar(&opts.largeTrxDuration, "large-trx-duration", 30*time.Second, i18n.T("cmd.analyze.flag.largeTrxDuration"))
	cmd.Flags().IntVar(&opts.topMinutes, "top-minutes", 60, i18n.T("cmd.analyze.flag.topMinutes"))
	cmd.Flags().IntVar(&opts.spikeWindow, "spike-window", 5, i18n.T("cmd.analyze.flag.spikeWindow"))
	cmd.Flags().Float64Var(&opts.spikeFactor, "spike-factor", 3.0, i18n.T("cmd.analyze.flag.spikeFactor"))
	cmd.Flags().IntVar(&opts.spikeMinRows, "spike-min-rows", 100, i18n.T("cmd.analyze.flag.spikeMinRows"))
	cmd.Flags().StringSliceVar(&opts.includeSchemas, "include-schema", nil, i18n.T("cmd.analyze.flag.includeSchema"))
	cmd.Flags().StringSliceVar(&opts.excludeSchemas, "exclude-schema", nil, i18n.T("cmd.analyze.flag.excludeSchema"))
	cmd.Flags().StringSliceVar(&opts.includeTables, "include-table", nil, i18n.T("cmd.analyze.flag.includeTable"))
	cmd.Flags().StringSliceVar(&opts.excludeTables, "exclude-table", nil, i18n.T("cmd.analyze.flag.excludeTable"))
	cmd.Flags().StringVar(&opts.detailStore, "detail-store", string(analyzer.DetailStoreNone), i18n.T("cmd.analyze.flag.detailStore"))

	return cmd
}

func resolveAnalyzePaths(args []string, opts *analyzeOptions) ([]string, bool, model.FileCoverage, error) {
	hasArgs := len(args) > 0
	hasFromDir := opts.fromDir != ""
	hasPrefix := opts.prefix != ""

	if hasArgs && (hasFromDir || hasPrefix) {
		return nil, false, model.FileCoverage{}, fmt.Errorf("%s", i18n.T("error.combineArgsWithDir"))
	}
	if hasFromDir != hasPrefix {
		return nil, false, model.FileCoverage{}, fmt.Errorf("%s", i18n.T("error.fromDirAndPrefixRequired"))
	}
	if hasArgs {
		return args, false, model.FileCoverage{}, nil
	}
	if hasFromDir {
		startTime, endTime, err := parseTimeRange(opts.startTime, opts.endTime)
		if err != nil {
			return nil, false, model.FileCoverage{}, err
		}
		plan, err := discoverBinlogPlanInWindow(opts.fromDir, opts.prefix, startTime, endTime)
		if err != nil {
			return nil, false, model.FileCoverage{}, err
		}
		return plan.Paths, true, plan.FileCoverage, nil
	}
	return nil, false, model.FileCoverage{}, fmt.Errorf("%s", i18n.T("error.requiresBinlogOrDir"))
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
		if _, ok := binlogNumericSuffix(entry.Name(), prefix); !ok {
			continue
		}
		candidates = append(candidates, filepath.Join(dir, entry.Name()))
	}

	sortBinlogPaths(candidates, prefix)
	if len(candidates) == 0 {
		return nil, fmt.Errorf("%s", noMatchingBinlogFilesError(dir, prefix))
	}
	return candidates, nil
}

func discoverBinlogPathsInWindow(dir, prefix string, startTime, endTime time.Time) ([]string, error) {
	plan, err := discoverBinlogPlanInWindow(dir, prefix, startTime, endTime)
	if err != nil {
		return nil, err
	}
	return plan.Paths, nil
}

func discoverBinlogPlanInWindow(dir, prefix string, startTime, endTime time.Time) (analyzePlan, error) {
	paths, err := discoverBinlogPaths(dir, prefix)
	if err != nil {
		return analyzePlan{}, err
	}
	coarsePaths, err := coarseFilterPathsByModTime(paths, startTime, endTime)
	if err != nil {
		return analyzePlan{}, err
	}
	if len(coarsePaths) == 0 {
		return analyzePlan{Paths: coarsePaths, WorkerCount: 1}, nil
	}

	workers := defaultAnalyzeProbeWorkers(len(coarsePaths))
	probes, err := probeAnalyzePaths(coarsePaths, workers)
	if err != nil {
		return analyzePlan{}, err
	}
	return buildAnalyzePlan(probes, startTime, endTime, workers), nil
}

func sortBinlogPaths(paths []string, prefix string) {
	sort.SliceStable(paths, func(i, j int) bool {
		leftBase := filepath.Base(paths[i])
		rightBase := filepath.Base(paths[j])
		leftSuffix, leftOK := binlogNumericSuffix(leftBase, prefix)
		rightSuffix, rightOK := binlogNumericSuffix(rightBase, prefix)
		if leftOK && rightOK {
			leftValue, leftErr := strconv.ParseInt(leftSuffix, 10, 64)
			rightValue, rightErr := strconv.ParseInt(rightSuffix, 10, 64)
			if leftErr == nil && rightErr == nil && leftValue != rightValue {
				return leftValue < rightValue
			}
		}
		return leftBase < rightBase
	})
}

// binlogNumericSuffix returns the numeric sequence after prefix.
// An optional '.' between the prefix and the digits is accepted so
// --prefix mysql-bin matches mysql-bin.000008 (log_bin_basename style).
func binlogNumericSuffix(name, prefix string) (string, bool) {
	if !strings.HasPrefix(name, prefix) {
		return "", false
	}
	suffix := strings.TrimPrefix(name, prefix)
	if suffix == "" {
		return "", false
	}
	if suffix[0] == '.' {
		suffix = suffix[1:]
	}
	if !isDigits(suffix) {
		return "", false
	}
	return suffix, true
}

func isDigits(value string) bool {
	for _, ch := range value {
		if ch < '0' || ch > '9' {
			return false
		}
	}
	return value != ""
}

func noMatchingBinlogFilesError(dir, prefix string) string {
	msg := i18n.Tf("error.noMatchingFiles", map[string]any{"Dir": dir, "Prefix": prefix})
	if prefix != "" && !strings.HasSuffix(prefix, ".") {
		msg += "; " + i18n.Tf("error.prefixDotHint", map[string]any{"Prefix": prefix + "."})
	}
	return msg
}

func readFirstBinlogTimestamp(path string) (time.Time, error) {
	probe, err := binlog.ProbeFile(path)
	if err != nil {
		return time.Time{}, err
	}
	return probe.FirstEventAt, nil
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
func runAnalysis(paths []string, opts analyzer.Options, format string) error {
	return runAnalysisWithReportOptions(paths, opts, report.DefaultOptions(), format)
}

func runAnalysisWithReportOptions(paths []string, opts analyzer.Options, reportOpts report.Options, format string) error {
	return runAnalysisWithReportAndSnapshotOptions(paths, opts, reportOpts, format, nil, model.FileCoverage{}, "", "")
}

func runAnalysisWithReportAndSnapshotOptions(paths []string, opts analyzer.Options, reportOpts report.Options, format string, snapshotMeta *model.Snapshot, fileCoverage model.FileCoverage, snapshotName, snapshotDir string) error {
	return runAnalysisWithParserAndTempDirAndReportAndSnapshotOptions(paths, opts, reportOpts, format, snapshotMeta, fileCoverage, snapshotName, snapshotDir, binlog.NewParser(), "", nil)
}

// runAnalysisWithParser executes the analysis pipeline with an injected parser.
// This allows testing with mock parsers without requiring real binlog files.
func runAnalysisWithParser(paths []string, opts analyzer.Options, format string, parser binlog.Parser) error {
	return runAnalysisWithParserAndTempDirAndReportOptions(paths, opts, report.DefaultOptions(), format, parser, "", nil)
}

func runAnalysisWithParserAndTempDir(paths []string, opts analyzer.Options, format string, parser binlog.Parser, tempRoot string, onStoreCreated func(string)) error {
	return runAnalysisWithParserAndTempDirAndReportOptions(paths, opts, report.DefaultOptions(), format, parser, tempRoot, onStoreCreated)
}

func runAnalysisWithParserAndTempDirAndReportOptions(paths []string, opts analyzer.Options, reportOpts report.Options, format string, parser binlog.Parser, tempRoot string, onStoreCreated func(string)) error {
	return runAnalysisWithParserAndTempDirAndReportAndSnapshotOptions(paths, opts, reportOpts, format, nil, model.FileCoverage{}, "", "", parser, tempRoot, onStoreCreated)
}

func runAnalysisWithParserAndTempDirAndReportAndSnapshotOptions(paths []string, opts analyzer.Options, reportOpts report.Options, format string, snapshotMeta *model.Snapshot, fileCoverage model.FileCoverage, snapshotName, snapshotDir string, parser binlog.Parser, tempRoot string, onStoreCreated func(string)) error {
	return runAnalysisStreamingFastWithSnapshot(paths, opts, reportOpts, format, parser, func(opts analyzer.Options, store *analyzer.DuckDBStore) commandAnalyzer {
		if opts.DetailStoreMode == analyzer.DetailStoreDuckDB {
			return analyzer.NewWithStore(opts, store)
		}
		return analyzer.New(opts)
	}, func(root string) (*analyzer.DuckDBStore, func() error, string, error) {
		store, cleanup, path, err := createDuckDBTempStore(root)
		if err == nil && onStoreCreated != nil {
			onStoreCreated(path)
		}
		return store, cleanup, path, err
	}, tempRoot, snapshotMeta, fileCoverage, snapshotName, snapshotDir)
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
	format string,
	parser binlog.Parser,
	normalize normalizeRawEventFunc,
	newAnalyzer commandAnalyzerFactory,
	newTempStore tempStoreFactory,
	tempRoot string,
) error {
	return runAnalysisStreamingWithSnapshotDeps(paths, opts, reportOpts, format, parser, normalize, newAnalyzer, newTempStore, tempRoot, nil, model.FileCoverage{}, "", "")
}

func runAnalysisStreamingWithSnapshotDeps(
	paths []string,
	opts analyzer.Options,
	reportOpts report.Options,
	format string,
	parser binlog.Parser,
	normalize normalizeRawEventFunc,
	newAnalyzer commandAnalyzerFactory,
	newTempStore tempStoreFactory,
	tempRoot string,
	snapshotMeta *model.Snapshot,
	fileCoverage model.FileCoverage,
	snapshotName string,
	snapshotDir string,
) error {
	progress, err := newAggregateProgress(paths, os.Stderr)
	if err != nil {
		return fmt.Errorf("%s", i18n.Tf("error.buildParseProgress", map[string]any{"Error": err.Error()}))
	}

	var store *analyzer.DuckDBStore
	if opts.DetailStoreMode == analyzer.DetailStoreDuckDB {
		var cleanup func() error
		store, cleanup, _, err = newTempStore(tempRoot)
		if err != nil {
			return fmt.Errorf("%s", i18n.Tf("error.createTempStore", map[string]any{"Error": err.Error()}))
		}
		defer cleanup()
	}

	streamAnalyzer := newAnalyzer(opts, store)
	var formatObserver binlog.FormatObserver
	rawEvents := 0

	handler := func(raw binlog.RawEvent) error {
		formatObserver.Observe(raw)
		rawEvents++
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
		if err := parseFilesWithProgressParallelOrdered(paths, progressParser, defaultAnalyzeProbeWorkers(len(paths)), func(progressEvent binlog.ParseProgress) {
			progress.Advance(progressEvent)
		}, handler); err != nil {
			return wrapParseError(err)
		}
		for index := range paths {
			progress.FinishFile(index)
		}
	} else {
		if err := parser.ParseFiles(paths, handler); err != nil {
			return wrapParseError(err)
		}
	}
	progress.FinishParse()
	progress.Finalizing()

	result, err := streamAnalyzer.Finalize()
	if err != nil {
		return fmt.Errorf("%s", i18n.Tf("error.analysisFinalizeError", map[string]any{"Error": err.Error()}))
	}
	if err := applyAnalyzeOutcomeGuards(paths, opts, result, rawEvents, progress.statusWriter); err != nil {
		return err
	}
	if hasFileCoverage(fileCoverage) {
		result.Diagnostics.FileCoverage = fileCoverage
	}
	if err := noteInputFormat(result, formatObserver); err != nil {
		return err
	}

	var renderErr error
	switch format {
	case "json":
		if snapshotMeta != nil {
			result.Snapshot = snapshotMeta
		}
		if snapshotName != "" {
			renderErr = saveAndWriteJSONReport(*result, reportOpts, snapshotName, snapshotDir)
		} else {
			renderErr = report.RenderJSONToStdoutWithOptions(*result, reportOpts)
		}
	case "markdown", "md":
		renderErr = report.RenderMarkdownToStdoutWithOptions(*result, reportOpts)
	case "html":
		renderErr = report.RenderHTMLToStdout(*result, reportOpts)
	default:
		renderErr = report.RenderTextToStdoutWithOptions(*result, reportOpts)
	}
	if renderErr != nil {
		return renderErr
	}
	return nil
}

func runAnalysisStreamingFastWithSnapshot(
	paths []string,
	opts analyzer.Options,
	reportOpts report.Options,
	format string,
	parser binlog.Parser,
	newAnalyzer commandAnalyzerFactory,
	newTempStore tempStoreFactory,
	tempRoot string,
	snapshotMeta *model.Snapshot,
	fileCoverage model.FileCoverage,
	snapshotName string,
	snapshotDir string,
) error {
	progress, err := newAggregateProgress(paths, os.Stderr)
	if err != nil {
		return fmt.Errorf("%s", i18n.Tf("error.buildParseProgress", map[string]any{"Error": err.Error()}))
	}

	var store *analyzer.DuckDBStore
	if opts.DetailStoreMode == analyzer.DetailStoreDuckDB {
		var cleanup func() error
		store, cleanup, _, err = newTempStore(tempRoot)
		if err != nil {
			return fmt.Errorf("%s", i18n.Tf("error.createTempStore", map[string]any{"Error": err.Error()}))
		}
		defer cleanup()
	}

	streamAnalyzer := newAnalyzer(opts, store)
	var formatObserver binlog.FormatObserver
	rawEvents := 0

	handler := func(raw binlog.RawEvent) error {
		formatObserver.Observe(raw)
		rawEvents++
		var normalized model.NormalizedEvent
		ok, err := binlog.NormalizeRawEventInto(raw, &normalized)
		if err != nil {
			return fmt.Errorf("%s", i18n.Tf("error.normalizeError", map[string]any{"Position": raw.Position, "Error": err.Error()}))
		}
		if !ok {
			return nil
		}
		if err := streamAnalyzer.Consume(normalized); err != nil {
			return fmt.Errorf("%s", i18n.Tf("error.analysisConsumeError", map[string]any{"Error": err.Error()}))
		}
		return nil
	}

	if progressParser, ok := parser.(binlog.ProgressParser); ok {
		if err := parseFilesWithProgressParallelOrdered(paths, progressParser, defaultAnalyzeProbeWorkers(len(paths)), func(progressEvent binlog.ParseProgress) {
			progress.Advance(progressEvent)
		}, handler); err != nil {
			return wrapParseError(err)
		}
		for index := range paths {
			progress.FinishFile(index)
		}
	} else {
		if err := parser.ParseFiles(paths, handler); err != nil {
			return wrapParseError(err)
		}
	}
	progress.FinishParse()
	progress.Finalizing()

	result, err := streamAnalyzer.Finalize()
	if err != nil {
		return fmt.Errorf("%s", i18n.Tf("error.analysisFinalizeError", map[string]any{"Error": err.Error()}))
	}
	if err := applyAnalyzeOutcomeGuards(paths, opts, result, rawEvents, progress.statusWriter); err != nil {
		return err
	}
	if hasFileCoverage(fileCoverage) {
		result.Diagnostics.FileCoverage = fileCoverage
	}
	if err := noteInputFormat(result, formatObserver); err != nil {
		return err
	}

	var renderErr error
	switch format {
	case "json":
		if snapshotMeta != nil {
			result.Snapshot = snapshotMeta
		}
		if snapshotName != "" {
			renderErr = saveAndWriteJSONReport(*result, reportOpts, snapshotName, snapshotDir)
		} else {
			renderErr = report.RenderJSONToStdoutWithOptions(*result, reportOpts)
		}
	case "markdown", "md":
		renderErr = report.RenderMarkdownToStdoutWithOptions(*result, reportOpts)
	case "html":
		renderErr = report.RenderHTMLToStdout(*result, reportOpts)
	default:
		renderErr = report.RenderTextToStdoutWithOptions(*result, reportOpts)
	}
	if renderErr != nil {
		return renderErr
	}
	return nil
}

func hasFileCoverage(fileCoverage model.FileCoverage) bool {
	return len(fileCoverage.Selected) > 0 || len(fileCoverage.Skipped) > 0
}

func noteInputFormat(result *model.AnalysisResult, observer binlog.FormatObserver) error {
	if result == nil {
		return nil
	}
	result.Diagnostics.InputFormatGuess = observer.Guess()
	result.Diagnostics.IgnoredQueryDMLEvents = observer.QueryDMLEvents
	result.Diagnostics.ServerVersion = observer.ServerVersion
	if observer.QueryDMLEvents == 0 {
		return nil
	}
	if observer.RowImageEvents == 0 {
		return fmt.Errorf("%s", binlog.StatementOrMixedWarning)
	}
	_, _ = fmt.Fprintln(os.Stderr, binlog.StatementOrMixedWarning)
	return nil
}

func runAnalysisWithOutput(paths []string, opts analyzer.Options, reportOpts report.Options, format string, snapshotMeta *model.Snapshot, fileCoverage model.FileCoverage, snapshotName, snapshotDir string, dest outputDestination) error {
	return runAnalysisStreamingFastWithOutput(paths, opts, reportOpts, format, binlog.NewParser(), func(opts analyzer.Options, store *analyzer.DuckDBStore) commandAnalyzer {
		if opts.DetailStoreMode == analyzer.DetailStoreDuckDB {
			return analyzer.NewWithStore(opts, store)
		}
		return analyzer.New(opts)
	}, func(root string) (*analyzer.DuckDBStore, func() error, string, error) {
		return createDuckDBTempStore(root)
	}, "", snapshotMeta, fileCoverage, snapshotName, snapshotDir, dest)
}

func runAnalysisStreamingFastWithOutput(
	paths []string,
	opts analyzer.Options,
	reportOpts report.Options,
	format string,
	parser binlog.Parser,
	newAnalyzer commandAnalyzerFactory,
	newTempStore tempStoreFactory,
	tempRoot string,
	snapshotMeta *model.Snapshot,
	fileCoverage model.FileCoverage,
	snapshotName string,
	snapshotDir string,
	dest outputDestination,
) error {
	progress, err := newAggregateProgress(paths, os.Stderr)
	if err != nil {
		return fmt.Errorf("%s", i18n.Tf("error.buildParseProgress", map[string]any{"Error": err.Error()}))
	}

	var store *analyzer.DuckDBStore
	if opts.DetailStoreMode == analyzer.DetailStoreDuckDB {
		var cleanup func() error
		store, cleanup, _, err = newTempStore(tempRoot)
		if err != nil {
			return fmt.Errorf("%s", i18n.Tf("error.createTempStore", map[string]any{"Error": err.Error()}))
		}
		defer cleanup()
	}

	streamAnalyzer := newAnalyzer(opts, store)
	var formatObserver binlog.FormatObserver
	rawEvents := 0

	handler := func(raw binlog.RawEvent) error {
		formatObserver.Observe(raw)
		rawEvents++
		var normalized model.NormalizedEvent
		ok, err := binlog.NormalizeRawEventInto(raw, &normalized)
		if err != nil {
			return fmt.Errorf("%s", i18n.Tf("error.normalizeError", map[string]any{"Position": raw.Position, "Error": err.Error()}))
		}
		if !ok {
			return nil
		}
		if err := streamAnalyzer.Consume(normalized); err != nil {
			return fmt.Errorf("%s", i18n.Tf("error.analysisConsumeError", map[string]any{"Error": err.Error()}))
		}
		return nil
	}

	if progressParser, ok := parser.(binlog.ProgressParser); ok {
		if err := parseFilesWithProgressParallelOrdered(paths, progressParser, defaultAnalyzeProbeWorkers(len(paths)), func(progressEvent binlog.ParseProgress) {
			progress.Advance(progressEvent)
		}, handler); err != nil {
			return wrapParseError(err)
		}
		for index := range paths {
			progress.FinishFile(index)
		}
	} else {
		if err := parser.ParseFiles(paths, handler); err != nil {
			return wrapParseError(err)
		}
	}
	progress.FinishParse()
	progress.Finalizing()

	result, err := streamAnalyzer.Finalize()
	if err != nil {
		return fmt.Errorf("%s", i18n.Tf("error.analysisFinalizeError", map[string]any{"Error": err.Error()}))
	}
	if err := applyAnalyzeOutcomeGuards(paths, opts, result, rawEvents, progress.statusWriter); err != nil {
		return err
	}
	if hasFileCoverage(fileCoverage) {
		result.Diagnostics.FileCoverage = fileCoverage
	}
	if err := noteInputFormat(result, formatObserver); err != nil {
		return err
	}

	var renderErr error
	switch format {
	case "json":
		if snapshotMeta != nil {
			result.Snapshot = snapshotMeta
		}
		if snapshotName != "" {
			renderErr = saveAndWriteJSONReport(*result, reportOpts, snapshotName, snapshotDir)
		} else {
			renderErr = report.RenderJSONToStdoutWithOptions(*result, reportOpts)
		}
	case "markdown", "md":
		renderErr = report.RenderMarkdownToStdoutWithOptions(*result, reportOpts)
	case "html":
		htmlContent, err := report.RenderHTMLWithOptions(*result, reportOpts)
		if err != nil {
			return err
		}
		if err := writeHTMLAtomically(dest, htmlContent); err != nil {
			return err
		}
		if dest.IsFile {
			printHTMLSaveConfirmation(dest.Path)
		}
	default:
		renderErr = report.RenderTextToStdoutWithOptions(*result, reportOpts)
	}
	if renderErr != nil {
		return renderErr
	}
	return nil
}

func saveAndWriteJSONReport(result model.AnalysisResult, reportOpts report.Options, snapshotName, snapshotDir string) error {
	result.Snapshot = resolveAnalyzeSnapshotMetadata(result.Snapshot, result.Summary)

	payload, err := report.RenderJSONWithOptions(result, reportOpts)
	if err != nil {
		return err
	}

	savedPath, err := snapshot.SaveJSON(snapshotDir, snapshotName, []byte(payload))
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(os.Stderr, "Saved snapshot %q to %s\n", snapshotName, savedPath); err != nil {
		return err
	}
	_, err = fmt.Fprint(os.Stdout, payload)
	return err
}

func resolveAnalyzeSnapshotMetadata(snapshotMeta *model.Snapshot, summary model.WorkloadSummary) *model.Snapshot {
	if snapshotMeta == nil {
		return nil
	}

	resolved := *snapshotMeta
	if resolved.Window.StartTime.IsZero() {
		resolved.Window.StartTime = summary.StartTime
	}
	if resolved.Window.EndTime.IsZero() {
		resolved.Window.EndTime = summary.EndTime
	}
	return &resolved
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

	if opts.top > 0 && !opts.topTablesChanged {
		result.TopTables = opts.top
	}
	if opts.top > 0 && !opts.topTransactionsChanged {
		result.TopTransactions = opts.top
	}

	// Override with CLI-specific values (only when non-zero, to preserve DefaultOptions fallback)
	if opts.topTablesChanged || (opts.top == 0 && opts.topTables != 0) {
		result.TopTables = opts.topTables
	}
	if opts.topTransactionsChanged || (opts.top == 0 && opts.topTransactions != 0) {
		result.TopTransactions = opts.topTransactions
	}
	if opts.topMinutes != 0 {
		result.TopMinutes = opts.topMinutes
	}
	result.DetectSpikes = opts.detectSpikes
	if opts.largeTrxRows != 0 {
		result.LargeTxnRows = opts.largeTrxRows
	}
	if opts.largeTrxDuration != 0 {
		result.LargeTxnDuration = opts.largeTrxDuration
	}
	if opts.spikeWindow != 0 {
		result.SpikeWindow = opts.spikeWindow
	}
	if opts.spikeFactor != 0 {
		result.SpikeFactor = opts.spikeFactor
	}
	if opts.spikeMinRows != 0 {
		result.SpikeMinRows = opts.spikeMinRows
	}
	result.IncludeSchemas = opts.includeSchemas
	result.ExcludeSchemas = opts.excludeSchemas
	result.IncludeTables = opts.includeTables
	result.ExcludeTables = opts.excludeTables
	if mode := analyzer.DetailStoreMode(opts.detailStore); mode != "" {
		result.DetailStoreMode = mode
	}

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
	return report.Options{
		SQLContextMode: mode,
		TopN:           opts.top,
		Details:        opts.details,
		ShowMinutes:    opts.showMinutes,
		ShowPatterns:   opts.showPatterns,
	}, nil
}

func validateAnalyzeOptions(opts *analyzeOptions) error {
	switch analyzer.DetailStoreMode(opts.detailStore) {
	case analyzer.DetailStoreNone, analyzer.DetailStoreDuckDB:
	default:
		return fmt.Errorf("invalid --detail-store %q: expected none or duckdb", opts.detailStore)
	}
	if opts.snapshotName != "" && opts.format != "json" {
		return fmt.Errorf("--snapshot-name requires --format json")
	}
	if opts.snapshotName != "" {
		return snapshot.ValidateName(opts.snapshotName)
	}
	return nil
}

func buildSnapshotMetadata(paths []string, opts *analyzeOptions, startTime, endTime time.Time, discovered bool) *model.Snapshot {
	if opts.snapshotName == "" {
		return nil
	}

	inputMode := "explicit"
	if discovered {
		inputMode = "discovery"
	} else {
		inputMode = "files"
	}

	return &model.Snapshot{
		Name:             opts.snapshotName,
		Label:            opts.snapshotName,
		CreatedAt:        time.Now().UTC(),
		BinlogvizVersion: version.Version,
		InputMode:        inputMode,
		Input: model.SnapshotInput{
			Files:   append([]string(nil), paths...),
			FromDir: opts.fromDir,
			Prefix:  opts.prefix,
		},
		Window: model.SnapshotWindow{
			StartTime: startTime,
			EndTime:   endTime,
		},
		Filters: model.SnapshotFilters{
			IncludeSchemas: append([]string(nil), opts.includeSchemas...),
			ExcludeSchemas: append([]string(nil), opts.excludeSchemas...),
			IncludeTables:  append([]string(nil), opts.includeTables...),
			ExcludeTables:  append([]string(nil), opts.excludeTables...),
		},
	}
}

func wrapParseError(err error) error {
	if err == nil {
		return nil
	}
	if mapped := mapBinlogParseError(err.Error()); mapped != "" {
		return fmt.Errorf("%s", mapped)
	}
	return fmt.Errorf("%s", i18n.Tf("error.parseError", map[string]any{"Error": err.Error()}))
}

func mapBinlogParseError(msg string) string {
	if msg == "" {
		return ""
	}
	lower := strings.ToLower(msg)
	switch {
	case strings.Contains(lower, "normalize error") || strings.Contains(lower, "analysis consume error"):
		return ""
	case strings.Contains(lower, "fe'bin"):
		return i18n.T("error.corruptBinlogMagic")
	case strings.Contains(lower, "get event"):
		return i18n.Tf("error.truncatedBinlog", map[string]any{"Detail": msg})
	case msg == "EOF" || strings.HasSuffix(msg, ": EOF") || lower == "eof":
		return i18n.T("error.emptyBinlog")
	default:
		return ""
	}
}

func applyAnalyzeOutcomeGuards(paths []string, opts analyzer.Options, result *model.AnalysisResult, rawEvents int, statusWriter io.Writer) error {
	if err := rejectEmptyOrIncompleteBinlog(paths, rawEvents); err != nil {
		return err
	}
	if result == nil {
		return nil
	}
	if (opts.Start != nil || opts.End != nil) && result.Summary.TotalEvents == 0 {
		if statusWriter != nil {
			_, _ = fmt.Fprintln(statusWriter, i18n.T("progress.windowMatchedZero"))
		}
	}
	return nil
}

func rejectEmptyOrIncompleteBinlog(paths []string, rawEvents int) error {
	if rawEvents > 0 || !anyNonEmptyFile(paths) {
		return nil
	}
	return fmt.Errorf("%s", i18n.T("error.noFormatDescription"))
}

func anyNonEmptyFile(paths []string) bool {
	for _, path := range paths {
		info, err := os.Stat(path)
		if err == nil && info.Size() > 0 {
			return true
		}
	}
	return false
}
