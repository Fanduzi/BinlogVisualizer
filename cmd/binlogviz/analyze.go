// Package binlogviz defines the analyze CLI command and manages command-scoped DuckDB temp-store lifecycle.
// input: CLI workload-identity, time/position/GTID/filter flags, parser callbacks including Format Description server version, command-owned temporary directory roots, and flashback --schema-file SQL. Path resolution lives in analyze_paths.go, --start/--end parsing in analyze_time.go, option building in analyze_options.go, and failure mapping in analyze_errors.go.
// output: rendered text/JSON/HTML report-v3 analysis with workload identity/scope, a Top Threads session ranking, selector evidence, selected-file/count coverage, unmapped parser-event counts, optional Ignored QUERY counts, and an optional open-explicit-group count; when Options.Flashback is set, undo SQL is written instead of a report; analyze --help names per-transaction server_id, thread_id, GTID, xid or XA xid, user@host, --sql-context, --diagnostics, and stdin `-`; a terminal or empty stdin `-` fails before parsing, --snapshot-name without json fails before rendering, a saved snapshot always keeps full diagnostics whatever --diagnostics says, DuckDB temp state is cleaned, and the analyze command does not register --schema-file or --schema-file-db. A failed activity-spill read, while the analysis finishes or while rendering, is exit 11 (kind spill), not an output write failure.
// pos: CLI orchestration layer between input resolution, parser normalization, analyzer execution, and final report rendering. report.Write owns the format switch. An unknown --format is still a usage error before any input is read.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"binlogviz/internal/clierr"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"

	"binlogviz/internal/analyzer"
	"binlogviz/internal/binlog"
	"binlogviz/internal/detailstore"
	"binlogviz/internal/displaytz"
	"binlogviz/internal/i18n"
	"binlogviz/internal/model"
	"binlogviz/internal/report"
	"binlogviz/internal/snapshot"
)

type commandAnalyzer interface {
	Consume(model.NormalizedEvent) error
	Finalize() (*model.AnalysisResult, error)
}

type normalizeRawEventFunc func(binlog.RawEvent) (*model.NormalizedEvent, error)
type commandAnalyzerFactory func(opts analyzer.Options, store *detailstore.DuckDBStore) commandAnalyzer
type tempStoreFactory func(root string) (*detailstore.DuckDBStore, func() error, string, error)

type aggregateProgress struct {
	bar          *progressbar.ProgressBar
	fileSizes    []int64
	offsets      []int64
	statusWriter io.Writer
}

var inspectFirstBinlogTimestamp = readFirstBinlogTimestamp

// analyzeOptions holds the parsed CLI flags for the analyze command.
type analyzeOptions struct {
	conflict               string
	plan                   bool
	conflictLimit          int
	startTime              string
	endTime                string
	startPosition          int64
	stopPosition           int64
	startPositionSet       bool
	stopPositionSet        bool
	includeGTIDs           []string
	excludeGTIDs           []string
	fromDir                string
	prefix                 string
	indexFile              string
	format                 string
	output                 string
	snapshotName           string
	snapshotDir            string
	workloadID             string
	sqlContext             string
	diagnostics            string
	top                    int
	topTables              int
	topTransactions        int
	topThreads             int
	topThreadsChanged      bool
	topRows                int
	topRowsChanged         bool
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
	schemaFile             string
	schemaFileDB           string
	schemaSQL              string
	dml                    []string
	dmlKinds               []string
	showRows               bool
	display                *time.Location
	topTablesChanged       bool
	topTransactionsChanged bool
	detailStore            string
	remote                 bool
	remoteHost             string
	remotePort             uint16
	remoteUser             string
	remotePassword         string
	remoteServerID         uint32
	remoteStartFile        string
	remoteStartPos         uint32
	remoteStartGTID        string
	remoteSSLMode          string
}

func newAnalyzeCommand() *cobra.Command {
	opts := &analyzeOptions{}

	cmd := &cobra.Command{
		Use:   i18n.T("cmd.analyze.use"),
		Short: i18n.T("cmd.analyze.short"),
		Long:  i18n.T("cmd.analyze.long"),
		Example: `  # Write spike in a UTC window, then the transaction, then a guarded flashback.
  # 14:05 at +08:00 is 06:05Z. Report times are UTC. Filters accept an offset.
  binlogviz analyze mysql-bin.000001 --start 2026-10-19T06:00:00Z --end 2026-10-19T06:10:00Z --detect-spikes
  binlogviz analyze mysql-bin.000001 --start 2026-10-19T06:05:00Z --end 2026-10-19T06:06:00Z --format json --diagnostics summary
  binlogviz flashback mysql-bin.000001 --plan
  binlogviz flashback mysql-bin.000001 --conflict guard`,
		SilenceUsage:  true,
		SilenceErrors: true,
		Args: func(cmd *cobra.Command, args []string) error {
			hasArgs, hasFromDir, hasPrefix, hasIndex := len(args) > 0, opts.fromDir != "", opts.prefix != "", opts.indexFile != ""
			if hasArgs && (hasFromDir || hasPrefix || hasIndex) {
				return clierr.Errorf(clierr.KindUsage, "%s", i18n.T("error.combineArgsWithDir"))
			}
			if (hasFromDir || hasPrefix || hasIndex) && (!hasFromDir || hasPrefix == hasIndex) {
				return clierr.Errorf(clierr.KindUsage, "%s", i18n.T("error.fromDirPrefixOrIndex"))
			}
			if err := validateRemoteArgs(cmd, args, opts); err != nil {
				return err
			}
			if hasArgs || hasFromDir || analyzeRemoteRequested(cmd) {
				return nil
			}
			return clierr.Errorf(clierr.KindUsage, "%s", i18n.T("error.requiresBinlogOrDir"))
		},
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			// Each stage gives its untyped errors a kind (ADR 0008): flags and
			// options are usage, finding and opening inputs is input; the
			// pipeline types its own errors.
			stage := clierr.KindUsage
			defer func() { err = clierr.Wrap(stage, err) }()
			// #251: an unknown --format is a usage error, before any input is read.
			if _, ferr := report.ParseFormat(opts.format); ferr != nil {
				return ferr
			}
			// A zoneless --start/--end uses this zone when --tz is set, including UTC.
			opts.display = displaytz.From(cmd.Context())
			startTime, endTime, err := parseTimeRange(opts.startTime, opts.endTime, opts.display)
			if err != nil {
				return err
			}

			opts.topTablesChanged = cmd.Flags().Changed("top-tables")
			opts.topTransactionsChanged = cmd.Flags().Changed("top-transactions")
			opts.topThreadsChanged = cmd.Flags().Changed("top-threads")
			opts.topRowsChanged = cmd.Flags().Changed("top-rows")
			opts.startPositionSet = cmd.Flags().Changed("start-position")
			opts.stopPositionSet = cmd.Flags().Changed("stop-position")
			if err := validateAnalyzeSelectionInput(args, opts); err != nil {
				return err
			}

			// An explicit empty flag is a usage error. ParseDiagnosticsMode
			// still maps "" to full for the report.Options zero value (#308).
			if cmd.Flags().Changed("diagnostics") && opts.diagnostics == "" {
				return clierr.Errorf(clierr.KindUsage, "invalid --diagnostics %q (allowed: full, summary, off)", opts.diagnostics)
			}
			reportOpts, err := buildReportOptions(opts)
			if err != nil {
				return err
			}
			if err := validateAnalyzeOptions(opts); err != nil {
				return err
			}
			reportOpts.ShowRows = opts.showRows
			reportOpts.Display = opts.display
			gtidSelector, err := buildGTIDSelector(opts)
			if err != nil {
				return err
			}

			opts.remote = analyzeRemoteRequested(cmd)
			parser := binlog.NewParser()
			var paths []string
			var pathAliases map[string]string
			var fileCoverage model.FileCoverage
			discovered := false
			if opts.remote {
				runCfg, err := buildRemoteRunConfig(cmd, opts)
				if err != nil {
					return err
				}
				stage = clierr.KindInput
				src, boundary, err := openRemoteInput(cmd.Context(), runCfg)
				if err != nil {
					return err
				}
				defer src.Close()
				parser = binlog.NewRemoteParser(cmd.Context(), src, boundary)
				paths = []string{runCfg.origin}
				discovered = true
				fileCoverage = remoteFileCoverage(runCfg.origin, src.Manifest(), runCfg.cfg.Start.File, runCfg.cfg.GTIDSet != nil)
				printResolvedPaths(infoWriter(os.Stderr), paths)
			} else {
				stage = clierr.KindInput
				var err error
				paths, discovered, fileCoverage, err = resolveAnalyzePaths(args, opts)
				if err != nil {
					return err
				}
				var cleanupInputs func()
				paths, pathAliases, cleanupInputs, err = materializeAnalyzePaths(paths)
				if cleanupInputs != nil {
					defer cleanupInputs()
				}
				if err != nil {
					return err
				}
				if !discovered {
					fileCoverage = fileCoverageForPaths(paths)
				}
				if discovered {
					printResolvedPaths(infoWriter(os.Stderr), paths)
				}
				if len(paths) == 0 {
					return clierr.Errorf(clierr.KindInput, "%s", i18n.T("error.noResolvedFiles"))
				}

				// Validate input files exist
				if err := validateFiles(paths); err != nil {
					return err
				}
			}

			// Resolve output destination
			stage = clierr.KindUsage
			dest, err := resolveOutputDestination(paths, discovered, opts.output, opts.format)
			if err != nil {
				return err
			}

			// Build analyzer options
			analyzerOpts := buildAnalyzerOptions(opts, startTime, endTime)
			analyzerOpts.GTIDSelector = gtidSelector
			if opts.showRows && reportOpts.SQLContextMode != report.SQLContextOff {
				analyzerOpts.CaptureRowImages = true
			}
			snapshotMeta := buildSnapshotMetadata(displayPaths(paths, pathAliases), opts, startTime, endTime, discovered)

			// Execute the analysis pipeline
			stage = clierr.KindError
			return runAnalysisStreamingFastWithOutput(paths, analyzerOpts, reportOpts, opts.format, parser, func(opts analyzer.Options, store *detailstore.DuckDBStore) commandAnalyzer {
				if opts.DetailStoreMode == analyzer.DetailStoreDuckDB {
					return analyzer.NewWithStore(opts, store)
				}
				return analyzer.New(opts)
			}, func(root string) (*detailstore.DuckDBStore, func() error, string, error) {
				return createDuckDBTempStore(root)
			}, "", snapshotMeta, fileCoverage, opts.snapshotName, opts.snapshotDir, dest, pathAliases)
		},
	}

	registerAnalyzeFlags(cmd, opts)
	return cmd
}

func buildGTIDSelector(opts *analyzeOptions) (*analyzer.GTIDSelector, error) {
	if opts == nil || len(opts.includeGTIDs) == 0 && len(opts.excludeGTIDs) == 0 {
		return nil, nil
	}
	return analyzer.ParseGTIDSelector(opts.includeGTIDs, opts.excludeGTIDs)
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
	return runAnalysisStreamingFastWithSnapshot(paths, opts, reportOpts, format, parser, func(opts analyzer.Options, store *detailstore.DuckDBStore) commandAnalyzer {
		if opts.DetailStoreMode == analyzer.DetailStoreDuckDB {
			return analyzer.NewWithStore(opts, store)
		}
		return analyzer.New(opts)
	}, func(root string) (*detailstore.DuckDBStore, func() error, string, error) {
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
	out = infoWriter(out)
	// The bar is for people at a terminal; a pipe or file gets only the
	// plain "Finalizing analysis..." line, and --quiet gets nothing (#253).
	if totalBytes <= 0 || len(fileSizes) == 0 || !interactiveStderr() {
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
	return runAnalysisStreamingWithSnapshotDeps(paths, opts, reportOpts, format, parser, normalize, newAnalyzer, newTempStore, tempRoot, nil, model.FileCoverage{}, "", "", outputDestination{}, nil)
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
	dest outputDestination,
	pathAliases map[string]string,
) error {
	if err := snapshotNameFormatError(format, snapshotName); err != nil {
		return err
	}
	positionValidator, err := newPositionBoundaryValidator(paths, opts)
	if err != nil {
		return err
	}
	progress, err := newAggregateProgress(paths, os.Stderr)
	if err != nil {
		return clierr.Errorf(clierr.KindError, "%s", i18n.Tf("error.buildParseProgress", map[string]any{"Error": err.Error()}))
	}
	if opts.CaptureRowImages {
		if rows, ok := parser.(binlog.RowImageParser); ok {
			rows.SetCaptureRowImages(true)
		}
	}
	if opts.Flashback {
		if rows, ok := parser.(binlog.FlashbackParser); ok {
			rows.SetCaptureFlashback(true)
		}
	}
	if opts.Redo {
		if rows, ok := parser.(binlog.RedoParser); ok {
			rows.SetCaptureRedo(true)
		}
	}

	var store *detailstore.DuckDBStore
	if opts.DetailStoreMode == analyzer.DetailStoreDuckDB {
		var cleanup func() error
		store, cleanup, _, err = newTempStore(tempRoot)
		if err != nil {
			return clierr.Errorf(clierr.KindOutput, "%s", i18n.Tf("error.createTempStore", map[string]any{"Error": err.Error()}))
		}
		defer cleanup()
	}

	streamAnalyzer := newAnalyzer(opts, store)
	var formatObserver binlog.FormatObserver
	rawEvents := 0

	handler := func(raw binlog.RawEvent) error {
		positionValidator.Observe(raw)
		formatObserver.Observe(raw)
		rawEvents++
		var ev model.NormalizedEvent
		if normalize != nil {
			normalized, err := normalize(raw)
			if err != nil {
				return clierr.Errorf(clierr.KindError, "%s", i18n.Tf("error.normalizeError", map[string]any{"Position": raw.Position, "Error": err.Error()}))
			}
			if normalized == nil {
				noteSkippedIgnoredQuery(streamAnalyzer, raw)
				return nil
			}
			ev = *normalized
		} else {
			ok, err := binlog.NormalizeRawEventInto(raw, &ev)
			if err != nil {
				return clierr.Errorf(clierr.KindError, "%s", i18n.Tf("error.normalizeError", map[string]any{"Position": raw.Position, "Error": err.Error()}))
			}
			if !ok {
				noteSkippedIgnoredQuery(streamAnalyzer, raw)
				return nil
			}
		}
		if err := streamAnalyzer.Consume(ev); err != nil {
			if operatorGroupError(err) {
				return err
			}
			if resource := analyzerResourceError(err); resource != nil {
				return resource
			}
			return clierr.Errorf(clierr.KindError, "%s", i18n.Tf("error.analysisConsumeError", map[string]any{"Error": err.Error()}))
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
	if err := positionValidator.Validate(); err != nil {
		return err
	}
	progress.FinishParse()
	progress.Finalizing()

	result, err := streamAnalyzer.Finalize()
	if err != nil {
		if localized := localizedOperatorGroupError(err); localized != nil {
			return localized
		}
		if resource := analyzerResourceError(err); resource != nil {
			return resource
		}
		if spill := asSpill(err); spill != nil {
			return spill
		}
		return clierr.Errorf(clierr.KindError, "%s", i18n.Tf("error.analysisFinalizeError", map[string]any{"Error": err.Error()}))
	}
	return finishStreamingRun(streamingRun{
		paths:          paths,
		opts:           opts,
		reportOpts:     reportOpts,
		format:         format,
		streamAnalyzer: streamAnalyzer,
		rawEvents:      rawEvents,
		formatObserver: formatObserver,
		result:         result,
		snapshotMeta:   snapshotMeta,
		fileCoverage:   fileCoverage,
		snapshotName:   snapshotName,
		snapshotDir:    snapshotDir,
		dest:           dest,
		pathAliases:    pathAliases,
	})
}

// asSpill maps a failed activity-spill read to kind spill. Analysis finalize
// and report rendering both use it. A spill write that falls back to memory
// is not this error.
func asSpill(err error) error {
	var spill *analyzer.SpillReadError
	if errors.As(err, &spill) {
		return clierr.Wrap(clierr.KindSpill, err)
	}
	return nil
}

// classifyReportWriteErr maps a spill read to kind spill. HTML keeps any
// other error untyped. Every other format's write failure is kind output.
func classifyReportWriteErr(format string, err error) error {
	if err == nil {
		return nil
	}
	if spill := asSpill(err); spill != nil {
		return spill
	}
	if format == "html" {
		return err
	}
	return clierr.Wrap(clierr.KindOutput, err)
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
	return runAnalysisStreamingWithSnapshotDeps(paths, opts, reportOpts, format, parser, nil, newAnalyzer, newTempStore, tempRoot, snapshotMeta, fileCoverage, snapshotName, snapshotDir, outputDestination{}, nil)
}

func hasFileCoverage(fileCoverage model.FileCoverage) bool {
	return len(fileCoverage.Selected) > 0 || len(fileCoverage.Skipped) > 0
}

func applyFileCoverage(result *model.AnalysisResult, paths []string, fileCoverage model.FileCoverage) {
	if result == nil {
		return
	}
	if !hasFileCoverage(fileCoverage) {
		fileCoverage = fileCoverageForPaths(paths)
	}
	if hasFileCoverage(fileCoverage) {
		result.Diagnostics.FileCoverage = fileCoverage
	}
}

func noteInputFormat(result *model.AnalysisResult, observer binlog.FormatObserver) error {
	if result == nil {
		return nil
	}
	result.Diagnostics.InputFormatGuess = observer.Guess()
	result.Diagnostics.IgnoredQueryDMLEvents = observer.QueryDMLEvents
	result.Diagnostics.IgnoredQueryEvents = observer.IgnoredQueryEvents
	result.Diagnostics.UnmappedEvents = observer.UnmappedEvents
	result.Diagnostics.ServerVersion = observer.ServerVersion
	if observer.QueryDMLEvents == 0 {
		return nil
	}
	if observer.RowImageEvents == 0 {
		return clierr.Errorf(clierr.KindUnsupported, "%s", binlog.StatementOrMixedWarning)
	}
	_, _ = fmt.Fprintln(os.Stderr, binlog.StatementOrMixedWarning)
	result.Alerts = append(result.Alerts, model.Alert{
		Type:     "input_format",
		Severity: "warning",
		Message: fmt.Sprintf("%s: counted %d ROW images, ignored %d Query-DML events",
			observer.Guess(), observer.RowImageEvents, observer.QueryDMLEvents),
	})
	return nil
}

func runAnalysisWithOutput(paths []string, opts analyzer.Options, reportOpts report.Options, format string, snapshotMeta *model.Snapshot, fileCoverage model.FileCoverage, snapshotName, snapshotDir string, dest outputDestination, pathAliases map[string]string) error {
	return runAnalysisStreamingFastWithOutput(paths, opts, reportOpts, format, binlog.NewParser(), func(opts analyzer.Options, store *detailstore.DuckDBStore) commandAnalyzer {
		if opts.DetailStoreMode == analyzer.DetailStoreDuckDB {
			return analyzer.NewWithStore(opts, store)
		}
		return analyzer.New(opts)
	}, func(root string) (*detailstore.DuckDBStore, func() error, string, error) {
		return createDuckDBTempStore(root)
	}, "", snapshotMeta, fileCoverage, snapshotName, snapshotDir, dest, pathAliases)
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
	pathAliases map[string]string,
) error {
	return runAnalysisStreamingWithSnapshotDeps(paths, opts, reportOpts, format, parser, nil, newAnalyzer, newTempStore, tempRoot, snapshotMeta, fileCoverage, snapshotName, snapshotDir, dest, pathAliases)
}

func saveAndWriteJSONReport(result model.AnalysisResult, reportOpts report.Options, snapshotName, snapshotDir string) error {
	result.Snapshot = resolveAnalyzeSnapshotMetadata(result.Snapshot, result.Summary)

	// The snapshot is an archive and always keeps full diagnostics; only the
	// stdout document takes the --diagnostics budget (ADR 0011 §3).
	// The file is an archive other commands read later. It stays UTC.
	// --tz changes only the stdout copy (ADR 0017). time.UTC (--tz UTC)
	// keeps the historical spelling, so it uses this same render.
	archiveOpts := report.ArchiveOptions(reportOpts)
	archiveOpts.Display = nil
	archive, err := report.RenderJSONWithOptions(result, archiveOpts)
	if err != nil {
		return err
	}
	payload := archive
	if (reportOpts.Display != nil && reportOpts.Display != time.UTC) || (reportOpts.DiagnosticsMode != report.DiagnosticsFull && reportOpts.DiagnosticsMode != "") {
		payload, err = report.RenderJSONWithOptions(result, reportOpts)
		if err != nil {
			return err
		}
	}

	savedPath, err := snapshot.SaveJSON(snapshotDir, snapshotName, []byte(archive))
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(infoWriter(os.Stderr), "Saved snapshot %q to %s\n", snapshotName, savedPath); err != nil {
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

func createDuckDBTempStore(root string) (*detailstore.DuckDBStore, func() error, string, error) {
	tempDir, err := os.MkdirTemp(root, "binlogviz-duckdb-*")
	if err != nil {
		return nil, nil, "", err
	}
	path := filepath.Join(tempDir, "analysis.duckdb")
	store, err := detailstore.NewDuckDBStore(path, detailstore.DefaultBatchFlushRows)
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
