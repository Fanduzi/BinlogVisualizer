// Package binlogviz builds analyze's analyzer.Options, report.Options, and
// snapshot metadata from parsed flags, and validates the flag combination.
// input: analyzeOptions flags and resolved input paths.
// output: the option structs, or a usage error for an invalid combination.
// pos: CLI orchestration helper for analyze.go.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"strings"
	"time"

	"binlogviz/internal/analyzer"
	"binlogviz/internal/clierr"
	"binlogviz/internal/i18n"
	"binlogviz/internal/model"
	"binlogviz/internal/mysqlexpr"
	"binlogviz/internal/report"
	"binlogviz/internal/snapshot"
	"binlogviz/internal/version"

	"github.com/spf13/cobra"
)

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
	result.IncludeDML = append([]string(nil), opts.dmlKinds...)
	result.RowWhere = opts.whereExpr
	result.SchemaSQL = opts.schemaSQL
	result.SchemaFileDB = opts.schemaFileDB
	result.Split = opts.split
	result.WorkloadID = strings.TrimSpace(opts.workloadID)
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
	if opts.startPositionSet {
		result.StartPosition = &opts.startPosition
	}
	if opts.stopPositionSet {
		result.StopPosition = &opts.stopPosition
	}

	return result
}

func buildReportOptions(opts *analyzeOptions) (report.Options, error) {
	mode, err := report.ParseSQLContextMode(opts.sqlContext)
	if err != nil {
		return report.Options{}, err
	}
	diagnostics, err := report.ParseDiagnosticsMode(opts.diagnostics)
	if err != nil {
		return report.Options{}, err
	}
	topTables := opts.topTables
	if !opts.topTablesChanged {
		topTables = opts.top
	}
	topThreads := opts.topThreads
	if !opts.topThreadsChanged {
		topThreads = opts.top
	}
	topRows := opts.topRows
	if !opts.topRowsChanged {
		topRows = opts.top
	}
	topFingerprints := opts.topFingerprints
	if !opts.topFingerprintsChanged {
		topFingerprints = opts.top
	}
	return report.Options{
		SQLContextMode:     mode,
		DiagnosticsMode:    diagnostics,
		TopN:               opts.top,
		TopTables:          topTables,
		TopTablesSet:       opts.topTablesChanged,
		TopThreads:         topThreads,
		TopThreadsSet:      opts.topThreadsChanged,
		TopRows:            topRows,
		TopRowsSet:         opts.topRowsChanged,
		TopFingerprints:    topFingerprints,
		TopFingerprintsSet: opts.topFingerprintsChanged,
		CSVDataset:         report.CSVDataset(opts.csvDataset),
		Details:            opts.details,
		ShowMinutes:        opts.showMinutes,
		ShowPatterns:       opts.showPatterns,
		ShowFingerprints:   opts.showFingerprints,
	}, nil
}

func validateAnalyzeOptions(opts *analyzeOptions) error {
	kinds, err := analyzer.ParseDMLKinds(opts.dml)
	if err != nil {
		return err
	}
	opts.dmlKinds = kinds
	switch analyzer.DetailStoreMode(opts.detailStore) {
	case analyzer.DetailStoreNone, analyzer.DetailStoreDuckDB:
	default:
		return clierr.Errorf(clierr.KindUsage, "invalid --detail-store %q: expected none or duckdb", opts.detailStore)
	}
	if err := validateNonNegativeCounts(opts); err != nil {
		return err
	}
	if err := validateCSVDataset(opts); err != nil {
		return err
	}
	if err := snapshotNameFormatError(opts.format, opts.snapshotName); err != nil {
		return err
	}
	if opts.snapshotName != "" {
		return snapshot.ValidateName(opts.snapshotName)
	}
	return nil
}

// validateNonNegativeCounts refuses a negative count or limit; none of them
// has a meaning below 0, and accepting one silently was #268.
func validateNonNegativeCounts(opts *analyzeOptions) error {
	counts := []struct {
		flag  string
		value int
	}{
		{"--top", opts.top},
		{"--top-tables", opts.topTables},
		{"--top-transactions", opts.topTransactions},
		{"--top-threads", opts.topThreads},
		{"--top-rows", opts.topRows},
		{"--top-fingerprints", opts.topFingerprints},
		{"--top-minutes", opts.topMinutes},
		{"--large-trx-rows", opts.largeTrxRows},
		{"--spike-window", opts.spikeWindow},
		{"--spike-min-rows", opts.spikeMinRows},
	}
	for _, c := range counts {
		if c.value < 0 {
			return clierr.Errorf(clierr.KindUsage, "invalid %s %d: must be 0 or more", c.flag, c.value)
		}
	}
	if opts.spikeFactor < 0 {
		return clierr.Errorf(clierr.KindUsage, "invalid --spike-factor %v: must be 0 or more", opts.spikeFactor)
	}
	if opts.largeTrxDuration < 0 {
		return clierr.Errorf(clierr.KindUsage, "invalid --large-trx-duration %s: must be 0 or more", opts.largeTrxDuration)
	}
	return nil
}

// compileWhereFlag parses --where once, before any input is read: a
// malformed or out-of-subset predicate is a usage error (ADR 0024).
func compileWhereFlag(opts *analyzeOptions) error {
	if strings.TrimSpace(opts.where) == "" {
		return nil
	}
	w, err := mysqlexpr.CompileWhere(opts.where)
	if err != nil {
		return clierr.Errorf(clierr.KindUsage, "%s", i18n.Tf("error.whereInvalid", map[string]any{
			"Expr":   opts.where,
			"Detail": err.Error(),
		}))
	}
	opts.whereExpr = w
	return nil
}

// markAnalyzeFlagsChanged records which flags the user explicitly set —
// inheritance (a top-* flag left unset follows --top) and explicit-zero
// semantics both need the distinction (ADR 0026 for --top-fingerprints).
func markAnalyzeFlagsChanged(cmd *cobra.Command, opts *analyzeOptions) {
	opts.topTablesChanged = cmd.Flags().Changed("top-tables")
	opts.topTransactionsChanged = cmd.Flags().Changed("top-transactions")
	opts.topThreadsChanged = cmd.Flags().Changed("top-threads")
	opts.topRowsChanged = cmd.Flags().Changed("top-rows")
	opts.topFingerprintsChanged = cmd.Flags().Changed("top-fingerprints")
	opts.csvDatasetChanged = cmd.Flags().Changed("csv-dataset")
	opts.startPositionSet = cmd.Flags().Changed("start-position")
	opts.stopPositionSet = cmd.Flags().Changed("stop-position")
}

// validateCSVDataset names the dataset --format csv/tsv writes, and refuses
// the flag under any other format instead of silently ignoring it — the same
// contract --output has with --format html (ADR 0026).
func validateCSVDataset(opts *analyzeOptions) error {
	if _, err := report.ParseCSVDataset(opts.csvDataset); err != nil {
		return clierr.Errorf(clierr.KindUsage, "%s", err)
	}
	if opts.csvDatasetChanged && !report.Format(opts.format).IsDelimited() {
		return clierr.Errorf(clierr.KindUsage, "--csv-dataset requires --format csv or tsv")
	}
	return nil
}

func snapshotNameFormatError(format, snapshotName string) error {
	if snapshotName != "" && format != "json" {
		return clierr.Errorf(clierr.KindUsage, "--snapshot-name requires --format json")
	}
	return nil
}

func buildSnapshotMetadata(paths []string, opts *analyzeOptions, startTime, endTime time.Time, discovered bool) *model.Snapshot {
	if opts.snapshotName == "" {
		return nil
	}

	inputMode := "files"
	if discovered {
		inputMode = "discovery"
	}
	if opts.remote {
		inputMode = "remote"
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
			IncludeDML:     append([]string(nil), opts.dmlKinds...),
		},
	}
}
