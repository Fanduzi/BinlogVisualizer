package binlogviz

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"binlogviz/internal/analyzer"
	"binlogviz/internal/i18n"
	"binlogviz/internal/report"
)

func newFlashbackCommand() *cobra.Command {
	opts := &analyzeOptions{}
	cmd := &cobra.Command{
		Use:           i18n.T("cmd.flashback.use"),
		Short:         i18n.T("cmd.flashback.short"),
		Long:          i18n.T("cmd.flashback.long"),
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
			startTime, endTime, err := parseTimeRange(opts.startTime, opts.endTime)
			if err != nil {
				return err
			}
			opts.startPositionSet = cmd.Flags().Changed("start-position")
			opts.stopPositionSet = cmd.Flags().Changed("stop-position")
			if err := validateAnalyzeSelectionInput(args, opts); err != nil {
				return err
			}
			mode, err := report.ParseSQLContextMode(opts.sqlContext)
			if err != nil {
				return err
			}
			if mode == report.SQLContextOff {
				return fmt.Errorf("%s", i18n.T("error.flashbackSQLContext"))
			}
			kinds, err := analyzer.ParseDMLKinds(opts.dml)
			if err != nil {
				return err
			}
			opts.dmlKinds = kinds
			gtidSelector, err := buildGTIDSelector(opts)
			if err != nil {
				return err
			}
			paths, discovered, fileCoverage, err := resolveAnalyzePaths(args, opts)
			if err != nil {
				return err
			}
			paths, pathAliases, cleanupInputs, err := materializeAnalyzePaths(paths)
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
				printResolvedPaths(os.Stderr, paths)
			}
			if len(paths) == 0 {
				return fmt.Errorf("%s", i18n.T("error.noResolvedFiles"))
			}
			if err := validateFiles(paths); err != nil {
				return err
			}
			analyzerOpts := buildAnalyzerOptions(opts, startTime, endTime)
			analyzerOpts.GTIDSelector = gtidSelector
			analyzerOpts.Flashback = true
			return runAnalysisWithOutput(paths, analyzerOpts, report.Options{}, "text", nil, fileCoverage, "", "", outputDestination{}, pathAliases)
		},
	}
	cmd.Flags().StringVar(&opts.startTime, "start", "", i18n.T("cmd.analyze.flag.start"))
	cmd.Flags().StringVar(&opts.endTime, "end", "", i18n.T("cmd.analyze.flag.end"))
	cmd.Flags().Int64Var(&opts.startPosition, "start-position", 0, "Start position (inclusive event boundary)")
	cmd.Flags().Int64Var(&opts.stopPosition, "stop-position", 0, "Stop position (exclusive event boundary or EOF)")
	cmd.Flags().StringSliceVar(&opts.includeGTIDs, "include-gtids", nil, "Include complete transaction groups matching this GTID set")
	cmd.Flags().StringSliceVar(&opts.excludeGTIDs, "exclude-gtids", nil, "Exclude complete transaction groups matching this GTID set")
	cmd.Flags().StringVar(&opts.fromDir, "from-dir", "", i18n.T("cmd.analyze.flag.fromDir"))
	cmd.Flags().StringVar(&opts.prefix, "prefix", "", i18n.T("cmd.analyze.flag.prefix"))
	cmd.Flags().StringVar(&opts.sqlContext, "sql-context", string(report.SQLContextSummary), i18n.T("cmd.analyze.flag.sqlContext"))
	cmd.Flags().StringSliceVar(&opts.includeSchemas, "include-schema", nil, i18n.T("cmd.analyze.flag.includeSchema"))
	cmd.Flags().StringSliceVar(&opts.excludeSchemas, "exclude-schema", nil, i18n.T("cmd.analyze.flag.excludeSchema"))
	cmd.Flags().StringSliceVar(&opts.includeTables, "include-table", nil, i18n.T("cmd.analyze.flag.includeTable"))
	cmd.Flags().StringSliceVar(&opts.excludeTables, "exclude-table", nil, i18n.T("cmd.analyze.flag.excludeTable"))
	cmd.Flags().StringSliceVar(&opts.dml, "dml", nil, i18n.T("cmd.analyze.flag.dml"))
	return cmd
}

func writeFlashbackSQL(stream commandAnalyzer) error {
	src, ok := stream.(*analyzer.Analyzer)
	if !ok || src == nil {
		return fmt.Errorf("%s", i18n.Tf("error.flashbackCapture", map[string]any{"Table": "selected range"}))
	}
	sql, err := src.FlashbackSQL()
	if err != nil {
		return err
	}
	if sql == "" {
		return &ExitError{Code: 2, Msg: i18n.T("error.noAnalyzableEvents")}
	}
	_, err = fmt.Fprint(os.Stdout, sql)
	return err
}
