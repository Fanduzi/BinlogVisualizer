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
			if opts.schemaFile != "" {
				body, err := os.ReadFile(opts.schemaFile)
				if err != nil {
					return fmt.Errorf("%s", i18n.Tf("error.flashbackSchemaFile", map[string]any{
						"Path":  opts.schemaFile,
						"Error": err.Error(),
					}))
				}
				opts.schemaSQL = string(body)
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
	cmd.Flags().Int64Var(&opts.startPosition, "start-position", 0, i18n.T("cmd.flashback.flag.startPosition"))
	cmd.Flags().Int64Var(&opts.stopPosition, "stop-position", 0, i18n.T("cmd.flashback.flag.stopPosition"))
	cmd.Flags().StringSliceVar(&opts.includeGTIDs, "include-gtids", nil, i18n.T("cmd.flashback.flag.includeGtids"))
	cmd.Flags().StringSliceVar(&opts.excludeGTIDs, "exclude-gtids", nil, i18n.T("cmd.flashback.flag.excludeGtids"))
	cmd.Flags().StringVar(&opts.fromDir, "from-dir", "", i18n.T("cmd.analyze.flag.fromDir"))
	cmd.Flags().StringVar(&opts.prefix, "prefix", "", i18n.T("cmd.analyze.flag.prefix"))
	cmd.Flags().StringVar(&opts.sqlContext, "sql-context", string(report.SQLContextSummary), i18n.T("cmd.analyze.flag.sqlContext"))
	cmd.Flags().StringSliceVar(&opts.includeSchemas, "include-schema", nil, i18n.T("cmd.flashback.flag.includeSchema"))
	cmd.Flags().StringSliceVar(&opts.excludeSchemas, "exclude-schema", nil, i18n.T("cmd.flashback.flag.excludeSchema"))
	cmd.Flags().StringSliceVar(&opts.includeTables, "include-table", nil, i18n.T("cmd.flashback.flag.includeTable"))
	cmd.Flags().StringSliceVar(&opts.excludeTables, "exclude-table", nil, i18n.T("cmd.flashback.flag.excludeTable"))
	cmd.Flags().StringVar(&opts.schemaFile, "schema-file", "", i18n.T("cmd.flashback.flag.schemaFile"))
	cmd.Flags().StringVar(&opts.schemaFileDB, "schema-file-db", "", i18n.T("cmd.flashback.flag.schemaFileDB"))
	cmd.Flags().BoolVar(&opts.allowUnverifiedGenerated, "allow-unverified-generated", false, i18n.T("cmd.flashback.flag.allowUnverifiedGenerated"))
	cmd.Flags().StringSliceVar(&opts.dml, "dml", nil, i18n.T("cmd.flashback.flag.dml"))
	help := cmd.HelpFunc()
	cmd.SetHelpFunc(func(cmd *cobra.Command, args []string) {
		if langFlag != "" {
			_ = i18n.Init(langFlag)
		}
		refreshFlashbackHelp(cmd)
		help(cmd, args)
	})
	refreshFlashbackHelp(cmd)
	return cmd
}

func refreshFlashbackHelp(cmd *cobra.Command) {
	cmd.Use = i18n.T("cmd.flashback.use")
	cmd.Short = i18n.T("cmd.flashback.short")
	cmd.Long = i18n.T("cmd.flashback.long")
	cmd.SetUsageTemplate(flashbackUsageTemplate())
	usage := map[string]string{
		"start":                      "cmd.analyze.flag.start",
		"end":                        "cmd.analyze.flag.end",
		"start-position":             "cmd.flashback.flag.startPosition",
		"stop-position":              "cmd.flashback.flag.stopPosition",
		"include-gtids":              "cmd.flashback.flag.includeGtids",
		"exclude-gtids":              "cmd.flashback.flag.excludeGtids",
		"from-dir":                   "cmd.analyze.flag.fromDir",
		"prefix":                     "cmd.analyze.flag.prefix",
		"sql-context":                "cmd.analyze.flag.sqlContext",
		"include-schema":             "cmd.flashback.flag.includeSchema",
		"exclude-schema":             "cmd.flashback.flag.excludeSchema",
		"include-table":              "cmd.flashback.flag.includeTable",
		"exclude-table":              "cmd.flashback.flag.excludeTable",
		"schema-file":                "cmd.flashback.flag.schemaFile",
		"schema-file-db":             "cmd.flashback.flag.schemaFileDB",
		"allow-unverified-generated": "cmd.flashback.flag.allowUnverifiedGenerated",
		"dml":                        "cmd.flashback.flag.dml",
	}
	for name, key := range usage {
		if flag := cmd.Flags().Lookup(name); flag != nil {
			flag.Usage = i18n.T(key)
		}
	}
	if cmd.Parent() != nil {
		if flag := cmd.Parent().PersistentFlags().Lookup("lang"); flag != nil {
			flag.Usage = i18n.T("cmd.root.flag.lang")
		}
	}
}

func flashbackUsageTemplate() string {
	return i18n.T("cmd.flashback.help.usage") + `:{{if .Runnable}}
  {{.UseLine}}{{end}}{{if .HasAvailableLocalFlags}}

` + i18n.T("cmd.flashback.help.flags") + `:
{{.LocalFlags.FlagUsages | trimTrailingWhitespaces}}{{end}}{{if .HasAvailableInheritedFlags}}

` + i18n.T("cmd.flashback.help.globalFlags") + `:
{{.InheritedFlags.FlagUsages | trimTrailingWhitespaces}}{{end}}
`
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
	for _, warning := range src.FlashbackWarnings() {
		fmt.Fprintln(os.Stderr, warning)
	}
	_, err = fmt.Fprint(os.Stdout, sql)
	return err
}
