package binlogviz

import (
	"binlogviz/internal/clierr"
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"

	"binlogviz/internal/analyzer"
	"binlogviz/internal/flashback"
	"binlogviz/internal/i18n"
	"binlogviz/internal/report"
)

func newFlashbackCommand() *cobra.Command {
	opts := &analyzeOptions{}
	cmd := &cobra.Command{
		Use:   i18n.T("cmd.flashback.use"),
		Short: i18n.T("cmd.flashback.short"),
		Long:  i18n.T("cmd.flashback.long"),
		Example: `  # After analyze names the GTID (alerts[].gtid or transactions[].gtid):
  binlogviz flashback mysql-bin.000001 --include-gtids <gtid> --plan
  binlogviz flashback mysql-bin.000001 --include-gtids <gtid> --conflict guard`,
		SilenceUsage:  true,
		SilenceErrors: true,
		Args: func(cmd *cobra.Command, args []string) error {
			hasArgs := len(args) > 0
			hasFromDir := opts.fromDir != ""
			hasPrefix := opts.prefix != ""
			if hasArgs && (hasFromDir || hasPrefix) {
				return clierr.Errorf(clierr.KindUsage, "%s", i18n.T("error.combineArgsWithDir"))
			}
			if hasFromDir != hasPrefix {
				return clierr.Errorf(clierr.KindUsage, "%s", i18n.T("error.fromDirAndPrefixRequired"))
			}
			if hasArgs || hasFromDir {
				return nil
			}
			return clierr.Errorf(clierr.KindUsage, "%s", i18n.T("error.requiresBinlogOrDir"))
		},
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			stage := clierr.KindUsage
			defer func() { err = clierr.Wrap(stage, err) }()
			if err := flashback.ValidateResourceEnv(); err != nil {
				return err
			}
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
			if mode == report.SQLContextOff && !opts.plan {
				return clierr.WithDetails(clierr.KindRefused, clierr.Errorf(clierr.KindRefused, "%s", i18n.T("error.flashbackSQLContext")), map[string]any{"reason": "sql_context_off"})
			}
			if opts.schemaFile != "" {
				body, err := os.ReadFile(opts.schemaFile)
				if err != nil {
					return clierr.Wrap(clierr.KindInput, clierr.Errorf(clierr.KindInput, "%s", i18n.Tf("error.flashbackSchemaFile", map[string]any{
						"Path":  opts.schemaFile,
						"Error": err.Error(),
					})))
				}
				opts.schemaSQL = string(body)
			}
			if !flashback.ValidFlashbackConflict(opts.conflict) {
				return clierr.Errorf(clierr.KindUsage, "%s", i18n.Tf("error.flashbackConflictFlag", map[string]any{"Value": opts.conflict}))
			}
			if opts.conflictLimit < 0 {
				return clierr.Errorf(clierr.KindUsage, "%s", i18n.Tf("error.flashbackConflictLimit", map[string]any{"Value": opts.conflictLimit}))
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
			stage = clierr.KindInput
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
				printResolvedPaths(infoWriter(os.Stderr), paths)
			}
			if len(paths) == 0 {
				return clierr.Errorf(clierr.KindInput, "%s", i18n.T("error.noResolvedFiles"))
			}
			if err := validateFiles(paths); err != nil {
				return err
			}
			stage = clierr.KindError
			analyzerOpts := buildAnalyzerOptions(opts, startTime, endTime)
			analyzerOpts.GTIDSelector = gtidSelector
			analyzerOpts.Flashback = true
			analyzerOpts.FlashbackConflict = opts.conflict
			analyzerOpts.FlashbackPlan = opts.plan
			analyzerOpts.FlashbackConflictLimit = opts.conflictLimit
			if opts.conflictLimit == 0 {
				analyzerOpts.FlashbackConflictLimit = -1 // --conflict-limit 0 lists every conflict
			}
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
	cmd.Flags().StringVar(&opts.sqlContext, "sql-context", string(report.SQLContextSummary), i18n.T("cmd.flashback.flag.sqlContext"))
	cmd.Flags().StringSliceVar(&opts.includeSchemas, "include-schema", nil, i18n.T("cmd.flashback.flag.includeSchema"))
	cmd.Flags().StringSliceVar(&opts.excludeSchemas, "exclude-schema", nil, i18n.T("cmd.flashback.flag.excludeSchema"))
	cmd.Flags().StringSliceVar(&opts.includeTables, "include-table", nil, i18n.T("cmd.flashback.flag.includeTable"))
	cmd.Flags().StringSliceVar(&opts.excludeTables, "exclude-table", nil, i18n.T("cmd.flashback.flag.excludeTable"))
	cmd.Flags().StringVar(&opts.schemaFile, "schema-file", "", i18n.T("cmd.flashback.flag.schemaFile"))
	cmd.Flags().StringVar(&opts.schemaFileDB, "schema-file-db", "", i18n.T("cmd.flashback.flag.schemaFileDB"))
	cmd.Flags().StringSliceVar(&opts.dml, "dml", nil, i18n.T("cmd.flashback.flag.dml"))
	cmd.Flags().StringVar(&opts.conflict, "conflict", flashback.FlashbackConflictRefuse, i18n.T("cmd.flashback.flag.conflict"))
	cmd.Flags().BoolVar(&opts.plan, "plan", false, i18n.T("cmd.flashback.flag.plan"))
	cmd.Flags().IntVar(&opts.conflictLimit, "conflict-limit", 20, i18n.T("cmd.flashback.flag.conflictLimit"))
	help := cmd.HelpFunc()
	cmd.SetHelpFunc(func(c *cobra.Command, args []string) {
		if langFlag != "" {
			_ = i18n.Init(langFlag)
		}
		if c == cmd {
			refreshFlashbackHelp(c)
		} else {
			refreshFlashbackChildHelp(c)
		}
		c.SetUsageTemplate(flashbackUsageTemplate())
		help(c, args)
	})
	cmd.AddCommand(newFlashbackVerifyCommand())
	refreshFlashbackHelp(cmd)
	return cmd
}

func refreshFlashbackHelp(cmd *cobra.Command) {
	cmd.Use = i18n.T("cmd.flashback.use")
	cmd.Short = i18n.T("cmd.flashback.short")
	cmd.Long = i18n.T("cmd.flashback.long")
	cmd.SetUsageTemplate(flashbackUsageTemplate())
	usage := map[string]string{
		"start":          "cmd.analyze.flag.start",
		"end":            "cmd.analyze.flag.end",
		"start-position": "cmd.flashback.flag.startPosition",
		"stop-position":  "cmd.flashback.flag.stopPosition",
		"include-gtids":  "cmd.flashback.flag.includeGtids",
		"exclude-gtids":  "cmd.flashback.flag.excludeGtids",
		"from-dir":       "cmd.analyze.flag.fromDir",
		"prefix":         "cmd.analyze.flag.prefix",
		"sql-context":    "cmd.flashback.flag.sqlContext",
		"include-schema": "cmd.flashback.flag.includeSchema",
		"exclude-schema": "cmd.flashback.flag.excludeSchema",
		"include-table":  "cmd.flashback.flag.includeTable",
		"exclude-table":  "cmd.flashback.flag.excludeTable",
		"schema-file":    "cmd.flashback.flag.schemaFile",
		"schema-file-db": "cmd.flashback.flag.schemaFileDB",
		"dml":            "cmd.flashback.flag.dml",
		"conflict-limit": "cmd.flashback.flag.conflictLimit",
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

func refreshFlashbackChildHelp(cmd *cobra.Command) {
	switch cmd.Name() {
	case "verify":
		cmd.Use = i18n.T("cmd.flashback.verify.use")
		cmd.Short = i18n.T("cmd.flashback.verify.short")
		cmd.Long = i18n.T("cmd.flashback.verify.long")
		if flag := cmd.Flags().Lookup("format"); flag != nil {
			flag.Usage = i18n.T("cmd.flashback.verify.flag.format")
		}
	}
	if cmd.Parent() != nil && cmd.Parent().Parent() != nil {
		if flag := cmd.Parent().Parent().PersistentFlags().Lookup("lang"); flag != nil {
			flag.Usage = i18n.T("cmd.root.flag.lang")
		}
	}
}

func flashbackUsageTemplate() string {
	return i18n.T("cmd.flashback.help.usage") + `:{{if .Runnable}}
  {{.UseLine}}{{end}}{{if .HasExample}}

` + i18n.T("cmd.flashback.help.examples") + `:
{{.Example}}{{end}}{{if .HasAvailableLocalFlags}}

` + i18n.T("cmd.flashback.help.flags") + `:
{{.LocalFlags.FlagUsages | trimTrailingWhitespaces}}{{end}}{{if .HasAvailableInheritedFlags}}

` + i18n.T("cmd.flashback.help.globalFlags") + `:
{{.InheritedFlags.FlagUsages | trimTrailingWhitespaces}}{{end}}{{if .HasAvailableSubCommands}}

{{range .Commands}}{{if .IsAvailableCommand}}  {{rpad .Name .NamePadding }} {{.Short}}
{{end}}{{end}}{{end}}
`
}

// flashbackExit types a flashback refusal: a conflict (#259) is its own
// kind, exit 7; a resource ceiling is exit 10 (ADR 0012); any other refusal
// is exit 6 (ADR 0008).
func flashbackExit(err error) error {
	if err == nil {
		return nil
	}
	var resource *flashback.ResourceError
	if errors.As(err, &resource) {
		return clierr.WithDetails(clierr.KindResource, err, map[string]any{"reason": resource.Reason})
	}
	var conflict *flashback.FlashbackConflictError
	if errors.As(err, &conflict) {
		return clierr.WithDetails(clierr.KindConflict, err, map[string]any{"count": conflict.Count, "listed": conflict.Listed})
	}
	if reason, table, ok := flashback.RefusalDetails(err); ok {
		details := map[string]any{"reason": reason}
		if table != "" && table != "selected range" {
			details["table"] = table
		}
		return clierr.WithDetails(clierr.KindRefused, err, details)
	}
	return clierr.Wrap(clierr.KindRefused, err)
}

func writeFlashbackSQL(stream commandAnalyzer) error {
	src, ok := stream.(*analyzer.Analyzer)
	if !ok || src == nil || src.Planner() == nil {
		return clierr.Errorf(clierr.KindError, "%s", i18n.Tf("error.flashbackCapture", map[string]any{"Table": "selected range"}))
	}
	plan := src.Planner()
	if plan.PlanMode() {
		out := bufio.NewWriter(os.Stdout)
		err := plan.WritePlan(out)
		if ferr := out.Flush(); err == nil && ferr != nil {
			return clierr.Wrap(clierr.KindOutput, ferr)
		}
		return flashbackExit(err)
	}
	// Every refusal happens here, before any SQL is written.
	rows, err := plan.Prepare()
	if err != nil {
		return flashbackExit(err)
	}
	if rows == 0 {
		return &ExitError{Code: 2, Msg: i18n.T("error.noAnalyzableEvents")}
	}
	for _, warning := range plan.Warnings() {
		fmt.Fprintln(os.Stderr, warning)
	}
	// The sealed script is written to a temp file and copied to stdout only
	// after that write succeeds, so exit 8 does not leave a prefix (ADR 0013).
	err = flashback.Publish(os.Stdout, func(w io.Writer) error {
		_, werr := plan.WriteTo(w)
		return werr
	})
	if err != nil {
		var refusal *flashback.FlashbackConflictError
		var resource *flashback.ResourceError
		if errors.As(err, &refusal) || errors.As(err, &resource) {
			return flashbackExit(err)
		}
		return clierr.Wrap(clierr.KindOutput, err)
	}
	return nil
}
