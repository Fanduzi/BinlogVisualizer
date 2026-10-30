package binlogviz

import (
	"binlogviz/internal/clierr"
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"binlogviz/internal/analyzer"
	"binlogviz/internal/binlog"
	"binlogviz/internal/detailstore"
	"binlogviz/internal/displaytz"
	"binlogviz/internal/flashback"
	"binlogviz/internal/i18n"
	"binlogviz/internal/model"
	"binlogviz/internal/remote"
	"binlogviz/internal/report"
	"binlogviz/internal/splitout"
)

func newFlashbackCommand() *cobra.Command {
	opts := &analyzeOptions{}
	warn := experimentalWarnOnce()
	cmd := &cobra.Command{
		Use:   i18n.T("cmd.flashback.use"),
		Short: i18n.T("cmd.flashback.short"),
		Long:  i18n.T("cmd.flashback.long"),
		Example: `  # After analyze names the GTID (alerts[].gtid or transactions[].gtid), pass it with --include-gtids.
  binlogviz flashback mysql-bin.000001 --plan
  binlogviz flashback mysql-bin.000001 --conflict guard`,
		SilenceUsage:  true,
		SilenceErrors: true,
		Args: func(cmd *cobra.Command, args []string) error {
			warn()
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
			warn()
			stage := clierr.KindUsage
			defer func() { err = clierr.Wrap(stage, err) }()
			if err := flashback.ValidateResourceEnv(); err != nil {
				return err
			}
			opts.display = displaytz.From(cmd.Context())
			startTime, endTime, err := parseTimeRange(opts.startTime, opts.endTime, opts.display)
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
				msg := i18n.T("error.flashbackSQLContext")
				err := clierr.WithDetails(clierr.KindRefused, clierr.Errorf(clierr.KindRefused, "%s", msg), map[string]any{"reason": "sql_context_off"})
				if !opts.plan {
					return err
				}
				body, jerr := json.MarshalIndent(flashback.SQLContextOffPlan(opts.conflict, msg), "", "  ")
				if jerr != nil {
					return jerr
				}
				if _, werr := os.Stdout.Write(append(body, '\n')); werr != nil {
					return werr
				}
				return err
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
			if err := compileWhereFlag(opts); err != nil {
				return err
			}
			if err := compileSplitFlags(opts, true, cmd.Flags().Changed("max-statements-per-file")); err != nil {
				return err
			}
			gtidSelector, err := buildGTIDSelector(opts)
			if err != nil {
				return err
			}

			opts.remote = analyzeRemoteRequested(cmd)
			stage = clierr.KindInput
			in, err := openFlashbackInput(cmd, args, opts)
			if err != nil {
				return err
			}
			defer in.cleanup()
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
			return runAnalysisStreamingFastWithOutput(in.paths, analyzerOpts, report.Options{}, "text", in.parser, func(opts analyzer.Options, store *detailstore.DuckDBStore) commandAnalyzer {
				if opts.DetailStoreMode == analyzer.DetailStoreDuckDB {
					return analyzer.NewWithStore(opts, store)
				}
				return analyzer.New(opts)
			}, func(root string) (*detailstore.DuckDBStore, func() error, string, error) {
				return createDuckDBTempStore(root)
			}, "", nil, in.fileCoverage, "", "", outputDestination{}, in.pathAliases)
		},
	}
	registerFlashbackFlags(cmd, opts)
	// A usage error still runs the warning once: flag parse errors never
	// reach Args or RunE (ADR 0020's "every run").
	cmd.SetFlagErrorFunc(func(c *cobra.Command, err error) error {
		warn()
		return err
	})
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

// flashbackInput is one run's resolved input: the parser, the paths it
// feeds, file coverage, and the deferred cleanup the run must call.
type flashbackInput struct {
	parser       binlog.Parser
	paths        []string
	pathAliases  map[string]string
	fileCoverage model.FileCoverage
	cleanup      func()
}

// openFlashbackInput resolves the run's input half (ADR 0023): remote opens
// the dump and injects the remote parser; local resolves, materializes, and
// checks the file list. Flag/config errors stay usage; input failures stay
// input — the stage boundary the remote and local branches draw inline in
// redo.go.
func openFlashbackInput(cmd *cobra.Command, args []string, opts *analyzeOptions) (flashbackInput, error) {
	in := flashbackInput{parser: binlog.NewParser(), cleanup: func() {}}
	if opts.remote {
		runCfg, err := buildRemoteRunConfig(cmd, opts)
		if err != nil {
			return in, clierr.Wrap(clierr.KindUsage, err)
		}
		src, boundary, err := openRemoteInput(cmd.Context(), runCfg)
		if err != nil {
			return in, clierr.Wrap(clierr.KindInput, err)
		}
		in.cleanup = src.Close
		in.parser = binlog.NewRemoteParser(cmd.Context(), src, boundary)
		in.paths = []string{runCfg.origin}
		in.fileCoverage = remoteFileCoverage(runCfg.origin, src.Manifest(), runCfg.cfg.Start.File, runCfg.cfg.GTIDSet != nil)
		printResolvedPaths(infoWriter(os.Stderr), in.paths)
		return in, nil
	}
	paths, discovered, fileCoverage, err := resolveAnalyzePaths(args, opts)
	if err != nil {
		return in, clierr.Wrap(clierr.KindInput, err)
	}
	paths, pathAliases, cleanupInputs, err := materializeAnalyzePaths(paths)
	if cleanupInputs != nil {
		in.cleanup = cleanupInputs
	}
	if err != nil {
		in.cleanup()
		return in, clierr.Wrap(clierr.KindInput, err)
	}
	if !discovered {
		fileCoverage = fileCoverageForPaths(paths)
	}
	if discovered {
		printResolvedPaths(infoWriter(os.Stderr), paths)
	}
	if len(paths) == 0 {
		return in, clierr.Errorf(clierr.KindInput, "%s", i18n.T("error.noResolvedFiles"))
	}
	if err := validateFiles(paths); err != nil {
		return in, clierr.Wrap(clierr.KindInput, err)
	}
	in.paths = paths
	in.pathAliases = pathAliases
	in.fileCoverage = fileCoverage
	return in, nil
}

// registerFlashbackFlags binds the flashback flag surface: analyze's
// selectors and input modes including remote, plus the undo-specific
// conflict and schema-file flags (ADR 0023).
func registerFlashbackFlags(cmd *cobra.Command, opts *analyzeOptions) {
	cmd.Flags().StringVar(&opts.startTime, "start", "", i18n.T("cmd.analyze.flag.start"))
	cmd.Flags().StringVar(&opts.endTime, "end", "", i18n.T("cmd.analyze.flag.end"))
	cmd.Flags().Int64Var(&opts.startPosition, "start-position", 0, i18n.T("cmd.flashback.flag.startPosition"))
	cmd.Flags().Int64Var(&opts.stopPosition, "stop-position", 0, i18n.T("cmd.flashback.flag.stopPosition"))
	cmd.Flags().StringSliceVar(&opts.includeGTIDs, "include-gtids", nil, i18n.T("cmd.flashback.flag.includeGtids"))
	cmd.Flags().StringSliceVar(&opts.excludeGTIDs, "exclude-gtids", nil, i18n.T("cmd.flashback.flag.excludeGtids"))
	cmd.Flags().StringVar(&opts.fromDir, "from-dir", "", i18n.T("cmd.analyze.flag.fromDir"))
	cmd.Flags().StringVar(&opts.prefix, "prefix", "", i18n.T("cmd.analyze.flag.prefix"))
	cmd.Flags().StringVar(&opts.indexFile, "index-file", "", i18n.T("cmd.analyze.flag.indexFile"))
	cmd.Flags().StringVar(&opts.sqlContext, "sql-context", string(report.SQLContextSummary), i18n.T("cmd.flashback.flag.sqlContext"))
	cmd.Flags().StringSliceVar(&opts.includeSchemas, "include-schema", nil, i18n.T("cmd.flashback.flag.includeSchema"))
	cmd.Flags().StringSliceVar(&opts.excludeSchemas, "exclude-schema", nil, i18n.T("cmd.flashback.flag.excludeSchema"))
	cmd.Flags().StringSliceVar(&opts.includeTables, "include-table", nil, i18n.T("cmd.flashback.flag.includeTable"))
	cmd.Flags().StringSliceVar(&opts.excludeTables, "exclude-table", nil, i18n.T("cmd.flashback.flag.excludeTable"))
	cmd.Flags().StringVar(&opts.schemaFile, "schema-file", "", i18n.T("cmd.flashback.flag.schemaFile"))
	cmd.Flags().StringVar(&opts.schemaFileDB, "schema-file-db", "", i18n.T("cmd.flashback.flag.schemaFileDB"))
	cmd.Flags().StringSliceVar(&opts.dml, "dml", nil, i18n.T("cmd.flashback.flag.dml"))
	cmd.Flags().StringVar(&opts.where, "where", "", i18n.T("cmd.flashback.flag.where"))
	cmd.Flags().StringVar(&opts.conflict, "conflict", flashback.FlashbackConflictRefuse, i18n.T("cmd.flashback.flag.conflict"))
	cmd.Flags().BoolVar(&opts.plan, "plan", false, i18n.T("cmd.flashback.flag.plan"))
	cmd.Flags().IntVar(&opts.conflictLimit, "conflict-limit", 20, i18n.T("cmd.flashback.flag.conflictLimit"))
	cmd.Flags().StringVar(&opts.splitDir, "output-dir", "", i18n.T("cmd.flashback.flag.outputDir"))
	cmd.Flags().BoolVar(&opts.filePerTable, "file-per-table", false, i18n.T("cmd.flashback.flag.filePerTable"))
	cmd.Flags().Int64Var(&opts.maxStmtsPerFile, "max-statements-per-file", 0, i18n.T("cmd.flashback.flag.maxStatementsPerFile"))
	cmd.Flags().StringVar(&opts.remoteHost, "host", "", i18n.T("cmd.analyze.flag.host"))
	cmd.Flags().Uint16Var(&opts.remotePort, "port", 3306, i18n.T("cmd.analyze.flag.port"))
	cmd.Flags().StringVar(&opts.remoteUser, "user", "", i18n.T("cmd.analyze.flag.user"))
	cmd.Flags().StringVar(&opts.remotePassword, "password", "", i18n.T("cmd.analyze.flag.password"))
	cmd.Flags().Uint32Var(&opts.remoteServerID, "server-id", 0, i18n.T("cmd.analyze.flag.serverID"))
	cmd.Flags().StringVar(&opts.remoteStartFile, "start-file", "", i18n.T("cmd.analyze.flag.startFile"))
	cmd.Flags().Uint32Var(&opts.remoteStartPos, "start-pos", 4, i18n.T("cmd.analyze.flag.startPos"))
	cmd.Flags().StringVar(&opts.remoteStartGTID, "start-gtid", "", i18n.T("cmd.analyze.flag.startGTID"))
	cmd.Flags().StringVar(&opts.remoteSSLMode, "ssl-mode", string(remote.SSLPreferred), i18n.T("cmd.analyze.flag.sslMode"))
}

const experimentalMark = "[experimental] "

func markExperimental(s string) string {
	if strings.HasPrefix(s, experimentalMark) {
		return s
	}
	return experimentalMark + s
}

// experimentalWarnOnce returns the one-time stderr warning for one command
// execution. Cobra reaches Args, the flag-error hook, or RunE first, so all
// three call it and only the first prints (ADR 0020's "every run").
func experimentalWarnOnce() func() {
	warned := false
	return func() {
		if warned {
			return
		}
		warned = true
		warnFlashbackExperimental()
	}
}

// warnFlashbackExperimental is the one stderr line every flashback run prints
// (ADR 0020). --quiet does not hide it. Help is not a run and does not print it.
func warnFlashbackExperimental() {
	fmt.Fprintf(os.Stderr, "%s\n", i18n.Tf("warning.flashbackLine", map[string]any{
		"Message": i18n.T("warning.flashbackExperimental"),
	}))
}

func refreshFlashbackHelp(cmd *cobra.Command) {
	cmd.Use = i18n.T("cmd.flashback.use")
	cmd.Short = markExperimental(i18n.T("cmd.flashback.short"))
	cmd.Long = markExperimental(i18n.T("cmd.flashback.long"))
	cmd.SetUsageTemplate(flashbackUsageTemplate())
	usage := map[string]string{
		"start":                   "cmd.analyze.flag.start",
		"end":                     "cmd.analyze.flag.end",
		"start-position":          "cmd.flashback.flag.startPosition",
		"stop-position":           "cmd.flashback.flag.stopPosition",
		"include-gtids":           "cmd.flashback.flag.includeGtids",
		"exclude-gtids":           "cmd.flashback.flag.excludeGtids",
		"from-dir":                "cmd.analyze.flag.fromDir",
		"prefix":                  "cmd.analyze.flag.prefix",
		"index-file":              "cmd.analyze.flag.indexFile",
		"sql-context":             "cmd.flashback.flag.sqlContext",
		"include-schema":          "cmd.flashback.flag.includeSchema",
		"exclude-schema":          "cmd.flashback.flag.excludeSchema",
		"include-table":           "cmd.flashback.flag.includeTable",
		"exclude-table":           "cmd.flashback.flag.excludeTable",
		"schema-file":             "cmd.flashback.flag.schemaFile",
		"schema-file-db":          "cmd.flashback.flag.schemaFileDB",
		"dml":                     "cmd.flashback.flag.dml",
		"where":                   "cmd.flashback.flag.where",
		"conflict":                "cmd.flashback.flag.conflict",
		"plan":                    "cmd.flashback.flag.plan",
		"conflict-limit":          "cmd.flashback.flag.conflictLimit",
		"output-dir":              "cmd.flashback.flag.outputDir",
		"file-per-table":          "cmd.flashback.flag.filePerTable",
		"max-statements-per-file": "cmd.flashback.flag.maxStatementsPerFile",
		"host":                    "cmd.analyze.flag.host",
		"port":                    "cmd.analyze.flag.port",
		"user":                    "cmd.analyze.flag.user",
		"password":                "cmd.analyze.flag.password",
		"server-id":               "cmd.analyze.flag.serverID",
		"start-file":              "cmd.analyze.flag.startFile",
		"start-pos":               "cmd.analyze.flag.startPos",
		"start-gtid":              "cmd.analyze.flag.startGTID",
		"ssl-mode":                "cmd.analyze.flag.sslMode",
	}
	for name, key := range usage {
		if flag := cmd.Flags().Lookup(name); flag != nil {
			flag.Usage = markExperimental(i18n.T(key))
		}
	}
	for _, child := range cmd.Commands() {
		refreshFlashbackChildHelp(child)
	}
	refreshInheritedFlashbackFlags(cmd)
}

func refreshInheritedFlashbackFlags(cmd *cobra.Command) {
	root := cmd
	for root.Parent() != nil {
		root = root.Parent()
	}
	for _, spec := range []struct{ name, key string }{
		{"lang", "cmd.root.flag.lang"},
		{"quiet", "cmd.root.flag.quiet"},
		{"error-format", "cmd.root.flag.errorFormat"},
	} {
		if flag := root.PersistentFlags().Lookup(spec.name); flag != nil {
			flag.Usage = i18n.T(spec.key)
		}
	}
}

func refreshFlashbackChildHelp(cmd *cobra.Command) {
	switch cmd.Name() {
	case "verify":
		cmd.Use = i18n.T("cmd.flashback.verify.use")
		cmd.Short = markExperimental(i18n.T("cmd.flashback.verify.short"))
		cmd.Long = markExperimental(i18n.T("cmd.flashback.verify.long"))
		if flag := cmd.Flags().Lookup("format"); flag != nil {
			flag.Usage = markExperimental(i18n.T("cmd.flashback.verify.flag.format"))
		}
	}
	refreshInheritedFlashbackFlags(cmd)
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

func writeFlashbackSQL(stream commandAnalyzer, spec *splitout.Spec) error {
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
	if spec != nil {
		// The split layout decides the file-spanning warnings before the
		// first file opens (ADR 0025).
		if err := plan.PrepareSplit(*spec); err != nil {
			return flashbackSplitExit(err)
		}
	}
	for _, warning := range plan.Warnings() {
		fmt.Fprintln(os.Stderr, warning)
	}
	if spec != nil {
		return writeFlashbackSplit(plan, spec)
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
