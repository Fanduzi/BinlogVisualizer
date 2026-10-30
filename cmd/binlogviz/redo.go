// Package binlogviz defines the redo CLI command: forward (redo) SQL output.
// input: the same four input modes and selector/filter flags as analyze (positional files, stdin -, --from-dir with --prefix or --index-file, --host remote), plus --schema-file for MINIMAL-metadata name and type-fact resolution.
// output: replayable SQL staged in a temp file and copied to stdout only after parse, planning, and rendering all succeed; refusals leave nothing on stdout (ADR 0022).
// pos: command-layer orchestration reusing the analyze pipeline; report rendering is skipped — the redo branch returns before it, like flashback.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"

	"binlogviz/internal/analyzer"
	"binlogviz/internal/binlog"
	"binlogviz/internal/clierr"
	"binlogviz/internal/detailstore"
	"binlogviz/internal/displaytz"
	"binlogviz/internal/flashback"
	"binlogviz/internal/i18n"
	"binlogviz/internal/model"
	"binlogviz/internal/redo"
	"binlogviz/internal/remote"
	"binlogviz/internal/report"
)

func newRedoCommand() *cobra.Command {
	opts := &analyzeOptions{}
	cmd := &cobra.Command{
		Use:     i18n.T("cmd.redo.use"),
		Short:   i18n.T("cmd.redo.short"),
		Long:    i18n.T("cmd.redo.long"),
		Example: i18n.T("cmd.redo.example"),
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
			stage := clierr.KindUsage
			defer func() { err = clierr.Wrap(stage, err) }()
			if err := redo.ValidateResourceEnv(); err != nil {
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
			if opts.schemaFileDB != "" && opts.schemaFile == "" {
				return clierr.Errorf(clierr.KindUsage, "%s", i18n.T("error.schemaFileDBRequiresFile"))
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
			kinds, err := analyzer.ParseDMLKinds(opts.dml)
			if err != nil {
				return err
			}
			opts.dmlKinds = kinds
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
				if err := validateFiles(paths); err != nil {
					return err
				}
			}

			stage = clierr.KindError
			analyzerOpts := buildAnalyzerOptions(opts, startTime, endTime)
			analyzerOpts.GTIDSelector = gtidSelector
			analyzerOpts.Redo = true
			return runAnalysisStreamingFastWithOutput(paths, analyzerOpts, report.Options{}, "text", parser, func(opts analyzer.Options, store *detailstore.DuckDBStore) commandAnalyzer {
				if opts.DetailStoreMode == analyzer.DetailStoreDuckDB {
					return analyzer.NewWithStore(opts, store)
				}
				return analyzer.New(opts)
			}, func(root string) (*detailstore.DuckDBStore, func() error, string, error) {
				return createDuckDBTempStore(root)
			}, "", nil, fileCoverage, "", "", outputDestination{}, pathAliases)
		},
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	registerRedoFlags(cmd, opts)
	return cmd
}

// registerRedoFlags binds the redo flag surface: analyze's selectors and
// input modes plus --schema-file for MINIMAL-metadata resolution (ADR 0022).
func registerRedoFlags(cmd *cobra.Command, opts *analyzeOptions) {
	cmd.Flags().StringVar(&opts.startTime, "start", "", i18n.T("cmd.analyze.flag.start"))
	cmd.Flags().StringVar(&opts.endTime, "end", "", i18n.T("cmd.analyze.flag.end"))
	cmd.Flags().Int64Var(&opts.startPosition, "start-position", 0, i18n.T("cmd.redo.flag.startPosition"))
	cmd.Flags().Int64Var(&opts.stopPosition, "stop-position", 0, i18n.T("cmd.redo.flag.stopPosition"))
	cmd.Flags().StringSliceVar(&opts.includeGTIDs, "include-gtids", nil, i18n.T("cmd.redo.flag.includeGtids"))
	cmd.Flags().StringSliceVar(&opts.excludeGTIDs, "exclude-gtids", nil, i18n.T("cmd.redo.flag.excludeGtids"))
	cmd.Flags().StringVar(&opts.fromDir, "from-dir", "", i18n.T("cmd.analyze.flag.fromDir"))
	cmd.Flags().StringVar(&opts.prefix, "prefix", "", i18n.T("cmd.analyze.flag.prefix"))
	cmd.Flags().StringVar(&opts.indexFile, "index-file", "", i18n.T("cmd.analyze.flag.indexFile"))
	cmd.Flags().StringSliceVar(&opts.includeSchemas, "include-schema", nil, i18n.T("cmd.redo.flag.includeSchema"))
	cmd.Flags().StringSliceVar(&opts.excludeSchemas, "exclude-schema", nil, i18n.T("cmd.redo.flag.excludeSchema"))
	cmd.Flags().StringSliceVar(&opts.includeTables, "include-table", nil, i18n.T("cmd.redo.flag.includeTable"))
	cmd.Flags().StringSliceVar(&opts.excludeTables, "exclude-table", nil, i18n.T("cmd.redo.flag.excludeTable"))
	cmd.Flags().StringVar(&opts.schemaFile, "schema-file", "", i18n.T("cmd.redo.flag.schemaFile"))
	cmd.Flags().StringVar(&opts.schemaFileDB, "schema-file-db", "", i18n.T("cmd.redo.flag.schemaFileDB"))
	cmd.Flags().StringSliceVar(&opts.dml, "dml", nil, i18n.T("cmd.redo.flag.dml"))
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

// redoExit types a redo refusal: a resource ceiling is exit 10 (ADR 0012);
// any other refusal is exit 6 with details.reason (ADR 0022's taxonomy).
func redoExit(err error) error {
	if err == nil {
		return nil
	}
	var resource *redo.ResourceError
	if errors.As(err, &resource) {
		return clierr.WithDetails(clierr.KindResource, err, map[string]any{"reason": resource.Reason})
	}
	if reason, table, ok := redo.RefusalDetails(err); ok {
		details := map[string]any{"reason": reason}
		if table != "" && table != "selected range" {
			details["table"] = table
		}
		return clierr.WithDetails(clierr.KindRefused, err, details)
	}
	return clierr.Wrap(clierr.KindRefused, err)
}

// writeRedoSQL runs after the parse completes. A statement-format DML event
// anywhere in the input refuses before planning: normalization dropped it,
// so emitting a script would replay changes outside the selection (exit 9,
// statement_format). Every other refusal is decided by Prepare before a
// byte reaches stdout; the sealed script is staged and copied out only on
// success, the same contract flashback.Publish gives undo (ADR 0022).
func writeRedoSQL(stream commandAnalyzer, observer binlog.FormatObserver) error {
	if observer.QueryDMLEvents > 0 {
		return clierr.WithDetails(clierr.KindUnsupported, clierr.Errorf(clierr.KindUnsupported, "%s", i18n.T("error.redoStatementFormat")), map[string]any{"reason": "statement_format"})
	}
	src, ok := stream.(*analyzer.Analyzer)
	if !ok || src == nil || src.RedoPlanner() == nil {
		return clierr.Errorf(clierr.KindError, "%s", i18n.Tf("error.flashbackCapture", map[string]any{"Table": "selected range"}))
	}
	plan := src.RedoPlanner()
	if _, err := plan.Prepare(); err != nil {
		return redoExit(err)
	}
	if plan.Statements() == 0 {
		return &ExitError{Code: 2, Msg: i18n.T("error.noAnalyzableEvents")}
	}
	for _, warning := range plan.Warnings() {
		fmt.Fprintln(os.Stderr, warning)
	}
	err := flashback.Publish(os.Stdout, func(w io.Writer) error {
		_, werr := plan.WriteTo(w)
		return werr
	})
	if err != nil {
		var resource *redo.ResourceError
		if errors.As(err, &resource) {
			return redoExit(err)
		}
		if _, _, ok := redo.RefusalDetails(err); ok {
			return redoExit(err)
		}
		return clierr.Wrap(clierr.KindOutput, err)
	}
	return nil
}
