// Package binlogviz owns the post-parse tail of the streaming run.
// input: the finalized result, the parse observers, and the output destinations resolved before parsing.
// output: the SQL-output branches — redo via the staged planner, flashback via the undo planner — or the report writers (JSON snapshot, HTML, text/markdown/json report).
// pos: extracted from analyze.go so the streaming entrypoint stays inside the codehealth line ceiling; behavior is the same tail, same order.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"os"

	"binlogviz/internal/analyzer"
	"binlogviz/internal/binlog"
	"binlogviz/internal/model"
	"binlogviz/internal/report"
)

// streamingRun carries the finished-parse state into finishStreamingRun.
type streamingRun struct {
	paths          []string
	opts           analyzer.Options
	reportOpts     report.Options
	format         string
	streamAnalyzer commandAnalyzer
	rawEvents      int
	formatObserver binlog.FormatObserver
	result         *model.AnalysisResult
	snapshotMeta   *model.Snapshot
	fileCoverage   model.FileCoverage
	snapshotName   string
	snapshotDir    string
	dest           outputDestination
	pathAliases    map[string]string
}

// finishStreamingRun owns the post-parse tail: the SQL-output branches
// (redo, then the report-side no-data guards, then flashback) and the
// report writers.
func finishStreamingRun(run streamingRun) error {
	opts := run.opts
	result := run.result
	formatObserver := run.formatObserver
	if opts.Redo {
		// The report-shaped no-data guards do not fit redo: DDL/ADMIN
		// passthrough can still emit when no row matched. The corrupt-input
		// check still applies; the planner's own statement count is the
		// truthful no_data arbiter (ADR 0022).
		if err := rejectEmptyOrIncompleteBinlog(run.paths, run.rawEvents); err != nil {
			return err
		}
		return writeRedoSQL(run.streamAnalyzer, formatObserver)
	}
	if err := applyAnalyzeOutcomeGuards(run.paths, opts, result, run.rawEvents, formatObserver); err != nil {
		return err
	}
	applyFileCoverage(result, run.paths, run.fileCoverage)
	aliasAnalysisPaths(result, run.pathAliases)
	if err := noteInputFormat(result, formatObserver); err != nil {
		return err
	}
	if opts.Flashback {
		return writeFlashbackSQL(run.streamAnalyzer)
	}
	if run.snapshotMeta != nil {
		result.Snapshot = run.snapshotMeta
	}
	if run.snapshotName != "" {
		if err := saveAndWriteJSONReport(*result, run.reportOpts, run.snapshotName, run.snapshotDir); err != nil {
			return classifyReportWriteErr("json", err)
		}
		return nil
	}
	// An HTML file, or HTML on a resolved stdout destination, keeps the
	// previous error kind: a render or write failure returns as-is.
	// A spill read is its own kind. Every other report goes through
	// report.Write, which builds the view and then refuses an unknown
	// format instead of rendering text.
	if run.format == "html" && (run.dest.IsFile || run.dest.IsStdout) {
		body, err := report.Render(*result, run.reportOpts, run.format)
		if err != nil {
			return classifyReportWriteErr(run.format, err)
		}
		if err := writeHTMLAtomically(run.dest, body); err != nil {
			return err
		}
		if run.dest.IsFile {
			printHTMLSaveConfirmation(run.dest.Path)
		}
		return nil
	}
	if err := report.Write(os.Stdout, *result, run.reportOpts, run.format); err != nil {
		return classifyReportWriteErr(run.format, err)
	}
	return nil
}
