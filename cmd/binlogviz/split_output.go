// Package binlogviz binds the split-output flags to the writers (ADR 0025).
// input: --output-dir / --file-per-table / --max-statements-per-file flag values.
// output: a validated splitout.Spec for the analyzer options, the staged
// directory writers, or a usage error.
// pos: command-layer validation and output mount; the writers live in
// internal/flashback and internal/redo, the staging sink in internal/splitout.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"errors"
	"fmt"
	"os"

	"binlogviz/internal/clierr"
	"binlogviz/internal/flashback"
	"binlogviz/internal/i18n"
	"binlogviz/internal/redo"
	"binlogviz/internal/splitout"
)

// compileSplitFlags validates the split combination and fills opts.split.
// Called with the other flag checks, before any input is read, so a bad
// combination or unusable directory is a usage error (exit 3). maxChanged
// is the flag's Changed bit: an explicit 0 is invalid, not unset.
func compileSplitFlags(opts *analyzeOptions, forFlashback, maxChanged bool) error {
	if opts.splitDir == "" && !opts.filePerTable && !maxChanged {
		return nil
	}
	if opts.splitDir == "" {
		return clierr.Errorf(clierr.KindUsage, "%s", i18n.T("error.splitNeedsOutputDir"))
	}
	if maxChanged && opts.maxStmtsPerFile < 1 {
		return clierr.Errorf(clierr.KindUsage, "%s", i18n.Tf("error.splitMaxInvalid", map[string]any{"Value": opts.maxStmtsPerFile}))
	}
	if !opts.filePerTable && opts.maxStmtsPerFile == 0 {
		return clierr.Errorf(clierr.KindUsage, "%s", i18n.T("error.splitOutputNeedsFlag"))
	}
	if forFlashback && opts.plan {
		return clierr.Errorf(clierr.KindUsage, "%s", i18n.T("error.splitPlan"))
	}
	if err := splitout.CheckDir(opts.splitDir); err != nil {
		return err
	}
	opts.split = &splitout.Spec{Dir: opts.splitDir, PerTable: opts.filePerTable, MaxStatements: opts.maxStmtsPerFile}
	return nil
}

// writeFlashbackSplit seals one script per unit into the staging sink, then
// publishes the directory (ADR 0025). Any failure leaves no target dir.
func writeFlashbackSplit(plan *flashback.Planner, spec *splitout.Spec) error {
	sink, err := splitout.Open(*spec)
	if err != nil {
		return err
	}
	defer sink.Abort()
	if _, err := plan.WriteSplit(sink, *spec); err != nil {
		return flashbackSplitExit(err)
	}
	if err := sink.Publish(); err != nil {
		return err
	}
	return printSplitResult(len(sink.Manifest()), spec.Dir)
}

// writeRedoSplit streams the part files into the staging sink, then
// publishes the directory (ADR 0025). Any failure leaves no target dir.
func writeRedoSplit(plan *redo.Planner, spec *splitout.Spec) error {
	sink, err := splitout.Open(*spec)
	if err != nil {
		return err
	}
	defer sink.Abort()
	if _, err := plan.WriteSplit(sink, *spec); err != nil {
		return redoSplitExit(err)
	}
	if err := sink.Publish(); err != nil {
		return err
	}
	return printSplitResult(len(sink.Manifest()), spec.Dir)
}

// flashbackSplitExit maps a WriteSplit failure: planner refusals keep their
// kinds; everything else is an output failure, never a refusal.
func flashbackSplitExit(err error) error {
	var kerr *clierr.Error
	if errors.As(err, &kerr) {
		return err
	}
	if _, _, ok := flashback.RefusalDetails(err); ok {
		return flashbackExit(err)
	}
	var resource *flashback.ResourceError
	var conflict *flashback.FlashbackConflictError
	if errors.As(err, &resource) || errors.As(err, &conflict) {
		return flashbackExit(err)
	}
	return clierr.Wrap(clierr.KindOutput, err)
}

// redoSplitExit is flashbackSplitExit for the redo refusal taxonomy.
func redoSplitExit(err error) error {
	var kerr *clierr.Error
	if errors.As(err, &kerr) {
		return err
	}
	if _, _, ok := redo.RefusalDetails(err); ok {
		return redoExit(err)
	}
	var resource *redo.ResourceError
	if errors.As(err, &resource) {
		return redoExit(err)
	}
	return clierr.Wrap(clierr.KindOutput, err)
}

func printSplitResult(files int, dir string) error {
	_, err := fmt.Fprintf(infoWriter(os.Stderr), "%s\n", i18n.Tf("progress.splitWrote", map[string]any{"Count": files, "Dir": dir}))
	return err
}
