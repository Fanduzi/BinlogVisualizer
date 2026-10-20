// Package binlogviz defines command-layer report comparison.
// input: two analyze JSON paths or snapshot names, snapshot directory, and text/JSON/HTML format.
// output: validated compare reports rendered to the command output stream with actionable load errors.
// pos: CLI boundary between report/snapshot loading and the shared compare engine and renderers.
// note: if this file changes, update this header and cmd/binlogviz/README.md.
package binlogviz

import (
	"binlogviz/internal/clierr"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"strings"
	"time"

	"github.com/spf13/cobra"

	comparepkg "binlogviz/internal/compare"
	"binlogviz/internal/displaytz"
	snapshotpkg "binlogviz/internal/snapshot"
)

type compareOptions struct {
	format           string
	currentSnapshot  string
	baselineSnapshot string
	snapshotDir      string
}

func newCompareCommand() *cobra.Command {
	opts := &compareOptions{}

	cmd := &cobra.Command{
		Use:   "compare [<current.json> <baseline.json>]",
		Short: "Compare two BinlogViz JSON analysis reports",
		Example: `  binlogviz analyze mysql-bin.000001 --format json --snapshot-name current --snapshot-dir snaps
  binlogviz analyze mysql-bin.000002 --format json --snapshot-name baseline --snapshot-dir snaps
  binlogviz compare --current-snapshot current --baseline-snapshot baseline --snapshot-dir snaps`,
		SilenceUsage:  true,
		SilenceErrors: true,
		Args: func(cmd *cobra.Command, args []string) error {
			hasCurrentSnapshot := strings.TrimSpace(opts.currentSnapshot) != ""
			hasBaselineSnapshot := strings.TrimSpace(opts.baselineSnapshot) != ""

			if hasCurrentSnapshot != hasBaselineSnapshot {
				return clierr.Errorf(clierr.KindUsage, "current-snapshot and baseline-snapshot must be provided together")
			}
			if hasCurrentSnapshot {
				if len(args) > 0 {
					return clierr.Errorf(clierr.KindUsage, "cannot combine file and snapshot compare modes")
				}
				return nil
			}
			return cobra.ExactArgs(2)(cmd, args)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return runCompare(opts, args, cmd.OutOrStdout(), displaytz.From(cmd.Context()))
		},
	}

	cmd.Flags().StringVar(&opts.format, "format", "text", "Compare output format: text, json, or html")
	cmd.Flags().StringVar(&opts.currentSnapshot, "current-snapshot", "", "Snapshot name to use as the current report")
	cmd.Flags().StringVar(&opts.baselineSnapshot, "baseline-snapshot", "", "Snapshot name to use as the baseline report")
	cmd.Flags().StringVar(&opts.snapshotDir, "snapshot-dir", "", "Directory to load snapshots from")

	return cmd
}

func runCompare(opts *compareOptions, args []string, out io.Writer, loc *time.Location) error {
	switch opts.format {
	case "text", "json", "html":
	default:
		return clierr.Errorf(clierr.KindUsage, "unsupported compare format: %s", opts.format)
	}

	current, baseline, err := resolveCompareReports(args, opts)
	if err != nil {
		return err
	}

	result := comparepkg.BuildCompareResult(current, baseline)

	var output string
	switch opts.format {
	case "json":
		output, err = comparepkg.RenderJSON(result)
	case "html":
		output, err = comparepkg.RenderHTMLIn(result, loc)
	default:
		output, err = comparepkg.RenderText(result)
	}
	if err != nil {
		return clierr.Errorf(clierr.KindError, "render compare output (%s): %w", opts.format, err)
	}

	_, err = fmt.Fprint(out, output)
	if err != nil {
		return clierr.Errorf(clierr.KindOutput, "write compare output: %w", err)
	}
	return nil
}

func resolveCompareReports(args []string, opts *compareOptions) (comparepkg.InputReport, comparepkg.InputReport, error) {
	if strings.TrimSpace(opts.currentSnapshot) != "" {
		current, err := loadSnapshotCompareReport("current", opts.snapshotDir, opts.currentSnapshot)
		if err != nil {
			return comparepkg.InputReport{}, comparepkg.InputReport{}, err
		}
		baseline, err := loadSnapshotCompareReport("baseline", opts.snapshotDir, opts.baselineSnapshot)
		if err != nil {
			return comparepkg.InputReport{}, comparepkg.InputReport{}, err
		}
		return current, baseline, nil
	}

	current, err := loadCompareReport("current", args[0])
	if err != nil {
		return comparepkg.InputReport{}, comparepkg.InputReport{}, err
	}
	baseline, err := loadCompareReport("baseline", args[1])
	if err != nil {
		return comparepkg.InputReport{}, comparepkg.InputReport{}, err
	}
	return current, baseline, nil
}

func loadCompareReport(side, path string) (comparepkg.InputReport, error) {
	report, err := comparepkg.LoadReport(path)
	if err == nil {
		return report, nil
	}

	var syntaxErr *json.SyntaxError
	if errors.As(err, &syntaxErr) {
		return comparepkg.InputReport{}, clierr.New(clierr.KindCorrupt, "compare wants two JSON reports or --current-snapshot/--baseline-snapshot")
	}
	// A report that cannot be opened or read (missing, permission, a
	// directory) is an input error, exit 4, as for analyze (#268). One that
	// reads but does not decode, or lacks required fields, is corrupt; a
	// report_version from a newer build keeps its unsupported kind (#277).
	kind := clierr.KindCorrupt
	var pathErr *fs.PathError
	switch {
	case errors.As(err, &pathErr):
		kind = clierr.KindInput
	}
	return comparepkg.InputReport{}, clierr.Errorf(kind, "load %s report %s: %w", side, path, err)
}

func loadSnapshotCompareReport(side, dir, name string) (comparepkg.InputReport, error) {
	_, data, err := snapshotpkg.LoadSnapshot(dir, name)
	if err != nil {
		return comparepkg.InputReport{}, clierr.Errorf(clierr.KindInput, "load %s snapshot %q: %w", side, name, err)
	}

	report, err := comparepkg.DecodeReportJSON(data)
	if err != nil {
		return comparepkg.InputReport{}, clierr.Errorf(clierr.KindCorrupt, "load %s snapshot %q: %w", side, name, err)
	}
	return report, nil
}
