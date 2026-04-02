package binlogviz

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	comparepkg "binlogviz/internal/compare"
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
		Args: func(cmd *cobra.Command, args []string) error {
			hasCurrentSnapshot := strings.TrimSpace(opts.currentSnapshot) != ""
			hasBaselineSnapshot := strings.TrimSpace(opts.baselineSnapshot) != ""

			if hasCurrentSnapshot != hasBaselineSnapshot {
				return fmt.Errorf("current-snapshot and baseline-snapshot must be provided together")
			}
			if hasCurrentSnapshot {
				if len(args) > 0 {
					return fmt.Errorf("cannot combine file and snapshot compare modes")
				}
				return nil
			}
			return cobra.ExactArgs(2)(cmd, args)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			switch opts.format {
			case "text", "json", "html":
			default:
				return fmt.Errorf("unsupported compare format: %s", opts.format)
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
				output, err = comparepkg.RenderHTML(result)
			default:
				output, err = comparepkg.RenderText(result)
			}
			if err != nil {
				return fmt.Errorf("render compare output (%s): %w", opts.format, err)
			}

			_, err = fmt.Fprint(cmd.OutOrStdout(), output)
			if err != nil {
				return fmt.Errorf("write compare output: %w", err)
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&opts.format, "format", "text", "Compare output format: text, json, or html")
	cmd.Flags().StringVar(&opts.currentSnapshot, "current-snapshot", "", "Snapshot name to use as the current report")
	cmd.Flags().StringVar(&opts.baselineSnapshot, "baseline-snapshot", "", "Snapshot name to use as the baseline report")
	cmd.Flags().StringVar(&opts.snapshotDir, "snapshot-dir", "", "Directory to load snapshots from")

	return cmd
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

	current, err := comparepkg.LoadReport(args[0])
	if err != nil {
		return comparepkg.InputReport{}, comparepkg.InputReport{}, fmt.Errorf("load current report %s: %w", args[0], err)
	}
	baseline, err := comparepkg.LoadReport(args[1])
	if err != nil {
		return comparepkg.InputReport{}, comparepkg.InputReport{}, fmt.Errorf("load baseline report %s: %w", args[1], err)
	}
	return current, baseline, nil
}

func loadSnapshotCompareReport(side, dir, name string) (comparepkg.InputReport, error) {
	_, data, err := snapshotpkg.LoadSnapshot(dir, name)
	if err != nil {
		return comparepkg.InputReport{}, fmt.Errorf("load %s snapshot %q: %w", side, name, err)
	}

	report, err := comparepkg.DecodeReportJSON(data)
	if err != nil {
		return comparepkg.InputReport{}, fmt.Errorf("load %s snapshot %q: %w", side, name, err)
	}
	return report, nil
}
