package binlogviz

import (
	"fmt"
	"io"
	"path"
	"strings"

	"github.com/spf13/cobra"

	comparepkg "binlogviz/internal/compare"
	snapshotpkg "binlogviz/internal/snapshot"
	trendpkg "binlogviz/internal/trend"
)

type trendOptions struct {
	format           string
	fromSnapshots    string
	baselineSnapshot string
	snapshotDir      string
	topTables        int
}

func newTrendCommand() *cobra.Command {
	opts := &trendOptions{}

	cmd := &cobra.Command{
		Use:   "trend [<snapshot...>]",
		Short: "Analyze multiple snapshots as an ordered trend",
		Args: func(cmd *cobra.Command, args []string) error {
			hasPattern := strings.TrimSpace(opts.fromSnapshots) != ""
			if hasPattern && len(args) > 0 {
				return fmt.Errorf("cannot combine explicit snapshot arguments with --from-snapshots")
			}
			if !hasPattern && len(args) < 2 {
				return fmt.Errorf("trend requires at least 2 snapshots")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return runTrend(opts, args, cmd.OutOrStdout())
		},
	}

	cmd.Flags().StringVar(&opts.format, "format", "text", "Trend output format: text, json, or html")
	cmd.Flags().StringVar(&opts.fromSnapshots, "from-snapshots", "", "Pattern used to select snapshots by name")
	cmd.Flags().StringVar(&opts.baselineSnapshot, "baseline-snapshot", "", "Optional snapshot name to use as baseline deltas")
	cmd.Flags().StringVar(&opts.snapshotDir, "snapshot-dir", "", "Directory to load snapshots from")
	cmd.Flags().IntVar(&opts.topTables, "top-tables", 10, "Number of top tables to include in trend output")

	return cmd
}

func runTrend(opts *trendOptions, args []string, out io.Writer) error {
	switch opts.format {
	case "text", "json", "html":
	default:
		return fmt.Errorf("unsupported trend format: %s", opts.format)
	}

	points, inputMode, resolvedDir, err := resolveTrendInputs(args, opts)
	if err != nil {
		return err
	}
	var baseline *trendpkg.BuildInput
	if strings.TrimSpace(opts.baselineSnapshot) != "" {
		loaded, err := loadTrendSnapshot(opts.snapshotDir, opts.baselineSnapshot)
		if err != nil {
			return trendLoadError("baseline", opts.baselineSnapshot, err)
		}
		baseline = &loaded
	}

	result, err := trendpkg.BuildResult(trendpkg.BuildOptions{
		InputMode:   inputMode,
		SnapshotDir: resolvedDir,
		Points:      points,
		Baseline:    baseline,
		TopTables:   opts.topTables,
	})
	if err != nil {
		return err
	}

	var output string
	switch opts.format {
	case "json":
		output, err = trendpkg.RenderJSON(result)
	case "html":
		output, err = trendpkg.RenderHTML(result)
	default:
		output, err = trendpkg.RenderText(result)
	}
	if err != nil {
		return fmt.Errorf("render trend output (%s): %w", opts.format, err)
	}
	_, err = fmt.Fprint(out, output)
	if err != nil {
		return fmt.Errorf("write trend output: %w", err)
	}
	return nil
}

func resolveTrendInputs(args []string, opts *trendOptions) ([]trendpkg.BuildInput, string, string, error) {
	resolvedDir, err := snapshotpkg.ResolveSnapshotDir(opts.snapshotDir)
	if err != nil {
		return nil, "", "", err
	}

	if strings.TrimSpace(opts.fromSnapshots) != "" {
		pattern := strings.TrimSpace(opts.fromSnapshots)
		entries, err := snapshotpkg.ListSnapshots(opts.snapshotDir)
		if err != nil {
			return nil, "", "", err
		}
		selected := make([]trendpkg.BuildInput, 0, len(entries))
		for _, entry := range entries {
			matched, matchErr := path.Match(pattern, entry.Name)
			if matchErr != nil {
				return nil, "", "", fmt.Errorf("invalid snapshot pattern %q: %w", pattern, matchErr)
			}
			if !matched {
				continue
			}
			loaded, err := loadTrendSnapshot(opts.snapshotDir, entry.Name)
			if err != nil {
				return nil, "", "", trendLoadError("trend", entry.Name, err)
			}
			selected = append(selected, loaded)
		}
		if len(selected) < 2 {
			return nil, "", "", fmt.Errorf("trend requires at least 2 snapshots after resolving pattern %q", pattern)
		}
		return selected, "pattern", resolvedDir, nil
	}

	selected := make([]trendpkg.BuildInput, 0, len(args))
	for _, name := range args {
		loaded, err := loadTrendSnapshot(opts.snapshotDir, name)
		if err != nil {
			return nil, "", "", trendLoadError("trend", name, err)
		}
		selected = append(selected, loaded)
	}
	return selected, "explicit", resolvedDir, nil
}

func loadTrendSnapshot(dir, name string) (trendpkg.BuildInput, error) {
	path, data, err := snapshotpkg.LoadSnapshot(dir, name)
	if err != nil {
		return trendpkg.BuildInput{}, err
	}
	report, err := comparepkg.DecodeReportJSON(data)
	if err != nil {
		return trendpkg.BuildInput{}, err
	}
	return trendpkg.BuildInput{
		Path:   path,
		Report: report,
	}, nil
}

func trendLoadError(side, name string, err error) error {
	return fmt.Errorf("load %s snapshot %q: %w", side, name, err)
}
