package binlogviz

import (
	"binlogviz/internal/clierr"
	"fmt"
	"io"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/spf13/cobra"

	comparepkg "binlogviz/internal/compare"
	"binlogviz/internal/displaytz"
	"binlogviz/internal/i18n"
	snapshotpkg "binlogviz/internal/snapshot"
	trendpkg "binlogviz/internal/trend"
)

type trendOptions struct {
	format           string
	fromSnapshots    string
	baselineSnapshot string
	snapshotDir      string
	topTables        int
	order            string
}

// trendFormats is the authoritative --format list: runTrend validates
// against it and renderTrend must render each entry (its switch has no
// default arm, so a listed format with no case errors instead of silently
// rendering text).
var trendFormats = []string{"text", "json", "html"}

func newTrendCommand() *cobra.Command {
	opts := &trendOptions{}

	cmd := &cobra.Command{
		Use:   i18n.T("cmd.trend.use"),
		Short: i18n.T("cmd.trend.short"),
		Example: `  binlogviz analyze mysql-bin.000001 --format json --snapshot-name before --snapshot-dir snaps
  binlogviz analyze mysql-bin.000002 --format json --snapshot-name during --snapshot-dir snaps
  binlogviz analyze mysql-bin.000001 --format json --snapshot-name after --snapshot-dir snaps
  binlogviz trend before during after --snapshot-dir snaps
  binlogviz trend --from-snapshots '*' --snapshot-dir snaps --format json`,
		SilenceUsage:  true,
		SilenceErrors: true,
		Args: func(cmd *cobra.Command, args []string) error {
			hasPattern := strings.TrimSpace(opts.fromSnapshots) != ""
			if hasPattern && len(args) > 0 {
				return clierr.Errorf(clierr.KindUsage, "%s", i18n.T("error.trendArgsWithPattern"))
			}
			if !hasPattern && len(args) < 2 {
				return clierr.Errorf(clierr.KindUsage, "%s", i18n.T("error.trendMinSnapshots"))
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return runTrend(opts, args, cmd.OutOrStdout(), cmd.ErrOrStderr(), displaytz.From(cmd.Context()))
		},
	}

	cmd.Flags().StringVar(&opts.format, "format", "text", i18n.T("cmd.trend.flag.format"))
	cmd.Flags().StringVar(&opts.fromSnapshots, "from-snapshots", "", i18n.T("cmd.trend.flag.fromSnapshots"))
	cmd.Flags().StringVar(&opts.baselineSnapshot, "baseline-snapshot", "", i18n.T("cmd.trend.flag.baselineSnapshot"))
	cmd.Flags().StringVar(&opts.snapshotDir, "snapshot-dir", "", i18n.T("cmd.trend.flag.snapshotDir"))
	cmd.Flags().IntVar(&opts.topTables, "top-tables", 10, i18n.T("cmd.trend.flag.topTables"))
	cmd.Flags().StringVar(&opts.order, "order", "cli", i18n.T("cmd.trend.flag.order"))

	return cmd
}

// renderTrend renders result in format. The switch has no default arm:
// a format trendFormats accepts with no case returns an unhandled-format
// error instead of silently rendering text.
func renderTrend(result trendpkg.Result, format string, loc *time.Location) (string, error) {
	switch format {
	case "text":
		return trendpkg.RenderText(result)
	case "json":
		return trendpkg.RenderJSON(result)
	case "html":
		return trendpkg.RenderHTMLIn(result, loc)
	}
	return "", clierr.Errorf(clierr.KindError, "unhandled trend format %q", format)
}

func runTrend(opts *trendOptions, args []string, out, errOut io.Writer, loc *time.Location) error {
	if !slices.Contains(trendFormats, opts.format) {
		return clierr.Errorf(clierr.KindUsage, "%s", i18n.Tf("error.unsupportedFormat", map[string]any{"Subject": "trend", "Value": opts.format}))
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
		Order:       opts.order,
	})
	if err != nil {
		return err
	}
	if result.Reordered {
		names := make([]string, 0, len(result.Points))
		for _, point := range result.Points {
			names = append(names, point.Snapshot.Name)
		}
		fmt.Fprint(errOut, i18n.Tf("cmd.trend.reordered", map[string]any{"Names": strings.Join(names, ", ")}))
	}

	output, err := renderTrend(result, opts.format, loc)
	if err != nil {
		return clierr.Errorf(clierr.KindError, "render trend output (%s): %w", opts.format, err)
	}
	_, err = fmt.Fprint(out, output)
	if err != nil {
		return clierr.Errorf(clierr.KindOutput, "write trend output: %w", err)
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
				return nil, "", "", clierr.Errorf(clierr.KindUsage, "invalid snapshot pattern %q: %w", pattern, matchErr)
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
			return nil, "", "", clierr.Errorf(clierr.KindUsage, "%s", i18n.Tf("error.trendMinSnapshotsPattern", map[string]any{"Pattern": pattern}))
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
	return clierr.Errorf(clierr.KindInput, "load %s snapshot %q: %w", side, name, err)
}
