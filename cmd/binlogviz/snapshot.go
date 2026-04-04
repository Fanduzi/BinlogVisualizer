package binlogviz

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	comparepkg "binlogviz/internal/compare"
	snapshotpkg "binlogviz/internal/snapshot"
	"binlogviz/internal/version"
)

type snapshotOptions struct {
	dir    string
	name   string
	format string
}

func newSnapshotCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "snapshot",
		Short: "Manage snapshots",
		Run: func(cmd *cobra.Command, args []string) {
			_ = cmd.Help()
		},
	}

	cmd.AddCommand(newSnapshotSaveCommand())
	cmd.AddCommand(newSnapshotListCommand())
	cmd.AddCommand(newSnapshotShowCommand())
	cmd.AddCommand(newSnapshotRenameCommand())
	cmd.AddCommand(newSnapshotDeleteCommand())

	return cmd
}

func newSnapshotSaveCommand() *cobra.Command {
	opts := &snapshotOptions{}

	cmd := &cobra.Command{
		Use:   "save <report.json>",
		Short: "Save an analyze JSON report as a named snapshot",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			data, err := os.ReadFile(args[0])
			if err != nil {
				return fmt.Errorf("read snapshot source %s: %w", args[0], err)
			}
			if _, err := comparepkg.DecodeReportJSON(data); err != nil {
				if isUnsupportedReportShape(err) {
					return err
				}
				return fmt.Errorf("decode snapshot source %s: %w", args[0], err)
			}

			normalized, err := normalizeSnapshotReport(data, opts.name, time.Now().UTC())
			if err != nil {
				return fmt.Errorf("normalize snapshot source %s: %w", args[0], err)
			}

			savedPath, err := snapshotpkg.SaveJSON(opts.dir, opts.name, normalized)
			if err != nil {
				return err
			}
			_, err = fmt.Fprintf(cmd.ErrOrStderr(), "Saved snapshot %q to %s\n", opts.name, savedPath)
			return err
		},
	}

	cmd.Flags().StringVar(&opts.name, "name", "", "Snapshot name")
	cmd.Flags().StringVar(&opts.dir, "snapshot-dir", "", "Directory to save snapshots")
	_ = cmd.MarkFlagRequired("name")

	return cmd
}

func newSnapshotListCommand() *cobra.Command {
	opts := &snapshotOptions{format: "text"}

	cmd := &cobra.Command{
		Use:   "list",
		Short: "List saved snapshots",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			entries, err := snapshotpkg.ListSnapshots(opts.dir)
			if err != nil {
				return err
			}
			switch opts.format {
			case "text":
				return writeSnapshotListText(cmd.OutOrStdout(), entries)
			case "json":
				resolvedDir, err := snapshotpkg.ResolveSnapshotDir(opts.dir)
				if err != nil {
					return err
				}
				return writeSnapshotJSON(cmd.OutOrStdout(), map[string]any{
					"snapshot_dir": resolvedDir,
					"snapshots":    entries,
				})
			default:
				return fmt.Errorf("unsupported snapshot list format: %s", opts.format)
			}
		},
	}

	cmd.Flags().StringVar(&opts.dir, "snapshot-dir", "", "Directory to load snapshots from")
	cmd.Flags().StringVar(&opts.format, "format", "text", "Snapshot list output format: text or json")
	return cmd
}

func newSnapshotShowCommand() *cobra.Command {
	opts := &snapshotOptions{format: "text"}

	cmd := &cobra.Command{
		Use:   "show <name>",
		Short: "Show snapshot metadata and summary",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			switch opts.format {
			case "text", "json":
			default:
				return fmt.Errorf("unsupported snapshot show format: %s", opts.format)
			}

			desc, err := snapshotpkg.DescribeSnapshot(opts.dir, args[0])
			if err != nil {
				return err
			}
			switch opts.format {
			case "text":
				for _, line := range buildSnapshotShowLines(desc) {
					if _, err := fmt.Fprintln(cmd.OutOrStdout(), line); err != nil {
						return err
					}
				}
				return nil
			case "json":
				return writeSnapshotJSON(cmd.OutOrStdout(), map[string]any{
					"snapshot": desc,
				})
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&opts.dir, "snapshot-dir", "", "Directory to load snapshots from")
	cmd.Flags().StringVar(&opts.format, "format", "text", "Snapshot show output format: text or json")
	return cmd
}

func isUnsupportedReportShape(err error) bool {
	return err != nil && err.Error() == "unsupported BinlogViz report shape"
}

func newSnapshotRenameCommand() *cobra.Command {
	opts := &snapshotOptions{}

	cmd := &cobra.Command{
		Use:   "rename <old-name> <new-name>",
		Short: "Rename a stored snapshot",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			newPath, err := snapshotpkg.RenameSnapshot(opts.dir, args[0], args[1])
			if err != nil {
				return err
			}
			_, err = fmt.Fprintf(cmd.ErrOrStderr(), "Renamed snapshot %q to %q at %s\n", args[0], args[1], newPath)
			return err
		},
	}

	cmd.Flags().StringVar(&opts.dir, "snapshot-dir", "", "Directory to load snapshots from")
	return cmd
}

func newSnapshotDeleteCommand() *cobra.Command {
	opts := &snapshotOptions{}

	cmd := &cobra.Command{
		Use:   "delete <name>",
		Short: "Delete a stored snapshot",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			deletedPath, err := snapshotpkg.DeleteSnapshot(opts.dir, args[0])
			if err != nil {
				return err
			}
			_, err = fmt.Fprintf(cmd.ErrOrStderr(), "Deleted snapshot %q at %s\n", args[0], deletedPath)
			return err
		},
	}

	cmd.Flags().StringVar(&opts.dir, "snapshot-dir", "", "Directory to load snapshots from")
	return cmd
}

func normalizeSnapshotReport(data []byte, name string, now time.Time) ([]byte, error) {
	var payload map[string]any
	if err := json.Unmarshal(data, &payload); err != nil {
		return nil, err
	}

	snapshot := map[string]any{}
	if existing, ok := payload["snapshot"].(map[string]any); ok {
		snapshot = existing
	}

	snapshot["name"] = name
	if strings.TrimSpace(asString(snapshot["label"])) == "" {
		snapshot["label"] = name
	}
	snapshot["created_at"] = now.Format(time.RFC3339)
	snapshot["binlogviz_version"] = version.Version
	if strings.TrimSpace(asString(snapshot["input_mode"])) == "" {
		snapshot["input_mode"] = "imported"
	}
	if _, ok := snapshot["input"].(map[string]any); !ok {
		snapshot["input"] = map[string]any{
			"files":    []any{},
			"from_dir": "",
			"prefix":   "",
		}
	}
	if _, ok := snapshot["filters"].(map[string]any); !ok {
		snapshot["filters"] = map[string]any{
			"include_schema": []any{},
			"exclude_schema": []any{},
			"include_table":  []any{},
			"exclude_table":  []any{},
		}
	}
	if _, ok := snapshot["window"].(map[string]any); !ok {
		snapshot["window"] = deriveSnapshotWindow(payload["summary"])
	}

	payload["snapshot"] = snapshot
	return json.MarshalIndent(payload, "", "  ")
}

func deriveSnapshotWindow(summaryValue any) map[string]any {
	window := map[string]any{
		"start_time": "",
		"end_time":   "",
	}
	summary, ok := summaryValue.(map[string]any)
	if !ok {
		return window
	}
	window["start_time"] = asString(summary["start_time"])
	window["end_time"] = asString(summary["end_time"])
	return window
}

func asString(value any) string {
	s, _ := value.(string)
	return s
}

func buildSnapshotShowLines(desc snapshotpkg.Descriptor) []string {
	lines := []string{
		fmt.Sprintf("name: %s", desc.Name),
		fmt.Sprintf("path: %s", desc.Path),
	}
	if desc.Label != "" {
		lines = append(lines, fmt.Sprintf("label: %s", desc.Label))
	}
	if desc.CreatedAt != "" {
		lines = append(lines, fmt.Sprintf("created_at: %s", desc.CreatedAt))
	}
	if desc.BinlogvizVersion != "" {
		lines = append(lines, fmt.Sprintf("binlogviz_version: %s", desc.BinlogvizVersion))
	}
	if desc.InputMode != "" {
		lines = append(lines, fmt.Sprintf("input_mode: %s", desc.InputMode))
	}
	if source := formatSnapshotSource(desc.Input); source != "" {
		lines = append(lines, fmt.Sprintf("input_source: %s", source))
	}
	if filters := formatSnapshotFilters(desc.Filters); filters != "" {
		lines = append(lines, fmt.Sprintf("filters: %s", filters))
	}
	lines = append(lines,
		fmt.Sprintf("total_transactions: %d", desc.Summary.TotalTransactions),
		fmt.Sprintf("total_rows: %d", desc.Summary.TotalRows),
		fmt.Sprintf("total_events: %d", desc.Summary.TotalEvents),
		fmt.Sprintf("warnings: %d", desc.Warnings),
	)
	return lines
}

func writeSnapshotJSON(out io.Writer, payload any) error {
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(out, string(data))
	return err
}

func writeSnapshotListText(out io.Writer, entries []snapshotpkg.Entry) error {
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "NAME\tLABEL\tCREATED_AT\tINPUT_MODE\tWINDOW"); err != nil {
		return err
	}
	for _, entry := range entries {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%s\t%s\n",
			entry.Name,
			orDash(entry.Label),
			orDash(entry.CreatedAt),
			orDash(entry.InputMode),
			formatSnapshotWindow(entry.Window),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func formatSnapshotSource(input snapshotpkg.Input) string {
	parts := make([]string, 0, 3)
	if len(input.Files) > 0 {
		parts = append(parts, fmt.Sprintf("files=%d", len(input.Files)))
	}
	if input.FromDir != "" {
		parts = append(parts, "from_dir="+input.FromDir)
	}
	if input.Prefix != "" {
		parts = append(parts, "prefix="+input.Prefix)
	}
	return strings.Join(parts, " ")
}

func formatSnapshotFilters(filters snapshotpkg.Filters) string {
	parts := make([]string, 0, 4)
	if len(filters.IncludeSchemas) > 0 {
		parts = append(parts, "include_schema="+strings.Join(filters.IncludeSchemas, ","))
	}
	if len(filters.ExcludeSchemas) > 0 {
		parts = append(parts, "exclude_schema="+strings.Join(filters.ExcludeSchemas, ","))
	}
	if len(filters.IncludeTables) > 0 {
		parts = append(parts, "include_table="+strings.Join(filters.IncludeTables, ","))
	}
	if len(filters.ExcludeTables) > 0 {
		parts = append(parts, "exclude_table="+strings.Join(filters.ExcludeTables, ","))
	}
	slices.Sort(parts)
	return strings.Join(parts, " ")
}

func formatSnapshotWindow(window snapshotpkg.Window) string {
	start := strings.TrimSpace(window.StartTime)
	end := strings.TrimSpace(window.EndTime)
	switch {
	case start != "" && end != "":
		return start + " -> " + end
	case start != "":
		return start
	case end != "":
		return end
	default:
		return "-"
	}
}

func orDash(value string) string {
	if strings.TrimSpace(value) == "" {
		return "-"
	}
	return value
}
