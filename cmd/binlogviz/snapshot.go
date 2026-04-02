package binlogviz

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	comparepkg "binlogviz/internal/compare"
	snapshotpkg "binlogviz/internal/snapshot"
	"binlogviz/internal/version"
)

type snapshotOptions struct {
	dir  string
	name string
}

type snapshotEnvelope struct {
	Snapshot *snapshotMetadata `json:"snapshot"`
}

type snapshotMetadata struct {
	Name             string `json:"name"`
	Label            string `json:"label"`
	CreatedAt        string `json:"created_at"`
	BinlogvizVersion string `json:"binlogviz_version"`
	InputMode        string `json:"input_mode"`
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
	opts := &snapshotOptions{}

	cmd := &cobra.Command{
		Use:   "list",
		Short: "List saved snapshots",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			entries, err := snapshotpkg.ListSnapshots(opts.dir)
			if err != nil {
				return err
			}
			for _, entry := range entries {
				if _, err := fmt.Fprintln(cmd.OutOrStdout(), entry.Name); err != nil {
					return err
				}
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&opts.dir, "snapshot-dir", "", "Directory to load snapshots from")
	return cmd
}

func newSnapshotShowCommand() *cobra.Command {
	opts := &snapshotOptions{}

	cmd := &cobra.Command{
		Use:   "show <name>",
		Short: "Show snapshot metadata and summary",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			path, data, err := snapshotpkg.LoadSnapshot(opts.dir, args[0])
			if err != nil {
				return err
			}

			report, err := comparepkg.DecodeReportJSON(data)
			if err != nil {
				if isUnsupportedReportShape(err) {
					return err
				}
				return fmt.Errorf("decode snapshot %q: %w", args[0], err)
			}

			meta := snapshotMetadata{Name: args[0]}
			var envelope snapshotEnvelope
			if err := json.Unmarshal(data, &envelope); err == nil && envelope.Snapshot != nil {
				meta = *envelope.Snapshot
				if meta.Name == "" {
					meta.Name = args[0]
				}
			}

			lines := []string{
				fmt.Sprintf("name: %s", meta.Name),
				fmt.Sprintf("path: %s", path),
			}
			if meta.Label != "" {
				lines = append(lines, fmt.Sprintf("label: %s", meta.Label))
			}
			if meta.CreatedAt != "" {
				lines = append(lines, fmt.Sprintf("created_at: %s", meta.CreatedAt))
			}
			if meta.BinlogvizVersion != "" {
				lines = append(lines, fmt.Sprintf("binlogviz_version: %s", meta.BinlogvizVersion))
			}
			if meta.InputMode != "" {
				lines = append(lines, fmt.Sprintf("input_mode: %s", meta.InputMode))
			}
			lines = append(lines,
				fmt.Sprintf("total_transactions: %d", report.Summary.TotalTransactions),
				fmt.Sprintf("total_rows: %d", report.Summary.TotalRows),
				fmt.Sprintf("total_events: %d", report.Summary.TotalEvents),
				fmt.Sprintf("warnings: %d", report.Warnings),
			)

			for _, line := range lines {
				if _, err := fmt.Fprintln(cmd.OutOrStdout(), line); err != nil {
					return err
				}
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&opts.dir, "snapshot-dir", "", "Directory to load snapshots from")
	return cmd
}

func isUnsupportedReportShape(err error) bool {
	return err != nil && err.Error() == "unsupported BinlogViz report shape"
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
