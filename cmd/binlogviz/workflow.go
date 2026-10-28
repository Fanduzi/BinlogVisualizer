// Package binlogviz defines the workflow CLI commands for run, resume, validate, describe, status, clean, and export.
// input: workflow plan YAML, output directories, cobra flags, and workflow package contracts. Output writers live in workflow_output.go and plan execution in workflow_exec.go.
// output: workflow artifacts, canonical rooted plan references, operator stderr status, and cobra command trees that print Error once without Usage on failure.
// pos: CLI orchestration layer for multi-step investigation workflows.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	"binlogviz/internal/clierr"
	"binlogviz/internal/displaytz"
	"binlogviz/internal/snapshot"
	"binlogviz/internal/workflow"
)

func newWorkflowCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "workflow",
		Short: "Run multi-step BinlogViz investigation workflows",
		Example: `  binlogviz workflow validate incident.yaml
  binlogviz workflow run incident.yaml --output-dir incident-out
  binlogviz workflow status incident-out`,
		SilenceUsage:  true,
		SilenceErrors: true,
		Run: func(cmd *cobra.Command, args []string) {
			_ = cmd.Help()
		},
	}
	cmd.AddCommand(newWorkflowRunCommand())
	cmd.AddCommand(newWorkflowResumeCommand())
	cmd.AddCommand(newWorkflowValidateCommand())
	cmd.AddCommand(newWorkflowDescribeCommand())
	cmd.AddCommand(newWorkflowStatusCommand())
	cmd.AddCommand(newWorkflowCleanCommand())
	cmd.AddCommand(newWorkflowExportCommand())
	return cmd
}

type workflowDescribeOptions struct {
	format string
}

type workflowStatusOptions struct {
	format string
}

type workflowCleanOptions struct {
	snapshotDir      string
	force            bool
	apply            bool
	includeSnapshots bool
	format           string
}

type workflowExportOptions struct {
	output           string
	includeSnapshots bool
	format           string
}

func newWorkflowStatusCommand() *cobra.Command {
	opts := &workflowStatusOptions{}

	cmd := &cobra.Command{
		Use:   "status <output_dir>",
		Short: "Inspect workflow runtime state without modifying it",
		Example: `  binlogviz workflow status incident-out
  binlogviz workflow status incident-out --format json`,
		SilenceUsage:  true,
		SilenceErrors: true,
		Args:          cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if opts.format != "text" && opts.format != "json" {
				return clierr.Errorf(clierr.KindUsage, "unsupported workflow status format %q (allowed: text, json)", opts.format)
			}

			outputDir := args[0]
			manifestPath := filepath.Join(outputDir, "manifest.json")
			manifest, err := workflowManifestFromJSON(manifestPath)
			if err != nil {
				return clierr.Errorf(clierr.KindInput, "read workflow manifest %s: %w", manifestPath, err)
			}

			var plan *workflow.Plan
			var planLoadErr error
			if manifest.PlanPath != "" {
				// Trust-boundary check: only open the rooted plan.yaml.
				if canonicalPlanPath, trustErr := workflow.ValidateWorkflowPlanPath(outputDir, manifest.PlanPath); trustErr == nil {
					f, err := os.Open(canonicalPlanPath)
					if err == nil {
						defer f.Close()
						loaded, loadErr := workflow.LoadPlan(f)
						if loadErr == nil {
							plan = &loaded
						} else {
							planLoadErr = clierr.Errorf(clierr.KindInput, "cannot resume: load plan: %w", loadErr)
						}
					}
				}
			}

			status, err := workflow.BuildStatus(outputDir, manifest, plan)
			if planLoadErr != nil {
				status.Resumable = false
				status.ResumeError = planLoadErr.Error()
				status.ResumePreview = nil
			}
			if err != nil {
				return err
			}
			return writeWorkflowStatusOutput(cmd.OutOrStdout(), opts.format, status)
		},
	}

	cmd.Flags().StringVar(&opts.format, "format", "text", "Output format: text or json")

	return cmd
}

func newWorkflowCleanCommand() *cobra.Command {
	opts := &workflowCleanOptions{}

	cmd := &cobra.Command{
		Use:   "clean <output_dir>",
		Short: "Preview or delete orphaned workflow artifacts safely",
		Example: `  binlogviz workflow clean incident-out
  binlogviz workflow clean incident-out --format json`,
		SilenceUsage:  true,
		SilenceErrors: true,
		Args:          cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if opts.format != "text" && opts.format != "json" {
				return clierr.Errorf(clierr.KindUsage, "unsupported workflow clean format %q (allowed: text, json)", opts.format)
			}

			outputDir := args[0]
			manifestPath := filepath.Join(outputDir, "manifest.json")
			manifest, err := workflowManifestFromJSON(manifestPath)
			if err != nil {
				return clierr.Errorf(clierr.KindInput, "read workflow manifest %s: %w", manifestPath, err)
			}

			unlock, err := workflow.LockDir(outputDir)
			if err != nil {
				return err
			}
			defer unlock()

			snapshotDir := ""
			if opts.includeSnapshots {
				// Never from the manifest alone (ADR 0009 §8).
				snapshotDir, err = snapshot.ResolveSnapshotDir(opts.snapshotDir)
				if err != nil {
					return clierr.Errorf(clierr.KindInput, "resolve snapshot dir: %w", err)
				}
			}
			result, err := workflow.DiscoverCleanCandidatesWithOptions(outputDir, manifest, workflow.CleanOptions{
				IncludeSnapshots: opts.includeSnapshots,
				Force:            opts.force,
				SnapshotDir:      snapshotDir,
			})
			if err != nil {
				return err
			}
			if opts.apply {
				result, err = workflow.ApplyClean(result)
				if err != nil {
					return err
				}
			}
			if err := writeWorkflowCleanOutput(cmd.OutOrStdout(), opts.format, result); err != nil {
				return err
			}
			if opts.apply && len(result.Skipped) > 0 {
				causes := make([]string, 0, len(result.Skipped))
				for _, path := range result.Skipped {
					causes = append(causes, path+": "+result.SkipReasons[path])
				}
				return clierr.Errorf(clierr.KindOutput, "workflow clean could not move %d file(s) to trash: %s", len(result.Skipped), strings.Join(causes, "; "))
			}
			return nil
		},
	}

	cmd.Flags().BoolVar(&opts.apply, "apply", false, "Move orphaned files into <output_dir>/.trash/<id>/ instead of previewing them")
	cmd.Flags().BoolVar(&opts.includeSnapshots, "include-snapshots", false, "Also delete superseded snapshots this workflow created (per ledger.jsonl)")
	cmd.Flags().StringVar(&opts.snapshotDir, "snapshot-dir", "", "Snapshot directory for --include-snapshots (default: the standard snapshot dir); must match the manifest's snapshot_dir")
	cmd.Flags().BoolVar(&opts.force, "force", false, "Also delete unattributed outputs of recorded step kinds, and allow a workflow dir without ledger.jsonl; never deletes snapshots the ledger does not attribute")
	cmd.Flags().StringVar(&opts.format, "format", "text", "Output format: text or json")

	return cmd
}

func newWorkflowExportCommand() *cobra.Command {
	opts := &workflowExportOptions{}

	cmd := &cobra.Command{
		Use:   "export <output_dir>",
		Short: "Bundle workflow artifacts into a read-only archive",
		Example: `  binlogviz workflow export incident-out --output incident.zip
  binlogviz workflow export incident-out --format json --output incident.zip`,
		SilenceUsage:  true,
		SilenceErrors: true,
		Args:          cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if opts.format != "text" && opts.format != "json" {
				return clierr.Errorf(clierr.KindUsage, "unsupported workflow export format %q (allowed: text, json)", opts.format)
			}

			outputDir := args[0]
			manifestPath := filepath.Join(outputDir, "manifest.json")
			manifest, err := workflowManifestFromJSON(manifestPath)
			if err != nil {
				return clierr.Errorf(clierr.KindInput, "read workflow manifest %s: %w", manifestPath, err)
			}

			archivePath, err := resolveWorkflowExportArchivePath(outputDir, opts.output)
			if err != nil {
				return err
			}

			result, err := workflow.BuildExport(outputDir, manifest, workflow.ExportOptions{IncludeSnapshots: opts.includeSnapshots})
			if err != nil {
				return err
			}
			if err := workflow.WriteExportArchive(archivePath, result); err != nil {
				return err
			}
			if !result.PlanIncluded {
				// Scripts that read only the exit code must still see this.
				// With JSON errors it is one JSON line, like errors.
				if jsonErrors() {
					line, _ := json.Marshal(map[string]any{"warning": map[string]string{"code": "plan_not_included", "message": result.PlanWarning}})
					_, _ = fmt.Fprintln(cmd.ErrOrStderr(), string(line))
				} else {
					_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "warning: plan.yaml not included in export: %s\n", result.PlanWarning)
				}
			}

			return writeWorkflowExportOutput(cmd.OutOrStdout(), opts.format, buildWorkflowExportOutput(manifest.WorkflowName, outputDir, archivePath, opts.format, result))
		},
	}

	cmd.Flags().StringVarP(&opts.output, "output", "o", "", "Archive output path (default: <output_dir>.zip)")
	cmd.Flags().BoolVar(&opts.includeSnapshots, "include-snapshots", false, "Include referenced snapshot JSON files in the archive")
	cmd.Flags().StringVar(&opts.format, "format", "text", "Output format: text or json")

	return cmd
}

func newWorkflowDescribeCommand() *cobra.Command {
	opts := &workflowDescribeOptions{}

	cmd := &cobra.Command{
		Use:   "describe <plan.yaml>",
		Short: "Describe a workflow plan without executing it",
		Example: `  binlogviz workflow describe incident.yaml
  binlogviz workflow describe incident.yaml --format json`,
		SilenceUsage:  true,
		SilenceErrors: true,
		Args:          cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if opts.format != "text" && opts.format != "json" {
				return clierr.Errorf(clierr.KindUsage, "unsupported workflow describe format %q (allowed: text, json)", opts.format)
			}

			planPath := args[0]
			f, err := os.Open(planPath)
			if err != nil {
				openErr := clierr.Errorf(clierr.KindInput, "open workflow plan: %w", err)
				writeWorkflowValidateFailure(cmd.OutOrStdout(), opts.format, openErr)
				return openErr
			}
			defer f.Close()

			plan, err := workflow.LoadPlan(f)
			if err != nil {
				writeWorkflowValidateFailure(cmd.OutOrStdout(), opts.format, err)
				return err
			}

			desc := workflow.BuildDescriptionIn(plan, displaytz.From(cmd.Context()))
			return writeWorkflowDescribeOutput(cmd.OutOrStdout(), opts.format, desc)
		},
	}

	cmd.Flags().StringVar(&opts.format, "format", "text", "Output format: text or json")

	return cmd
}

type workflowValidateOptions struct {
	format string
}

func newWorkflowValidateCommand() *cobra.Command {
	opts := &workflowValidateOptions{}

	cmd := &cobra.Command{
		Use:   "validate <plan.yaml>",
		Short: "Validate a workflow plan without executing it",
		Example: `  binlogviz workflow validate incident.yaml
  binlogviz workflow validate incident.yaml --format json`,
		SilenceUsage:  true,
		SilenceErrors: true,
		Args:          cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if opts.format != "text" && opts.format != "json" {
				return clierr.Errorf(clierr.KindUsage, "unsupported workflow validate format %q (allowed: text, json)", opts.format)
			}

			planPath := args[0]
			f, err := os.Open(planPath)
			if err != nil {
				openErr := clierr.Errorf(clierr.KindInput, "open workflow plan: %w", err)
				writeWorkflowValidateFailure(cmd.OutOrStdout(), opts.format, openErr)
				return openErr
			}
			defer f.Close()

			plan, err := workflow.LoadPlan(f)
			if err != nil {
				writeWorkflowValidateFailure(cmd.OutOrStdout(), opts.format, err)
				return err
			}

			return writeWorkflowValidateSuccess(cmd.OutOrStdout(), opts.format, plan, workflow.PlanInputWarnings(plan))
		},
	}

	cmd.Flags().StringVar(&opts.format, "format", "text", "Output format: text or json")

	return cmd
}

type workflowRunOptions struct {
	outputDir   string
	snapshotDir string
}

func newWorkflowRunCommand() *cobra.Command {
	opts := &workflowRunOptions{}

	cmd := &cobra.Command{
		Use:           "run <plan.yaml>",
		Short:         "Execute a workflow plan",
		Example:       `  binlogviz workflow run incident.yaml --output-dir incident-run`,
		SilenceUsage:  true,
		SilenceErrors: true,
		Args:          cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			planPath := args[0]
			stderr := cmd.OutOrStderr()

			f, err := os.Open(planPath)
			if err != nil {
				return clierr.Errorf(clierr.KindInput, "open workflow plan: %w", err)
			}
			defer f.Close()

			plan, err := workflow.LoadPlan(f)
			if err != nil {
				return err
			}

			outputDir := plan.Workflow.OutputDir
			if opts.outputDir != "" {
				outputDir = opts.outputDir
			}
			absPlanPath, _ := filepath.Abs(planPath)

			return executeWorkflow(plan, outputDir, opts.snapshotDir, absPlanPath, stderr, displaytz.From(cmd.Context()))
		},
	}

	cmd.Flags().StringVar(&opts.outputDir, "output-dir", "", "Override the plan-defined output directory")
	cmd.Flags().StringVar(&opts.snapshotDir, "snapshot-dir", "", "Override the snapshot storage directory")

	return cmd
}

type workflowResumeOptions struct {
	snapshotDir string
	rerun       []string
}

func newWorkflowResumeCommand() *cobra.Command {
	opts := &workflowResumeOptions{}

	cmd := &cobra.Command{
		Use:           "resume <output_dir>",
		Short:         "Resume a previously run workflow from its output directory",
		Example:       `  binlogviz workflow resume incident-out`,
		SilenceUsage:  true,
		SilenceErrors: true,
		Args:          cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			outputDir := args[0]
			stderr := cmd.OutOrStderr()

			return executeResume(outputDir, opts.snapshotDir, opts.rerun, stderr, displaytz.From(cmd.Context()))
		},
	}

	cmd.Flags().StringVar(&opts.snapshotDir, "snapshot-dir", "", "Override the snapshot storage directory")
	cmd.Flags().StringArrayVar(&opts.rerun, "rerun", nil, "Explicit step to rerun (repeatable, format: kind:name)")

	return cmd
}
