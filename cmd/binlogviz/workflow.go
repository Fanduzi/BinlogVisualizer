// Package binlogviz defines the workflow CLI commands for run, resume, validate, describe, status, clean, and export.
// input: workflow plan YAML, output directories, cobra flags, and workflow package contracts.
// output: workflow artifacts, operator stderr status, and cobra command trees that print Error once without Usage on failure.
// pos: CLI orchestration layer for multi-step investigation workflows.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"binlogviz/internal/analyzer"
	"binlogviz/internal/binlog"
	comparepkg "binlogviz/internal/compare"
	"binlogviz/internal/model"
	"binlogviz/internal/report"
	"binlogviz/internal/snapshot"
	trendpkg "binlogviz/internal/trend"
	"binlogviz/internal/version"
	"binlogviz/internal/workflow"
)

func newWorkflowCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:           "workflow",
		Short:         "Run multi-step BinlogViz investigation workflows",
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
		Use:           "status <output_dir>",
		Short:         "Inspect workflow runtime state without modifying it",
		SilenceUsage:  true,
		SilenceErrors: true,
		Args:          cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if opts.format != "text" && opts.format != "json" {
				return fmt.Errorf("unsupported workflow status format %q (allowed: text, json)", opts.format)
			}

			outputDir := args[0]
			manifestPath := filepath.Join(outputDir, "manifest.json")
			manifest, err := workflowManifestFromJSON(manifestPath)
			if err != nil {
				return fmt.Errorf("read workflow manifest %s: %w", manifestPath, err)
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
							planLoadErr = fmt.Errorf("cannot resume: load plan: %w", loadErr)
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
		Use:           "clean <output_dir>",
		Short:         "Preview or delete orphaned workflow artifacts safely",
		SilenceUsage:  true,
		SilenceErrors: true,
		Args:          cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if opts.format != "text" && opts.format != "json" {
				return fmt.Errorf("unsupported workflow clean format %q (allowed: text, json)", opts.format)
			}

			outputDir := args[0]
			manifestPath := filepath.Join(outputDir, "manifest.json")
			manifest, err := workflowManifestFromJSON(manifestPath)
			if err != nil {
				return fmt.Errorf("read workflow manifest %s: %w", manifestPath, err)
			}

			result, err := workflow.DiscoverCleanCandidates(outputDir, manifest, opts.includeSnapshots)
			if err != nil {
				return err
			}
			if opts.apply {
				result = workflow.ApplyClean(result)
			}
			if err := writeWorkflowCleanOutput(cmd.OutOrStdout(), opts.format, result); err != nil {
				return err
			}
			if opts.apply && len(result.Skipped) > 0 {
				return fmt.Errorf("workflow clean completed with %d skipped deletions", len(result.Skipped))
			}
			return nil
		},
	}

	cmd.Flags().BoolVar(&opts.apply, "apply", false, "Delete orphaned files instead of previewing them")
	cmd.Flags().BoolVar(&opts.includeSnapshots, "include-snapshots", false, "Include orphaned snapshot JSON files in cleanup")
	cmd.Flags().StringVar(&opts.format, "format", "text", "Output format: text or json")

	return cmd
}

func newWorkflowExportCommand() *cobra.Command {
	opts := &workflowExportOptions{}

	cmd := &cobra.Command{
		Use:           "export <output_dir>",
		Short:         "Bundle workflow artifacts into a read-only archive",
		SilenceUsage:  true,
		SilenceErrors: true,
		Args:          cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if opts.format != "text" && opts.format != "json" {
				return fmt.Errorf("unsupported workflow export format %q (allowed: text, json)", opts.format)
			}

			outputDir := args[0]
			manifestPath := filepath.Join(outputDir, "manifest.json")
			manifest, err := workflowManifestFromJSON(manifestPath)
			if err != nil {
				return fmt.Errorf("read workflow manifest %s: %w", manifestPath, err)
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

			return writeWorkflowExportOutput(cmd.OutOrStdout(), opts.format, buildWorkflowExportOutput(manifest.WorkflowName, outputDir, archivePath, opts.format, result))
		},
	}

	cmd.Flags().StringVar(&opts.output, "output", "", "Archive output path (default: <output_dir>.zip)")
	cmd.Flags().BoolVar(&opts.includeSnapshots, "include-snapshots", false, "Include referenced snapshot JSON files in the archive")
	cmd.Flags().StringVar(&opts.format, "format", "text", "Output format: text or json")

	return cmd
}

func newWorkflowDescribeCommand() *cobra.Command {
	opts := &workflowDescribeOptions{}

	cmd := &cobra.Command{
		Use:           "describe <plan.yaml>",
		Short:         "Describe a workflow plan without executing it",
		SilenceUsage:  true,
		SilenceErrors: true,
		Args:          cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if opts.format != "text" && opts.format != "json" {
				return fmt.Errorf("unsupported workflow describe format %q (allowed: text, json)", opts.format)
			}

			planPath := args[0]
			f, err := os.Open(planPath)
			if err != nil {
				writeWorkflowValidateFailure(cmd.OutOrStdout(), opts.format, fmt.Errorf("open workflow plan: %w", err))
				return err
			}
			defer f.Close()

			plan, err := workflow.LoadPlan(f)
			if err != nil {
				writeWorkflowValidateFailure(cmd.OutOrStdout(), opts.format, err)
				return err
			}

			desc := workflow.BuildDescription(plan)
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
		Use:           "validate <plan.yaml>",
		Short:         "Validate a workflow plan without executing it",
		SilenceUsage:  true,
		SilenceErrors: true,
		Args:          cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if opts.format != "text" && opts.format != "json" {
				return fmt.Errorf("unsupported workflow validate format %q (allowed: text, json)", opts.format)
			}

			planPath := args[0]
			f, err := os.Open(planPath)
			if err != nil {
				writeWorkflowValidateFailure(cmd.OutOrStdout(), opts.format, fmt.Errorf("open workflow plan: %w", err))
				return err
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
		SilenceUsage:  true,
		SilenceErrors: true,
		Args:          cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			planPath := args[0]
			stderr := cmd.OutOrStderr()

			f, err := os.Open(planPath)
			if err != nil {
				return fmt.Errorf("open workflow plan: %w", err)
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

			return executeWorkflow(plan, outputDir, opts.snapshotDir, absPlanPath, stderr)
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
		SilenceUsage:  true,
		SilenceErrors: true,
		Args:          cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			outputDir := args[0]
			stderr := cmd.OutOrStderr()

			return executeResume(outputDir, opts.snapshotDir, opts.rerun, stderr)
		},
	}

	cmd.Flags().StringVar(&opts.snapshotDir, "snapshot-dir", "", "Override the snapshot storage directory")
	cmd.Flags().StringArrayVar(&opts.rerun, "rerun", nil, "Explicit step to rerun (repeatable, format: kind:name)")

	return cmd
}

func writeWorkflowValidateSuccess(out io.Writer, format string, plan workflow.Plan, warnings []string) error {
	if warnings == nil {
		warnings = []string{}
	}
	payload := struct {
		Valid        bool     `json:"valid"`
		WorkflowName string   `json:"workflow_name"`
		Windows      int      `json:"windows"`
		CompareJobs  int      `json:"compare_jobs"`
		TrendJobs    int      `json:"trend_jobs"`
		OutputDir    string   `json:"output_dir"`
		Warnings     []string `json:"warnings"`
	}{
		Valid:        true,
		WorkflowName: plan.Workflow.Name,
		Windows:      len(plan.Windows),
		CompareJobs:  len(plan.Compare),
		TrendJobs:    len(plan.Trend),
		OutputDir:    plan.Workflow.OutputDir,
		Warnings:     warnings,
	}

	if format == "json" {
		data, err := json.MarshalIndent(payload, "", "  ")
		if err != nil {
			return fmt.Errorf("encode workflow validate output: %w", err)
		}
		_, err = fmt.Fprintln(out, string(data))
		return err
	}

	if _, err := fmt.Fprintf(out, "Workflow plan valid\n- workflow: %s\n- windows: %d\n- compare jobs: %d\n- trend jobs: %d\n- output root: %s\n",
		plan.Workflow.Name,
		len(plan.Windows),
		len(plan.Compare),
		len(plan.Trend),
		plan.Workflow.OutputDir,
	); err != nil {
		return err
	}
	if len(warnings) == 0 {
		return nil
	}
	if _, err := fmt.Fprintln(out, "Warnings"); err != nil {
		return err
	}
	for _, warning := range warnings {
		if _, err := fmt.Fprintf(out, "- %s\n", warning); err != nil {
			return err
		}
	}
	return nil
}

func writeWorkflowValidateFailure(out io.Writer, format string, err error) {
	if format == "json" {
		data, marshalErr := json.MarshalIndent(struct {
			Valid bool   `json:"valid"`
			Error string `json:"error"`
		}{
			Valid: false,
			Error: err.Error(),
		}, "", "  ")
		if marshalErr == nil {
			_, _ = fmt.Fprintln(out, string(data))
			return
		}
	}
	_, _ = fmt.Fprintf(out, "Workflow plan invalid\n- %s\n", err.Error())
}

func writeWorkflowDescribeOutput(out io.Writer, format string, desc workflow.Description) error {
	if format == "json" {
		data, err := json.MarshalIndent(desc, "", "  ")
		if err != nil {
			return fmt.Errorf("encode workflow describe output: %w", err)
		}
		_, err = fmt.Fprintln(out, string(data))
		return err
	}

	if _, err := fmt.Fprintf(out, "Workflow: %s\nOutput Root: %s\nSnapshot Save: %t\n\n",
		desc.WorkflowName,
		desc.OutputDir,
		desc.SnapshotSave,
	); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(out, "Analyze Windows"); err != nil {
		return err
	}
	for _, window := range desc.Windows {
		if _, err := fmt.Fprintf(out, "- %s: %s -> %s\n", window.Name, window.Start, window.End); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(out, "  artifacts: %s\n", strings.Join(window.Artifacts, ", ")); err != nil {
			return err
		}
		if window.SnapshotName != "" {
			if _, err := fmt.Fprintf(out, "  snapshot: %s\n", window.SnapshotName); err != nil {
				return err
			}
		}
	}
	if _, err := fmt.Fprintln(out, "\nCompare Jobs"); err != nil {
		return err
	}
	for _, job := range desc.Compare {
		if _, err := fmt.Fprintf(out, "- %s\n", job.Name); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(out, "  depends on: %s, %s\n", job.Current, job.Baseline); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(out, "  artifacts: %s\n", strings.Join(job.Artifacts, ", ")); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintln(out, "\nTrend Jobs"); err != nil {
		return err
	}
	for _, job := range desc.Trend {
		if _, err := fmt.Fprintf(out, "- %s\n", job.Name); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(out, "  depends on: %s\n", strings.Join(job.Snapshots, ", ")); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(out, "  artifacts: %s\n", strings.Join(job.Artifacts, ", ")); err != nil {
			return err
		}
	}
	return nil
}

func writeWorkflowStatusOutput(out io.Writer, format string, status workflow.Status) error {
	if format == "json" {
		data, err := json.MarshalIndent(status, "", "  ")
		if err != nil {
			return fmt.Errorf("encode workflow status output: %w", err)
		}
		_, err = fmt.Fprintln(out, string(data))
		return err
	}

	resumable := "no"
	if status.Resumable {
		resumable = "yes"
	}
	if _, err := fmt.Fprintf(out,
		"Workflow Status: %s\nOutput Root: %s\nManifest Version: %d\nMode: %s\nAttempt: %d\nStatus: %s\nRuntime State: %s\nResumable: %s\n",
		status.WorkflowName,
		status.OutputDir,
		status.ManifestVersion,
		status.Mode,
		status.Attempt,
		status.Status,
		status.RuntimeState,
		resumable,
	); err != nil {
		return err
	}
	if status.ResumeError != "" {
		if _, err := fmt.Fprintf(out, "Reason: %s\n", status.ResumeError); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintln(out, "\nSteps"); err != nil {
		return err
	}
	for _, step := range status.Steps {
		if _, err := fmt.Fprintf(out, "- %s:%s\n", step.Kind, step.Name); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(out, "  status: %s\n", step.Status); err != nil {
			return err
		}
		if step.Execution != "" {
			if _, err := fmt.Fprintf(out, "  execution: %s\n", step.Execution); err != nil {
				return err
			}
		}
		if len(step.Artifacts) == 0 {
			if _, err := fmt.Fprintln(out, "  artifacts: none"); err != nil {
				return err
			}
			continue
		}
		artifactLines := make([]string, 0, len(step.Artifacts))
		for _, artifact := range step.Artifacts {
			if artifact.Exists {
				artifactLines = append(artifactLines, artifact.Path)
				continue
			}
			artifactLines = append(artifactLines, "missing "+artifact.Path)
		}
		if _, err := fmt.Fprintf(out, "  artifacts: %s\n", strings.Join(artifactLines, ", ")); err != nil {
			return err
		}
	}
	if len(status.WorkflowSummary.Recommendations) > 0 {
		if _, err := fmt.Fprintln(out, "\nWorkflow Recommendations"); err != nil {
			return err
		}
		for _, recommendation := range status.WorkflowSummary.Recommendations {
			if _, err := fmt.Fprintf(out, "- [%s] %s\n", recommendation.Priority, recommendation.Title); err != nil {
				return err
			}
			if recommendation.Summary != "" {
				if _, err := fmt.Fprintf(out, "  %s\n", recommendation.Summary); err != nil {
					return err
				}
			}
		}
	}
	if len(status.WorkflowSummary.Findings) > 0 {
		if _, err := fmt.Fprintln(out, "\nWorkflow Findings"); err != nil {
			return err
		}
		for _, finding := range status.WorkflowSummary.Findings {
			if _, err := fmt.Fprintf(out, "- %s\n", finding.Title); err != nil {
				return err
			}
			if finding.Summary != "" {
				if _, err := fmt.Fprintf(out, "  %s\n", finding.Summary); err != nil {
					return err
				}
			}
		}
	}
	if len(status.WorkflowSummary.Warnings) > 0 {
		if _, err := fmt.Fprintln(out, "\nWorkflow Summary Warnings"); err != nil {
			return err
		}
		for _, warning := range status.WorkflowSummary.Warnings {
			if _, err := fmt.Fprintf(out, "- %s\n", warning); err != nil {
				return err
			}
		}
	}
	if len(status.ResumePreview) == 0 {
		return nil
	}
	if _, err := fmt.Fprintln(out, "\nResume Preview"); err != nil {
		return err
	}
	for _, step := range status.ResumePreview {
		if _, err := fmt.Fprintf(out, "- %s %s:%s", step.Action, step.Kind, step.Name); err != nil {
			return err
		}
		if step.Reason != "" {
			if _, err := fmt.Fprintf(out, " (%s)", step.Reason); err != nil {
				return err
			}
		}
		if _, err := fmt.Fprintln(out); err != nil {
			return err
		}
	}
	return nil
}

func writeWorkflowCleanOutput(out io.Writer, format string, result workflow.CleanResult) error {
	if format == "json" {
		data, err := json.MarshalIndent(result, "", "  ")
		if err != nil {
			return fmt.Errorf("encode workflow clean output: %w", err)
		}
		_, err = fmt.Fprintln(out, string(data))
		return err
	}

	includeSnapshots := "no"
	if result.IncludeSnapshots {
		includeSnapshots = "yes"
	}
	if _, err := fmt.Fprintf(out,
		"Workflow Clean: %s\nOutput Root: %s\nMode: %s\nInclude Snapshots: %s\nArtifact Orphans: %d\nSnapshot Orphans: %d\nDeleted: %d\nSkipped: %d\n",
		result.WorkflowName,
		result.OutputDir,
		result.Mode,
		includeSnapshots,
		result.Counts.ArtifactOrphans,
		result.Counts.SnapshotOrphans,
		result.Counts.Deleted,
		result.Counts.Skipped,
	); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(out, "\nOrphaned Artifacts"); err != nil {
		return err
	}
	if len(result.ArtifactOrphans) == 0 {
		if _, err := fmt.Fprintln(out, "- none"); err != nil {
			return err
		}
	} else {
		for _, path := range result.ArtifactOrphans {
			if _, err := fmt.Fprintf(out, "- %s\n", path); err != nil {
				return err
			}
		}
	}
	if _, err := fmt.Fprintln(out, "\nOrphaned Snapshots"); err != nil {
		return err
	}
	if len(result.SnapshotOrphans) == 0 {
		if _, err := fmt.Fprintln(out, "- none"); err != nil {
			return err
		}
	} else {
		for _, path := range result.SnapshotOrphans {
			if _, err := fmt.Fprintf(out, "- %s\n", path); err != nil {
				return err
			}
		}
	}
	if _, err := fmt.Fprintln(out, "\nDeleted"); err != nil {
		return err
	}
	if len(result.Deleted) == 0 {
		if _, err := fmt.Fprintln(out, "- none"); err != nil {
			return err
		}
	} else {
		for _, path := range result.Deleted {
			if _, err := fmt.Fprintf(out, "- %s\n", path); err != nil {
				return err
			}
		}
	}
	if _, err := fmt.Fprintln(out, "\nSkipped"); err != nil {
		return err
	}
	if len(result.Skipped) == 0 {
		if _, err := fmt.Fprintln(out, "- none"); err != nil {
			return err
		}
	} else {
		for _, path := range result.Skipped {
			if _, err := fmt.Fprintf(out, "- %s\n", path); err != nil {
				return err
			}
		}
	}
	return nil
}

type workflowExportOutput struct {
	WorkflowName      string   `json:"workflow_name"`
	OutputDir         string   `json:"output_dir"`
	ArchivePath       string   `json:"archive_path"`
	Format            string   `json:"format"`
	IncludedFiles     int      `json:"included_files"`
	IncludedSnapshots int      `json:"included_snapshots"`
	Warnings          []string `json:"warnings"`
}

func resolveWorkflowExportArchivePath(outputDir string, output string) (string, error) {
	if output == "" {
		return filepath.Clean(outputDir) + ".zip", nil
	}
	archivePath, err := filepath.Abs(output)
	if err != nil {
		return "", fmt.Errorf("resolve archive path: %w", err)
	}
	return archivePath, nil
}

func buildWorkflowExportOutput(workflowName, outputDir, archivePath, format string, result workflow.ExportResult) workflowExportOutput {
	warnings := append([]string(nil), result.Warnings...)
	if warnings == nil {
		warnings = []string{}
	}

	snapshotCount := 0
	for _, entry := range result.Entries {
		if strings.Contains(entry.ArchivePath, "/snapshots/") {
			snapshotCount++
		}
	}

	return workflowExportOutput{
		WorkflowName:      workflowName,
		OutputDir:         outputDir,
		ArchivePath:       archivePath,
		Format:            "zip",
		IncludedFiles:     len(result.Entries),
		IncludedSnapshots: snapshotCount,
		Warnings:          warnings,
	}
}

func writeWorkflowExportOutput(out io.Writer, format string, payload workflowExportOutput) error {
	if format == "json" {
		data, err := json.MarshalIndent(payload, "", "  ")
		if err != nil {
			return fmt.Errorf("encode workflow export output: %w", err)
		}
		_, err = fmt.Fprintln(out, string(data))
		return err
	}

	if _, err := fmt.Fprintf(out,
		"Workflow Export\nWorkflow: %s\nOutput Root: %s\nArchive: %s\nFormat: %s\nIncluded Files: %d\nIncluded Snapshots: %d\n",
		payload.WorkflowName,
		payload.OutputDir,
		payload.ArchivePath,
		payload.Format,
		payload.IncludedFiles,
		payload.IncludedSnapshots,
	); err != nil {
		return err
	}
	if len(payload.Warnings) == 0 {
		return nil
	}
	if _, err := fmt.Fprintln(out, "\nWarnings"); err != nil {
		return err
	}
	for _, warning := range payload.Warnings {
		if _, err := fmt.Fprintf(out, "- %s\n", warning); err != nil {
			return err
		}
	}
	return nil
}

func executeResume(outputDir, snapshotDir string, rerunSelectors []string, stderr io.Writer) error {
	startedAt := time.Now().UTC()

	// Load existing manifest
	manifestPath := filepath.Join(outputDir, "manifest.json")
	mf, err := workflowManifestFromJSON(manifestPath)
	if err != nil {
		return fmt.Errorf("load manifest from %s: %w", outputDir, err)
	}

	// Early validation before trying to load the plan
	if mf.ManifestVersion == 0 {
		return fmt.Errorf("cannot resume: manifest has no manifest_version (legacy format); run a fresh workflow instead")
	}
	if mf.ManifestVersion != 2 {
		return fmt.Errorf("cannot resume: manifest_version %d is not supported (only version 2 is supported)", mf.ManifestVersion)
	}
	if mf.PlanPath == "" {
		return fmt.Errorf("cannot resume: manifest has no plan_path")
	}

	// Trust-boundary check: reject before opening any file; use canonical path.
	canonicalPlanPath, err := workflow.ValidateWorkflowPlanPath(outputDir, mf.PlanPath)
	if err != nil {
		return err
	}

	// Load the plan referenced by the manifest using the canonical trusted path.
	planPath := canonicalPlanPath

	f, err := os.Open(planPath)
	if err != nil {
		return fmt.Errorf("open plan file %s: %w", planPath, err)
	}
	defer f.Close()

	plan, err := workflow.LoadPlan(f)
	if err != nil {
		return fmt.Errorf("load plan: %w", err)
	}

	// Compute current plan hash and validate resumability
	planSHA256, err := computeFileSHA256(planPath)
	if err != nil {
		return fmt.Errorf("hash plan file: %w", err)
	}

	if err := workflow.ValidateResumableManifest(mf, outputDir, planPath, planSHA256); err != nil {
		return err
	}

	// Use manifest's snapshot_dir if not overridden
	effectiveSnapshotDir := mf.SnapshotDir
	if snapshotDir != "" {
		effectiveSnapshotDir = snapshotDir
	}

	// Build the resume plan
	resumePlan, err := workflow.BuildResumePlan(plan, mf, rerunSelectors, outputDir, effectiveSnapshotDir)
	if errors.Is(err, workflow.ErrNothingToResume) {
		fmt.Fprintln(stderr, "nothing to resume")
		return nil
	}
	if err != nil {
		return err
	}

	fmt.Fprintf(stderr, "workflow %q: resuming (attempt %d)\n", plan.Workflow.Name, resumePlan.UpdatedManifest.Attempt)

	// Build analyzer and report options from plan defaults
	analyzerOpts := buildWorkflowAnalyzerOptions(plan)
	reportOpts, err := buildWorkflowReportOptions(plan)
	if err != nil {
		return err
	}

	// Initialize the updated manifest
	newMf := resumePlan.UpdatedManifest
	newMf.RunStartedAt = startedAt.Format(time.RFC3339)
	newMf.Steps = []workflow.StepRecord{}

	// Reuse input files from original run
	inputFiles := resumePlan.InputFiles

	// Track analyze reports for compare/trend consumption
	windowReports := make(map[string]comparepkg.InputReport)

	// Execute or reuse steps in declared order
	for _, planned := range resumePlan.Steps {
		if !planned.Execute {
			// Reuse: carry forward the prior step record
			priorStep := findPriorStep(mf, planned.Kind, planned.Name)
			if priorStep == nil {
				return fmt.Errorf("internal error: no prior step for reusable %s:%s", planned.Kind, planned.Name)
			}
			reused := *priorStep
			reused.Execution = "reused"
			newMf.Steps = append(newMf.Steps, reused)

			// For reused analyze steps, we need to load the report for downstream consumption
			if planned.Kind == "analyze" {
				artifactPath := filepath.Join(outputDir, "analyze", planned.Name+".json")
				data, err := os.ReadFile(artifactPath)
				if err != nil {
					return fmt.Errorf("read reusable analyze artifact %s: %w", artifactPath, err)
				}
				reportInput, err := comparepkg.DecodeReportJSON(data)
				if err != nil {
					return fmt.Errorf("decode reusable analyze report for %q: %w", planned.Name, err)
				}
				windowReports[planned.Name] = reportInput
			}

			fmt.Fprintf(stderr, "  reuse %s %q\n", planned.Kind, planned.Name)
			continue
		}

		// Execute the step
		switch planned.Kind {
		case "analyze":
			rec, _, reportInput, aerr := runWorkflowAnalyzeWindow(
				inputFiles, analyzerOpts, reportOpts, plan,
				findWindow(plan, planned.Name),
				outputDir, effectiveSnapshotDir, stderr,
				true, // overwrite snapshots on resume
			)
			newMf.Steps = append(newMf.Steps, rec)
			if aerr != nil {
				return finalizeWorkflow(outputDir, &newMf, startedAt, stderr, aerr)
			}
			windowReports[planned.Name] = reportInput

		case "compare":
			job := findCompareJob(plan, planned.Name)
			rec, cerr := runWorkflowCompareJob(outputDir, job, windowReports)
			newMf.Steps = append(newMf.Steps, rec)
			if cerr != nil {
				return finalizeWorkflow(outputDir, &newMf, startedAt, stderr, cerr)
			}

		case "trend":
			job := findTrendJob(plan, planned.Name)
			rec, terr := runWorkflowTrendJob(outputDir, job, windowReports)
			newMf.Steps = append(newMf.Steps, rec)
			if terr != nil {
				return finalizeWorkflow(outputDir, &newMf, startedAt, stderr, terr)
			}
		}
	}

	return finalizeWorkflow(outputDir, &newMf, startedAt, stderr, nil)
}

func findPriorStep(mf workflow.Manifest, kind, name string) *workflow.StepRecord {
	for i := range mf.Steps {
		if mf.Steps[i].Kind == kind && mf.Steps[i].Name == name {
			return &mf.Steps[i]
		}
	}
	return nil
}

func findWindow(plan workflow.Plan, name string) workflow.Window {
	for _, w := range plan.Windows {
		if w.Name == name {
			return w
		}
	}
	return workflow.Window{Name: name}
}

func findCompareJob(plan workflow.Plan, name string) workflow.CompareJob {
	for _, j := range plan.Compare {
		if j.Name == name {
			return j
		}
	}
	return workflow.CompareJob{Name: name}
}

func findTrendJob(plan workflow.Plan, name string) workflow.TrendJob {
	for _, j := range plan.Trend {
		if j.Name == name {
			return j
		}
	}
	return workflow.TrendJob{Name: name}
}

func executeWorkflow(plan workflow.Plan, outputDir, snapshotDir, planPath string, stderr io.Writer) error {
	startedAt := time.Now().UTC()

	fmt.Fprintf(stderr, "workflow %q: starting\n", plan.Workflow.Name)

	if err := workflow.EnsureLayout(outputDir); err != nil {
		return fmt.Errorf("create output layout: %w", err)
	}

	copiedPlanPath, err := copyWorkflowPlanIntoOutputDir(outputDir, planPath)
	if err != nil {
		return fmt.Errorf("copy plan file: %w", err)
	}
	planSHA256, err := computeFileSHA256(copiedPlanPath)
	if err != nil {
		return fmt.Errorf("hash plan file: %w", err)
	}

	effectiveSnapshotDir := ""
	if plan.Defaults.Snapshot.Save {
		effectiveSnapshotDir, err = snapshot.ResolveSnapshotDir(snapshotDir)
		if err != nil {
			return fmt.Errorf("resolve snapshot dir: %w", err)
		}
	}

	mf := workflow.Manifest{
		ManifestVersion:     2,
		Mode:                "run",
		Attempt:             1,
		WorkflowName:        plan.Workflow.Name,
		WorkflowPlanVersion: plan.Version,
		BinlogvizVersion:    version.Version,
		PlanPath:            copiedPlanPath,
		PlanSHA256:          planSHA256,
		SnapshotDir:         effectiveSnapshotDir,
		RunStartedAt:        startedAt.Format(time.RFC3339),
		Status:              "success",
		Steps:               []workflow.StepRecord{},
	}

	// Resolve input paths via discovery mode
	paths, err := discoverBinlogPaths(plan.Defaults.Input.FromDir, plan.Defaults.Input.Prefix)
	if err != nil {
		return finalizeWorkflow(outputDir, &mf, startedAt, stderr, fmt.Errorf("discover binlog files: %w", err))
	}
	mf.ResolvedInputFiles = paths

	// Build analyzer options from plan defaults
	analyzerOpts := buildWorkflowAnalyzerOptions(plan)
	reportOpts, err := buildWorkflowReportOptions(plan)
	if err != nil {
		return finalizeWorkflow(outputDir, &mf, startedAt, stderr, err)
	}

	// --- Analyze phase ---
	windowReports := make(map[string]comparepkg.InputReport, len(plan.Windows))
	for _, w := range plan.Windows {
		fmt.Fprintf(stderr, "  analyze %q\n", w.Name)
		rec, _, reportInput, aerr := runWorkflowAnalyzeWindow(
			paths, analyzerOpts, reportOpts, plan, w, outputDir, effectiveSnapshotDir, stderr,
			false, // do not overwrite snapshots on fresh run
		)
		mf.Steps = append(mf.Steps, rec)
		if aerr != nil {
			return finalizeWorkflow(outputDir, &mf, startedAt, stderr, aerr)
		}
		windowReports[w.Name] = reportInput
	}

	// --- Compare phase ---
	for _, job := range plan.Compare {
		fmt.Fprintf(stderr, "  compare %q\n", job.Name)
		rec, cerr := runWorkflowCompareJob(outputDir, job, windowReports)
		mf.Steps = append(mf.Steps, rec)
		if cerr != nil {
			return finalizeWorkflow(outputDir, &mf, startedAt, stderr, cerr)
		}
	}

	// --- Trend phase ---
	for _, job := range plan.Trend {
		fmt.Fprintf(stderr, "  trend %q\n", job.Name)
		rec, terr := runWorkflowTrendJob(outputDir, job, windowReports)
		mf.Steps = append(mf.Steps, rec)
		if terr != nil {
			return finalizeWorkflow(outputDir, &mf, startedAt, stderr, terr)
		}
	}

	return finalizeWorkflow(outputDir, &mf, startedAt, stderr, nil)
}

func finalizeWorkflow(outputDir string, mf *workflow.Manifest, startedAt time.Time, stderr io.Writer, stepErr error) error {
	mf.RunFinishedAt = time.Now().UTC().Format(time.RFC3339)

	if stepErr != nil {
		mf.Status = "failed"
		mf.Error = stepErr.Error()
	}

	mf.WorkflowSummary = workflow.BuildWorkflowSummary(outputDir, *mf)

	manifestPath := filepath.Join(outputDir, "manifest.json")
	if writeErr := workflow.WriteManifest(manifestPath, *mf); writeErr != nil {
		fmt.Fprintf(stderr, "workflow: failed to write manifest: %v\n", writeErr)
		if stepErr == nil {
			return fmt.Errorf("write manifest: %w", writeErr)
		}
	} else {
		fmt.Fprintf(stderr, "workflow: manifest written to %s\n", manifestPath)
	}

	// Write index.html on both success and failure paths
	if indexErr := writeWorkflowIndex(outputDir, *mf); indexErr != nil {
		fmt.Fprintf(stderr, "workflow: failed to write index: %v\n", indexErr)
		if stepErr == nil {
			return fmt.Errorf("write index: %w", indexErr)
		}
	} else {
		fmt.Fprintf(stderr, "workflow: index written to %s\n", filepath.Join(outputDir, "index.html"))
	}

	if stepErr != nil {
		fmt.Fprintf(stderr, "workflow %q: failed\n", mf.WorkflowName)
	}
	return stepErr
}

func writeWorkflowIndex(outputDir string, manifest workflow.Manifest) error {
	html, err := workflow.RenderIndex(workflow.IndexInput{
		OutputRoot: outputDir,
		Manifest:   manifest,
	})
	if err != nil {
		return fmt.Errorf("render workflow index: %w", err)
	}
	indexPath := filepath.Join(outputDir, "index.html")
	return os.WriteFile(indexPath, []byte(html), 0o644)
}

func runWorkflowAnalyzeWindow(
	paths []string,
	analyzerOpts analyzer.Options,
	reportOpts report.Options,
	plan workflow.Plan,
	w workflow.Window,
	outputDir string,
	snapshotDir string,
	stderr io.Writer,
	overwriteSnapshot bool,
) (workflow.StepRecord, string, comparepkg.InputReport, error) {
	rec := workflow.StepRecord{
		Kind: "analyze",
		Name: w.Name,
	}

	windowOpts := analyzerOpts
	windowOpts.Start = &w.Start
	windowOpts.End = &w.End

	snapshotMeta := &model.Snapshot{
		Name:             w.Name,
		Label:            w.Name,
		CreatedAt:        time.Now().UTC(),
		BinlogvizVersion: version.Version,
		InputMode:        "discovery",
		Input: model.SnapshotInput{
			Files:   append([]string(nil), paths...),
			FromDir: plan.Defaults.Input.FromDir,
			Prefix:  plan.Defaults.Input.Prefix,
		},
		Window: model.SnapshotWindow{
			StartTime: w.Start,
			EndTime:   w.End,
		},
	}

	// Execute analysis pipeline to get result directly
	result, err := runAnalyzeToResult(paths, windowOpts, stderr)
	if err != nil {
		rec.Status = "failed"
		rec.Error = err.Error()
		return rec, "", comparepkg.InputReport{}, fmt.Errorf("analyze window %q: %w", w.Name, err)
	}

	// Resolve snapshot metadata
	result.Snapshot = resolveAnalyzeSnapshotMetadata(snapshotMeta, result.Summary)

	// Render to JSON
	payload, err := report.RenderJSONWithOptions(*result, reportOpts)
	if err != nil {
		rec.Status = "failed"
		rec.Error = err.Error()
		return rec, "", comparepkg.InputReport{}, fmt.Errorf("render analyze JSON for %q: %w", w.Name, err)
	}

	// Write to artifact path
	artifactPath := workflow.ArtifactPath(outputDir, "analyze", w.Name, "json")
	if err := os.WriteFile(artifactPath, []byte(payload), 0o644); err != nil {
		rec.Status = "failed"
		rec.Error = err.Error()
		return rec, "", comparepkg.InputReport{}, fmt.Errorf("write analyze artifact for %q: %w", w.Name, err)
	}

	// Save snapshot if configured (must succeed before marking step success)
	if plan.Defaults.Snapshot.Save {
		var savedPath string
		var err error
		if overwriteSnapshot {
			savedPath, err = snapshot.SaveJSONOverwrite(snapshotDir, w.Name, []byte(payload))
		} else {
			savedPath, err = snapshot.SaveJSON(snapshotDir, w.Name, []byte(payload))
		}
		if err != nil {
			rec.Status = "failed"
			rec.Error = err.Error()
			return rec, "", comparepkg.InputReport{}, fmt.Errorf("save snapshot %q: %w", w.Name, err)
		}
		rec.SnapshotName = w.Name
		fmt.Fprintf(stderr, "  saved snapshot %q to %s\n", w.Name, savedPath)
	}

	rec.Status = "success"
	rec.Execution = "executed"
	rec.Artifacts = []string{"analyze/" + w.Name + ".json"}

	// Parse the JSON for downstream compare/trend consumption
	reportInput, err := comparepkg.DecodeReportJSON([]byte(payload))
	if err != nil {
		rec.Status = "failed"
		rec.Error = fmt.Sprintf("decode generated report: %v", err)
		return rec, payload, comparepkg.InputReport{}, fmt.Errorf("decode analyze report for %q: %w", w.Name, err)
	}

	return rec, payload, reportInput, nil
}

// runAnalyzeToResult runs the analysis pipeline and returns the raw result without rendering.
func runAnalyzeToResult(paths []string, opts analyzer.Options, stderr io.Writer) (*model.AnalysisResult, error) {
	progress, err := newAggregateProgress(paths, stderr)
	if err != nil {
		return nil, fmt.Errorf("build parse progress: %w", err)
	}

	store, cleanup, _, err := createDuckDBTempStore("")
	if err != nil {
		return nil, fmt.Errorf("create temp store: %w", err)
	}
	defer cleanup()

	a := analyzer.NewWithStore(opts, store)

	parser := binlog.NewParser()
	handler := func(raw binlog.RawEvent) error {
		normalized, err := binlog.NormalizeRawEvent(raw)
		if err != nil {
			return fmt.Errorf("normalize at position %d: %w", raw.Position, err)
		}
		if normalized == nil {
			return nil
		}
		if err := a.Consume(*normalized); err != nil {
			return fmt.Errorf("consume: %w", err)
		}
		return nil
	}

	if pp, ok := parser.(binlog.ProgressParser); ok {
		if err := pp.ParseFilesWithProgress(paths, func(p binlog.ParseProgress) {
			progress.Advance(p)
		}, handler); err != nil {
			return nil, fmt.Errorf("parse: %w", err)
		}
		for i := range paths {
			progress.FinishFile(i)
		}
	} else {
		if err := parser.ParseFiles(paths, handler); err != nil {
			return nil, fmt.Errorf("parse: %w", err)
		}
	}
	progress.FinishParse()
	progress.Finalizing()

	result, err := a.Finalize()
	if err != nil {
		return nil, fmt.Errorf("finalize: %w", err)
	}
	return result, nil
}

func runWorkflowCompareJob(
	outputDir string,
	job workflow.CompareJob,
	windowReports map[string]comparepkg.InputReport,
) (workflow.StepRecord, error) {
	rec := workflow.StepRecord{
		Kind: "compare",
		Name: job.Name,
	}

	current, ok := windowReports[job.Current]
	if !ok {
		err := fmt.Errorf("compare %q: current window %q report not found", job.Name, job.Current)
		rec.Status = "failed"
		rec.Error = err.Error()
		return rec, err
	}
	baseline, ok := windowReports[job.Baseline]
	if !ok {
		err := fmt.Errorf("compare %q: baseline window %q report not found", job.Name, job.Baseline)
		rec.Status = "failed"
		rec.Error = err.Error()
		return rec, err
	}

	result := comparepkg.BuildCompareResult(current, baseline)

	for _, format := range job.Formats {
		var output string
		var err error
		switch format {
		case "json":
			output, err = comparepkg.RenderJSON(result)
		case "html":
			output, err = comparepkg.RenderHTML(result)
		default:
			err = fmt.Errorf("unsupported format %q", format)
		}
		if err != nil {
			rec.Status = "failed"
			rec.Error = err.Error()
			return rec, fmt.Errorf("render compare %q (%s): %w", job.Name, format, err)
		}

		artifactPath := workflow.ArtifactPath(outputDir, "compare", job.Name, format)
		if err := os.WriteFile(artifactPath, []byte(output), 0o644); err != nil {
			rec.Status = "failed"
			rec.Error = err.Error()
			return rec, fmt.Errorf("write compare artifact %q: %w", job.Name, err)
		}
		rec.Artifacts = append(rec.Artifacts, "compare/"+job.Name+"."+format)
	}

	rec.Status = "success"
	rec.Execution = "executed"
	return rec, nil
}

func runWorkflowTrendJob(
	outputDir string,
	job workflow.TrendJob,
	windowReports map[string]comparepkg.InputReport,
) (workflow.StepRecord, error) {
	rec := workflow.StepRecord{
		Kind: "trend",
		Name: job.Name,
	}

	points := make([]trendpkg.BuildInput, 0, len(job.Snapshots))
	for _, snapName := range job.Snapshots {
		reportInput, ok := windowReports[snapName]
		if !ok {
			err := fmt.Errorf("trend %q: snapshot %q report not found", job.Name, snapName)
			rec.Status = "failed"
			rec.Error = err.Error()
			return rec, err
		}
		points = append(points, trendpkg.BuildInput{
			Path:   fmt.Sprintf("analyze/%s.json", snapName),
			Report: trendpkg.InputReport(reportInput),
		})
	}

	result, err := trendpkg.BuildResult(trendpkg.BuildOptions{
		InputMode: "workflow",
		Points:    points,
		TopTables: 10,
		Order:     "cli",
	})
	if err != nil {
		rec.Status = "failed"
		rec.Error = err.Error()
		return rec, fmt.Errorf("build trend %q: %w", job.Name, err)
	}

	for _, format := range job.Formats {
		var output string
		var renderErr error
		switch format {
		case "json":
			output, renderErr = trendpkg.RenderJSON(result)
		case "html":
			output, renderErr = trendpkg.RenderHTML(result)
		default:
			renderErr = fmt.Errorf("unsupported format %q", format)
		}
		if renderErr != nil {
			rec.Status = "failed"
			rec.Error = renderErr.Error()
			return rec, fmt.Errorf("render trend %q (%s): %w", job.Name, format, renderErr)
		}

		artifactPath := workflow.ArtifactPath(outputDir, "trend", job.Name, format)
		if err := os.WriteFile(artifactPath, []byte(output), 0o644); err != nil {
			rec.Status = "failed"
			rec.Error = err.Error()
			return rec, fmt.Errorf("write trend artifact %q: %w", job.Name, err)
		}
		rec.Artifacts = append(rec.Artifacts, "trend/"+job.Name+"."+format)
	}

	rec.Status = "success"
	rec.Execution = "executed"
	return rec, nil
}

func buildWorkflowAnalyzerOptions(plan workflow.Plan) analyzer.Options {
	opts := analyzer.DefaultOptions()

	da := plan.Defaults.Analyze
	if da.TopTables != 0 {
		opts.TopTables = da.TopTables
	}
	if da.TopTransactions != 0 {
		opts.TopTransactions = da.TopTransactions
	}
	if da.TopMinutes != 0 {
		opts.TopMinutes = da.TopMinutes
	}
	opts.DetectSpikes = da.DetectSpikes
	if da.LargeTrxRows != 0 {
		opts.LargeTxnRows = da.LargeTrxRows
	}
	if da.LargeTrxDuration != "" {
		if d, err := time.ParseDuration(da.LargeTrxDuration); err == nil {
			opts.LargeTxnDuration = d
		}
	}
	if da.SpikeWindow != 0 {
		opts.SpikeWindow = da.SpikeWindow
	}
	if da.SpikeFactor != 0 {
		opts.SpikeFactor = da.SpikeFactor
	}
	if da.SpikeMinRows != 0 {
		opts.SpikeMinRows = da.SpikeMinRows
	}
	opts.IncludeSchemas = da.IncludeSchemas
	opts.ExcludeSchemas = da.ExcludeSchemas
	opts.IncludeTables = da.IncludeTables
	opts.ExcludeTables = da.ExcludeTables

	return opts
}

func buildWorkflowReportOptions(plan workflow.Plan) (report.Options, error) {
	mode := report.SQLContextSummary
	if da := plan.Defaults.Analyze; da.SQLContext != "" {
		var err error
		mode, err = report.ParseSQLContextMode(da.SQLContext)
		if err != nil {
			return report.Options{}, err
		}
	}
	return report.Options{SQLContextMode: mode}, nil
}

// workflowManifestFromJSON is a test helper to parse a manifest JSON file.
func workflowManifestFromJSON(path string) (workflow.Manifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return workflow.Manifest{}, err
	}
	var m workflow.Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return workflow.Manifest{}, err
	}
	return m, nil
}

func computeFileSHA256(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	h := sha256.Sum256(data)
	return fmt.Sprintf("%x", h[:]), nil
}

func copyWorkflowPlanIntoOutputDir(outputDir string, planPath string) (string, error) {
	data, err := os.ReadFile(planPath)
	if err != nil {
		return "", err
	}
	copiedPlanPath := filepath.Join(outputDir, "plan.yaml")
	if err := os.WriteFile(copiedPlanPath, data, 0o644); err != nil {
		return "", err
	}
	return copiedPlanPath, nil
}
