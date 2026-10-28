// Package binlogviz renders workflow command output: validate/describe/
// status/clean results in text or JSON, and the export summary payload.
// input: a finished workflow domain value plus the --output-format flag.
// output: the formatted line or JSON object on the command's writer.
// pos: CLI orchestration helper for workflow.go.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"binlogviz/internal/clierr"
	"binlogviz/internal/jsonout"
	"binlogviz/internal/workflow"
)

func writeWorkflowValidateSuccess(out io.Writer, format string, plan workflow.Plan, warnings []string) error {
	if warnings == nil {
		warnings = []string{}
	}
	payload := workflowValidateJSON{
		Valid:        true,
		WorkflowName: plan.Workflow.Name,
		Windows:      len(plan.Windows),
		CompareJobs:  len(plan.Compare),
		TrendJobs:    len(plan.Trend),
		OutputDir:    plan.Workflow.OutputDir,
		Warnings:     warnings,
	}

	if format == "json" {
		data, err := jsonout.Marshal(workflowValidateSchemaVersion, payload)
		if err != nil {
			return clierr.Errorf(clierr.KindOutput, "encode workflow validate output: %w", err)
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
		data, marshalErr := jsonout.Marshal(workflowValidateSchemaVersion, workflowValidateErrorJSON{
			Valid: false,
			Error: err.Error(),
		})
		if marshalErr == nil {
			_, _ = fmt.Fprintln(out, string(data))
			return
		}
	}
	_, _ = fmt.Fprintf(out, "Workflow plan invalid\n- %s\n", err.Error())
}

func writeWorkflowDescribeOutput(out io.Writer, format string, desc workflow.Description) error {
	if format == "json" {
		data, err := jsonout.Marshal(workflowDescribeSchemaVersion, desc)
		if err != nil {
			return clierr.Errorf(clierr.KindOutput, "encode workflow describe output: %w", err)
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
		data, err := jsonout.Marshal(workflowStatusSchemaVersion, status)
		if err != nil {
			return clierr.Errorf(clierr.KindOutput, "encode workflow status output: %w", err)
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
		data, err := jsonout.Marshal(workflowCleanSchemaVersion, result)
		if err != nil {
			return clierr.Errorf(clierr.KindOutput, "encode workflow clean output: %w", err)
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
	if _, err := fmt.Fprintln(out, "\nKept (not attributable to this workflow; --force deletes artifacts of recorded kinds)"); err != nil {
		return err
	}
	if len(result.KeptOrphans) == 0 {
		if _, err := fmt.Fprintln(out, "- none"); err != nil {
			return err
		}
	} else {
		for _, path := range result.KeptOrphans {
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
	for _, dir := range []string{result.TrashDir, result.SnapshotTrashDir} {
		if dir == "" {
			continue
		}
		if _, err := fmt.Fprintf(out, "Moved to trash: %s (clean never empties it; delete it yourself when no longer needed)\n", dir); err != nil {
			return err
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

// workflowValidateJSON is the success document of workflow validate --format json.
type workflowValidateJSON struct {
	Valid        bool     `json:"valid"`
	WorkflowName string   `json:"workflow_name"`
	Windows      int      `json:"windows"`
	CompareJobs  int      `json:"compare_jobs"`
	TrendJobs    int      `json:"trend_jobs"`
	OutputDir    string   `json:"output_dir"`
	Warnings     []string `json:"warnings"`
}

// workflowValidateErrorJSON is the failure document. It shares schema_version
// and valid with the success document and carries error instead of the counts.
type workflowValidateErrorJSON struct {
	Valid bool   `json:"valid"`
	Error string `json:"error"`
}

// workflowDescribeErrorJSON is the describe failure branch's schema shape.
// It shares the wire shape of workflowValidateErrorJSON but pins valid to
// false so a valid:true error document matches neither describe branch.
type workflowDescribeErrorJSON struct {
	Valid bool   `json:"valid" jsonschema:"const:false"`
	Error string `json:"error"`
}

type workflowExportOutput struct {
	WorkflowName      string   `json:"workflow_name"`
	OutputDir         string   `json:"output_dir"`
	ArchivePath       string   `json:"archive_path"`
	Format            string   `json:"format"`
	IncludedFiles     int      `json:"included_files"`
	IncludedSnapshots int      `json:"included_snapshots"`
	PlanIncluded      bool     `json:"plan_included"`
	Warnings          []string `json:"warnings"`
}

func resolveWorkflowExportArchivePath(outputDir string, output string) (string, error) {
	if output == "" {
		return filepath.Clean(outputDir) + ".zip", nil
	}
	archivePath, err := filepath.Abs(output)
	if err != nil {
		return "", clierr.Errorf(clierr.KindInput, "resolve archive path: %w", err)
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
		PlanIncluded:      result.PlanIncluded,
		Warnings:          warnings,
	}
}

func writeWorkflowExportOutput(out io.Writer, format string, payload workflowExportOutput) error {
	if format == "json" {
		data, err := jsonout.Marshal(workflowExportSchemaVersion, payload)
		if err != nil {
			return clierr.Errorf(clierr.KindOutput, "encode workflow export output: %w", err)
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
