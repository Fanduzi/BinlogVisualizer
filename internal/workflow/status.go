package workflow

import (
	"crypto/sha256"
	"fmt"
	"os"
)

// Status is the read-only runtime view of a workflow root.
type Status struct {
	WorkflowName    string              `json:"workflow_name"`
	OutputDir       string              `json:"output_dir"`
	ManifestVersion int                 `json:"manifest_version"`
	Mode            string              `json:"mode"`
	Attempt         int                 `json:"attempt"`
	Status          string              `json:"status"`
	RuntimeState    string              `json:"runtime_state"`
	Resumable       bool                `json:"resumable"`
	ResumeError     string              `json:"resume_error"`
	WorkflowSummary WorkflowSummary     `json:"workflow_summary"`
	Steps           []StepStatus        `json:"steps"`
	ResumePreview   []ResumePreviewStep `json:"resume_preview,omitempty"`
}

// StepStatus is the status view of one recorded workflow step.
type StepStatus struct {
	Kind      string           `json:"kind"`
	Name      string           `json:"name"`
	Status    string           `json:"status"`
	Execution string           `json:"execution,omitempty"`
	Artifacts []ArtifactStatus `json:"artifacts,omitempty"`
}

// ArtifactStatus reports whether one deterministic artifact exists right now.
type ArtifactStatus struct {
	Path   string `json:"path"`
	Exists bool   `json:"exists"`
}

// ResumePreviewStep is one dry-run resume action.
type ResumePreviewStep struct {
	Kind   string `json:"kind"`
	Name   string `json:"name"`
	Action string `json:"action"`
	Reason string `json:"reason"`
}

// BuildStatus builds a read-only runtime status model from a manifest and optional plan.
func BuildStatus(outputDir string, manifest Manifest, plan *Plan) (Status, error) {
	status := Status{
		WorkflowName:    manifest.WorkflowName,
		OutputDir:       outputDir,
		ManifestVersion: manifest.ManifestVersion,
		Mode:            manifest.Mode,
		Attempt:         manifest.Attempt,
		Status:          manifest.Status,
		RuntimeState:    initialRuntimeState(manifest),
		ResumeError:     "",
		WorkflowSummary: normalizeWorkflowSummary(manifest.WorkflowSummary),
		Steps:           make([]StepStatus, 0, len(manifest.Steps)),
	}

	for _, step := range manifest.Steps {
		stepStatus := StepStatus{
			Kind:      step.Kind,
			Name:      step.Name,
			Status:    step.Status,
			Execution: step.Execution,
			Artifacts: make([]ArtifactStatus, 0, len(step.Artifacts)),
		}
		for _, artifact := range step.Artifacts {
			exists := artifactExists(outputDir, artifact)
			if !exists {
				status.RuntimeState = "incomplete"
			}
			stepStatus.Artifacts = append(stepStatus.Artifacts, ArtifactStatus{
				Path:   artifact,
				Exists: exists,
			})
		}
		status.Steps = append(status.Steps, stepStatus)
	}

	resumeErr := resumabilityError(outputDir, manifest)
	if resumeErr != nil {
		status.Resumable = false
		status.ResumeError = resumeErr.Error()
		return status, nil
	}
	status.Resumable = true

	if plan == nil {
		return status, nil
	}

	preview, err := buildResumePreview(*plan, manifest, outputDir, manifest.SnapshotDir)
	if err != nil {
		status.Resumable = false
		status.ResumeError = err.Error()
		status.ResumePreview = nil
		return status, nil
	}
	status.ResumePreview = preview
	if hasMissingReusableSnapshot(outputDir, *plan, manifest) {
		status.RuntimeState = "incomplete"
	}
	return status, nil
}

// initialRuntimeState is complete only when the run produced reusable work:
// at least one successful step and a non-empty resolved input list.
// An empty failed discovery (no steps, no files) must not look finished.
func initialRuntimeState(manifest Manifest) string {
	if len(manifest.ResolvedInputFiles) == 0 {
		return "incomplete"
	}
	for _, step := range manifest.Steps {
		if step.Status == "success" {
			return "complete"
		}
	}
	return "incomplete"
}

func hasMissingReusableSnapshot(outputDir string, plan Plan, manifest Manifest) bool {
	if !plan.Defaults.Snapshot.Save || manifest.SnapshotDir == "" {
		return false
	}
	for _, step := range manifest.Steps {
		if step.Kind != "analyze" || step.Status != "success" || step.SnapshotName == "" {
			continue
		}
		if !allArtifactsExist(outputDir, step.Artifacts) {
			continue
		}
		if !snapshotFileExists(manifest.SnapshotDir, step.SnapshotName) {
			return true
		}
	}
	return false
}

func artifactExists(outputDir, relativePath string) bool {
	_, err := os.Stat(resolveArtifactPath(outputDir, relativePath))
	return err == nil
}

func resumabilityError(outputDir string, manifest Manifest) error {
	if manifest.ManifestVersion == 0 {
		return ValidateResumableManifest(manifest, "", "", "")
	}
	if manifest.ManifestVersion != 2 {
		return ValidateResumableManifest(manifest, "", "", "")
	}
	if manifest.PlanPath == "" {
		return ValidateResumableManifest(manifest, "", "", "")
	}

	// Trust-boundary check before opening any file; use returned canonical path.
	canonicalPlanPath, err := ValidateWorkflowPlanPath(outputDir, manifest.PlanPath)
	if err != nil {
		return fmt.Errorf("cannot resume: %w", err)
	}

	planSHA256, err := computeStatusPlanSHA256(canonicalPlanPath)
	if err != nil {
		return fmt.Errorf("cannot resume: plan file %q not found", canonicalPlanPath)
	}
	return ValidateResumableManifest(manifest, outputDir, canonicalPlanPath, planSHA256)
}

func computeStatusPlanSHA256(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return fmt.Sprintf("%x", sum[:]), nil
}

func buildResumePreview(plan Plan, manifest Manifest, outputDir string, snapshotDir string) ([]ResumePreviewStep, error) {
	plannedSteps, _, err := planResumeSteps(plan, manifest, nil, outputDir, snapshotDir)
	if err != nil {
		return nil, err
	}

	preview := make([]ResumePreviewStep, 0, len(plannedSteps))
	for _, step := range plannedSteps {
		action := "reuse"
		if step.Execute {
			action = "rerun"
		}
		preview = append(preview, ResumePreviewStep{
			Kind:   step.Kind,
			Name:   step.Name,
			Action: action,
			Reason: step.Reason,
		})
	}
	return preview, nil
}
