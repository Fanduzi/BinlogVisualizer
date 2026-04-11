package workflow

import (
	"encoding/json"
	"os"
)

// Manifest is the machine-readable record of a workflow run.
type Manifest struct {
	ManifestVersion     int             `json:"manifest_version"`
	Mode                string          `json:"mode"`
	Attempt             int             `json:"attempt"`
	WorkflowName        string          `json:"workflow_name"`
	WorkflowPlanVersion int             `json:"workflow_plan_version"`
	BinlogvizVersion    string          `json:"binlogviz_version"`
	PlanPath            string          `json:"plan_path"`
	PlanSHA256          string          `json:"plan_sha256,omitempty"`
	ResolvedInputFiles  []string        `json:"resolved_input_files,omitempty"`
	SnapshotDir         string          `json:"snapshot_dir,omitempty"`
	RunStartedAt        string          `json:"run_started_at"`
	RunFinishedAt       string          `json:"run_finished_at"`
	Status              string          `json:"status"`
	Error               string          `json:"error,omitempty"`
	WorkflowSummary     WorkflowSummary `json:"workflow_summary"`
	Steps               []StepRecord    `json:"steps"`
}

// WorkflowSummary is the compact workflow-level rollup persisted into the manifest.
type WorkflowSummary struct {
	Findings        []WorkflowFinding        `json:"findings"`
	Recommendations []WorkflowRecommendation `json:"recommendations"`
	Warnings        []string                 `json:"warnings"`
}

// WorkflowFinding carries a compact, source-linked finding from compare/trend output.
type WorkflowFinding struct {
	Kind              string                `json:"kind"`
	Title             string                `json:"title"`
	Summary           string                `json:"summary"`
	SourceStepKind    string                `json:"source_step_kind"`
	SourceStepName    string                `json:"source_step_name"`
	SourceReportPath  string                `json:"source_report_path"`
	SourceReportLabel string                `json:"source_report_label"`
	EvidenceRefs      []WorkflowEvidenceRef `json:"evidence_refs,omitempty"`
}

// WorkflowRecommendation carries a compact, source-linked recommendation from compare/trend output.
type WorkflowRecommendation struct {
	Kind                string                `json:"kind"`
	Priority            string                `json:"priority"`
	Title               string                `json:"title"`
	Summary             string                `json:"summary"`
	RelatedFindingKinds []string              `json:"related_finding_kinds"`
	SourceStepKind      string                `json:"source_step_kind"`
	SourceStepName      string                `json:"source_step_name"`
	SourceReportPath    string                `json:"source_report_path"`
	SourceReportLabel   string                `json:"source_report_label"`
	EvidenceRefs        []WorkflowEvidenceRef `json:"evidence_refs,omitempty"`
}

// WorkflowEvidenceRef links a workflow summary item back to a source report section.
type WorkflowEvidenceRef struct {
	Section string `json:"section"`
	Key     string `json:"key,omitempty"`
	Label   string `json:"label"`
	Anchor  string `json:"anchor"`
}

// StepRecord records the outcome of one workflow step.
type StepRecord struct {
	Kind         string   `json:"kind"`
	Name         string   `json:"name"`
	Status       string   `json:"status"`
	Execution    string   `json:"execution,omitempty"`
	Artifacts    []string `json:"artifacts,omitempty"`
	SnapshotName string   `json:"snapshot_name,omitempty"`
	Error        string   `json:"error,omitempty"`
}

// WriteManifest serializes the manifest and writes it to path.
func WriteManifest(path string, m Manifest) error {
	data, err := json.MarshalIndent(normalizeManifest(m), "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func normalizeManifest(m Manifest) Manifest {
	m.WorkflowSummary = normalizeWorkflowSummary(m.WorkflowSummary)
	return m
}

func normalizeWorkflowSummary(summary WorkflowSummary) WorkflowSummary {
	if summary.Findings == nil {
		summary.Findings = []WorkflowFinding{}
	}
	if summary.Recommendations == nil {
		summary.Recommendations = []WorkflowRecommendation{}
	}
	if summary.Warnings == nil {
		summary.Warnings = []string{}
	}
	return summary
}
