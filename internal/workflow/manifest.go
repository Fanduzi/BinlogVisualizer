package workflow

import (
	"encoding/json"
	"os"
)

// Manifest is the machine-readable record of a workflow run.
type Manifest struct {
	ManifestVersion    int          `json:"manifest_version"`
	Mode               string       `json:"mode"`
	Attempt            int          `json:"attempt"`
	WorkflowName       string       `json:"workflow_name"`
	WorkflowPlanVersion int         `json:"workflow_plan_version"`
	BinlogvizVersion   string       `json:"binlogviz_version"`
	PlanPath           string       `json:"plan_path"`
	PlanSHA256         string       `json:"plan_sha256,omitempty"`
	ResolvedInputFiles []string     `json:"resolved_input_files,omitempty"`
	SnapshotDir        string       `json:"snapshot_dir,omitempty"`
	RunStartedAt       string       `json:"run_started_at"`
	RunFinishedAt      string       `json:"run_finished_at"`
	Status             string       `json:"status"`
	Error              string       `json:"error,omitempty"`
	Steps              []StepRecord `json:"steps"`
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
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}
