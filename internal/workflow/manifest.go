package workflow

import (
	"encoding/json"
	"os"
)

// Manifest is the machine-readable record of a workflow run.
type Manifest struct {
	WorkflowName        string       `json:"workflow_name"`
	WorkflowPlanVersion int          `json:"workflow_plan_version"`
	BinlogvizVersion    string       `json:"binlogviz_version"`
	PlanPath            string       `json:"plan_path"`
	RunStartedAt        string       `json:"run_started_at"`
	RunFinishedAt       string       `json:"run_finished_at"`
	Status              string       `json:"status"`
	Error               string       `json:"error,omitempty"`
	Steps               []StepRecord `json:"steps"`
}

// StepRecord records the outcome of one workflow step.
type StepRecord struct {
	Kind         string   `json:"kind"`
	Name         string   `json:"name"`
	Status       string   `json:"status"`
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
