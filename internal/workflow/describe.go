package workflow

// Description is a static preview of how a workflow plan will execute.
type Description struct {
	WorkflowName string               `json:"workflow_name"`
	OutputDir    string               `json:"output_dir"`
	SnapshotSave bool                 `json:"snapshot_save"`
	Windows      []WindowDescription  `json:"windows,omitempty"`
	Compare      []CompareDescription `json:"compare,omitempty"`
	Trend        []TrendDescription   `json:"trend,omitempty"`
}

// WindowDescription describes one analyze window in execution order.
type WindowDescription struct {
	Name         string   `json:"name"`
	Start        string   `json:"start"`
	End          string   `json:"end"`
	Artifacts    []string `json:"artifacts"`
	SnapshotName string   `json:"snapshot_name,omitempty"`
}

// CompareDescription describes one compare job and its deterministic outputs.
type CompareDescription struct {
	Name      string   `json:"name"`
	Current   string   `json:"current"`
	Baseline  string   `json:"baseline"`
	Artifacts []string `json:"artifacts"`
}

// TrendDescription describes one trend job and its deterministic outputs.
type TrendDescription struct {
	Name      string   `json:"name"`
	Snapshots []string `json:"snapshots"`
	Artifacts []string `json:"artifacts"`
}

// BuildDescription derives a static execution preview from a validated workflow plan.
func BuildDescription(plan Plan) Description {
	desc := Description{
		WorkflowName: plan.Workflow.Name,
		OutputDir:    plan.Workflow.OutputDir,
		SnapshotSave: plan.Defaults.Snapshot.Save,
		Windows:      make([]WindowDescription, 0, len(plan.Windows)),
		Compare:      make([]CompareDescription, 0, len(plan.Compare)),
		Trend:        make([]TrendDescription, 0, len(plan.Trend)),
	}

	for _, window := range plan.Windows {
		windowDesc := WindowDescription{
			Name:      window.Name,
			Start:     window.Start.UTC().Format("2006-01-02T15:04:05Z"),
			End:       window.End.UTC().Format("2006-01-02T15:04:05Z"),
			Artifacts: []string{"analyze/" + window.Name + ".json"},
		}
		if plan.Defaults.Snapshot.Save {
			windowDesc.SnapshotName = window.Name
		}
		desc.Windows = append(desc.Windows, windowDesc)
	}

	for _, job := range plan.Compare {
		artifacts := make([]string, 0, len(job.Formats))
		for _, format := range job.Formats {
			artifacts = append(artifacts, "compare/"+job.Name+"."+format)
		}
		desc.Compare = append(desc.Compare, CompareDescription{
			Name:      job.Name,
			Current:   job.Current,
			Baseline:  job.Baseline,
			Artifacts: artifacts,
		})
	}

	for _, job := range plan.Trend {
		artifacts := make([]string, 0, len(job.Formats))
		for _, format := range job.Formats {
			artifacts = append(artifacts, "trend/"+job.Name+"."+format)
		}
		desc.Trend = append(desc.Trend, TrendDescription{
			Name:      job.Name,
			Snapshots: append([]string(nil), job.Snapshots...),
			Artifacts: artifacts,
		})
	}

	return desc
}
