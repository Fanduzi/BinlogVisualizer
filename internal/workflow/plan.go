// Package workflow defines the YAML plan schema, validation, artifact layout,
// and manifest contracts for the workflow run orchestrator.
package workflow

import (
	"fmt"
	"io"
	"time"

	"gopkg.in/yaml.v3"
)

// Plan is the top-level workflow plan decoded from YAML.
type Plan struct {
	Version  int           `yaml:"version"`
	Workflow WorkflowMeta   `yaml:"workflow"`
	Defaults Defaults       `yaml:"defaults"`
	Windows  []Window       `yaml:"windows"`
	Compare  []CompareJob   `yaml:"compare"`
	Trend    []TrendJob     `yaml:"trend"`
}

// WorkflowMeta describes run-level metadata.
type WorkflowMeta struct {
	Name      string `yaml:"name"`
	OutputDir string `yaml:"output_dir"`
}

// Defaults holds shared configuration inherited by all steps.
type Defaults struct {
	Input   InputDefaults   `yaml:"input"`
	Analyze AnalyzeDefaults `yaml:"analyze"`
	Snapshot SnapshotConfig `yaml:"snapshot"`
}

// InputDefaults defines shared binlog input defaults.
type InputDefaults struct {
	FromDir string `yaml:"from_dir"`
	Prefix  string `yaml:"prefix"`
}

// AnalyzeDefaults holds shared analyze options.
type AnalyzeDefaults struct {
	Format           string   `yaml:"format"`
	SQLContext       string   `yaml:"sql_context"`
	TopTables        int      `yaml:"top_tables"`
	TopTransactions  int      `yaml:"top_transactions"`
	TopMinutes       int      `yaml:"top_minutes"`
	DetectSpikes     bool     `yaml:"detect_spikes"`
	LargeTrxRows     int      `yaml:"large_trx_rows"`
	LargeTrxDuration string   `yaml:"large_trx_duration"`
	SpikeWindow      int      `yaml:"spike_window"`
	SpikeFactor      float64  `yaml:"spike_factor"`
	SpikeMinRows     int      `yaml:"spike_min_rows"`
	IncludeSchemas   []string `yaml:"include_schema"`
	ExcludeSchemas   []string `yaml:"exclude_schema"`
	IncludeTables    []string `yaml:"include_table"`
	ExcludeTables    []string `yaml:"exclude_table"`
}

// SnapshotConfig controls whether analyze outputs are saved as snapshots.
type SnapshotConfig struct {
	Save bool `yaml:"save"`
}

// Window defines one time-bounded analysis window.
type Window struct {
	Name  string    `yaml:"name"`
	Start time.Time `yaml:"start"`
	End   time.Time `yaml:"end"`
}

// CompareJob defines one compare operation referencing named windows.
type CompareJob struct {
	Name     string   `yaml:"name"`
	Current  string   `yaml:"current"`
	Baseline string   `yaml:"baseline"`
	Formats  []string `yaml:"formats"`
}

// TrendJob defines one trend operation referencing named windows.
type TrendJob struct {
	Name      string   `yaml:"name"`
	Snapshots []string `yaml:"snapshots"`
	Formats   []string `yaml:"formats"`
}

// LoadPlan decodes and validates a YAML workflow plan.
func LoadPlan(r io.Reader) (Plan, error) {
	var plan Plan
	dec := yaml.NewDecoder(r)
	dec.KnownFields(true)
	if err := dec.Decode(&plan); err != nil {
		return Plan{}, fmt.Errorf("decode workflow plan: %w", err)
	}
	if err := ValidatePlan(plan); err != nil {
		return Plan{}, err
	}
	return plan, nil
}
