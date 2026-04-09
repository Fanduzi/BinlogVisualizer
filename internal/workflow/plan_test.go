package workflow

import (
	"strings"
	"testing"
)

func TestLoadPlanRejectsMissingVersion(t *testing.T) {
	_, err := LoadPlan(strings.NewReader(`
workflow:
  name: incident
  output_dir: ./artifacts
defaults:
  input:
    from_dir: /var/lib/mysql
    prefix: mysql-bin.
windows:
  - name: baseline
    start: 2026-04-09T10:00:00Z
    end: 2026-04-09T10:30:00Z
`))
	if err == nil || !strings.Contains(err.Error(), "workflow plan version is required") {
		t.Fatalf("expected missing version error, got %v", err)
	}
}

func TestLoadPlanRejectsUnsupportedVersion(t *testing.T) {
	_, err := LoadPlan(strings.NewReader(`
version: 2
workflow:
  name: incident
  output_dir: ./artifacts
defaults:
  input:
    from_dir: /var/lib/mysql
    prefix: mysql-bin.
windows:
  - name: baseline
    start: 2026-04-09T10:00:00Z
    end: 2026-04-09T10:30:00Z
`))
	if err == nil || !strings.Contains(err.Error(), "unsupported workflow plan version 2") {
		t.Fatalf("expected unsupported version error, got %v", err)
	}
}

func TestLoadPlanRejectsMissingWorkflowName(t *testing.T) {
	_, err := LoadPlan(strings.NewReader(`
version: 1
workflow:
  output_dir: ./artifacts
defaults:
  input:
    from_dir: /var/lib/mysql
    prefix: mysql-bin.
windows:
  - name: baseline
    start: 2026-04-09T10:00:00Z
    end: 2026-04-09T10:30:00Z
`))
	if err == nil || !strings.Contains(err.Error(), "workflow name is required") {
		t.Fatalf("expected missing workflow name error, got %v", err)
	}
}

func TestLoadPlanRejectsMissingOutputDir(t *testing.T) {
	_, err := LoadPlan(strings.NewReader(`
version: 1
workflow:
  name: incident
defaults:
  input:
    from_dir: /var/lib/mysql
    prefix: mysql-bin.
windows:
  - name: baseline
    start: 2026-04-09T10:00:00Z
    end: 2026-04-09T10:30:00Z
`))
	if err == nil || !strings.Contains(err.Error(), "workflow output_dir is required") {
		t.Fatalf("expected missing output_dir error, got %v", err)
	}
}

func TestLoadPlanRejectsMissingInputFromDir(t *testing.T) {
	_, err := LoadPlan(strings.NewReader(`
version: 1
workflow:
  name: incident
  output_dir: ./artifacts
defaults:
  input:
    prefix: mysql-bin.
windows:
  - name: baseline
    start: 2026-04-09T10:00:00Z
    end: 2026-04-09T10:30:00Z
`))
	if err == nil || !strings.Contains(err.Error(), "defaults.input.from_dir is required") {
		t.Fatalf("expected missing from_dir error, got %v", err)
	}
}

func TestLoadPlanRejectsMissingInputPrefix(t *testing.T) {
	_, err := LoadPlan(strings.NewReader(`
version: 1
workflow:
  name: incident
  output_dir: ./artifacts
defaults:
  input:
    from_dir: /var/lib/mysql
windows:
  - name: baseline
    start: 2026-04-09T10:00:00Z
    end: 2026-04-09T10:30:00Z
`))
	if err == nil || !strings.Contains(err.Error(), "defaults.input.prefix is required") {
		t.Fatalf("expected missing prefix error, got %v", err)
	}
}

func TestLoadPlanRejectsEmptyWindows(t *testing.T) {
	_, err := LoadPlan(strings.NewReader(`
version: 1
workflow:
  name: incident
  output_dir: ./artifacts
defaults:
  input:
    from_dir: /var/lib/mysql
    prefix: mysql-bin.
`))
	if err == nil || !strings.Contains(err.Error(), "at least one window is required") {
		t.Fatalf("expected empty windows error, got %v", err)
	}
}

func TestLoadPlanRejectsDuplicateWindowNames(t *testing.T) {
	_, err := LoadPlan(strings.NewReader(`
version: 1
workflow:
  name: incident
  output_dir: ./artifacts
defaults:
  input:
    from_dir: /var/lib/mysql
    prefix: mysql-bin.
windows:
  - name: baseline
    start: 2026-04-09T10:00:00Z
    end: 2026-04-09T10:30:00Z
  - name: baseline
    start: 2026-04-09T11:00:00Z
    end: 2026-04-09T11:30:00Z
`))
	if err == nil || !strings.Contains(err.Error(), `duplicate window name "baseline"`) {
		t.Fatalf("expected duplicate window name error, got %v", err)
	}
}

func TestLoadPlanRejectsNonJSONAnalyzeFormat(t *testing.T) {
	_, err := LoadPlan(strings.NewReader(`
version: 1
workflow:
  name: incident
  output_dir: ./artifacts
defaults:
  input:
    from_dir: /var/lib/mysql
    prefix: mysql-bin.
  analyze:
    format: text
windows:
  - name: baseline
    start: 2026-04-09T10:00:00Z
    end: 2026-04-09T10:30:00Z
`))
	if err == nil || !strings.Contains(err.Error(), "defaults.analyze.format must be json") {
		t.Fatalf("expected format error, got %v", err)
	}
}

func TestLoadPlanRejectsUnknownCompareWindow(t *testing.T) {
	_, err := LoadPlan(strings.NewReader(`
version: 1
workflow:
  name: incident
  output_dir: ./artifacts
defaults:
  input:
    from_dir: /var/lib/mysql
    prefix: mysql-bin.
  analyze:
    format: json
windows:
  - name: baseline
    start: 2026-04-09T10:00:00Z
    end: 2026-04-09T10:30:00Z
compare:
  - name: drift
    current: incident
    baseline: baseline
    formats: [json]
`))
	if err == nil || !strings.Contains(err.Error(), `compare "drift" references unknown current window "incident"`) {
		t.Fatalf("expected unknown-window error, got %v", err)
	}
}

func TestLoadPlanRejectsUnknownTrendSnapshot(t *testing.T) {
	_, err := LoadPlan(strings.NewReader(`
version: 1
workflow:
  name: incident
  output_dir: ./artifacts
defaults:
  input:
    from_dir: /var/lib/mysql
    prefix: mysql-bin.
  analyze:
    format: json
windows:
  - name: baseline
    start: 2026-04-09T10:00:00Z
    end: 2026-04-09T10:30:00Z
trend:
  - name: series
    snapshots: [baseline, missing]
    formats: [json]
`))
	if err == nil || !strings.Contains(err.Error(), `trend "series" references unknown snapshot window "missing"`) {
		t.Fatalf("expected unknown snapshot error, got %v", err)
	}
}

func TestLoadPlanRejectsUnsupportedCompareFormat(t *testing.T) {
	_, err := LoadPlan(strings.NewReader(`
version: 1
workflow:
  name: incident
  output_dir: ./artifacts
defaults:
  input:
    from_dir: /var/lib/mysql
    prefix: mysql-bin.
windows:
  - name: baseline
    start: 2026-04-09T10:00:00Z
    end: 2026-04-09T10:30:00Z
  - name: incident
    start: 2026-04-09T11:00:00Z
    end: 2026-04-09T11:30:00Z
compare:
  - name: drift
    current: incident
    baseline: baseline
    formats: [text]
`))
	if err == nil || !strings.Contains(err.Error(), `compare "drift": unsupported format "text"`) {
		t.Fatalf("expected unsupported format error, got %v", err)
	}
}

func TestLoadPlanRejectsWindowEndBeforeStart(t *testing.T) {
	_, err := LoadPlan(strings.NewReader(`
version: 1
workflow:
  name: incident
  output_dir: ./artifacts
defaults:
  input:
    from_dir: /var/lib/mysql
    prefix: mysql-bin.
windows:
  - name: bad
    start: 2026-04-09T11:00:00Z
    end: 2026-04-09T10:00:00Z
`))
	if err == nil || !strings.Contains(err.Error(), `window "bad": end must be after start`) {
		t.Fatalf("expected end-before-start error, got %v", err)
	}
}

func TestLoadPlanAcceptsValidMinimalPlan(t *testing.T) {
	plan, err := LoadPlan(strings.NewReader(`
version: 1
workflow:
  name: incident
  output_dir: ./artifacts
defaults:
  input:
    from_dir: /var/lib/mysql
    prefix: mysql-bin.
windows:
  - name: baseline
    start: 2026-04-09T10:00:00Z
    end: 2026-04-09T10:30:00Z
`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Version != 1 {
		t.Fatalf("expected version 1, got %d", plan.Version)
	}
	if plan.Workflow.Name != "incident" {
		t.Fatalf("expected workflow name incident, got %s", plan.Workflow.Name)
	}
	if len(plan.Windows) != 1 {
		t.Fatalf("expected 1 window, got %d", len(plan.Windows))
	}
	if plan.Windows[0].Name != "baseline" {
		t.Fatalf("expected window name baseline, got %s", plan.Windows[0].Name)
	}
}

func TestLoadPlanAcceptsValidFullPlan(t *testing.T) {
	plan, err := LoadPlan(strings.NewReader(`
version: 1
workflow:
  name: incident-orders-write-spike
  output_dir: ./artifacts/incident
defaults:
  input:
    from_dir: /var/lib/mysql
    prefix: mysql-bin.
  analyze:
    format: json
    top_tables: 10
  snapshot:
    save: true
windows:
  - name: baseline_10_00
    start: 2026-04-09T10:00:00Z
    end: 2026-04-09T10:30:00Z
  - name: incident_11_00
    start: 2026-04-09T11:00:00Z
    end: 2026-04-09T11:30:00Z
compare:
  - name: incident_vs_baseline
    current: incident_11_00
    baseline: baseline_10_00
    formats: [json, html]
trend:
  - name: incident_series
    snapshots: [baseline_10_00, incident_11_00]
    formats: [json, html]
`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(plan.Compare) != 1 {
		t.Fatalf("expected 1 compare job, got %d", len(plan.Compare))
	}
	if len(plan.Trend) != 1 {
		t.Fatalf("expected 1 trend job, got %d", len(plan.Trend))
	}
	if !plan.Defaults.Snapshot.Save {
		t.Fatal("expected snapshot.save = true")
	}
}

func TestLoadPlanRejectsUnknownYAMLFields(t *testing.T) {
	_, err := LoadPlan(strings.NewReader(`
version: 1
workflow:
  name: incident
  output_dir: ./artifacts
defaults:
  input:
    from_dir: /var/lib/mysql
    prefix: mysql-bin.
  unknown_field: oops
windows:
  - name: baseline
    start: 2026-04-09T10:00:00Z
    end: 2026-04-09T10:30:00Z
`))
	if err == nil {
		t.Fatal("expected unknown field error, got nil")
	}
}
