package workflow

import (
	"reflect"
	"testing"
	"time"
)

func TestBuildDescriptionIncludesDeterministicArtifactPaths(t *testing.T) {
	desc := BuildDescription(sampleDescriptionPlan())

	if got := desc.Windows[0].Artifacts; !reflect.DeepEqual(got, []string{"analyze/baseline.json"}) {
		t.Fatalf("unexpected analyze artifacts: %#v", got)
	}
	if got := desc.Compare[0].Artifacts; !reflect.DeepEqual(got, []string{
		"compare/incident_vs_baseline.json",
		"compare/incident_vs_baseline.html",
	}) {
		t.Fatalf("unexpected compare artifacts: %#v", got)
	}
	if got := desc.Trend[0].Artifacts; !reflect.DeepEqual(got, []string{
		"trend/weekly_series.json",
		"trend/weekly_series.html",
	}) {
		t.Fatalf("unexpected trend artifacts: %#v", got)
	}
}

func TestBuildDescriptionIncludesSnapshotNameOnlyWhenSaveEnabled(t *testing.T) {
	withSnapshots := sampleDescriptionPlan()
	withDesc := BuildDescription(withSnapshots)
	if got := withDesc.Windows[0].SnapshotName; got != "baseline" {
		t.Fatalf("expected snapshot name baseline, got %q", got)
	}

	withoutSnapshots := sampleDescriptionPlan()
	withoutSnapshots.Defaults.Snapshot.Save = false
	withoutDesc := BuildDescription(withoutSnapshots)
	if got := withoutDesc.Windows[0].SnapshotName; got != "" {
		t.Fatalf("expected empty snapshot name when snapshot.save=false, got %q", got)
	}
}

func TestBuildDescriptionPreservesDependenciesAndOrder(t *testing.T) {
	desc := BuildDescription(sampleDescriptionPlan())

	if got := desc.WorkflowName; got != "incident-orders-write-spike" {
		t.Fatalf("unexpected workflow name: %q", got)
	}
	if got := desc.OutputDir; got != "./artifacts/incident-orders-write-spike" {
		t.Fatalf("unexpected output dir: %q", got)
	}
	if !desc.SnapshotSave {
		t.Fatal("expected snapshot_save=true")
	}
	if len(desc.Windows) != 2 {
		t.Fatalf("expected 2 windows, got %d", len(desc.Windows))
	}
	if desc.Windows[0].Name != "baseline" || desc.Windows[1].Name != "incident" {
		t.Fatalf("unexpected window order: %#v", desc.Windows)
	}
	if got := desc.Compare[0].Current; got != "incident" {
		t.Fatalf("unexpected compare current: %q", got)
	}
	if got := desc.Compare[0].Baseline; got != "baseline" {
		t.Fatalf("unexpected compare baseline: %q", got)
	}
	if got := desc.Trend[0].Snapshots; !reflect.DeepEqual(got, []string{"baseline", "incident"}) {
		t.Fatalf("unexpected trend snapshots: %#v", got)
	}
}

func sampleDescriptionPlan() Plan {
	return Plan{
		Version: 1,
		Workflow: WorkflowMeta{
			Name:      "incident-orders-write-spike",
			OutputDir: "./artifacts/incident-orders-write-spike",
		},
		Defaults: Defaults{
			Snapshot: SnapshotConfig{Save: true},
		},
		Windows: []Window{
			{
				Name:  "baseline",
				Start: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
				End:   time.Date(2025, 1, 1, 1, 0, 0, 0, time.UTC),
			},
			{
				Name:  "incident",
				Start: time.Date(2025, 1, 7, 10, 0, 0, 0, time.UTC),
				End:   time.Date(2025, 1, 7, 11, 0, 0, 0, time.UTC),
			},
		},
		Compare: []CompareJob{
			{
				Name:     "incident_vs_baseline",
				Current:  "incident",
				Baseline: "baseline",
				Formats:  []string{"json", "html"},
			},
		},
		Trend: []TrendJob{
			{
				Name:      "weekly_series",
				Snapshots: []string{"baseline", "incident"},
				Formats:   []string{"json", "html"},
			},
		},
	}
}
