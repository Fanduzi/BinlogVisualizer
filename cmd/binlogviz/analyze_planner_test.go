// Package binlogviz verifies analyze discovery planning derived from probed binlog coverage.
// input: probed binlog file metadata plus requested analyze time windows and worker limits.
// output: regression coverage for file narrowing and worker-count selection semantics.
// pos: focused test layer for Task 4 analyze planning before command-level integration wiring.
package binlogviz

import (
	"testing"
	"time"

	"binlogviz/internal/binlog"
)

func TestBuildAnalyzePlanNarrowsToCoveredFiles(t *testing.T) {
	start := time.Date(2026, 4, 5, 9, 0, 0, 0, time.UTC)
	end := time.Date(2026, 4, 5, 10, 0, 0, 0, time.UTC)

	probes := []binlog.FileProbe{
		{
			BinlogPath:   "mysql-bin.000044",
			FirstEventAt: start.Add(-time.Hour),
			LastEventAt:  end.Add(-time.Minute),
		},
		{
			BinlogPath:   "mysql-bin.000045",
			FirstEventAt: end.Add(time.Minute),
			LastEventAt:  end.Add(2 * time.Hour),
		},
	}

	plan := buildAnalyzePlan(probes, start, end, 4)
	if len(plan.Paths) != 1 || plan.Paths[0] != "mysql-bin.000044" {
		t.Fatalf("unexpected plan: %+v", plan)
	}
	if plan.WorkerCount != 1 {
		t.Fatalf("expected 1 worker for 1 covered file, got %d", plan.WorkerCount)
	}
}

func TestBuildAnalyzePlanFallsBackToAllProbesWithoutWindow(t *testing.T) {
	probes := []binlog.FileProbe{
		{BinlogPath: "mysql-bin.000044"},
		{BinlogPath: "mysql-bin.000045"},
	}

	plan := buildAnalyzePlan(probes, time.Time{}, time.Time{}, 8)
	if len(plan.Paths) != 2 {
		t.Fatalf("expected all paths without window, got %+v", plan)
	}
	if plan.WorkerCount != 2 {
		t.Fatalf("expected worker count capped to path count, got %d", plan.WorkerCount)
	}
}
