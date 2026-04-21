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

func TestProbeOverlapsWindowIncludesFileWhenLastEventAtIsZero(t *testing.T) {
	// When offset probing fails, LastEventAt stays zero. The planner must
	// treat zero LastEventAt as "unknown end time" and include the file
	// conservatively, as long as FirstEventAt is not after the window end.
	probe := binlog.FileProbe{
		BinlogPath:   "mysql-bin.000044",
		FirstEventAt: time.Date(2026, 4, 5, 4, 0, 0, 0, time.UTC),
		LastEventAt:  time.Time{}, // zero — offset probe failed
	}
	start := time.Date(2026, 4, 5, 14, 0, 0, 0, time.UTC)
	end := time.Date(2026, 4, 5, 15, 0, 0, 0, time.UTC)

	if !probeOverlapsWindow(probe, start, end) {
		t.Fatal("expected file with zero LastEventAt to be included (conservative)")
	}
}

func TestProbeOverlapsWindowExcludesFileWhenFirstEventAfterEnd(t *testing.T) {
	probe := binlog.FileProbe{
		BinlogPath:   "mysql-bin.000045",
		FirstEventAt: time.Date(2026, 4, 5, 16, 0, 0, 0, time.UTC),
		LastEventAt:  time.Date(2026, 4, 5, 17, 0, 0, 0, time.UTC),
	}
	start := time.Date(2026, 4, 5, 14, 0, 0, 0, time.UTC)
	end := time.Date(2026, 4, 5, 15, 0, 0, 0, time.UTC)

	if probeOverlapsWindow(probe, start, end) {
		t.Fatal("expected file starting after window end to be excluded")
	}
}

func TestBuildAnalyzePlanIncludesFileWithZeroLastEventAt(t *testing.T) {
	start := time.Date(2026, 4, 5, 14, 0, 0, 0, time.UTC)
	end := time.Date(2026, 4, 5, 15, 0, 0, 0, time.UTC)

	probes := []binlog.FileProbe{
		{
			BinlogPath:   "mysql-bin.000044",
			FirstEventAt: time.Date(2026, 4, 5, 4, 0, 0, 0, time.UTC),
			LastEventAt:  time.Time{}, // zero — large file, offset probe failed
		},
		{
			BinlogPath:   "mysql-bin.000045",
			FirstEventAt: time.Date(2026, 4, 5, 15, 30, 0, 0, time.UTC),
			LastEventAt:  time.Date(2026, 4, 5, 16, 0, 0, 0, time.UTC),
		},
	}

	plan := buildAnalyzePlan(probes, start, end, 4)
	if len(plan.Paths) != 1 || plan.Paths[0] != "mysql-bin.000044" {
		t.Fatalf("expected only file with zero LastEventAt included, got %+v", plan.Paths)
	}
}
