// Package analyzer verifies file coverage normalization helpers for selected and skipped binlog files.
// input: file probes, reason overrides, and raw coverage item slices.
// output: regression coverage for path sorting, reason assignment, and defensive copying.
// pos: contract tests for later Analyzer diagnostics wiring without changing command-layer selection code.
package analyzer

import (
	"testing"
	"time"

	"binlogviz/internal/binlog"
	"binlogviz/internal/model"
)

func TestFileCoverageItemsFromProbesUsesReasonOverrides(t *testing.T) {
	first := time.Date(2026, 4, 15, 8, 0, 0, 0, time.UTC)
	last := first.Add(30 * time.Minute)

	got := FileCoverageItemsFromProbes([]binlog.FileProbe{
		{BinlogPath: "mysql-bin.000002", SizeBytes: 200, FirstEventAt: first, LastEventAt: last},
		{BinlogPath: "mysql-bin.000001", SizeBytes: 100, FirstEventAt: first.Add(-time.Hour), LastEventAt: first},
	}, "selected", map[string]string{
		"mysql-bin.000002": "trimmed at end boundary",
	})

	if len(got) != 2 {
		t.Fatalf("expected 2 items, got %d", len(got))
	}
	if got[0].BinlogPath != "mysql-bin.000001" || got[0].Reason != "selected" {
		t.Fatalf("expected first sorted item to use default reason, got %+v", got[0])
	}
	if got[1].BinlogPath != "mysql-bin.000002" || got[1].Reason != "trimmed at end boundary" {
		t.Fatalf("expected second sorted item to use override reason, got %+v", got[1])
	}
}

func TestBuildFileCoverageSortsAndClonesItems(t *testing.T) {
	selected := []model.FileCoverageItem{
		{BinlogPath: "mysql-bin.000002", Reason: "selected"},
		{BinlogPath: "mysql-bin.000001", Reason: "selected"},
	}
	skipped := []model.FileCoverageItem{
		{BinlogPath: "mysql-bin.000004", Reason: "outside window"},
		{BinlogPath: "mysql-bin.000003", Reason: "outside window"},
	}

	got := BuildFileCoverage(selected, skipped)
	selected[0].Reason = "mutated"
	skipped[0].Reason = "mutated"

	if got.Selected[0].BinlogPath != "mysql-bin.000001" || got.Selected[0].Reason != "selected" {
		t.Fatalf("expected selected items sorted and cloned, got %+v", got.Selected)
	}
	if got.Skipped[0].BinlogPath != "mysql-bin.000003" || got.Skipped[0].Reason != "outside window" {
		t.Fatalf("expected skipped items sorted and cloned, got %+v", got.Skipped)
	}
}
