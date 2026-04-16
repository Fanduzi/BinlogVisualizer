// Package analyzer normalizes file coverage selections into deterministic model.Diagnostics payloads.
// input: binlog file probes plus selected/skipped coverage items with reason annotations.
// output: sorted model.FileCoverageItem slices and assembled model.FileCoverage values.
// pos: analyzer-side coverage normalization layer used by later planner/report integration.
// note: if this file changes, update this header and README.md.
package analyzer

import (
	"sort"

	"binlogviz/internal/binlog"
	"binlogviz/internal/model"
)

// FileCoverageItemsFromProbes converts file probes into sorted coverage items with default or overridden reasons.
func FileCoverageItemsFromProbes(probes []binlog.FileProbe, defaultReason string, overrides map[string]string) []model.FileCoverageItem {
	items := make([]model.FileCoverageItem, 0, len(probes))
	for _, probe := range probes {
		reason := defaultReason
		if override, ok := overrides[probe.BinlogPath]; ok && override != "" {
			reason = override
		}
		items = append(items, model.FileCoverageItem{
			BinlogPath:   probe.BinlogPath,
			Reason:       reason,
			Size:         probe.SizeBytes,
			FirstEventAt: probe.FirstEventAt,
			LastEventAt:  probe.LastEventAt,
		})
	}
	sortFileCoverageItems(items)
	return items
}

// BuildFileCoverage clones and sorts selected and skipped coverage items.
func BuildFileCoverage(selected, skipped []model.FileCoverageItem) model.FileCoverage {
	coverage := model.FileCoverage{
		Selected: append([]model.FileCoverageItem(nil), selected...),
		Skipped:  append([]model.FileCoverageItem(nil), skipped...),
	}
	sortFileCoverageItems(coverage.Selected)
	sortFileCoverageItems(coverage.Skipped)
	return coverage
}

func sortFileCoverageItems(items []model.FileCoverageItem) {
	sort.Slice(items, func(i, j int) bool {
		if items[i].BinlogPath != items[j].BinlogPath {
			return items[i].BinlogPath < items[j].BinlogPath
		}
		if items[i].Reason != items[j].Reason {
			return items[i].Reason < items[j].Reason
		}
		return items[i].Size < items[j].Size
	})
}
