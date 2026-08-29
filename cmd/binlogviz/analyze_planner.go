// Package binlogviz plans analyze discovery work from probed binlog time coverage.
// input: sorted candidate binlog paths, per-file probe metadata, requested time windows, and worker limits.
// output: narrowed analyzePlan values with overlapping files plus one adjacent boundary-evidence file per bounded side.
// pos: CLI discovery planning layer between filesystem candidate enumeration and parse/analyze execution.
// note: if this file changes, update this header and README.md.
package binlogviz

import (
	"os"
	"runtime"
	"time"

	"binlogviz/internal/analyzer"
	"binlogviz/internal/binlog"
	"binlogviz/internal/model"
)

type analyzePlan struct {
	Paths        []string
	WorkerCount  int
	FileCoverage model.FileCoverage
}

var probeAnalyzePaths = func(paths []string, workerCount int) ([]binlog.FileProbe, error) {
	return probeFilesWithWorkers(paths, workerCount, binlog.ProbeFile)
}

func buildAnalyzePlan(probes []binlog.FileProbe, start, end time.Time, maxWorkers int) analyzePlan {
	paths := make([]string, 0, len(probes))
	selectedProbes := make([]binlog.FileProbe, 0, len(probes))
	skippedProbes := make([]binlog.FileProbe, 0, len(probes))
	selected := make([]bool, len(probes))
	first, last := -1, -1
	for index, probe := range probes {
		if probeOverlapsWindow(probe, start, end) {
			selected[index] = true
			if first < 0 {
				first = index
			}
			last = index
		}
	}
	if (!start.IsZero() || !end.IsZero()) && first >= 0 {
		// ponytail: one adjacent rotation on each side; parse all rotations if
		// real workloads show transactions spanning more than this margin.
		if first > 0 {
			selected[first-1] = true
		}
		if last+1 < len(probes) {
			selected[last+1] = true
		}
	}
	for index, probe := range probes {
		if !selected[index] {
			skippedProbes = append(skippedProbes, probe)
			continue
		}
		paths = append(paths, probe.BinlogPath)
		selectedProbes = append(selectedProbes, probe)
	}

	workerCount := maxWorkers
	if workerCount <= 0 {
		workerCount = 1
	}
	if len(paths) > 0 && workerCount > len(paths) {
		workerCount = len(paths)
	}
	if workerCount < 1 {
		workerCount = 1
	}

	return analyzePlan{
		Paths:       paths,
		WorkerCount: workerCount,
		FileCoverage: analyzer.BuildFileCoverage(
			analyzer.FileCoverageItemsFromProbes(selectedProbes, "selected", nil),
			analyzer.FileCoverageItemsFromProbes(skippedProbes, "outside selected time window", nil),
		),
	}
}

func defaultAnalyzeProbeWorkers(pathCount int) int {
	if pathCount <= 0 {
		return 1
	}
	workers := runtime.GOMAXPROCS(0)
	if workers < 1 {
		workers = 1
	}
	if workers > pathCount {
		workers = pathCount
	}
	return workers
}

func coarseFilterPathsByModTime(paths []string, startTime, endTime time.Time) ([]string, error) {
	if len(paths) == 0 || (startTime.IsZero() && endTime.IsZero()) {
		return append([]string(nil), paths...), nil
	}

	modTimes := make([]time.Time, len(paths))
	for index, path := range paths {
		info, err := os.Stat(path)
		if err != nil {
			return nil, err
		}
		modTimes[index] = info.ModTime().UTC()
	}

	startIndex := 0
	if !startTime.IsZero() {
		startIndex = len(paths) - 1
		for index, modTime := range modTimes {
			if !modTime.Before(startTime) {
				startIndex = index
				break
			}
		}
		if startIndex > 0 {
			startIndex--
		}
	}

	endIndex := len(paths) - 1
	if !endTime.IsZero() {
		for index, modTime := range modTimes {
			if !modTime.Before(endTime) {
				endIndex = index
				break
			}
		}
		if endIndex+1 < len(paths) {
			endIndex++
		}
	}
	if endIndex < startIndex {
		endIndex = startIndex
	}

	return append([]string(nil), paths[startIndex:endIndex+1]...), nil
}

func probeOverlapsWindow(probe binlog.FileProbe, start, end time.Time) bool {
	if !start.IsZero() && !probe.LastEventAt.IsZero() && probe.LastEventAt.Before(start) {
		return false
	}
	if !end.IsZero() && !probe.FirstEventAt.IsZero() && probe.FirstEventAt.After(end) {
		return false
	}
	return true
}
