package workflow

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

// CleanOptions controls workflow cleanup behavior.
type CleanOptions struct {
	Apply            bool
	IncludeSnapshots bool
}

// CleanCounts summarizes cleanup candidate and action totals.
type CleanCounts struct {
	ArtifactOrphans int `json:"artifact_orphans"`
	SnapshotOrphans int `json:"snapshot_orphans"`
	Deleted         int `json:"deleted"`
	Skipped         int `json:"skipped"`
}

// CleanResult is the machine-readable result of workflow cleanup discovery or apply.
type CleanResult struct {
	WorkflowName     string      `json:"workflow_name"`
	OutputDir        string      `json:"output_dir"`
	SnapshotDir      string      `json:"-"`
	Mode             string      `json:"mode"`
	IncludeSnapshots bool        `json:"include_snapshots"`
	ArtifactOrphans  []string    `json:"artifact_orphans"`
	SnapshotOrphans  []string    `json:"snapshot_orphans"`
	Deleted          []string    `json:"deleted"`
	Skipped          []string    `json:"skipped"`
	Counts           CleanCounts `json:"counts"`
}

// DiscoverCleanCandidates discovers orphaned workflow artifacts and optional snapshots.
func DiscoverCleanCandidates(outputDir string, manifest Manifest, includeSnapshots bool) (CleanResult, error) {
	result := CleanResult{
		WorkflowName:     manifest.WorkflowName,
		OutputDir:        outputDir,
		SnapshotDir:      manifest.SnapshotDir,
		Mode:             "dry-run",
		IncludeSnapshots: includeSnapshots,
		ArtifactOrphans:  []string{},
		SnapshotOrphans:  []string{},
		Deleted:          []string{},
		Skipped:          []string{},
	}

	liveArtifacts := liveArtifactSet(manifest)
	artifactOrphans, err := discoverArtifactOrphans(outputDir, liveArtifacts)
	if err != nil {
		return CleanResult{}, err
	}
	result.ArtifactOrphans = artifactOrphans

	if includeSnapshots && manifest.SnapshotDir != "" {
		snapshotOrphans, err := discoverSnapshotOrphans(manifest)
		if err != nil {
			return CleanResult{}, err
		}
		result.SnapshotOrphans = snapshotOrphans
	}

	result.Counts = result.counts()
	return result, nil
}

func (r CleanResult) counts() CleanCounts {
	return CleanCounts{
		ArtifactOrphans: len(r.ArtifactOrphans),
		SnapshotOrphans: len(r.SnapshotOrphans),
		Deleted:         len(r.Deleted),
		Skipped:         len(r.Skipped),
	}
}

func liveArtifactSet(manifest Manifest) map[string]struct{} {
	live := make(map[string]struct{})
	for _, step := range manifest.Steps {
		for _, artifact := range step.Artifacts {
			live[filepath.Clean(filepath.ToSlash(artifact))] = struct{}{}
		}
	}
	return live
}

func discoverArtifactOrphans(outputDir string, liveArtifacts map[string]struct{}) ([]string, error) {
	discovered := make([]string, 0)
	for kind, extensions := range map[string]map[string]struct{}{
		"analyze": {".json": {}},
		"compare": {".json": {}, ".html": {}},
		"trend":   {".json": {}, ".html": {}},
	} {
		orphans, err := discoverOrphansInDir(filepath.Join(outputDir, kind), kind, liveArtifacts, extensions)
		if err != nil {
			return nil, err
		}
		discovered = append(discovered, orphans...)
	}
	sort.Strings(discovered)
	return discovered, nil
}

func discoverOrphansInDir(dirPath, kind string, liveArtifacts map[string]struct{}, extensions map[string]struct{}) ([]string, error) {
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read %s cleanup directory: %w", dirPath, err)
	}

	orphans := make([]string, 0)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if _, ok := extensions[filepath.Ext(entry.Name())]; !ok {
			continue
		}
		relPath := filepath.ToSlash(filepath.Join(kind, entry.Name()))
		if _, ok := liveArtifacts[relPath]; ok {
			continue
		}
		orphans = append(orphans, relPath)
	}
	return orphans, nil
}

func discoverSnapshotOrphans(manifest Manifest) ([]string, error) {
	entries, err := os.ReadDir(manifest.SnapshotDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read snapshot cleanup directory: %w", err)
	}

	liveSnapshots := liveSnapshotSet(manifest)
	orphans := make([]string, 0)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		if _, ok := liveSnapshots[entry.Name()]; ok {
			continue
		}
		orphans = append(orphans, entry.Name())
	}
	sort.Strings(orphans)
	return orphans, nil
}

func liveSnapshotSet(manifest Manifest) map[string]struct{} {
	live := make(map[string]struct{})
	for _, step := range manifest.Steps {
		if step.Kind != "analyze" || step.Status != "success" || step.SnapshotName == "" {
			continue
		}
		live[step.SnapshotName+".json"] = struct{}{}
	}
	return live
}

// ApplyClean deletes discovered cleanup candidates and records deleted/skipped paths.
func ApplyClean(result CleanResult) CleanResult {
	applied := result
	applied.Mode = "apply"
	applied.Deleted = []string{}
	applied.Skipped = []string{}

	for _, relPath := range applied.ArtifactOrphans {
		if err := os.Remove(resolveArtifactPath(applied.OutputDir, relPath)); err != nil {
			applied.Skipped = append(applied.Skipped, relPath)
			continue
		}
		applied.Deleted = append(applied.Deleted, relPath)
	}
	if applied.IncludeSnapshots && applied.SnapshotDir != "" {
		for _, name := range applied.SnapshotOrphans {
			snapshotPath := filepath.Join(applied.SnapshotDir, name)
			if err := os.Remove(snapshotPath); err != nil {
				applied.Skipped = append(applied.Skipped, name)
				continue
			}
			applied.Deleted = append(applied.Deleted, name)
		}
	}
	applied.Counts = applied.counts()
	return applied
}
