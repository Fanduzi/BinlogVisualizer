package workflow

import (
	"binlogviz/internal/clierr"
	"os"
	"path/filepath"
	"sort"
)

// CleanOptions controls workflow cleanup behavior.
type CleanOptions struct {
	Apply            bool
	IncludeSnapshots bool
	// Force also deletes unattributed artifacts (not in the ledger, or edited
	// since) of kinds the manifest recorded, and allows a clean without a
	// ledger. It never deletes a snapshot the ledger does not attribute, and
	// never overrides run_not_successful.
	Force bool
}

// CleanCounts summarizes cleanup candidate and action totals.
type CleanCounts struct {
	ArtifactOrphans int `json:"artifact_orphans"`
	SnapshotOrphans int `json:"snapshot_orphans"`
	KeptOrphans     int `json:"kept_orphans"`
	Deleted         int `json:"deleted"`
	Skipped         int `json:"skipped"`
}

// CleanResult is the machine-readable result of workflow cleanup discovery or apply.
//
// ArtifactOrphans and SnapshotOrphans are what this invocation deletes (or,
// in dry-run, would delete). KeptOrphans are files no current step owns that
// clean keeps because the ledger does not attribute them (ADR 0009 §8).
type CleanResult struct {
	WorkflowName     string      `json:"workflow_name"`
	OutputDir        string      `json:"output_dir"`
	SnapshotDir      string      `json:"-"`
	Mode             string      `json:"mode"`
	IncludeSnapshots bool        `json:"include_snapshots"`
	Force            bool        `json:"force"`
	Ledger           string      `json:"ledger"`
	ArtifactOrphans  []string    `json:"artifact_orphans"`
	SnapshotOrphans  []string    `json:"snapshot_orphans"`
	KeptOrphans      []string    `json:"kept_orphans"`
	Deleted          []string    `json:"deleted"`
	Skipped          []string    `json:"skipped"`
	Counts           CleanCounts `json:"counts"`
}

// Clean refusal reasons (details.reason).
const (
	ReasonRunNotSuccessful = "run_not_successful"
	ReasonNoLedger         = "no_ledger"
)

// DiscoverCleanCandidates is DiscoverCleanCandidatesWithOptions without --force.
func DiscoverCleanCandidates(outputDir string, manifest Manifest, includeSnapshots bool) (CleanResult, error) {
	return DiscoverCleanCandidatesWithOptions(outputDir, manifest, CleanOptions{IncludeSnapshots: includeSnapshots})
}

// DiscoverCleanCandidatesWithOptions decides what clean may delete (ADR 0009 §8):
//  1. the manifest passes CheckCleanTrust;
//  2. the latest run succeeded, else refused (6, run_not_successful);
//  3. a ledger exists, else refused (6, no_ledger) unless Force;
//  4. an artifact no current step owns is deleted only if the ledger
//     attributes it (path recorded and sha256 still matches); others are
//     kept, or deleted with Force when their kind has a recorded step;
//  5. with IncludeSnapshots, only ledger-attributed snapshots no current
//     step owns are deleted.
func DiscoverCleanCandidatesWithOptions(outputDir string, manifest Manifest, opts CleanOptions) (CleanResult, error) {
	result := CleanResult{
		WorkflowName:     manifest.WorkflowName,
		OutputDir:        outputDir,
		SnapshotDir:      manifest.SnapshotDir,
		Mode:             "dry-run",
		IncludeSnapshots: opts.IncludeSnapshots,
		Force:            opts.Force,
		Ledger:           "missing",
		ArtifactOrphans:  []string{},
		SnapshotOrphans:  []string{},
		KeptOrphans:      []string{},
		Deleted:          []string{},
		Skipped:          []string{},
	}

	if err := CheckCleanTrust(outputDir, manifest); err != nil {
		return CleanResult{}, err
	}
	if manifest.Status != "success" {
		return CleanResult{}, clierr.Reasonf(clierr.KindRefused, ReasonRunNotSuccessful,
			"refusing to clean: the latest run's status is %q, not success, so the manifest may not describe what is on disk; fix the plan and re-run `binlogviz workflow run` (or `workflow resume`) until it succeeds", manifest.Status)
	}
	ledger, err := ReadLedger(outputDir)
	if err != nil {
		return CleanResult{}, err
	}
	if ledger.Present {
		result.Ledger = "present"
	} else if !opts.Force {
		return CleanResult{}, clierr.Reasonf(clierr.KindRefused, ReasonNoLedger,
			"refusing to clean: %s has no %s, so no file can be attributed to this workflow (it was written by an older binlogviz); re-run `binlogviz workflow run <plan.yaml>` into this directory to rebuild the ledger, or pass --force to delete unlisted outputs of recorded step kinds", outputDir, LedgerFile)
	}

	live := liveArtifactSet(manifest)
	kinds := recordedKinds(manifest)
	for _, kind := range artifactKinds {
		entries, err := os.ReadDir(filepath.Join(outputDir, kind))
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return CleanResult{}, clierr.Errorf(clierr.KindInput, "read %s cleanup directory: %w", kind, err)
		}
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			if ext := filepath.Ext(entry.Name()); ext != ".json" && !(ext == ".html" && kind != "analyze") {
				continue
			}
			rel := kind + "/" + entry.Name()
			if _, ok := live[rel]; ok {
				continue
			}
			sum, err := FileSHA256(filepath.Join(outputDir, kind, entry.Name()))
			switch {
			case err == nil && ledger.Attributed(LedgerArtifact, rel, sum):
				result.ArtifactOrphans = append(result.ArtifactOrphans, rel)
			case opts.Force && kinds[kind]:
				result.ArtifactOrphans = append(result.ArtifactOrphans, rel)
			default:
				result.KeptOrphans = append(result.KeptOrphans, rel)
			}
		}
	}
	sort.Strings(result.ArtifactOrphans)
	sort.Strings(result.KeptOrphans)

	if opts.IncludeSnapshots {
		liveSnaps := liveSnapshotPaths(manifest)
		for _, path := range ledger.Snapshots() {
			if liveSnaps[path] {
				continue
			}
			sum, err := FileSHA256(filepath.FromSlash(path))
			if err != nil {
				continue // already gone or unreadable: nothing to delete
			}
			if ledger.Attributed(LedgerSnapshot, path, sum) {
				result.SnapshotOrphans = append(result.SnapshotOrphans, path)
			} else {
				result.KeptOrphans = append(result.KeptOrphans, path)
			}
		}
		sort.Strings(result.SnapshotOrphans)
	}

	result.Counts = result.counts()
	return result, nil
}

func (r CleanResult) counts() CleanCounts {
	return CleanCounts{
		ArtifactOrphans: len(r.ArtifactOrphans),
		SnapshotOrphans: len(r.SnapshotOrphans),
		KeptOrphans:     len(r.KeptOrphans),
		Deleted:         len(r.Deleted),
		Skipped:         len(r.Skipped),
	}
}

func liveArtifactSet(manifest Manifest) map[string]struct{} {
	live := make(map[string]struct{})
	for _, step := range manifest.Steps {
		for _, artifact := range step.Artifacts {
			live[filepath.ToSlash(filepath.Clean(artifact))] = struct{}{}
		}
	}
	return live
}

// liveSnapshotPaths are the snapshots the current manifest's successful
// analyze steps own, as absolute slash paths (the ledger's form).
func liveSnapshotPaths(manifest Manifest) map[string]bool {
	live := map[string]bool{}
	if manifest.SnapshotDir == "" {
		return live
	}
	for _, step := range manifest.Steps {
		if step.Kind != "analyze" || step.Status != "success" || step.SnapshotName == "" {
			continue
		}
		if abs, err := filepath.Abs(filepath.Join(manifest.SnapshotDir, step.SnapshotName+".json")); err == nil {
			live[filepath.ToSlash(abs)] = true
		}
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
	if applied.IncludeSnapshots {
		for _, name := range applied.SnapshotOrphans {
			// Snapshot orphans are absolute paths from the ledger.
			if err := os.Remove(filepath.FromSlash(name)); err != nil {
				applied.Skipped = append(applied.Skipped, name)
				continue
			}
			applied.Deleted = append(applied.Deleted, name)
		}
	}
	applied.Counts = applied.counts()
	return applied
}
