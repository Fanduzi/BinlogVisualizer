package workflow

import (
	"binlogviz/internal/clierr"
	"os"
	"path/filepath"
	"sort"
	"time"
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
	// SnapshotDir is the snapshot dir the caller vouches for (--snapshot-dir
	// or the default). With IncludeSnapshots, a manifest snapshot_dir that
	// resolves elsewhere is refused (6, snapshot_dir_mismatch). Empty means
	// the manifest's value, for library callers only.
	SnapshotDir string
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
	WorkflowName     string            `json:"workflow_name"`
	OutputDir        string            `json:"output_dir"`
	SnapshotDir      string            `json:"-"`
	Mode             string            `json:"mode"`
	IncludeSnapshots bool              `json:"include_snapshots"`
	Force            bool              `json:"force"`
	Ledger           string            `json:"ledger"`
	ArtifactOrphans  []string          `json:"artifact_orphans"`
	SnapshotOrphans  []string          `json:"snapshot_orphans"`
	KeptOrphans      []string          `json:"kept_orphans"`
	Deleted          []string          `json:"deleted"`
	Skipped          []string          `json:"skipped"`
	SkipReasons      map[string]string `json:"skip_reasons,omitempty"`
	// TrashDir and SnapshotTrashDir are where --apply moved deleted files
	// (<root>/.trash/<id>, <snapshot-dir>/.trash/<id>). Clean never purges
	// them; empty them by hand once you no longer need the files.
	TrashID          string      `json:"-"`
	TrashDir         string      `json:"trash_dir,omitempty"`
	SnapshotTrashDir string      `json:"snapshot_trash_dir,omitempty"`
	Counts           CleanCounts `json:"counts"`
}

// Clean refusal reasons (details.reason).
const (
	ReasonRunNotSuccessful    = "run_not_successful"
	ReasonNoLedger            = "no_ledger"
	ReasonSnapshotDirMismatch = "snapshot_dir_mismatch"
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

	// Every kind dir must be a real directory inside the root before
	// anything is considered, --force or not.
	present := map[string]bool{}
	for _, kind := range artifactKinds {
		ok, err := CheckKindDir(outputDir, kind)
		if err != nil {
			return CleanResult{}, err
		}
		present[kind] = ok
	}

	live := liveArtifactSet(manifest)
	kinds := recordedKinds(manifest)
	for _, kind := range artifactKinds {
		if !present[kind] {
			continue
		}
		entries, err := os.ReadDir(filepath.Join(outputDir, kind))
		if err != nil {
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
			if !IsPlainFile(outputDir, rel) {
				// Symlinks, hidden names and non-regular files are never deleted.
				result.KeptOrphans = append(result.KeptOrphans, rel)
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

	if opts.IncludeSnapshots && manifest.SnapshotDir != "" {
		dir, err := trustedSnapshotDir(outputDir, manifest.SnapshotDir, opts.SnapshotDir)
		if err != nil {
			return CleanResult{}, err
		}
		result.SnapshotDir = dir
		manifest.SnapshotDir = dir
		liveSnaps := liveSnapshotNames(manifest)
		for _, name := range ledger.Snapshots() {
			// Ledger validation guarantees a bare <name>.json.
			if liveSnaps[name] || !IsPlainFile(manifest.SnapshotDir, name) {
				continue
			}
			sum, err := FileSHA256(filepath.Join(manifest.SnapshotDir, name))
			if err != nil {
				continue // already gone or unreadable: nothing to delete
			}
			if ledger.Attributed(LedgerSnapshot, name, sum) {
				result.SnapshotOrphans = append(result.SnapshotOrphans, name)
			} else {
				result.KeptOrphans = append(result.KeptOrphans, name)
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

// ResolveManifestSnapshotDir returns the manifest's snapshot_dir as an
// absolute path; a relative value (as written by export) is relative to the
// workflow root.
func ResolveManifestSnapshotDir(outputDir, dir string) string {
	if dir == "" || filepath.IsAbs(dir) {
		return dir
	}
	return filepath.Join(outputDir, filepath.FromSlash(dir))
}

func canonicalDir(p string) string {
	abs, err := filepath.Abs(p)
	if err != nil {
		return filepath.Clean(p)
	}
	if real, err := filepath.EvalSymlinks(abs); err == nil {
		return real
	}
	return abs
}

// trustedSnapshotDir takes the snapshot dir from the caller, never from the
// manifest alone: the manifest is a file in the workflow dir and may be
// forged or restored from another machine (ADR 0009 §8).
func trustedSnapshotDir(outputDir, manifestDir, flagDir string) (string, error) {
	recorded := ResolveManifestSnapshotDir(outputDir, manifestDir)
	if flagDir == "" {
		return recorded, nil
	}
	if canonicalDir(recorded) != canonicalDir(flagDir) {
		return "", clierr.Reasonf(clierr.KindRefused, ReasonSnapshotDirMismatch,
			"refusing to clean snapshots: the manifest records snapshot_dir %s but this clean uses %s (from --snapshot-dir or the default); clean only touches the snapshot dir you name, so pass --snapshot-dir %s if that is really this workflow's snapshot dir", recorded, flagDir, recorded)
	}
	return flagDir, nil
}

// liveSnapshotNames are the snapshot file names (relative to the snapshot
// dir, the ledger's form) the current manifest's successful analyze steps own.
func liveSnapshotNames(manifest Manifest) map[string]bool {
	live := map[string]bool{}
	for _, step := range manifest.Steps {
		if step.Kind != "analyze" || step.Status != "success" || step.SnapshotName == "" {
			continue
		}
		live[step.SnapshotName+".json"] = true
	}
	return live
}

// ApplyClean moves each candidate into the trash with TrashFile (the only
// deletion primitive); nothing is unlinked. Every target on both sides is
// preflighted first, so a refusal (6) or an unusable trash (8) returns an
// error with nothing moved. A move that still fails is reported in Skipped
// and SkipReasons.
func ApplyClean(result CleanResult) (CleanResult, error) {
	applied := result
	applied.Mode = "apply"
	applied.Deleted = []string{}
	applied.Skipped = []string{}
	id := applied.TrashID
	if id == "" {
		id = NewRunID(time.Now())
	}
	type target struct{ root, rel string }
	var targets []target
	for _, rel := range applied.ArtifactOrphans {
		targets = append(targets, target{applied.OutputDir, rel})
	}
	if applied.IncludeSnapshots {
		for _, name := range applied.SnapshotOrphans {
			targets = append(targets, target{applied.SnapshotDir, name})
		}
	}
	for _, tg := range targets {
		if err := PrepareTrash(tg.root, tg.rel, id); err != nil {
			return CleanResult{}, clierr.Errorf(clierr.KindOutput, "nothing was moved: %w", err)
		}
	}
	for i, tg := range targets {
		dir, err := TrashFile(tg.root, tg.rel, id)
		if err != nil {
			applied.Skipped = append(applied.Skipped, tg.rel)
			if applied.SkipReasons == nil {
				applied.SkipReasons = map[string]string{}
			}
			applied.SkipReasons[tg.rel] = err.Error()
			continue
		}
		if i < len(applied.ArtifactOrphans) {
			applied.TrashDir = dir
		} else {
			applied.SnapshotTrashDir = dir
		}
		applied.Deleted = append(applied.Deleted, tg.rel)
	}
	applied.Counts = applied.counts()
	return applied, nil
}
