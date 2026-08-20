package workflow

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// ErrNothingToResume is returned when every planned step already succeeded
// with intact artifacts and no --rerun selector was given.
var ErrNothingToResume = errors.New("nothing to resume")

// RerunSelector identifies a single step to rerun.
type RerunSelector struct {
	Kind string // "analyze", "compare", or "trend"
	Name string // window/job name
}

// PlannedStep is one declared workflow step with a resolved execution decision.
type PlannedStep struct {
	Kind    string // "analyze", "compare", or "trend"
	Name    string
	Execute bool   // true = run this step; false = reuse prior output
	Reason  string // human-readable explanation of the decision
}

// ResumePlan is the output of the resume planner.
type ResumePlan struct {
	Plan             Plan
	UpdatedManifest  Manifest // copy of the input manifest with mode/attempt updated
	Steps            []PlannedStep
	InputFiles       []string // reused from prior manifest
	SnapshotDir      string
}

// ParseRerunSelectors parses raw "--rerun kind:name" strings into selectors,
// validating that each kind is known and each name exists in the plan.
func ParseRerunSelectors(plan Plan, selectors []string) ([]RerunSelector, error) {
	// Build lookup sets from the plan
	analyzeNames := make(map[string]bool, len(plan.Windows))
	for _, w := range plan.Windows {
		analyzeNames[w.Name] = true
	}
	compareNames := make(map[string]bool, len(plan.Compare))
	for _, j := range plan.Compare {
		compareNames[j.Name] = true
	}
	trendNames := make(map[string]bool, len(plan.Trend))
	for _, j := range plan.Trend {
		trendNames[j.Name] = true
	}

	result := make([]RerunSelector, 0, len(selectors))
	for _, raw := range selectors {
		parts := strings.SplitN(raw, ":", 2)
		if len(parts) != 2 || parts[1] == "" {
			return nil, fmt.Errorf("invalid rerun selector %q: expected format kind:name", raw)
		}
		kind, name := parts[0], parts[1]
		switch kind {
		case "analyze":
			if !analyzeNames[name] {
				return nil, fmt.Errorf("unknown analyze window %q in rerun selector", name)
			}
		case "compare":
			if !compareNames[name] {
				return nil, fmt.Errorf("unknown compare job %q in rerun selector", name)
			}
		case "trend":
			if !trendNames[name] {
				return nil, fmt.Errorf("unknown trend job %q in rerun selector", name)
			}
		default:
			return nil, fmt.Errorf("invalid rerun selector kind %q: must be analyze, compare, or trend", kind)
		}
		result = append(result, RerunSelector{Kind: kind, Name: name})
	}
	return result, nil
}

// ValidateResumableManifest checks that a manifest is safe to resume from.
// outputDir is the workflow root used for trust-boundary validation of plan_path.
// planPath and planSHA256 are the canonical plan path and its current hash.
func ValidateResumableManifest(m Manifest, outputDir string, planPath string, planSHA256 string) error {
	if m.ManifestVersion == 0 {
		return fmt.Errorf("cannot resume: manifest has no manifest_version (legacy format); run a fresh workflow instead")
	}
	if m.ManifestVersion != 2 {
		return fmt.Errorf("cannot resume: manifest_version %d is not supported (only version 2 is supported)", m.ManifestVersion)
	}
	if m.PlanPath == "" {
		return fmt.Errorf("cannot resume: manifest has no plan_path")
	}
	if outputDir != "" {
		if _, err := ValidateWorkflowPlanPath(outputDir, m.PlanPath); err != nil {
			return fmt.Errorf("cannot resume: %w", err)
		}
	}
	if planPath != "" {
		if _, err := os.Stat(planPath); err != nil {
			return fmt.Errorf("cannot resume: plan file %q not found", planPath)
		}
	}
	if m.PlanSHA256 == "" {
		return fmt.Errorf("cannot resume: manifest has no plan_sha256")
	}
	if planSHA256 != m.PlanSHA256 {
		return fmt.Errorf("cannot resume: plan file has changed since the original run (hash mismatch)")
	}
	if len(m.ResolvedInputFiles) == 0 {
		return fmt.Errorf("cannot resume: manifest has no resolved_input_files")
	}
	return nil
}

// planResumeSteps computes execution/reuse decisions and reports whether any work is required.
func planResumeSteps(plan Plan, m Manifest, selectors []string, outputDir string, snapshotDir string) ([]PlannedStep, bool, error) {
	parsedSels, err := ParseRerunSelectors(plan, selectors)
	if err != nil {
		return nil, false, err
	}

	rerunSet := make(map[string]bool)
	for _, s := range parsedSels {
		rerunSet[s.Kind+":"+s.Name] = true
	}

	priorStatus := make(map[string]StepRecord, len(m.Steps))
	for _, s := range m.Steps {
		priorStatus[s.Kind+":"+s.Name] = s
	}

	steps := make([]PlannedStep, 0, len(plan.Windows)+len(plan.Compare)+len(plan.Trend))

	for _, w := range plan.Windows {
		key := "analyze:" + w.Name
		execute := false
		reason := "reused: prior success with intact artifacts"

		if rerunSet[key] {
			execute = true
			reason = "explicit rerun"
		} else if prior, ok := priorStatus[key]; !ok || prior.Status != "success" {
			execute = true
			reason = "prior step failed or missing"
		} else if !allArtifactsExist(outputDir, prior.Artifacts) {
			execute = true
			reason = "artifact file missing"
		} else if plan.Defaults.Snapshot.Save && snapshotDir != "" && prior.SnapshotName != "" {
			if !snapshotFileExists(snapshotDir, prior.SnapshotName) {
				execute = true
				reason = "snapshot file missing"
			}
		}

		steps = append(steps, PlannedStep{Kind: "analyze", Name: w.Name, Execute: execute, Reason: reason})
	}

	analyzeRerun := make(map[string]bool)
	for _, s := range steps {
		if s.Kind == "analyze" && s.Execute {
			analyzeRerun[s.Name] = true
		}
	}

	for _, j := range plan.Compare {
		key := "compare:" + j.Name
		execute := false
		reason := "reused: prior success with intact artifacts"

		if rerunSet[key] {
			execute = true
			reason = "explicit rerun"
		} else if analyzeRerun[j.Current] || analyzeRerun[j.Baseline] {
			execute = true
			reason = "upstream analyze window rerun"
		} else if prior, ok := priorStatus[key]; !ok || prior.Status != "success" {
			execute = true
			reason = "prior step failed or missing"
		} else if !allArtifactsExist(outputDir, prior.Artifacts) {
			execute = true
			reason = "artifact file missing"
		}

		steps = append(steps, PlannedStep{Kind: "compare", Name: j.Name, Execute: execute, Reason: reason})
	}

	compareRerun := make(map[string]bool)
	for _, s := range steps {
		if s.Kind == "compare" && s.Execute {
			compareRerun[s.Name] = true
		}
	}

	for _, j := range plan.Trend {
		key := "trend:" + j.Name
		execute := false
		reason := "reused: prior success with intact artifacts"

		if rerunSet[key] {
			execute = true
			reason = "explicit rerun"
		} else {
			for _, snapName := range j.Snapshots {
				if analyzeRerun[snapName] {
					execute = true
					reason = "upstream analyze window rerun"
					break
				}
			}
		}
		if !execute {
			_ = compareRerun
		}
		if !execute {
			if prior, ok := priorStatus[key]; !ok || prior.Status != "success" {
				execute = true
				reason = "prior step failed or missing"
			} else if !allArtifactsExist(outputDir, prior.Artifacts) {
				execute = true
				reason = "artifact file missing"
			}
		}

		steps = append(steps, PlannedStep{Kind: "trend", Name: j.Name, Execute: execute, Reason: reason})
	}

	hasWork := false
	for _, s := range steps {
		if s.Execute {
			hasWork = true
			break
		}
	}

	return steps, hasWork, nil
}

// BuildResumePlan computes which steps need execution and which can be reused.
func BuildResumePlan(plan Plan, m Manifest, selectors []string, outputDir string, snapshotDir string) (ResumePlan, error) {
	steps, hasWork, err := planResumeSteps(plan, m, selectors, outputDir, snapshotDir)
	if err != nil {
		return ResumePlan{}, err
	}
	if !hasWork {
		return ResumePlan{}, ErrNothingToResume
	}

	updated := m
	updated.Mode = "resume"
	updated.Attempt = m.Attempt + 1
	updated.Status = "success"
	updated.Error = ""
	updated.Steps = nil

	return ResumePlan{
		Plan:            plan,
		UpdatedManifest: updated,
		Steps:           steps,
		InputFiles:      m.ResolvedInputFiles,
		SnapshotDir:     snapshotDir,
	}, nil
}

// allArtifactsExist checks that every expected artifact file exists in the output directory.
func allArtifactsExist(outputDir string, artifacts []string) bool {
	for _, art := range artifacts {
		p := resolveArtifactPath(outputDir, art)
		if _, err := os.Stat(p); err != nil {
			return false
		}
	}
	return true
}

// resolveArtifactPath resolves a relative artifact path against the output root.
func resolveArtifactPath(outputDir, rel string) string {
	return filepath.Join(outputDir, rel)
}

// snapshotFileExists checks whether a snapshot file exists in the snapshot directory.
func snapshotFileExists(snapshotDir, name string) bool {
	path := filepath.Join(snapshotDir, name+".json")
	_, err := os.Stat(path)
	return err == nil
}
