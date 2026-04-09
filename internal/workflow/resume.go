package workflow

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

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
func ValidateResumableManifest(m Manifest, planPath string, planSHA256 string) error {
	if m.ManifestVersion == 0 {
		return fmt.Errorf("cannot resume: manifest has no manifest_version (legacy format); run a fresh workflow instead")
	}
	if m.ManifestVersion != 2 {
		return fmt.Errorf("cannot resume: manifest_version %d is not supported (only version 2 is supported)", m.ManifestVersion)
	}
	if m.PlanPath == "" {
		return fmt.Errorf("cannot resume: manifest has no plan_path")
	}
	if planPath != "" {
		if _, err := os.Stat(planPath); err != nil {
			return fmt.Errorf("cannot resume: plan file %q not found", planPath)
		}
	}
	if planSHA256 != "" && m.PlanSHA256 != "" && planSHA256 != m.PlanSHA256 {
		return fmt.Errorf("cannot resume: plan file has changed since the original run (hash mismatch)")
	}
	if len(m.ResolvedInputFiles) == 0 {
		return fmt.Errorf("cannot resume: manifest has no resolved_input_files")
	}
	return nil
}

// BuildResumePlan computes which steps need execution and which can be reused.
func BuildResumePlan(plan Plan, m Manifest, selectors []string, outputDir string, snapshotDir string) (ResumePlan, error) {
	parsedSels, err := ParseRerunSelectors(plan, selectors)
	if err != nil {
		return ResumePlan{}, err
	}

	// Build a set of explicitly rerun steps
	rerunSet := make(map[string]bool) // "kind:name" -> true
	for _, s := range parsedSels {
		rerunSet[s.Kind+":"+s.Name] = true
	}

	// Build lookup of prior step status by kind:name
	priorStatus := make(map[string]StepRecord, len(m.Steps))
	for _, s := range m.Steps {
		priorStatus[s.Kind+":"+s.Name] = s
	}

	// Step 1: Determine initial execute/reuse decisions
	steps := make([]PlannedStep, 0, len(plan.Windows)+len(plan.Compare)+len(plan.Trend))

	// Analyze steps
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
		}

		steps = append(steps, PlannedStep{
			Kind:    "analyze",
			Name:    w.Name,
			Execute: execute,
			Reason:  reason,
		})
	}

	// Track which analyze windows are being rerun for downstream invalidation
	analyzeRerun := make(map[string]bool)
	for _, s := range steps {
		if s.Kind == "analyze" && s.Execute {
			analyzeRerun[s.Name] = true
		}
	}

	// Compare steps
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

		steps = append(steps, PlannedStep{
			Kind:    "compare",
			Name:    j.Name,
			Execute: execute,
			Reason:  reason,
		})
	}

	// Track which compare jobs are being rerun for downstream invalidation
	compareRerun := make(map[string]bool)
	for _, s := range steps {
		if s.Kind == "compare" && s.Execute {
			compareRerun[s.Name] = true
		}
	}

	// Trend steps
	for _, j := range plan.Trend {
		key := "trend:" + j.Name
		execute := false
		reason := "reused: prior success with intact artifacts"

		if rerunSet[key] {
			execute = true
			reason = "explicit rerun"
		} else {
			// Check if any upstream analyze window is being rerun
			for _, snapName := range j.Snapshots {
				if analyzeRerun[snapName] {
					execute = true
					reason = "upstream analyze window rerun"
					break
				}
			}
		}
		if !execute {
			// Check if any upstream compare job is being rerun (compare results feed into trends)
			// Note: In the current architecture, trend reads analyze outputs directly,
			// not compare outputs. So compare rerun does NOT invalidate trend.
			// Keeping this check for future extensibility but NOT enabling it now.
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

		steps = append(steps, PlannedStep{
			Kind:    "trend",
			Name:    j.Name,
			Execute: execute,
			Reason:  reason,
		})
	}

	// Check if anything needs to be done
	hasWork := false
	for _, s := range steps {
		if s.Execute {
			hasWork = true
			break
		}
	}
	if !hasWork {
		return ResumePlan{}, fmt.Errorf("nothing to resume: all steps succeeded with intact artifacts and no explicit rerun requested")
	}

	// Build updated manifest
	updated := m
	updated.Mode = "resume"
	updated.Attempt = m.Attempt + 1
	updated.Status = "success" // will be overwritten if any step fails during execution
	updated.Error = ""
	updated.Steps = nil        // will be populated during execution

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
