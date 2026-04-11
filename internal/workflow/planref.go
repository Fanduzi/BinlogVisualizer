package workflow

import (
	"fmt"
	"path/filepath"
	"strings"
)

// ValidateWorkflowPlanPath validates that planPath is the rooted plan.yaml copy
// inside the workflow root (outputDir). It returns the canonical absolute path on
// success, or an error if the path is empty, outside-root, symlink-escaped, not
// named "plan.yaml", or not a direct child of the root.
//
// The returned canonical path must be used for all subsequent file operations
// (Open, ReadFile, SHA256) instead of the raw manifest.PlanPath.
func ValidateWorkflowPlanPath(outputDir string, planPath string) (string, error) {
	if planPath == "" {
		return "", fmt.Errorf("plan_path is empty: manifest has no plan_path")
	}

	// Canonicalize the workflow root.
	canonicalRoot, err := filepath.Abs(outputDir)
	if err != nil {
		return "", fmt.Errorf("cannot canonicalize workflow root %q: %w", outputDir, err)
	}
	canonicalRoot = filepath.Clean(canonicalRoot)

	// Resolve the plan path as absolute first.
	absPlanPath := planPath
	if !filepath.IsAbs(absPlanPath) {
		absPlanPath = filepath.Join(canonicalRoot, absPlanPath)
	}
	absPlanPath = filepath.Clean(absPlanPath)

	// Evaluate symlinks to detect escape.
	resolvedPlanPath, err := filepath.EvalSymlinks(absPlanPath)
	if err != nil {
		// If the file does not exist, EvalSymlinks fails on the last component.
		// Resolve any existing prefix symlinks instead.
		resolvedPlanPath, err = resolveExistingPrefix(absPlanPath)
		if err != nil {
			return "", fmt.Errorf("cannot resolve plan_path %q: %w", planPath, err)
		}
	}

	// Also evaluate symlinks on the root itself.
	resolvedRoot, err := filepath.EvalSymlinks(canonicalRoot)
	if err != nil {
		resolvedRoot = canonicalRoot
	}
	resolvedRoot = filepath.Clean(resolvedRoot)

	// Ensure the resolved plan path is inside the resolved root.
	if !isInsideDir(resolvedRoot, resolvedPlanPath) {
		return "", fmt.Errorf("plan_path %q resolves outside workflow root: trust boundary violated", planPath)
	}

	// Tighten: only <root>/plan.yaml is accepted — not nested, not renamed.
	expectedPath := filepath.Join(resolvedRoot, "plan.yaml")
	if resolvedPlanPath != expectedPath {
		return "", fmt.Errorf("plan_path must be <root>/plan.yaml, got %q: trust boundary violated", planPath)
	}

	return resolvedPlanPath, nil
}

// isInsideDir returns true if target is inside dir (or equals dir).
func isInsideDir(dir string, target string) bool {
	// Ensure trailing separator so prefix match is exact.
	if !strings.HasSuffix(dir, string(filepath.Separator)) {
		dir += string(filepath.Separator)
	}
	return strings.HasPrefix(target+string(filepath.Separator), dir) || target == dir[:len(dir)-1]
}

// resolveExistingPrefix resolves symlinks on all existing ancestor directories.
func resolveExistingPrefix(path string) (string, error) {
	dir := filepath.Dir(path)
	base := filepath.Base(path)

	resolvedDir, err := filepath.EvalSymlinks(dir)
	if err != nil {
		return "", err
	}
	return filepath.Join(resolvedDir, base), nil
}
