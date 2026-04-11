package workflow

import (
	"fmt"
	"path/filepath"
	"strings"
)

// ValidateWorkflowPlanPath checks that planPath is a trusted plan reference
// inside the workflow root (outputDir). It rejects empty paths, outside-root
// resolution, and symlink escape.
func ValidateWorkflowPlanPath(outputDir string, planPath string) error {
	if planPath == "" {
		return fmt.Errorf("plan_path is empty: manifest has no plan_path")
	}

	// Canonicalize the workflow root.
	canonicalRoot, err := filepath.Abs(outputDir)
	if err != nil {
		return fmt.Errorf("cannot canonicalize workflow root %q: %w", outputDir, err)
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
			return fmt.Errorf("cannot resolve plan_path %q: %w", planPath, err)
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
		return fmt.Errorf("plan_path %q resolves outside workflow root: trust boundary violated", planPath)
	}

	return nil
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
