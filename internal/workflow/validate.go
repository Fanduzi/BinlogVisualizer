package workflow

import (
	"fmt"
	"os"
	"strings"
	"time"
)

// ValidatePlan checks structural and cross-reference rules for a workflow plan.
func ValidatePlan(plan Plan) error {
	if plan.Version == 0 {
		return fmt.Errorf("workflow plan version is required")
	}
	if plan.Version != 1 {
		return fmt.Errorf("unsupported workflow plan version %d (only version 1 is supported)", plan.Version)
	}
	if plan.Workflow.Name == "" {
		return fmt.Errorf("workflow name is required")
	}
	if plan.Workflow.OutputDir == "" {
		return fmt.Errorf("workflow output_dir is required")
	}
	if plan.Defaults.Input.FromDir == "" {
		return fmt.Errorf("defaults.input.from_dir is required")
	}
	if plan.Defaults.Input.Prefix == "" {
		return fmt.Errorf("defaults.input.prefix is required")
	}
	if len(plan.Windows) == 0 {
		return fmt.Errorf("at least one window is required")
	}

	// Validate analyze format is json in v1
	if plan.Defaults.Analyze.Format != "" && plan.Defaults.Analyze.Format != "json" {
		return fmt.Errorf("defaults.analyze.format must be json in v1 (got %q)", plan.Defaults.Analyze.Format)
	}

	// Unique window names
	seen := make(map[string]bool, len(plan.Windows))
	for i, w := range plan.Windows {
		if w.Name == "" {
			return fmt.Errorf("window[%d]: name is required", i)
		}
		if seen[w.Name] {
			return fmt.Errorf("duplicate window name %q", w.Name)
		}
		seen[w.Name] = true
		if w.Start.IsZero() {
			return fmt.Errorf("window %q: start is required", w.Name)
		}
		if w.End.IsZero() {
			return fmt.Errorf("window %q: end is required", w.Name)
		}
		if !w.End.After(w.Start) {
			return fmt.Errorf("window %q: end must be after start", w.Name)
		}
	}

	// Validate compare jobs
	seenCompare := make(map[string]bool, len(plan.Compare))
	for _, job := range plan.Compare {
		if job.Name == "" {
			return fmt.Errorf("compare job: name is required")
		}
		if seenCompare[job.Name] {
			return fmt.Errorf("duplicate compare job name %q", job.Name)
		}
		seenCompare[job.Name] = true
		if !seen[job.Current] {
			return fmt.Errorf("compare %q references unknown current window %q", job.Name, job.Current)
		}
		if !seen[job.Baseline] {
			return fmt.Errorf("compare %q references unknown baseline window %q", job.Name, job.Baseline)
		}
		if err := validateFormats(job.Formats, "compare", job.Name); err != nil {
			return err
		}
	}

	// Validate trend jobs
	seenTrend := make(map[string]bool, len(plan.Trend))
	for _, job := range plan.Trend {
		if job.Name == "" {
			return fmt.Errorf("trend job: name is required")
		}
		if seenTrend[job.Name] {
			return fmt.Errorf("duplicate trend job name %q", job.Name)
		}
		seenTrend[job.Name] = true
		if len(job.Snapshots) < 2 {
			return fmt.Errorf("trend %q requires at least 2 snapshots", job.Name)
		}
		for _, snap := range job.Snapshots {
			if !seen[snap] {
				return fmt.Errorf("trend %q references unknown snapshot window %q", job.Name, snap)
			}
		}
		if err := validateFormats(job.Formats, "trend", job.Name); err != nil {
			return err
		}
	}

	// Validate large_trx_duration if provided
	if plan.Defaults.Analyze.LargeTrxDuration != "" {
		if _, err := time.ParseDuration(plan.Defaults.Analyze.LargeTrxDuration); err != nil {
			return fmt.Errorf("defaults.analyze.large_trx_duration: %w", err)
		}
	}

	return nil
}

// PlanInputWarnings returns non-fatal operator warnings for defaults.input.from_dir.
// Validate still succeeds; these catch plans that are structurally valid but
// obviously not runnable (placeholders or a missing input directory).
func PlanInputWarnings(plan Plan) []string {
	fromDir := strings.TrimSpace(plan.Defaults.Input.FromDir)
	if fromDir == "" {
		return nil
	}

	var warnings []string
	if looksLikeInputPlaceholder(fromDir) {
		warnings = append(warnings, fmt.Sprintf("defaults.input.from_dir looks like a placeholder: %s", fromDir))
	}
	info, err := os.Stat(fromDir)
	if err != nil {
		if os.IsNotExist(err) {
			warnings = append(warnings, fmt.Sprintf("defaults.input.from_dir does not exist: %s", fromDir))
		} else {
			warnings = append(warnings, fmt.Sprintf("defaults.input.from_dir is not readable: %s (%v)", fromDir, err))
		}
		return warnings
	}
	if !info.IsDir() {
		warnings = append(warnings, fmt.Sprintf("defaults.input.from_dir is not a directory: %s", fromDir))
	}
	return warnings
}

func looksLikeInputPlaceholder(path string) bool {
	lower := strings.ToLower(path)
	for _, token := range []string{"placeholder", "changeme", "your-binlog", "path/to", "<", "fixme", "todo", "xxx"} {
		if strings.Contains(lower, token) {
			return true
		}
	}
	return false
}

func validateFormats(formats []string, kind, name string) error {
	if len(formats) == 0 {
		return fmt.Errorf("%s %q: at least one format is required", kind, name)
	}
	seen := make(map[string]bool, len(formats))
	for _, f := range formats {
		switch f {
		case "json", "html":
		default:
			return fmt.Errorf("%s %q: unsupported format %q (allowed: json, html)", kind, name, f)
		}
		if seen[f] {
			return fmt.Errorf("%s %q: duplicate format %q", kind, name, f)
		}
		seen[f] = true
	}
	return nil
}
