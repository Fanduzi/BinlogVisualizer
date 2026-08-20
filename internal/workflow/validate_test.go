package workflow

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPlanInputWarningsPlaceholder(t *testing.T) {
	plan := Plan{
		Defaults: Defaults{Input: InputDefaults{FromDir: "PLACEHOLDER/binlog", Prefix: "mysql-bin."}},
	}
	warnings := PlanInputWarnings(plan)
	if len(warnings) < 2 {
		t.Fatalf("expected placeholder and missing-dir warnings, got %v", warnings)
	}
	joined := strings.Join(warnings, "\n")
	if !strings.Contains(joined, "placeholder") {
		t.Fatalf("expected placeholder warning, got %v", warnings)
	}
	if !strings.Contains(joined, "does not exist") {
		t.Fatalf("expected missing from_dir warning, got %v", warnings)
	}
}

func TestPlanInputWarningsMissingDirectory(t *testing.T) {
	plan := Plan{
		Defaults: Defaults{Input: InputDefaults{FromDir: filepath.Join(t.TempDir(), "no-such-binlog"), Prefix: "mysql-bin."}},
	}
	warnings := PlanInputWarnings(plan)
	if len(warnings) != 1 {
		t.Fatalf("expected 1 missing-dir warning, got %v", warnings)
	}
	if !strings.Contains(warnings[0], "does not exist") {
		t.Fatalf("expected missing from_dir warning, got %v", warnings)
	}
}

func TestPlanInputWarningsExistingDirectory(t *testing.T) {
	dir := t.TempDir()
	plan := Plan{
		Defaults: Defaults{Input: InputDefaults{FromDir: dir, Prefix: "mysql-bin."}},
	}
	warnings := PlanInputWarnings(plan)
	if len(warnings) != 0 {
		t.Fatalf("expected no warnings for existing from_dir, got %v", warnings)
	}
}

func TestPlanInputWarningsNotADirectory(t *testing.T) {
	file := filepath.Join(t.TempDir(), "mysql-bin.000001")
	if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}
	plan := Plan{
		Defaults: Defaults{Input: InputDefaults{FromDir: file, Prefix: "mysql-bin."}},
	}
	warnings := PlanInputWarnings(plan)
	if len(warnings) != 1 || !strings.Contains(warnings[0], "not a directory") {
		t.Fatalf("expected not-a-directory warning, got %v", warnings)
	}
}
