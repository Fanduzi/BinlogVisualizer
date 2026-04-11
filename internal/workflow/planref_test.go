package workflow

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidateWorkflowPlanPathTrustedRootedPlan(t *testing.T) {
	root := t.TempDir()
	planPath := filepath.Join(root, "plan.yaml")
	if err := os.WriteFile(planPath, []byte("version: 1\n"), 0o644); err != nil {
		t.Fatalf("write plan: %v", err)
	}
	canonical, err := ValidateWorkflowPlanPath(root, planPath)
	if err != nil {
		t.Fatalf("expected rooted plan to be trusted, got: %v", err)
	}
	// Verify returned canonical path resolves to the same file.
	expected, _ := filepath.EvalSymlinks(planPath)
	if canonical != expected {
		t.Fatalf("canonical = %q, want %q", canonical, expected)
	}
}

func TestValidateWorkflowPlanPathRejectsRelativeEscape(t *testing.T) {
	root := t.TempDir()
	escaped := filepath.Join(root, "..", "plan.yaml")
	if _, err := ValidateWorkflowPlanPath(root, escaped); err == nil {
		t.Fatal("expected ../plan.yaml escape to be rejected")
	}
}

func TestValidateWorkflowPlanPathRejectsAbsoluteOutside(t *testing.T) {
	root := t.TempDir()
	outside := filepath.Join(t.TempDir(), "plan.yaml")
	if _, err := ValidateWorkflowPlanPath(root, outside); err == nil {
		t.Fatal("expected absolute outside-root path to be rejected")
	}
}

func TestValidateWorkflowPlanPathRejectsSymlinkEscape(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir() + "-outside"
	if err := os.MkdirAll(outside, 0o755); err != nil {
		t.Fatalf("mkdir outside: %v", err)
	}
	outsidePlan := filepath.Join(outside, "plan.yaml")
	if err := os.WriteFile(outsidePlan, []byte("version: 1\n"), 0o644); err != nil {
		t.Fatalf("write outside plan: %v", err)
	}

	linkPath := filepath.Join(root, "plan.yaml")
	if err := os.Symlink(outsidePlan, linkPath); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	if _, err := ValidateWorkflowPlanPath(root, linkPath); err == nil {
		t.Fatal("expected symlink-escaped plan path to be rejected")
	}
}

func TestValidateWorkflowPlanPathRejectsEmpty(t *testing.T) {
	root := t.TempDir()
	if _, err := ValidateWorkflowPlanPath(root, ""); err == nil {
		t.Fatal("expected empty plan_path to be rejected")
	}
}

func TestValidateWorkflowPlanPathRejectsNestedPath(t *testing.T) {
	root := t.TempDir()
	nested := filepath.Join(root, "sub", "dir", "plan.yaml")
	if err := os.MkdirAll(filepath.Dir(nested), 0o755); err != nil {
		t.Fatalf("mkdir nested: %v", err)
	}
	if err := os.WriteFile(nested, []byte("version: 1\n"), 0o644); err != nil {
		t.Fatalf("write plan: %v", err)
	}
	if _, err := ValidateWorkflowPlanPath(root, nested); err == nil {
		t.Fatal("expected nested plan path to be rejected — only <root>/plan.yaml is trusted")
	}
}

func TestValidateWorkflowPlanPathRejectsRenamedPlan(t *testing.T) {
	root := t.TempDir()
	renamed := filepath.Join(root, "other-plan.yaml")
	if err := os.WriteFile(renamed, []byte("version: 1\n"), 0o644); err != nil {
		t.Fatalf("write plan: %v", err)
	}
	if _, err := ValidateWorkflowPlanPath(root, renamed); err == nil {
		t.Fatal("expected renamed plan file to be rejected — only plan.yaml is trusted")
	}
}

func TestValidateWorkflowPlanPathResolvesRelativePlanPath(t *testing.T) {
	root := t.TempDir()
	planPath := filepath.Join(root, "plan.yaml")
	if err := os.WriteFile(planPath, []byte("version: 1\n"), 0o644); err != nil {
		t.Fatalf("write plan: %v", err)
	}

	// Use a relative path "plan.yaml" — should resolve against root and return canonical.
	canonical, err := ValidateWorkflowPlanPath(root, "plan.yaml")
	if err != nil {
		t.Fatalf("expected relative plan.yaml to be trusted, got: %v", err)
	}
	expected, _ := filepath.EvalSymlinks(planPath)
	if canonical != expected {
		t.Fatalf("canonical = %q, want %q", canonical, expected)
	}
}

func TestValidateWorkflowPlanPathErrorMessages(t *testing.T) {
	root := t.TempDir()

	t.Run("empty", func(t *testing.T) {
		_, err := ValidateWorkflowPlanPath(root, "")
		if err == nil {
			t.Fatal("expected error")
		}
		if !strings.Contains(err.Error(), "plan_path") {
			t.Fatalf("expected plan_path in error, got: %v", err)
		}
	})

	t.Run("outside root", func(t *testing.T) {
		outside := filepath.Join(t.TempDir(), "plan.yaml")
		_, err := ValidateWorkflowPlanPath(root, outside)
		if err == nil {
			t.Fatal("expected error")
		}
		if !strings.Contains(err.Error(), "outside") && !strings.Contains(err.Error(), "trust") {
			t.Fatalf("expected trust/outside in error, got: %v", err)
		}
	})

	t.Run("nested path", func(t *testing.T) {
		nested := filepath.Join(root, "sub", "plan.yaml")
		os.MkdirAll(filepath.Dir(nested), 0o755)
		os.WriteFile(nested, []byte("version: 1\n"), 0o644)
		_, err := ValidateWorkflowPlanPath(root, nested)
		if err == nil {
			t.Fatal("expected error")
		}
		if !strings.Contains(err.Error(), "trust") {
			t.Fatalf("expected trust in error, got: %v", err)
		}
	})
}
