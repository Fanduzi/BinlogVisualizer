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
	if err := ValidateWorkflowPlanPath(root, planPath); err != nil {
		t.Fatalf("expected rooted plan to be trusted, got: %v", err)
	}
}

func TestValidateWorkflowPlanPathRejectsRelativeEscape(t *testing.T) {
	root := t.TempDir()
	escaped := filepath.Join(root, "..", "plan.yaml")
	if err := ValidateWorkflowPlanPath(root, escaped); err == nil {
		t.Fatal("expected ../plan.yaml escape to be rejected")
	}
}

func TestValidateWorkflowPlanPathRejectsAbsoluteOutside(t *testing.T) {
	root := t.TempDir()
	outside := filepath.Join(t.TempDir(), "plan.yaml")
	if err := ValidateWorkflowPlanPath(root, outside); err == nil {
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

	if err := ValidateWorkflowPlanPath(root, linkPath); err == nil {
		t.Fatal("expected symlink-escaped plan path to be rejected")
	}
}

func TestValidateWorkflowPlanPathAcceptsEmpty(t *testing.T) {
	root := t.TempDir()
	if err := ValidateWorkflowPlanPath(root, ""); err == nil {
		t.Fatal("expected empty plan_path to be rejected")
	}
}

func TestValidateWorkflowPlanPathTrustedNestedPath(t *testing.T) {
	root := t.TempDir()
	nested := filepath.Join(root, "sub", "dir", "plan.yaml")
	if err := os.MkdirAll(filepath.Dir(nested), 0o755); err != nil {
		t.Fatalf("mkdir nested: %v", err)
	}
	if err := os.WriteFile(nested, []byte("version: 1\n"), 0o644); err != nil {
		t.Fatalf("write plan: %v", err)
	}
	if err := ValidateWorkflowPlanPath(root, nested); err != nil {
		t.Fatalf("expected nested rooted plan to be trusted, got: %v", err)
	}
}

func TestValidateWorkflowPlanPathErrorMessages(t *testing.T) {
	root := t.TempDir()

	t.Run("empty", func(t *testing.T) {
		err := ValidateWorkflowPlanPath(root, "")
		if err == nil {
			t.Fatal("expected error")
		}
		if !strings.Contains(err.Error(), "plan_path") {
			t.Fatalf("expected plan_path in error, got: %v", err)
		}
	})

	t.Run("outside root", func(t *testing.T) {
		outside := filepath.Join(t.TempDir(), "plan.yaml")
		err := ValidateWorkflowPlanPath(root, outside)
		if err == nil {
			t.Fatal("expected error")
		}
		if !strings.Contains(err.Error(), "outside") && !strings.Contains(err.Error(), "trust") {
			t.Fatalf("expected trust/outside in error, got: %v", err)
		}
	})
}
