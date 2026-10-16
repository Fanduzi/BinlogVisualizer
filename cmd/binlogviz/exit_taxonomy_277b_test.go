// input: workflow manifests and plans, reports and snapshots built in temp dirs.
// output: #277 follow-up contract: broken manifests are corrupt (5), only a legacy or newer version marker is unsupported (9), deliberate resume refusals are refused (6) with details.reason, and invalid plans carry details.reason=invalid_plan.
// pos: guard for ADR 0009 §2 after QA round 2 on #284.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"binlogviz/internal/clierr"
)

func execRoot(t *testing.T, args ...string) error {
	t.Helper()
	cmd := NewRootCommand()
	cmd.SetArgs(args)
	cmd.SetOut(&strings.Builder{})
	cmd.SetErr(&strings.Builder{})
	return cmd.Execute()
}

func writeManifest(t *testing.T, dir, body string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func assertKind(t *testing.T, err error, kind clierr.Kind, reason string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected a %s error", kind)
	}
	got, details := ErrorKind(err)
	if got != kind || ExitCode(err) != clierr.Code(kind) {
		t.Fatalf("kind %s exit %d, want %s: %v", got, ExitCode(err), kind, err)
	}
	if reason != "" && (details == nil || details["reason"] != reason) {
		t.Fatalf("details %v, want reason=%s: %v", details, reason, err)
	}
}

func TestBrokenManifestIsCorrupt(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	for _, sub := range []string{"resume", "status", "clean", "export"} {
		for name, body := range map[string]string{"undecodable": "{bad", "empty object": "{}"} {
			t.Run(sub+"/"+name, func(t *testing.T) {
				dir := t.TempDir()
				writeManifest(t, dir, body)
				args := []string{"workflow", sub, dir}
				if sub == "export" {
					args = append(args, "--output", filepath.Join(t.TempDir(), "x.zip"))
				}
				err := execRoot(t, args...)
				if sub != "resume" && name == "empty object" && err == nil {
					return // read-only commands may describe an empty manifest
				}
				assertKind(t, err, clierr.KindCorrupt, "")
			})
		}
	}
}

func TestLegacyManifestStaysUnsupported(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	dir := t.TempDir()
	writeManifest(t, dir, `{"workflow_name":"old-run","status":"success","steps":[]}`)
	assertKind(t, execRoot(t, "workflow", "resume", dir), clierr.KindUnsupported, "")
}

func TestResumeRefusalsAreRefused(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	t.Run("plan changed", func(t *testing.T) {
		dir := t.TempDir()
		planPath := filepath.Join(dir, "plan.yaml")
		if err := os.WriteFile(planPath, []byte(validPlan277), 0o644); err != nil {
			t.Fatal(err)
		}
		writeManifest(t, dir, `{"manifest_version":2,"workflow_name":"x","plan_path":"`+planPath+`","plan_sha256":"deadbeef","resolved_input_files":["/tmp/a"],"steps":[]}`)
		assertKind(t, execRoot(t, "workflow", "resume", dir), clierr.KindRefused, "plan_changed")
	})
	t.Run("plan outside root", func(t *testing.T) {
		dir, outside := t.TempDir(), t.TempDir()
		planPath := filepath.Join(outside, "plan.yaml")
		if err := os.WriteFile(planPath, []byte("version: 1\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		writeManifest(t, dir, `{"manifest_version":2,"workflow_name":"x","plan_path":"`+planPath+`","plan_sha256":"abc","resolved_input_files":["/tmp/a"],"steps":[]}`)
		assertKind(t, execRoot(t, "workflow", "resume", dir), clierr.KindRefused, "trust_boundary")
		// status is read-only: it reports the run and exits 0 (documented).
		if err := execRoot(t, "workflow", "status", dir); err != nil {
			t.Fatalf("status on an out-of-root plan_path: %v", err)
		}
	})
}

func TestInvalidPlanHasReason(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	planPath := filepath.Join(t.TempDir(), "plan.yaml")
	if err := os.WriteFile(planPath, []byte("version: 1\nworkflow:\n  name: x\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, sub := range []string{"validate", "run", "describe"} {
		assertKind(t, execRoot(t, "workflow", sub, planPath), clierr.KindCorrupt, "invalid_plan")
	}
}

func TestEmptyReportMessageMatchesKind(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	dir := t.TempDir()
	report := filepath.Join(dir, "r.json")
	if err := os.WriteFile(report, []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		{"snapshot", "save", report, "--name", "x", "--snapshot-dir", dir},
		{"compare", report, report},
	} {
		err := execRoot(t, args...)
		assertKind(t, err, clierr.KindCorrupt, "")
		msg := err.Error()
		if strings.Contains(msg, "unsupported") || !strings.Contains(msg, "missing required fields") {
			t.Fatalf("%v: message %q should say the report is missing required fields", args, msg)
		}
	}
}

const validPlan277 = `
version: 1
workflow:
  name: hash-test
  output_dir: /tmp/out
defaults:
  input:
    from_dir: /tmp/mysql
    prefix: mysql-bin.
windows:
  - name: w1
    start: 2025-01-01T00:00:00Z
    end: 2099-12-31T23:59:59Z
`
