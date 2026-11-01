package binlogviz

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"binlogviz/internal/workflow"
)

type terminalOutputMode struct {
	name  string
	flags []string
}

func terminalOutputModes() []terminalOutputMode {
	return []terminalOutputMode{
		{name: "normal"},
		{name: "quiet", flags: []string{"--quiet"}},
		{name: "json-error", flags: []string{"--error-format", "json"}},
	}
}

func terminalWorkflowPlan(t *testing.T, withBinlog bool) (string, string, string) {
	t.Helper()
	base := t.TempDir()
	plan, err := os.ReadFile(filepath.Join("testdata", "workflow", "basic-plan.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	planPath := filepath.Join(base, "plan.yaml")
	if err := os.WriteFile(planPath, []byte(strings.ReplaceAll(string(plan), "PLACEHOLDER", base)), 0o600); err != nil {
		t.Fatal(err)
	}
	if withBinlog {
		dir := filepath.Join(base, "binlog")
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		fixture, err := os.ReadFile(filepath.Join("testdata", "minimal.binlog"))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "mysql-bin.000001"), fixture, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return planPath, filepath.Join(base, "out"), t.TempDir()
}

func TestWorkflowOutputModesInputFailure(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	t.Setenv("BINLOGVIZ_ERROR_FORMAT", "")
	for _, mode := range terminalOutputModes() {
		t.Run(mode.name, func(t *testing.T) {
			plan, output, snapshots := terminalWorkflowPlan(t, false)
			args := append([]string{"--lang", "en"}, mode.flags...)
			args = append(args, "workflow", "run", plan, "--output-dir", output, "--snapshot-dir", snapshots)
			code, stdout, stderr := runRoot(t, args...)
			if code != 4 || stdout != "" {
				t.Fatalf("exit=%d stdout=%q stderr=%q", code, stdout, stderr)
			}
			for _, name := range []string{"manifest.json", "index.html"} {
				if _, err := os.Stat(filepath.Join(output, name)); err != nil {
					t.Errorf("missing %s: %v", name, err)
				}
			}
			switch mode.name {
			case "normal":
				for _, want := range []string{"starting", "manifest written", "index written", "Error:"} {
					if !strings.Contains(stderr, want) {
						t.Errorf("missing %q in stderr: %q", want, stderr)
					}
				}
			case "quiet":
				lines := strings.Split(strings.TrimSpace(stderr), "\n")
				if len(lines) != 1 || !strings.HasPrefix(lines[0], "Error: ") {
					t.Errorf("quiet stderr must contain only the error: %q", stderr)
				}
			case "json-error":
				var envelope struct {
					Error struct {
						Kind string `json:"kind"`
						Code int    `json:"code"`
					} `json:"error"`
				}
				if err := json.Unmarshal([]byte(strings.TrimSpace(stderr)), &envelope); err != nil {
					t.Errorf("stderr is not one JSON error: %v: %q", err, stderr)
				} else if envelope.Error.Kind != "input" || envelope.Error.Code != 4 {
					t.Errorf("unexpected error envelope: %+v", envelope)
				}
			}
		})
	}
}

func TestWorkflowOutputModesSuccessfulRun(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	t.Setenv("BINLOGVIZ_ERROR_FORMAT", "")
	for _, mode := range terminalOutputModes() {
		t.Run(mode.name, func(t *testing.T) {
			plan, output, snapshots := terminalWorkflowPlan(t, true)
			args := append([]string{"--lang", "en"}, mode.flags...)
			args = append(args, "workflow", "run", plan, "--output-dir", output, "--snapshot-dir", snapshots)
			code, stdout, stderr := runRoot(t, args...)
			if code != 0 || stdout != "" {
				t.Fatalf("exit=%d stdout=%q stderr=%q", code, stdout, stderr)
			}
			manifest, err := workflowManifestFromJSON(filepath.Join(output, "manifest.json"))
			if err != nil {
				t.Fatal(err)
			}
			if manifest.Status != "success" {
				t.Errorf("manifest status=%q", manifest.Status)
			}
			wantSteps := map[string]bool{"analyze:week1": false, "analyze:week2": false, "compare:week2_vs_week1": false, "trend:weekly_series": false}
			for _, step := range manifest.Steps {
				key := step.Kind + ":" + step.Name
				if _, ok := wantSteps[key]; ok && step.Status == "success" {
					wantSteps[key] = true
				}
			}
			for step, found := range wantSteps {
				if !found {
					t.Errorf("missing successful step %s: %+v", step, manifest.Steps)
				}
			}
			for _, name := range []string{"compare/week2_vs_week1.json", "compare/week2_vs_week1.html", "trend/weekly_series.json", "trend/weekly_series.html"} {
				if _, err := os.Stat(filepath.Join(output, name)); err != nil {
					t.Errorf("missing %s: %v", name, err)
				}
			}
			if mode.name == "normal" {
				for _, want := range []string{`analyze "week1"`, `analyze "week2"`, `compare "week2_vs_week1"`, `trend "weekly_series"`} {
					if !strings.Contains(stderr, want) {
						t.Errorf("missing progress %q in %q", want, stderr)
					}
				}
			} else if stderr != "" {
				t.Errorf("informational stderr must be suppressed: %q", stderr)
			}
		})
	}
}

func TestWorkflowQuietPreservesLedgerWarning(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	t.Setenv("BINLOGVIZ_ERROR_FORMAT", "")
	oldQuiet, oldFormat := quietFlag, errorFormat
	t.Cleanup(func() { quietFlag, errorFormat = oldQuiet, oldFormat })
	quietFlag, errorFormat = true, ""
	output := t.TempDir()
	if err := os.Mkdir(filepath.Join(output, "analyze"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(output, "analyze", "week1.json"), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(output, "ledger.jsonl"), 0o700); err != nil {
		t.Fatal(err)
	}
	manifest := workflow.Manifest{
		WorkflowName: "warning-test",
		Status:       "success",
		Steps: []workflow.StepRecord{{
			Kind: "analyze", Name: "week1", Status: "success", Execution: "executed", Artifacts: []string{"analyze/week1.json"},
		}},
	}
	var stderr bytes.Buffer
	if err := finalizeWorkflow(output, &manifest, time.Now(), &stderr, nil); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stderr.String(), "failed to append ledger") {
		t.Errorf("ledger warning lost: %q", stderr.String())
	}
	if strings.Contains(stderr.String(), "manifest written") || strings.Contains(stderr.String(), "index written") {
		t.Errorf("quiet must suppress ordinary progress: %q", stderr.String())
	}
}
