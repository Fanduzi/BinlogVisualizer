// input: a real workflow run whose compare or trend JSON artifact is rewritten to a schema_version this build does not know.
// output: P2 guard (#282, ADR 0010): workflow status, resume and clean refuse with exit 9 (unsupported, details.reason unsupported_artifact_version), print nothing on stdout, and clean deletes nothing; known and legacy versions keep working.
// pos: ADR 0010 consumer rule ("refuse a version it does not know"), the workflow counterpart of report_version handling in compare/trend/snapshot.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"binlogviz/internal/clierr"
)

// runWorkflowForArtifactVersion runs basic-plan.yaml and returns the output
// and snapshot dirs.
func runWorkflowForArtifactVersion(t *testing.T) (outputDir, snapshotDir string) {
	t.Helper()
	_, outputDir, planPath, snapshotDir := setupWorkflowTestWithSnapshots(t, "basic-plan.yaml")
	if err := execRoot(t, "workflow", "run", planPath, "--output-dir", outputDir, "--snapshot-dir", snapshotDir); err != nil {
		t.Fatalf("workflow run: %v", err)
	}
	return outputDir, snapshotDir
}

// setArtifactSchemaVersion rewrites schema_version in one artifact; a nil
// version removes the key (a pre-#282 artifact).
func setArtifactSchemaVersion(t *testing.T, path string, version any) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatal(err)
	}
	if _, ok := doc["schema_version"]; !ok {
		t.Fatalf("%s has no schema_version to rewrite", path)
	}
	if version == nil {
		delete(doc, "schema_version")
	} else {
		doc["schema_version"] = version
	}
	out, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, out, 0o644); err != nil {
		t.Fatal(err)
	}
}

func execRootCapture(t *testing.T, args ...string) (string, error) {
	t.Helper()
	cmd := NewRootCommand()
	cmd.SetArgs(args)
	var stdout strings.Builder
	cmd.SetOut(&stdout)
	cmd.SetErr(&strings.Builder{})
	err := cmd.Execute()
	return stdout.String(), err
}

func TestWorkflowRefusesUnknownArtifactSchemaVersion(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	artifacts := []string{"compare/week2_vs_week1.json", "trend/weekly_series.json"}
	versions := []any{99, 0, "1"}
	for _, rel := range artifacts {
		for _, version := range versions {
			for _, command := range []string{"status", "resume", "clean", "clean-apply"} {
				name := strings.ReplaceAll(rel, "/", "_") + "/" + string(mustJSON(t, version)) + "/" + command
				t.Run(name, func(t *testing.T) {
					outputDir, snapshotDir := runWorkflowForArtifactVersion(t)
					artifact := filepath.Join(outputDir, filepath.FromSlash(rel))
					setArtifactSchemaVersion(t, artifact, version)
					before, err := os.ReadFile(artifact)
					if err != nil {
						t.Fatal(err)
					}
					var args []string
					switch command {
					case "status":
						args = []string{"workflow", "status", outputDir, "--format", "json"}
					case "resume":
						args = []string{"workflow", "resume", outputDir, "--snapshot-dir", snapshotDir}
					case "clean":
						args = []string{"workflow", "clean", outputDir, "--format", "json"}
					case "clean-apply":
						args = []string{"workflow", "clean", outputDir, "--format", "json", "--apply", "--force"}
					}
					stdout, err := execRootCapture(t, args...)
					assertKind(t, err, clierr.KindUnsupported, "unsupported_artifact_version")
					_, details := ErrorKind(err)
					if details["artifact"] != rel {
						t.Fatalf("details.artifact = %v, want %s", details["artifact"], rel)
					}
					if !strings.Contains(err.Error(), "schema_version") || !strings.Contains(err.Error(), rel) {
						t.Fatalf("message should name the artifact and schema_version: %v", err)
					}
					if stdout != "" {
						t.Fatalf("stdout must be empty on refusal, got %q", stdout)
					}
					after, err := os.ReadFile(artifact)
					if err != nil {
						t.Fatalf("artifact gone after refused %s: %v", command, err)
					}
					if string(after) != string(before) {
						t.Fatalf("artifact changed after refused %s", command)
					}
				})
			}
		}
	}
}

func TestWorkflowAcceptsKnownAndLegacyArtifactSchemaVersion(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	for _, version := range []any{1, nil} {
		t.Run(string(mustJSON(t, version)), func(t *testing.T) {
			outputDir, snapshotDir := runWorkflowForArtifactVersion(t)
			for _, rel := range []string{"compare/week2_vs_week1.json", "trend/weekly_series.json"} {
				setArtifactSchemaVersion(t, filepath.Join(outputDir, filepath.FromSlash(rel)), version)
			}
			for _, args := range [][]string{
				{"workflow", "status", outputDir, "--format", "json"},
				{"workflow", "clean", outputDir, "--format", "json"},
				{"workflow", "resume", outputDir, "--snapshot-dir", snapshotDir},
			} {
				if _, err := execRootCapture(t, args...); err != nil {
					t.Fatalf("%v: %v", args, err)
				}
			}
		})
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
