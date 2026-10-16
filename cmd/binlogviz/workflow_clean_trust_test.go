// input: workflow output dirs with real artifacts and untrustworthy manifests.
// output: P1 guard: workflow clean (preview and --apply) refuses an untrusted manifest and deletes nothing; export rejects a {} manifest and reports a missing plan on stderr and as plan_included:false.
// pos: ADR 0009 / workflow clean trust rule (#287 follow-up of #277).
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"binlogviz/internal/clierr"
)

var cleanTrustArtifacts = []string{
	"analyze/week1.json", "analyze/week2.json",
	"compare/w.json", "compare/w.html",
	"trend/t.json", "trend/t.html",
}

func makeArtifactDir(t *testing.T, manifest string) string {
	t.Helper()
	dir := t.TempDir()
	for _, rel := range cleanTrustArtifacts {
		p := filepath.Join(dir, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("{}"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	writeManifest(t, dir, manifest)
	return dir
}

func remainingArtifacts(t *testing.T, dir string) []string {
	t.Helper()
	var left []string
	for _, rel := range cleanTrustArtifacts {
		if _, err := os.Stat(filepath.Join(dir, rel)); err == nil {
			left = append(left, rel)
		}
	}
	sort.Strings(left)
	return left
}

func TestWorkflowCleanRefusesUntrustedManifest(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	cases := []struct {
		name, manifest string
		kind           clierr.Kind
	}{
		{"empty object", `{}`, clierr.KindCorrupt},
		{"missing steps", `{"manifest_version":2,"workflow_name":"w","status":"success"}`, clierr.KindRefused},
		{"empty steps", `{"manifest_version":2,"workflow_name":"w","status":"success","steps":[]}`, clierr.KindRefused},
	}
	for _, tc := range cases {
		for _, apply := range []bool{false, true} {
			name := tc.name + "/preview"
			if apply {
				name = tc.name + "/apply"
			}
			t.Run(name, func(t *testing.T) {
				dir := makeArtifactDir(t, tc.manifest)
				args := []string{"workflow", "clean", dir, "--format", "json"}
				if apply {
					args = append(args, "--apply")
				}
				assertKind(t, execRoot(t, args...), tc.kind, "untrusted_manifest")
				if left := remainingArtifacts(t, dir); len(left) != len(cleanTrustArtifacts) {
					t.Fatalf("clean deleted artifacts from an untrusted manifest; left %v", left)
				}
			})
		}
	}
}

// Files of a kind the manifest never recorded cannot be attributed to a
// step, so clean leaves them alone even with a trusted manifest.
func TestWorkflowCleanOnlyDeletesAttributableKinds(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	dir := makeArtifactDir(t, `{"manifest_version":2,"workflow_name":"w","status":"success","steps":[
		{"kind":"analyze","name":"week1","status":"success","artifacts":["analyze/week1.json"]}]}`)
	// No ledger here: --force is the legacy-dir path, and it still only
	// deletes outputs of kinds the manifest recorded.
	if err := execRoot(t, "workflow", "clean", dir, "--apply", "--force"); err != nil {
		t.Fatalf("clean: %v", err)
	}
	want := []string{"analyze/week1.json", "compare/w.html", "compare/w.json", "trend/t.html", "trend/t.json"}
	if got := remainingArtifacts(t, dir); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("remaining %v, want %v (only the stale analyze output is attributable)", got, want)
	}
}

func TestWorkflowExportRejectsEmptyManifest(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	dir := makeArtifactDir(t, `{}`)
	err := execRoot(t, "workflow", "export", dir, "--output", filepath.Join(t.TempDir(), "x.zip"))
	assertKind(t, err, clierr.KindCorrupt, "untrusted_manifest")
}

func TestWorkflowExportReportsMissingPlan(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	outside := filepath.Join(t.TempDir(), "plan.yaml")
	if err := os.WriteFile(outside, []byte(validPlan277), 0o644); err != nil {
		t.Fatal(err)
	}
	dir := makeArtifactDir(t, `{"manifest_version":2,"workflow_name":"w","status":"success","plan_path":"`+outside+`","plan_sha256":"abc","steps":[
		{"kind":"analyze","name":"week1","status":"success","artifacts":["analyze/week1.json"]}]}`)
	stdout, stderr, err := captureStdoutStderrRun(t, func() error {
		cmd := NewRootCommand()
		cmd.SetArgs([]string{"workflow", "export", dir, "--format", "json", "--output", filepath.Join(t.TempDir(), "x.zip")})
		return cmd.Execute()
	})
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if !strings.Contains(stderr, "plan.yaml") {
		t.Fatalf("stderr must warn that plan.yaml was not included, got %q", stderr)
	}
	var out struct {
		PlanIncluded *bool `json:"plan_included"`
	}
	if err := json.Unmarshal([]byte(stdout), &out); err != nil {
		t.Fatalf("%v\n%s", err, stdout)
	}
	if out.PlanIncluded == nil || *out.PlanIncluded {
		t.Fatalf("plan_included must be false: %s", stdout)
	}
}
