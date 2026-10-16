// input: real workflow runs with a forged or restored manifest snapshot_dir, a mismatched ledger step, blocked trash dirs, and an unwritable trash.
// output: clean takes the snapshot dir only from --snapshot-dir or the default (6 snapshot_dir_mismatch otherwise), export writes snapshot_dir relative, a snapshot entry must name its own step, trash targets are preflighted on both sides, and exit-8 says why.
// pos: ADR 0009 §8 guard for QA r2 on #290 (P2, P3).
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"archive/zip"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"binlogviz/internal/clierr"
)

func setManifestField(t *testing.T, outputDir, key string, value any) {
	t.Helper()
	p := filepath.Join(outputDir, "manifest.json")
	raw, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	m[key] = value
	out, _ := json.MarshalIndent(m, "", "  ")
	if err := os.WriteFile(p, out, 0o644); err != nil {
		t.Fatal(err)
	}
}

func dirSHAs(t *testing.T, dir string) map[string]string {
	t.Helper()
	out := map[string]string{}
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if e.Type().IsRegular() {
			out[e.Name()] = sha(t, filepath.Join(dir, e.Name()))
		}
	}
	return out
}

func sameSHAs(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

// P2: a forged manifest snapshot_dir must not redirect clean.
func TestCleanRefusesForgedManifestSnapshotDir(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	t.Setenv("HOME", t.TempDir())
	outputDir, _, snapshotDir := goodRun(t)
	other := t.TempDir()
	victim := filepath.Join(other, "victim.json")
	if err := os.WriteFile(victim, []byte("precious"), 0o644); err != nil {
		t.Fatal(err)
	}
	setManifestField(t, outputDir, "snapshot_dir", other)
	appendLedgerLine(t, outputDir, map[string]string{"path": "victim.json", "kind": "snapshot", "step": "analyze:victim", "run_id": "f", "sha256": sha(t, victim), "written_at": "x"})
	for _, args := range [][]string{
		{outputDir, "--apply", "--include-snapshots", "--snapshot-dir", snapshotDir},
		{outputDir, "--apply", "--include-snapshots", "--snapshot-dir", snapshotDir, "--force"},
		{outputDir, "--apply", "--include-snapshots"}, // default dir
	} {
		assertKind(t, cleanQuiet(t, args...), clierr.KindRefused, "snapshot_dir_mismatch")
	}
	mustExist(t, victim)
}

func TestCleanIncludeSnapshotsUsesSnapshotDirFlag(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	t.Setenv("HOME", t.TempDir())
	outputDir, _, snapshotDir := goodRun(t)
	assertKind(t, cleanQuiet(t, outputDir, "--include-snapshots"), clierr.KindRefused, "snapshot_dir_mismatch")
	if err := cleanQuiet(t, outputDir, "--include-snapshots", "--snapshot-dir", snapshotDir); err != nil {
		t.Fatalf("matching --snapshot-dir: %v", err)
	}
	// Without --include-snapshots the snapshot dir is not used.
	if err := cleanQuiet(t, outputDir); err != nil {
		t.Fatalf("clean without snapshots: %v", err)
	}
}

// P2: a restored export must never reach the original snapshot dir.
func TestExportWritesRelativeSnapshotDir(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	t.Setenv("HOME", t.TempDir())
	outputDir, snapshotDir := orphanRun(t)
	archive := filepath.Join(t.TempDir(), "wf.zip")
	if _, _, err := captureStdoutStderrRun(t, func() error {
		return execRoot(t, "workflow", "export", outputDir, "--include-snapshots", "--output", archive)
	}); err != nil {
		t.Fatalf("export: %v", err)
	}
	restore := t.TempDir()
	zr, err := zip.OpenReader(archive)
	if err != nil {
		t.Fatal(err)
	}
	root := ""
	for _, f := range zr.File {
		dst := filepath.Join(restore, filepath.FromSlash(f.Name))
		if root == "" {
			root = filepath.Join(restore, strings.SplitN(f.Name, "/", 2)[0])
		}
		_ = os.MkdirAll(filepath.Dir(dst), 0o755)
		rc, _ := f.Open()
		w, _ := os.Create(dst)
		_, _ = io.Copy(w, rc)
		_ = w.Close()
		_ = rc.Close()
	}
	_ = zr.Close()
	var m struct {
		SnapshotDir string `json:"snapshot_dir"`
	}
	raw, _ := os.ReadFile(filepath.Join(root, "manifest.json"))
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	if m.SnapshotDir != "snapshots" {
		t.Fatalf("exported snapshot_dir = %q, want relative \"snapshots\"", m.SnapshotDir)
	}
	before := dirSHAs(t, snapshotDir)
	assertKind(t, cleanQuiet(t, root, "--apply", "--include-snapshots"), clierr.KindRefused, "snapshot_dir_mismatch")
	assertKind(t, cleanQuiet(t, root, "--apply", "--include-snapshots", "--snapshot-dir", snapshotDir), clierr.KindRefused, "snapshot_dir_mismatch")
	if err := cleanQuiet(t, root, "--apply", "--include-snapshots", "--snapshot-dir", filepath.Join(root, "snapshots")); err != nil {
		t.Fatalf("clean restored copy with its own snapshot dir: %v", err)
	}
	if !sameSHAs(before, dirSHAs(t, snapshotDir)) {
		t.Fatalf("clean of a restored export changed the original snapshot dir")
	}
}

// The snapshot file name must be the recording step's snapshot_name.
func TestLedgerSnapshotMustMatchStep(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	outputDir, _, snapshotDir := goodRun(t)
	appendLedgerLine(t, outputDir, map[string]string{"path": "week1.json", "kind": "snapshot", "step": "analyze:week2", "run_id": "f", "sha256": sha(t, filepath.Join(snapshotDir, "week1.json")), "written_at": "x"})
	assertKind(t, cleanQuiet(t, outputDir, "--include-snapshots", "--snapshot-dir", snapshotDir), clierr.KindCorrupt, "untrusted_ledger")
}

func orphanRun(t *testing.T) (outputDir, snapshotDir string) {
	t.Helper()
	outputDir, planPath, snapshotDir := goodRun(t)
	if err := os.Remove(filepath.Join(snapshotDir, "week1.json")); err != nil {
		t.Fatal(err)
	}
	if err := ledgerRun(t, renamedPlan(t, planPath), outputDir, snapshotDir); err != nil {
		t.Fatalf("re-run: %v", err)
	}
	return outputDir, snapshotDir
}

// P3: a refusal on either side leaves nothing moved.
func TestCleanPreflightsBothSides(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	outside := t.TempDir()
	for name, block := range map[string]func(outputDir, snapshotDir string){
		"snapshot .trash is a file":    func(_, s string) { _ = os.WriteFile(filepath.Join(s, ".trash"), nil, 0o644) },
		"snapshot .trash is a symlink": func(_, s string) { _ = os.Symlink(outside, filepath.Join(s, ".trash")) },
		"root .trash is a symlink":     func(o, _ string) { _ = os.Symlink(outside, filepath.Join(o, ".trash")) },
		"root .trash is a file":        func(o, _ string) { _ = os.WriteFile(filepath.Join(o, ".trash"), nil, 0o644) },
	} {
		t.Run(name, func(t *testing.T) {
			outputDir, snapshotDir := orphanRun(t)
			block(outputDir, snapshotDir)
			err := cleanQuiet(t, outputDir, "--apply", "--include-snapshots", "--snapshot-dir", snapshotDir)
			if err == nil {
				t.Fatal("expected a refusal")
			}
			mustExist(t, filepath.Join(outputDir, "analyze", "week2.json"), filepath.Join(snapshotDir, "week2.json"))
			if entries, _ := os.ReadDir(outside); len(entries) != 0 {
				t.Fatalf("files moved outside: %v", entries)
			}
		})
	}
}

// P3: exit 8 says why.
func TestCleanOutputErrorSaysWhy(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	forceEnglishRuntimeOutput(t)
	outputDir, snapshotDir := orphanRun(t)
	trash := filepath.Join(outputDir, ".trash")
	if err := os.Mkdir(trash, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(trash, 0o755) })
	err := cleanQuiet(t, outputDir, "--apply", "--include-snapshots", "--snapshot-dir", snapshotDir)
	assertKind(t, err, clierr.KindOutput, "")
	if !strings.Contains(err.Error(), "permission denied") || !strings.Contains(err.Error(), ".trash") {
		t.Fatalf("exit-8 message must name the trash path and the cause: %v", err)
	}
	mustExist(t, filepath.Join(outputDir, "analyze", "week2.json"), filepath.Join(snapshotDir, "week2.json"))
}
