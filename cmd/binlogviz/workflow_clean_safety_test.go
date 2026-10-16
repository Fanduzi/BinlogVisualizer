// input: real workflow runs plus forged ledgers, symlinked kind dirs and files, a held lock, and an export archive.
// output: clean never deletes outside <root>/{analyze,compare,trend} or directly inside the snapshot dir; deletions are moves into .trash; run/clean/resume are serialized by <root>/.lock; export carries ledger.jsonl.
// pos: ADR 0009 §8 safety guard (QA r5 P1-A, P1-B on #290).
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"archive/zip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"binlogviz/internal/clierr"
	"binlogviz/internal/workflow"
)

func sha(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

func appendLedgerLine(t *testing.T, outputDir string, entry map[string]string) {
	t.Helper()
	line, _ := json.Marshal(entry)
	f, err := os.OpenFile(filepath.Join(outputDir, "ledger.jsonl"), os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.Write(append(line, '\n')); err != nil {
		t.Fatal(err)
	}
}

func goodRun(t *testing.T) (outputDir, planPath, snapshotDir string) {
	t.Helper()
	_, outputDir, planPath, snapshotDir = setupWorkflowTestWithSnapshots(t, "basic-plan.yaml")
	if err := ledgerRun(t, planPath, outputDir, snapshotDir); err != nil {
		t.Fatalf("run: %v", err)
	}
	return outputDir, planPath, snapshotDir
}

// P1-A: a forged snapshot entry must never reach a file outside the
// snapshot dir, whatever its path form.
func TestCleanRejectsForgedSnapshotLedgerEntries(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	for name, form := range map[string]func(snapDir, victim string) string{
		"absolute":        func(_, victim string) string { return victim },
		"dotdot":          func(snapDir, victim string) string { return snapDir + "/../outside/victim.json" },
		"relative dotdot": func(_, _ string) string { return "../outside/victim.json" },
	} {
		t.Run(name, func(t *testing.T) {
			outputDir, _, snapshotDir := goodRun(t)
			outside := filepath.Join(filepath.Dir(snapshotDir), "outside")
			if err := os.MkdirAll(outside, 0o755); err != nil {
				t.Fatal(err)
			}
			victim := filepath.Join(outside, "victim.json")
			if err := os.WriteFile(victim, []byte("precious"), 0o644); err != nil {
				t.Fatal(err)
			}
			appendLedgerLine(t, outputDir, map[string]string{"path": form(snapshotDir, victim), "kind": "snapshot", "step": "analyze:x", "run_id": "f", "sha256": sha(t, victim), "written_at": "x"})
			for _, args := range [][]string{{outputDir, "--include-snapshots", "--apply"}, {outputDir, "--include-snapshots", "--apply", "--force"}} {
				assertKind(t, cleanQuiet(t, args...), clierr.KindCorrupt, "untrusted_ledger")
			}
			mustExist(t, victim)
		})
	}
}

// P1-B: a kind dir that is a symlink is refused before anything is deleted,
// with or without --force, and on the no_ledger --force path.
func TestCleanRefusesSymlinkedKindDir(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	for _, kind := range []string{"analyze", "compare", "trend"} {
		for _, mode := range []string{"ledger", "force", "no_ledger_force"} {
			t.Run(kind+"/"+mode, func(t *testing.T) {
				outputDir, _, _ := goodRun(t)
				other := filepath.Join(t.TempDir(), "other")
				if err := os.Rename(filepath.Join(outputDir, kind), other); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(other, "x.json"), []byte("{}"), 0o644); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(other, filepath.Join(outputDir, kind)); err != nil {
					t.Fatal(err)
				}
				before, _ := os.ReadDir(other)
				args := []string{outputDir, "--apply", "--include-snapshots"}
				if mode != "ledger" {
					args = append(args, "--force")
				}
				if mode == "no_ledger_force" {
					if err := os.Remove(filepath.Join(outputDir, "ledger.jsonl")); err != nil {
						t.Fatal(err)
					}
				}
				assertKind(t, cleanQuiet(t, args...), clierr.KindRefused, "trust_boundary")
				after, _ := os.ReadDir(other)
				if len(after) != len(before) {
					t.Fatalf("clean touched %s through the symlink: %d -> %d files", other, len(before), len(after))
				}
			})
		}
	}
}

// A symlinked file inside a kind dir is never a candidate: neither the link
// nor its target is touched, even with a forged ledger entry and --force.
func TestCleanKeepsSymlinkedFileInKindDir(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	outputDir, _, _ := goodRun(t)
	target := filepath.Join(t.TempDir(), "target.json")
	if err := os.WriteFile(target, []byte("precious"), 0o644); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(outputDir, "analyze", "link.json")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	appendLedgerLine(t, outputDir, map[string]string{"path": "analyze/link.json", "kind": "artifact", "step": "analyze:link", "run_id": "f", "sha256": sha(t, target), "written_at": "x"})
	if err := cleanQuiet(t, outputDir, "--apply", "--force"); err != nil {
		t.Fatalf("clean: %v", err)
	}
	if fi, err := os.Lstat(link); err != nil || fi.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("symlink must be left alone: %v", err)
	}
	mustExist(t, target)
}

// Deletions are moves into <root>/.trash/<id>/ (and <snapshot-dir>/.trash/<id>/),
// and the output reports where.
func TestCleanMovesToTrash(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	outputDir, planPath, snapshotDir := goodRun(t)
	if err := os.Remove(filepath.Join(snapshotDir, "week1.json")); err != nil {
		t.Fatal(err)
	}
	if err := ledgerRun(t, renamedPlan(t, planPath), outputDir, snapshotDir); err != nil {
		t.Fatalf("renamed re-run: %v", err)
	}
	stdout, _, err := captureStdoutStderrRun(t, func() error {
		cmd := NewRootCommand()
		cmd.SetArgs([]string{"workflow", "clean", outputDir, "--apply", "--include-snapshots", "--snapshot-dir", snapshotDir, "--format", "json"})
		return cmd.Execute()
	})
	if err != nil {
		t.Fatalf("clean: %v", err)
	}
	var out struct {
		TrashDir         string   `json:"trash_dir"`
		SnapshotTrashDir string   `json:"snapshot_trash_dir"`
		Deleted          []string `json:"deleted"`
	}
	if err := json.Unmarshal([]byte(stdout), &out); err != nil {
		t.Fatalf("%v\n%s", err, stdout)
	}
	if !strings.HasPrefix(out.TrashDir, filepath.Join(outputDir, ".trash")+string(filepath.Separator)) {
		t.Fatalf("trash_dir %q must be under %s/.trash", out.TrashDir, outputDir)
	}
	mustBeGone(t, filepath.Join(outputDir, "analyze", "week2.json"), filepath.Join(snapshotDir, "week2.json"))
	mustExist(t, filepath.Join(out.TrashDir, "analyze", "week2.json"), filepath.Join(out.SnapshotTrashDir, "week2.json"))
	if !strings.HasPrefix(out.SnapshotTrashDir, filepath.Join(snapshotDir, ".trash")) {
		t.Fatalf("snapshot_trash_dir %q must be under the snapshot dir", out.SnapshotTrashDir)
	}
}

func TestWorkflowCommandsAreLocked(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	outputDir, planPath, snapshotDir := goodRun(t)
	unlock, err := workflow.LockDir(outputDir)
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	assertKind(t, cleanQuiet(t, outputDir, "--apply"), clierr.KindRefused, "locked")
	assertKind(t, ledgerRun(t, planPath, outputDir, snapshotDir), clierr.KindRefused, "locked")
	_, _, rerr := captureStdoutStderrRun(t, func() error { return execRoot(t, "workflow", "resume", outputDir) })
	assertKind(t, rerr, clierr.KindRefused, "locked")
	unlock()
	if err := cleanQuiet(t, outputDir); err != nil {
		t.Fatalf("clean after unlock: %v", err)
	}
}

func TestExportRoundTripKeepsLedger(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	outputDir, _, _ := goodRun(t)
	archive := filepath.Join(t.TempDir(), "wf.zip")
	if _, _, err := captureStdoutStderrRun(t, func() error {
		return execRoot(t, "workflow", "export", outputDir, "--output", archive)
	}); err != nil {
		t.Fatalf("export: %v", err)
	}
	restore := t.TempDir()
	zr, err := zip.OpenReader(archive)
	if err != nil {
		t.Fatal(err)
	}
	defer zr.Close()
	root := ""
	for _, f := range zr.File {
		dst := filepath.Join(restore, filepath.FromSlash(f.Name))
		if root == "" {
			root = filepath.Join(restore, strings.SplitN(f.Name, "/", 2)[0])
		}
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			t.Fatal(err)
		}
		rc, _ := f.Open()
		w, _ := os.Create(dst)
		_, _ = io.Copy(w, rc)
		_ = w.Close()
		_ = rc.Close()
	}
	mustExist(t, filepath.Join(root, "ledger.jsonl"))
	if err := cleanQuiet(t, root); err != nil {
		t.Fatalf("clean on a restored export must not be no_ledger: %v", err)
	}
}
