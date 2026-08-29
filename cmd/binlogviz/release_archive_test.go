// Package binlogviz tests the GitHub release archive packing contract.
// input: scripts/pack_release_archive.sh and the extra files it copies from the repository.
// output: a tar.gz listing that includes the binary, the 1500-byte sample ROW binlog, and a workflow plan whose from_dir exists inside the archive.
// pos: operator-facing packing check so a downloaded GitHub Release tarball is not binary-only.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestReleaseArchiveListsBinarySampleAndWorkflowPlan(t *testing.T) {
	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("repo root: %v", err)
	}
	packer := filepath.Join(repoRoot, "scripts", "pack_release_archive.sh")

	tmp := t.TempDir()
	dummyBin := filepath.Join(tmp, "binlogviz")
	if err := os.WriteFile(dummyBin, []byte("not-a-real-binary"), 0o755); err != nil {
		t.Fatalf("write dummy binary: %v", err)
	}

	archive := filepath.Join(tmp, "binlogviz_0.0.0_linux_amd64.tar.gz")
	pack := exec.Command("bash", packer, dummyBin, archive)
	pack.Dir = repoRoot
	if out, err := pack.CombinedOutput(); err != nil {
		t.Fatalf("pack_release_archive.sh: %v\n%s", err, out)
	}

	list := exec.Command("tar", "-tzf", archive)
	listed, err := list.Output()
	if err != nil {
		t.Fatalf("tar -tzf: %v", err)
	}
	listing := string(listed)

	required := []string{
		"binlogviz",
		"testdata/minimal.binlog",
		"testdata/sample-binlog/mysql-bin.000001",
		"incident.yaml",
	}
	for _, name := range required {
		if !tarListingContains(listing, name) {
			t.Fatalf("release archive must list %q so extract-and-run works without a git clone; got:\n%s", name, listing)
		}
	}

	extractDir := filepath.Join(tmp, "extract")
	if err := os.MkdirAll(extractDir, 0o755); err != nil {
		t.Fatalf("mkdir extract: %v", err)
	}
	untar := exec.Command("tar", "-xzf", archive, "-C", extractDir)
	if out, err := untar.CombinedOutput(); err != nil {
		t.Fatalf("tar -xzf: %v\n%s", err, out)
	}

	planPath := filepath.Join(extractDir, "incident.yaml")
	planBytes, err := os.ReadFile(planPath)
	if err != nil {
		t.Fatalf("read bundled incident.yaml: %v", err)
	}
	fromDir := bundledPlanFromDir(t, string(planBytes))
	if fromDir == "" || strings.HasPrefix(fromDir, "cmd/") || filepath.IsAbs(fromDir) {
		t.Fatalf("bundled plan from_dir must be archive-relative, got %q", fromDir)
	}

	info, err := os.Stat(filepath.Join(extractDir, fromDir))
	if err != nil {
		t.Fatalf("bundled plan from_dir %q must exist after extract: %v", fromDir, err)
	}
	if !info.IsDir() {
		t.Fatalf("bundled plan from_dir %q must be a directory", fromDir)
	}

	sample := filepath.Join(extractDir, "testdata", "minimal.binlog")
	st, err := os.Stat(sample)
	if err != nil {
		t.Fatalf("bundled sample binlog: %v", err)
	}
	if st.Size() != 1500 {
		t.Fatalf("bundled sample must be the 1500-byte ROW fixture, got %d bytes", st.Size())
	}
}

func tarListingContains(listing, name string) bool {
	for _, line := range strings.Split(listing, "\n") {
		if strings.TrimRight(strings.TrimSpace(line), "/") == name {
			return true
		}
	}
	return false
}

func bundledPlanFromDir(t *testing.T, plan string) string {
	t.Helper()
	for _, line := range strings.Split(plan, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "from_dir:") {
			return strings.TrimSpace(strings.TrimPrefix(trimmed, "from_dir:"))
		}
	}
	t.Fatal("bundled incident.yaml has no from_dir")
	return ""
}
