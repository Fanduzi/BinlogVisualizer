// Package binlogviz tests the GitHub release archive packing contract.
// input: scripts/pack_release_archive.sh and the extra files it copies from the repository.
// output: a tar.gz listing that includes the binary, the 1500-byte sample ROW binlog, a workflow plan whose from_dir exists inside the archive, the READMEs, the agent usage guide, and the user docs pages the README links to.
// pos: operator-facing packing check so a downloaded GitHub Release tarball is not binary-only.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
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
		"README.md",
		"README_ZH.md",
		"docs/agents/USING-BINLOGVIZ.md",
		"docs/agents/USING-BINLOGVIZ.zh-CN.md",
		// The README keeps a command and a link; the detail it links to
		// must be in the archive for hosts without internet access.
		"docs/reference/analyze-behavior.md",
		"docs/reference/analyze-behavior.zh-CN.md",
		"docs/recipe/flashback.md",
		"docs/recipe/flashback.zh-CN.md",
		"docs/recipe/compare-trend-workflow.md",
		"docs/recipe/compare-trend-workflow.zh-CN.md",
		"docs/concept/architecture.md",
		"docs/concept/architecture.zh-CN.md",
	}
	for _, name := range required {
		if !tarListingContains(listing, name) {
			t.Fatalf("release archive must list %q so extract-and-run works without a git clone; got:\n%s", name, listing)
		}
	}
	schemas, err := filepath.Glob(filepath.Join(repoRoot, "docs", "schema", "*.json"))
	if err != nil || len(schemas) == 0 {
		t.Fatalf("docs/schema: %v", err)
	}
	for _, path := range schemas {
		name := "docs/schema/" + filepath.Base(path)
		if !tarListingContains(listing, name) {
			t.Fatalf("release archive must list %q; got:\n%s", name, listing)
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

// Archive READMEs are copied as-is. A relative link must name a file the
// release archive ships. The file list is the one .goreleaser.yml and
// scripts/pack_release_archive.sh both define.
func TestReleaseArchiveReadmeLinksStayInside(t *testing.T) {
	root := repoRoot(t)
	shipped, err := releaseArchiveFiles(root)
	if err != nil {
		t.Fatal(err)
	}
	linkRe := regexp.MustCompile(`\]\(([^)]*)\)`)
	for _, name := range []string{"README.md", "README_ZH.md"} {
		body, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			t.Fatal(err)
		}
		for _, match := range linkRe.FindAllStringSubmatch(string(body), -1) {
			path, ok := relativeReadmeLink(match[1])
			if !ok {
				continue
			}
			if _, ship := shipped[path]; !ship {
				t.Errorf("%s links to %q, which the release archive does not ship", name, match[1])
			}
		}
	}
}

func releaseArchiveFiles(root string) (map[string]struct{}, error) {
	goreleaser, err := os.ReadFile(filepath.Join(root, ".goreleaser.yml"))
	if err != nil {
		return nil, err
	}
	packer, err := os.ReadFile(filepath.Join(root, "scripts", "pack_release_archive.sh"))
	if err != nil {
		return nil, err
	}
	fromYAML := goreleaserArchiveFiles(string(goreleaser))
	fromPack := packScriptArchiveFiles(string(packer))
	binary := goreleaserBinary(string(goreleaser))
	want := append([]string{}, fromYAML...)
	if binary != "" {
		want = append(want, binary)
	}
	sort.Strings(want)
	sort.Strings(fromPack)
	if strings.Join(want, "\n") != strings.Join(fromPack, "\n") {
		return nil, errArchiveList(fromYAML, binary, fromPack)
	}
	set := map[string]struct{}{}
	for _, name := range want {
		set[name] = struct{}{}
	}
	return set, nil
}

type archiveListError struct {
	msg string
}

func (e archiveListError) Error() string { return e.msg }

func errArchiveList(files []string, binary string, packed []string) error {
	return archiveListError{msg: "goreleaser files " + strings.Join(files, ", ") + " plus binary " + binary + " != pack script " + strings.Join(packed, ", ")}
}

func goreleaserBinary(yml string) string {
	for _, line := range strings.Split(yml, "\n") {
		trim := strings.TrimSpace(line)
		if strings.HasPrefix(trim, "binary:") {
			return strings.TrimSpace(strings.TrimPrefix(trim, "binary:"))
		}
	}
	return ""
}

func goreleaserArchiveFiles(yml string) []string {
	var files []string
	in := false
	base := 0
	for _, line := range strings.Split(yml, "\n") {
		if !in {
			if strings.TrimSpace(line) == "files:" {
				in = true
				base = len(line) - len(strings.TrimLeft(line, " "))
			}
			continue
		}
		if strings.TrimSpace(line) == "" {
			continue
		}
		indent := len(line) - len(strings.TrimLeft(line, " "))
		if indent <= base {
			break
		}
		trim := strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(trim, "dst:"):
			files = append(files, strings.TrimSpace(strings.TrimPrefix(trim, "dst:")))
		case strings.HasPrefix(trim, "- ") && !strings.Contains(strings.TrimPrefix(trim, "- "), ":"):
			files = append(files, strings.TrimSpace(strings.TrimPrefix(trim, "- ")))
		}
	}
	return files
}

func packScriptArchiveFiles(script string) []string {
	re := regexp.MustCompile(`\$\{staging\}/(\S+)`)
	var files []string
	seen := map[string]struct{}{}
	for _, line := range strings.Split(script, "\n") {
		trim := strings.TrimSpace(line)
		if !strings.HasPrefix(trim, "cp ") {
			continue
		}
		for _, match := range re.FindAllStringSubmatch(trim, -1) {
			name := strings.Trim(match[1], `"'`)
			if _, ok := seen[name]; ok {
				continue
			}
			seen[name] = struct{}{}
			files = append(files, name)
		}
	}
	return files
}

func relativeReadmeLink(dest string) (string, bool) {
	dest = strings.TrimSpace(dest)
	if dest == "" || strings.HasPrefix(dest, "#") || strings.Contains(dest, "://") || strings.HasPrefix(dest, "mailto:") {
		return "", false
	}
	if i := strings.IndexAny(dest, " \t"); i >= 0 {
		dest = dest[:i]
	}
	if i := strings.IndexByte(dest, '#'); i >= 0 {
		dest = dest[:i]
	}
	if i := strings.IndexByte(dest, '?'); i >= 0 {
		dest = dest[:i]
	}
	dest = strings.TrimPrefix(dest, "./")
	if dest == "" || strings.HasPrefix(dest, "/") || strings.Contains(dest, "..") {
		return dest, dest != ""
	}
	return strings.TrimSuffix(dest, "/"), dest != ""
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
