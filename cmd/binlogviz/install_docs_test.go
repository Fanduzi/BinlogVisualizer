// input: repository install.sh, README.md, README_ZH.md, and docs/landing/index.html.
// output: guards that install instructions, including the manual tarball snippets, follow the latest release instead of a pinned tag (#281, #285).
// pos: release-doc drift guard; the install path must never again silently install an old release.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func repoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func TestInstallScriptDefaultsToLatestRelease(t *testing.T) {
	if _, err := exec.LookPath("curl"); err != nil {
		t.Skip("curl not available")
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/releases/latest" {
			http.Redirect(w, r, "/releases/tag/v9.8.7", http.StatusFound)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	for _, args := range [][]string{{"--dry-run"}, {"--version", "latest", "--dry-run"}} {
		cmd := exec.Command("sh", append([]string{filepath.Join(repoRoot(t), "install.sh")}, args...)...)
		cmd.Env = append(os.Environ(), "BINLOGVIZ_LATEST_URL="+srv.URL+"/releases/latest")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("install.sh %v: %v\n%s", args, err, out)
		}
		if !strings.Contains(string(out), "version=v9.8.7\n") {
			t.Fatalf("install.sh %v did not resolve latest release:\n%s", args, out)
		}
	}
}

func TestInstallDocsDoNotPinAReleaseTag(t *testing.T) {
	pinnedScript := regexp.MustCompile(`BinlogVisualizer/v\d+\.\d+\.\d+/install\.sh`)
	pinnedRun := regexp.MustCompile(`install\.sh(</span>)?\s+(<span[^>]*>)?--version(</span>)?\s+(<span[^>]*>)?v\d`)
	pinnedTarball := regexp.MustCompile(`releases/download/v\d+\.\d+\.\d+/`)
	for _, name := range []string{"README.md", "README_ZH.md", filepath.Join("docs", "landing", "index.html")} {
		body, err := os.ReadFile(filepath.Join(repoRoot(t), name))
		if err != nil {
			t.Fatal(err)
		}
		for _, line := range strings.Split(string(body), "\n") {
			if pinnedScript.MatchString(line) {
				t.Errorf("%s fetches install.sh from a pinned tag; use main: %s", name, strings.TrimSpace(line))
			}
			code, _, _ := strings.Cut(line, "#")
			if pinnedRun.MatchString(code) {
				t.Errorf("%s runs install.sh with a pinned --version; default to latest: %s", name, strings.TrimSpace(line))
			}
			if pinnedTarball.MatchString(line) {
				t.Errorf("%s downloads a tarball from a pinned tag; resolve releases/latest first (#285): %s", name, strings.TrimSpace(line))
			}
		}
	}
}
