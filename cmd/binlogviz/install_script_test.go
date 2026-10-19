// input: repository install.sh run under sh against a local httptest release mirror.
// output: guards that install.sh fails loudly and closed on bad downloads, bad checksums, bad flags, and missing tools (#285).
// pos: installer behavior tests; the install path must never exit silently or install an unverified binary.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

type installMirror struct {
	srv      *httptest.Server
	version  string
	archive  string
	payload  []byte // archive bytes served
	checksum string // checksums.txt body served
}

func fakeReleaseArchive(t *testing.T, script string) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	if err := tw.WriteHeader(&tar.Header{Name: "binlogviz", Mode: 0o755, Size: int64(len(script))}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write([]byte(script)); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func newInstallMirror(t *testing.T, version string) *installMirror {
	t.Helper()
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("install.sh supports linux and darwin only")
	}
	if _, err := exec.LookPath("curl"); err != nil {
		t.Skip("curl not available")
	}
	m := &installMirror{version: version}
	stripped := strings.TrimPrefix(version, "v")
	m.archive = fmt.Sprintf("binlogviz_%s_%s_%s.tar.gz", stripped, runtime.GOOS, runtime.GOARCH)
	m.payload = fakeReleaseArchive(t, "#!/bin/sh\necho \"binlogviz "+version+"\"\n")
	sum := sha256.Sum256(m.payload)
	m.checksum = hex.EncodeToString(sum[:]) + "  " + m.archive + "\n"
	m.serve()
	t.Cleanup(func() { m.srv.Close() })
	return m
}

// serve snapshots the archive the handler will return. Tests that change
// payload or checksum close the server, edit, and serve again, so the
// handler does not read bytes the test is still writing.
func (m *installMirror) serve() {
	payload := append([]byte(nil), m.payload...)
	checksum := m.checksum
	archive := m.archive
	version := m.version
	stripped := strings.TrimPrefix(version, "v")
	m.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/download/" + version + "/" + archive:
			_, _ = w.Write(payload)
		case "/download/" + version + "/binlogviz_" + stripped + "_checksums.txt":
			_, _ = w.Write([]byte(checksum))
		default:
			http.NotFound(w, r)
		}
	}))
}

func (m *installMirror) run(t *testing.T, path string, args ...string) (string, error) {
	t.Helper()
	cmd := exec.Command("sh", append([]string{filepath.Join(repoRoot(t), "install.sh")}, args...)...)
	env := []string{"BINLOGVIZ_DOWNLOAD_BASE=" + m.srv.URL + "/download", "HOME=" + t.TempDir()}
	if path == "" {
		path = os.Getenv("PATH")
	}
	cmd.Env = append(env, "PATH="+path)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func TestInstallScriptInstallsVerifiedArchive(t *testing.T) {
	m := newInstallMirror(t, "v9.8.7")
	bin := t.TempDir()
	out, err := m.run(t, "", "--version", "9.8.7", "--bin-dir", bin)
	if err != nil {
		t.Fatalf("install failed: %v\n%s", err, out)
	}
	got, err := exec.Command(filepath.Join(bin, "binlogviz")).Output()
	if err != nil || strings.TrimSpace(string(got)) != "binlogviz v9.8.7" {
		t.Fatalf("installed binary not the served one: %q %v\n%s", got, err, out)
	}
	// A second install over the first names what it replaces.
	out, err = m.run(t, "", "--version", "v9.8.7", "--bin-dir", bin)
	if err != nil {
		t.Fatalf("reinstall failed: %v\n%s", err, out)
	}
	if !strings.Contains(out, "replacing existing "+filepath.Join(bin, "binlogviz")+" (binlogviz v9.8.7) with v9.8.7") {
		t.Fatalf("reinstall did not announce the replacement:\n%s", out)
	}
}

func TestInstallScriptRefusesBadDownloads(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(m *installMirror)
		args   []string
		want   string
	}{
		{
			name:   "archive with one byte appended",
			mutate: func(m *installMirror) { m.payload = append(m.payload, 'x') },
			want:   "checksum mismatch for binlogviz_9.8.7_",
		},
		{
			name:   "archive missing from checksums.txt",
			mutate: func(m *installMirror) { m.checksum = strings.Repeat("0", 64) + "  binlogviz_9.8.7_other_arch.tar.gz\n" },
			want:   "no checksum for binlogviz_9.8.7_",
		},
		{
			name: "release that does not exist",
			args: []string{"--version", "v9.9.9"},
			want: "release v9.9.9 may not exist",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := newInstallMirror(t, "v9.8.7")
			if tc.mutate != nil {
				m.srv.Close()
				tc.mutate(m)
				m.serve()
			}
			bin := t.TempDir()
			args := tc.args
			if args == nil {
				args = []string{"--version", "v9.8.7"}
			}
			out, err := m.run(t, "", append(args, "--bin-dir", bin)...)
			if err == nil {
				t.Fatalf("install succeeded, want failure:\n%s", out)
			}
			if !strings.Contains(out, tc.want) {
				t.Fatalf("missing %q in output:\n%s", tc.want, out)
			}
			if _, statErr := os.Stat(filepath.Join(bin, "binlogviz")); statErr == nil {
				t.Fatalf("a binary was installed despite the failure")
			}
		})
	}
}

func TestInstallScriptRejectsBadFlags(t *testing.T) {
	m := newInstallMirror(t, "v9.8.7")
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"--bin-dir"}, "--bin-dir needs a directory"},
		{[]string{"--version"}, "--version needs a value"},
		{[]string{"--version", "nightly"}, "invalid --version 'nightly'"},
		{[]string{"--frobnicate"}, "unknown argument: --frobnicate"},
	} {
		out, err := m.run(t, "", tc.args...)
		if err == nil || !strings.Contains(out, tc.want) {
			t.Errorf("install.sh %v: err=%v, want %q in:\n%s", tc.args, err, tc.want, out)
		}
	}
	out, err := m.run(t, "", "--version", "0.24.0", "--dry-run")
	if err != nil || !strings.Contains(out, "version=v0.24.0\n") {
		t.Errorf("0.24.0 not normalized to v0.24.0: %v\n%s", err, out)
	}
}

// pathWithout builds a PATH directory holding only the named tools.
func pathWithout(t *testing.T, tools []string) string {
	t.Helper()
	dir := t.TempDir()
	for _, tool := range tools {
		p, err := exec.LookPath(tool)
		if err != nil {
			t.Skipf("%s not available", tool)
		}
		if err := os.Symlink(p, filepath.Join(dir, tool)); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func TestInstallScriptFailsClosedWithoutShaTool(t *testing.T) {
	m := newInstallMirror(t, "v9.8.7")
	path := pathWithout(t, []string{"curl", "uname", "awk", "mktemp", "rm", "mkdir", "tar", "gzip", "install"})
	bin := t.TempDir()
	out, err := m.run(t, path, "--version", "v9.8.7", "--bin-dir", bin)
	if err == nil || !strings.Contains(out, "no sha256 tool found") {
		t.Fatalf("want fail-closed without a sha256 tool: err=%v\n%s", err, out)
	}
	if _, statErr := os.Stat(filepath.Join(bin, "binlogviz")); statErr == nil {
		t.Fatalf("a binary was installed without verification")
	}
	out, err = m.run(t, path, "--version", "v9.8.7", "--bin-dir", bin, "--no-verify")
	if err != nil || !strings.Contains(out, "installing binlogviz_9.8.7_") {
		t.Fatalf("--no-verify should install with a warning: err=%v\n%s", err, out)
	}
}

func TestInstallScriptNeedsADownloader(t *testing.T) {
	m := newInstallMirror(t, "v9.8.7")
	path := pathWithout(t, []string{"uname"})
	out, err := m.run(t, path, "--version", "v9.8.7", "--dry-run")
	if err == nil || !strings.Contains(out, "need curl or wget") {
		t.Fatalf("want a clear error without curl/wget: err=%v\n%s", err, out)
	}
}
