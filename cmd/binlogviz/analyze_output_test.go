// Package binlogviz verifies analyze HTML destination resolution for TTY, redirect, and --output.
// input: destination resolver arguments, analyze CLI flags, and fixture binlogs.
// output: evidence that omitted --output follows TTY vs redirected stdout and that explicit --output is unchanged.
// pos: CLI output-destination tests for the analyze HTML operator I/O contract.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestResolveOutputDestination_NonHTML_RejectsOutput(t *testing.T) {
	tests := []struct {
		name    string
		format  string
		output  string
		wantErr bool
	}{
		{name: "json with output", format: "json", output: "out.json", wantErr: true},
		{name: "json with stdout", format: "json", output: "-", wantErr: true},
		{name: "text with output", format: "text", output: "out.txt", wantErr: true},
		{name: "markdown with output", format: "markdown", output: "out.md", wantErr: true},
		{name: "json without output", format: "json", output: "", wantErr: false},
		{name: "text without output", format: "text", output: "", wantErr: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := resolveOutputDestination([]string{"file.binlog"}, false, tt.output, tt.format)
			if (err != nil) != tt.wantErr {
				t.Fatalf("resolveOutputDestination() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestResolveOutputDestination_HTML_Stdout(t *testing.T) {
	dest, err := resolveOutputDestinationWithCwd([]string{"file.binlog"}, false, "-", "html", "/tmp", false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !dest.IsStdout {
		t.Fatal("expected IsStdout=true for -")
	}
	if dest.IsFile {
		t.Fatal("expected IsFile=false for -")
	}
}

func TestResolveOutputDestination_HTML_OmittedOutputOnNonTTYUsesStdout(t *testing.T) {
	if stdoutIsTerminal() {
		t.Skip("stdout is a TTY; this test encodes the redirect/non-TTY contract")
	}
	dest, err := resolveOutputDestination([]string{"file.binlog"}, false, "", "html")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !dest.IsStdout || dest.IsFile {
		t.Fatalf("omitted --output with non-TTY stdout must write HTML to stdout so `analyze --format html > report.html` is not empty, got %+v", dest)
	}
}

func TestResolveOutputDestination_HTML_OmittedOutputOnTTYUsesCwdFile(t *testing.T) {
	dir := t.TempDir()
	dest, err := resolveOutputDestinationWithCwd([]string{"/data/mysql-bin.010958"}, false, "", "html", dir, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := filepath.Join(dir, "mysql-bin.010958.html")
	if dest.Path != expected || !dest.IsFile || dest.IsStdout {
		t.Fatalf("interactive TTY without --output must write a derived cwd file, got %+v", dest)
	}
}

func TestResolveOutputDestination_HTML_OmittedOutputNonTTYWithCwdUsesStdout(t *testing.T) {
	dir := t.TempDir()
	dest, err := resolveOutputDestinationWithCwd([]string{"/data/a.binlog", "/data/b.binlog"}, true, "", "html", dir, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !dest.IsStdout || dest.IsFile || dest.Path != "" {
		t.Fatalf("non-TTY stdout without --output must not derive a cwd file, got %+v", dest)
	}
}

func TestResolveOutputDestination_HTML_ExplicitPath(t *testing.T) {
	dest, err := resolveOutputDestinationWithCwd([]string{"file.binlog"}, false, "/tmp/custom.html", "html", "/tmp", false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dest.Path != "/tmp/custom.html" {
		t.Fatalf("expected path /tmp/custom.html, got %s", dest.Path)
	}
	if !dest.IsFile {
		t.Fatal("expected IsFile=true")
	}
}

func TestResolveOutputDestination_HTML_SingleInput(t *testing.T) {
	dir := t.TempDir()
	dest, err := resolveOutputDestinationWithCwd([]string{"/data/mysql-bin.010958"}, false, "", "html", dir, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := filepath.Join(dir, "mysql-bin.010958.html")
	if dest.Path != expected {
		t.Fatalf("expected path %s, got %s", expected, dest.Path)
	}
	if !dest.IsFile {
		t.Fatal("expected IsFile=true")
	}
}

func TestResolveOutputDestination_HTML_MultipleInputs(t *testing.T) {
	dir := t.TempDir()
	dest, err := resolveOutputDestinationWithCwd([]string{"/data/a.binlog", "/data/b.binlog"}, false, "", "html", dir, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := filepath.Join(dir, "binlogviz-report.html")
	if dest.Path != expected {
		t.Fatalf("expected path %s, got %s", expected, dest.Path)
	}
}

func TestResolveOutputDestination_HTML_DiscoveryMode(t *testing.T) {
	dir := t.TempDir()
	dest, err := resolveOutputDestinationWithCwd([]string{"/data/a.binlog"}, true, "", "html", dir, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := filepath.Join(dir, "binlogviz-report.html")
	if dest.Path != expected {
		t.Fatalf("expected path %s, got %s", expected, dest.Path)
	}
}

func TestFindAvailablePath_NoCollision(t *testing.T) {
	dir := t.TempDir()
	desired := filepath.Join(dir, "report.html")
	got := findAvailablePath(desired)
	if got != desired {
		t.Fatalf("expected %s, got %s", desired, got)
	}
}

func TestFindAvailablePath_WithCollision(t *testing.T) {
	dir := t.TempDir()
	desired := filepath.Join(dir, "report.html")
	os.WriteFile(desired, []byte("existing"), 0644)

	got := findAvailablePath(desired)
	expected := filepath.Join(dir, "report.1.html")
	if got != expected {
		t.Fatalf("expected %s, got %s", expected, got)
	}
}

func TestFindAvailablePath_MultipleCollisions(t *testing.T) {
	dir := t.TempDir()
	desired := filepath.Join(dir, "report.html")
	os.WriteFile(desired, []byte("0"), 0644)
	os.WriteFile(filepath.Join(dir, "report.1.html"), []byte("1"), 0644)
	os.WriteFile(filepath.Join(dir, "report.2.html"), []byte("2"), 0644)

	got := findAvailablePath(desired)
	expected := filepath.Join(dir, "report.3.html")
	if got != expected {
		t.Fatalf("expected %s, got %s", expected, got)
	}
}

func TestDeriveHTMLFilename_SingleInput(t *testing.T) {
	got := deriveHTMLFilename([]string{"/data/mysql-bin.010958"}, false)
	if got != "mysql-bin.010958.html" {
		t.Fatalf("expected mysql-bin.010958.html, got %s", got)
	}
}

func TestDeriveHTMLFilename_MultipleInputs(t *testing.T) {
	got := deriveHTMLFilename([]string{"/data/a.binlog", "/data/b.binlog"}, false)
	if got != "binlogviz-report.html" {
		t.Fatalf("expected binlogviz-report.html, got %s", got)
	}
}

func TestDeriveHTMLFilename_DiscoveryMode(t *testing.T) {
	got := deriveHTMLFilename([]string{"/data/a.binlog"}, true)
	if got != "binlogviz-report.html" {
		t.Fatalf("expected binlogviz-report.html, got %s", got)
	}
}

func TestWriteHTMLAtomically_Stdout(t *testing.T) {
	dest := outputDestination{IsStdout: true}
	err := writeHTMLAtomically(dest, "<html>test</html>")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestWriteHTMLAtomically_File(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "report.html")
	dest := outputDestination{Path: path, IsFile: true}

	content := "<html><body>test</body></html>"
	err := writeHTMLAtomically(dest, content)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read output: %v", err)
	}
	if string(got) != content {
		t.Fatalf("content mismatch: got %q, want %q", string(got), content)
	}
}

func TestWriteHTMLAtomically_OverwritesExisting(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "report.html")
	os.WriteFile(path, []byte("old content"), 0644)

	dest := outputDestination{Path: path, IsFile: true}
	newContent := "<html>new</html>"
	err := writeHTMLAtomically(dest, newContent)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read output: %v", err)
	}
	if string(got) != newContent {
		t.Fatalf("content mismatch: got %q, want %q", string(got), newContent)
	}
}

func TestPrintHTMLSaveConfirmationGoesToProvidedWriter(t *testing.T) {
	forceEnglishRuntimeOutput(t)

	var buf strings.Builder
	printHTMLSaveConfirmationTo(&buf, "/tmp/io-check.html")
	got := buf.String()
	if !strings.Contains(got, "HTML report saved to") {
		t.Fatalf("expected save confirmation, got %q", got)
	}
	if !strings.Contains(got, "io-check.html") {
		t.Fatalf("expected path in confirmation, got %q", got)
	}
}

func TestWriteHTMLAtomically_NoTempLeak(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "report.html")
	dest := outputDestination{Path: path, IsFile: true}

	err := writeHTMLAtomically(dest, "<html>test</html>")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if len(e.Name()) > 0 && e.Name()[0] == '.' {
			t.Fatalf("found leaked temp file: %s", e.Name())
		}
	}
}

func TestAnalyzeHTMLWithoutOutputOnRedirectedStdoutWritesDocumentNotCwdFile(t *testing.T) {
	forceEnglishRuntimeOutput(t)

	fixture, err := filepath.Abs(mustFixturePath(t, "minimal.binlog"))
	if err != nil {
		t.Fatalf("abs fixture: %v", err)
	}
	cwd := t.TempDir()
	t.Chdir(cwd)

	stdout, stderr, err := executeAnalyzeLikeMain(t, fixture, "--format", "html")
	if err != nil {
		t.Fatalf("analyze html redirect: %v\nstderr=%s", err, stderr)
	}
	if strings.Contains(stderr, "HTML report saved to") {
		t.Fatalf("redirected stdout must not print a saved-to path, got %q", stderr)
	}
	assertHTMLDocument(t, stdout)
	assertNoHTMLFiles(t, cwd)
}

func TestAnalyzeHTMLFromDirWithoutOutputOnRedirectedStdoutWritesDocumentNotCwdFile(t *testing.T) {
	forceEnglishRuntimeOutput(t)

	fixture, err := filepath.Abs(mustFixturePath(t, "minimal.binlog"))
	if err != nil {
		t.Fatalf("abs fixture: %v", err)
	}
	src, err := os.ReadFile(fixture)
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}

	binlogDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(binlogDir, "mysql-bin.000001"), src, 0o644); err != nil {
		t.Fatalf("copy binlog: %v", err)
	}

	cwd := t.TempDir()
	t.Chdir(cwd)

	stdout, stderr, err := executeAnalyzeLikeMain(t, "--from-dir", binlogDir, "--prefix", "mysql-bin.", "--format", "html")
	if err != nil {
		t.Fatalf("analyze html from-dir redirect: %v\nstderr=%s", err, stderr)
	}
	if strings.Contains(stderr, "HTML report saved to") {
		t.Fatalf("redirected stdout must not print a saved-to path, got %q", stderr)
	}
	assertHTMLDocument(t, stdout)
	assertNoHTMLFiles(t, cwd)
}

func TestAnalyzeHTMLOutputDashStillWritesStdout(t *testing.T) {
	forceEnglishRuntimeOutput(t)

	fixture := mustFixturePath(t, "minimal.binlog")
	stdout, stderr, err := executeAnalyzeLikeMain(t, fixture, "--format", "html", "--output", "-")
	if err != nil {
		t.Fatalf("analyze html --output -: %v\nstderr=%s", err, stderr)
	}
	if strings.Contains(stderr, "HTML report saved to") {
		t.Fatalf("--output - must not print a saved-to path, got %q", stderr)
	}
	assertHTMLDocument(t, stdout)
}

func TestAnalyzeHTMLExplicitOutputLeavesStdoutEmptyOfDocument(t *testing.T) {
	forceEnglishRuntimeOutput(t)

	fixture := mustFixturePath(t, "minimal.binlog")
	outPath := filepath.Join(t.TempDir(), "explicit.html")
	stdout, stderr, err := executeAnalyzeLikeMain(t, fixture, "--format", "html", "--output", outPath)
	if err != nil {
		t.Fatalf("analyze html --output path: %v\nstderr=%s", err, stderr)
	}
	if strings.Contains(stdout, "<html") || strings.Contains(stdout, "<!DOCTYPE html") {
		t.Fatalf("explicit --output must keep the HTML document off stdout, got %q", stdout)
	}
	if !strings.Contains(stderr, "HTML report saved to") || !strings.Contains(stderr, "explicit.html") {
		t.Fatalf("expected save confirmation on stderr, got %q", stderr)
	}
	data, readErr := os.ReadFile(outPath)
	if readErr != nil {
		t.Fatalf("read explicit html: %v", readErr)
	}
	assertHTMLDocument(t, string(data))
}

func assertHTMLDocument(t *testing.T, body string) {
	t.Helper()
	trimmed := strings.TrimSpace(body)
	if trimmed == "" {
		t.Fatal("expected a non-empty HTML document")
	}
	if !strings.HasPrefix(trimmed, "<!DOCTYPE html") && !strings.HasPrefix(trimmed, "<html") {
		t.Fatalf("expected HTML document starting with <!DOCTYPE html or <html, got %q", truncatedForTest(trimmed))
	}
}

func assertNoHTMLFiles(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read cwd: %v", err)
	}
	var htmlFiles []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(strings.ToLower(e.Name()), ".html") {
			htmlFiles = append(htmlFiles, e.Name())
		}
	}
	if len(htmlFiles) > 0 {
		t.Fatalf("redirected analyze html must not create a sibling cwd file, found %v", htmlFiles)
	}
}

func truncatedForTest(s string) string {
	if len(s) < 120 {
		return s
	}
	return s[:120]
}
