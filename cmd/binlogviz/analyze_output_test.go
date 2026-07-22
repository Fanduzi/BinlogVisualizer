package binlogviz

import (
	"os"
	"path/filepath"
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
	dest, err := resolveOutputDestinationWithCwd([]string{"file.binlog"}, false, "-", "html", "/tmp")
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

func TestResolveOutputDestination_HTML_ExplicitPath(t *testing.T) {
	dest, err := resolveOutputDestinationWithCwd([]string{"file.binlog"}, false, "/tmp/custom.html", "html", "/tmp")
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
	dest, err := resolveOutputDestinationWithCwd([]string{"/data/mysql-bin.010958"}, false, "", "html", dir)
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
	dest, err := resolveOutputDestinationWithCwd([]string{"/data/a.binlog", "/data/b.binlog"}, false, "", "html", dir)
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
	dest, err := resolveOutputDestinationWithCwd([]string{"/data/a.binlog"}, true, "", "html", dir)
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
