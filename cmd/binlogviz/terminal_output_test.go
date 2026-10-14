package binlogviz

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"
)

// #253: stderr that is a file or pipe gets no progress bar, no carriage
// returns and no ANSI codes.
func TestNonTTYStderrHasNoProgressOrANSI(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	code, _, stderr := runRoot(t, "analyze", "--format", "json", filepath.Join("testdata", "minimal.binlog"))
	if code != 0 {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	if strings.ContainsAny(stderr, "\r\x1b") || strings.Contains(stderr, "Parsing binlogs") {
		t.Fatalf("progress or ANSI on non-TTY stderr: %q", stderr)
	}
	code, _, stderr = runRoot(t, "analyze", filepath.Join(t.TempDir(), "missing.binlog"))
	if code == 0 || strings.ContainsAny(stderr, "\r\x1b") || !strings.HasPrefix(stderr, "Error: ") {
		t.Fatalf("error line on non-TTY stderr: %q", stderr)
	}
}

// On a terminal the bar still renders; --quiet turns it and info lines off.
func TestQuietSilencesProgressOnTTY(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	prev := stderrIsTerminal
	stderrIsTerminal = func() bool { return true }
	t.Setenv("TERM", "xterm-256color")
	t.Cleanup(func() { stderrIsTerminal = prev })
	fixture := filepath.Join("testdata", "minimal.binlog")

	_, _, stderr := runRoot(t, "analyze", "--format", "json", fixture)
	if !strings.Contains(stderr, "Parsing binlogs") {
		t.Fatalf("TTY keeps the bar: %q", stderr)
	}
	_, _, stderr = runRoot(t, "analyze", "--quiet", "--format", "json", fixture)
	if stderr != "" {
		t.Fatalf("--quiet stderr: %q", stderr)
	}
	t.Setenv("TERM", "dumb")
	_, _, stderr = runRoot(t, "analyze", "--format", "json", fixture)
	if strings.ContainsAny(stderr, "\r\x1b") {
		t.Fatalf("TERM=dumb: %q", stderr)
	}
}

func TestPrintCommandErrorPlainForNonTerminal(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	var buf bytes.Buffer
	PrintCommandError(&buf, errString("boom"))
	if buf.String() != "Error: boom\n" {
		t.Fatalf("%q", buf.String())
	}
}

type errString string

func (e errString) Error() string { return string(e) }
