package binlogviz

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

func TestAnalyzeDashReadsPipedBinlog(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	fixture := mustFixturePath(t, "minimal.binlog")
	data, err := os.ReadFile(fixture)
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	go func() {
		_, _ = w.Write(data)
		_ = w.Close()
	}()
	restoreStdin(t, r)

	stdout, stderr, err := executeAnalyzeLikeMain(t, "-", "--format", "text")
	if err != nil {
		t.Fatalf("analyze -: %v\nstderr=%s", err, stderr)
	}
	for _, want := range []string{"=== Top Threads (by rows) ===", "thread_id", "3"} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("stdin report missing %q\n%s", want, stdout)
		}
	}
	if strings.Contains(stdout, "user@host") {
		t.Fatalf("stdin report invented user@host:\n%s", stdout)
	}
	if strings.Contains(stdout, "binlogviz-stdin-") {
		t.Fatalf("stdin report leaked the temp path:\n%s", stdout)
	}
}

func TestAnalyzeDashOnTerminalIsAClearError(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	devNull, err := os.Open("/dev/null")
	if err != nil {
		t.Fatalf("open /dev/null: %v", err)
	}
	restoreStdin(t, devNull)

	stdout, stderr, err := executeAnalyzeLikeMain(t, "-")
	if err == nil {
		t.Fatal("expected terminal stdin to fail")
	}
	if stdout != "" {
		t.Fatalf("expected empty stdout, got %q", stdout)
	}
	if !strings.Contains(err.Error(), "stdin is a terminal") {
		t.Fatalf("error = %v", err)
	}
	if !strings.Contains(stderr, "Error:") || strings.Contains(stderr, "file not found: -") {
		t.Fatalf("stderr = %q", stderr)
	}
}

func TestAnalyzeDashEmptyPipeIsAClearError(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	_ = w.Close()
	restoreStdin(t, r)

	_, _, err = executeAnalyzeLikeMain(t, "-")
	if err == nil || !strings.Contains(err.Error(), "stdin has no data") {
		t.Fatalf("error = %v", err)
	}
}

func TestAnalyzeReadsNamedPipe(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	fixture := mustFixturePath(t, "minimal.binlog")
	data, err := os.ReadFile(fixture)
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	fifo := filepath.Join(t.TempDir(), "mysql-bin.pipe")
	if err := syscall.Mkfifo(fifo, 0o600); err != nil {
		t.Fatalf("mkfifo: %v", err)
	}
	errCh := make(chan error, 1)
	go func() {
		f, openErr := os.OpenFile(fifo, os.O_WRONLY, 0)
		if openErr != nil {
			errCh <- openErr
			return
		}
		_, writeErr := f.Write(data)
		closeErr := f.Close()
		if writeErr != nil {
			errCh <- writeErr
			return
		}
		errCh <- closeErr
	}()

	stdout, stderr, err := executeAnalyzeLikeMain(t, fifo, "--format", "json")
	if writeErr := <-errCh; writeErr != nil {
		t.Fatalf("fifo write: %v", writeErr)
	}
	if err != nil {
		t.Fatalf("analyze fifo: %v\nstderr=%s", err, stderr)
	}
	if !strings.Contains(stdout, `"threads"`) || !strings.Contains(stdout, `"thread_id": 3`) {
		t.Fatalf("fifo json missing threads:\n%s", stdout)
	}
}

func restoreStdin(t *testing.T, next *os.File) {
	t.Helper()
	old := os.Stdin
	os.Stdin = next
	t.Cleanup(func() {
		os.Stdin = old
		if next != nil {
			_ = next.Close()
		}
	})
}
