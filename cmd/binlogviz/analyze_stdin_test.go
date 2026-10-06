package binlogviz

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
	"golang.org/x/term"

	"binlogviz/internal/i18n"
)

func TestMain(m *testing.M) {
	if os.Getenv("BINLOGVIZ_SIGNAL_CHILD") == "1" {
		runStdinInterruptChild()
	}
	os.Exit(m.Run())
}

func runStdinInterruptChild() {
	_ = i18n.Init("en")
	InstallInterruptHandler()
	cmd := NewRootCommand()
	cmd.SetArgs([]string{"analyze", "-"})
	err := cmd.Execute()
	if err != nil {
		PrintCommandError(os.Stderr, err)
		os.Exit(ExitCode(err))
	}
	os.Exit(0)
}

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

func TestAnalyzeDashOnDevNullIsEmptyNotATerminal(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	devNull, err := os.Open("/dev/null")
	if err != nil {
		t.Fatalf("open /dev/null: %v", err)
	}
	if isTerminal(devNull) {
		t.Fatal("/dev/null must not look like a terminal")
	}
	restoreStdin(t, devNull)

	stdout, stderr, err := executeAnalyzeLikeMain(t, "-")
	if err == nil {
		t.Fatal("expected empty stdin to fail")
	}
	if stdout != "" {
		t.Fatalf("expected empty stdout, got %q", stdout)
	}
	if strings.Contains(err.Error(), "terminal") || !strings.Contains(err.Error(), "stdin has no data") {
		t.Fatalf("error = %v", err)
	}
	if !strings.Contains(stderr, "Error:") || strings.Contains(stderr, "file not found: -") {
		t.Fatalf("stderr = %q", stderr)
	}
}

func TestAnalyzeDashOnTTYIsAClearError(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	tty := openTestTTY(t)
	if !isTerminal(tty) {
		t.Fatal("pty slave must be a terminal")
	}
	restoreStdin(t, tty)

	stdout, _, err := executeAnalyzeLikeMain(t, "-")
	if err == nil {
		t.Fatal("expected a terminal stdin to fail")
	}
	if stdout != "" {
		t.Fatalf("expected empty stdout, got %q", stdout)
	}
	if !strings.Contains(err.Error(), "stdin is a terminal") {
		t.Fatalf("error = %v", err)
	}
}

func openTestTTY(t *testing.T) *os.File {
	t.Helper()
	master, err := os.OpenFile("/dev/ptmx", os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open ptmx: %v", err)
	}
	t.Cleanup(func() { _ = master.Close() })
	if err := unix.IoctlSetPointerInt(int(master.Fd()), unix.TIOCSPTLCK, 0); err != nil {
		t.Fatalf("unlockpt: %v", err)
	}
	fd, _, errno := unix.Syscall(unix.SYS_IOCTL, uintptr(master.Fd()), uintptr(unix.TIOCGPTPEER), uintptr(unix.O_RDWR|unix.O_NOCTTY))
	if errno != 0 {
		t.Fatalf("pty peer: %v", errno)
	}
	slave := os.NewFile(fd, "ptyslave")
	if !term.IsTerminal(int(slave.Fd())) {
		t.Fatal("slave is not a tty")
	}
	return slave
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

func TestAnalyzeStdinTempRemovedOnSuccessAndBadMagic(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	tmp := t.TempDir()
	t.Setenv("TMPDIR", tmp)

	fixture := mustFixturePath(t, "minimal.binlog")
	data, err := os.ReadFile(fixture)
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	feedStdin(t, data)
	if _, _, err := executeAnalyzeLikeMain(t, "-"); err != nil {
		t.Fatalf("analyze -: %v", err)
	}
	if leftovers := stdinTempLeftovers(t, tmp); len(leftovers) != 0 {
		t.Fatalf("success left stdin copies: %v", leftovers)
	}

	feedStdin(t, []byte("not a binlog"))
	if _, _, err := executeAnalyzeLikeMain(t, "-"); err == nil {
		t.Fatal("bad magic must fail")
	}
	if leftovers := stdinTempLeftovers(t, tmp); len(leftovers) != 0 {
		t.Fatalf("error left stdin copies: %v", leftovers)
	}
}

func TestAnalyzeStdinTempRemovedOnSignal(t *testing.T) {
	for _, tc := range []struct {
		name string
		sig  syscall.Signal
		code int
	}{
		{name: "SIGINT", sig: syscall.SIGINT, code: 130},
		{name: "SIGTERM", sig: syscall.SIGTERM, code: 143},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tmp := t.TempDir()
			cmd := exec.Command(os.Args[0], "-test.run=^$")
			cmd.Env = signalChildEnv(tmp)
			stdin, err := cmd.StdinPipe()
			if err != nil {
				t.Fatalf("stdin pipe: %v", err)
			}
			var stderr bytes.Buffer
			cmd.Stderr = &stderr
			cmd.Stdout = ioDiscard()
			if err := cmd.Start(); err != nil {
				t.Fatalf("start child: %v", err)
			}
			if _, err := stdin.Write([]byte{0xfe, 0x62, 0x69, 0x6e}); err != nil {
				t.Fatalf("write: %v", err)
			}
			waitForStdinCopy(t, tmp)
			if err := cmd.Process.Signal(tc.sig); err != nil {
				t.Fatalf("signal: %v", err)
			}
			waitErr := waitCmd(t, cmd)
			_ = stdin.Close()
			var exitErr *exec.ExitError
			if !errors.As(waitErr, &exitErr) {
				t.Fatalf("wait = %v stderr=%s", waitErr, stderr.String())
			}
			if exitErr.ExitCode() != tc.code {
				t.Fatalf("exit %d, want %d; stderr=%s", exitErr.ExitCode(), tc.code, stderr.String())
			}
			if !strings.Contains(stderr.String(), "Error: interrupted") {
				t.Fatalf("stderr = %q", stderr.String())
			}
			if leftovers := stdinTempLeftovers(t, tmp); len(leftovers) != 0 {
				t.Fatalf("signal left stdin copies: %v", leftovers)
			}
		})
	}
}

func TestAnalyzeFileNamedStdinStillReplays(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	fixture := mustFixturePath(t, "minimal.binlog")
	data, err := os.ReadFile(fixture)
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	path := filepath.Join(t.TempDir(), "stdin")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		t.Fatalf("abs: %v", err)
	}
	stdout, stderr, err := executeAnalyzeLikeMain(t, path, "--format", "json")
	if err != nil {
		t.Fatalf("analyze file stdin: %v\n%s", err, stderr)
	}
	if strings.Contains(stdout, "input came from stdin") || strings.Contains(stdout, "binlogviz-stdin-") {
		t.Fatalf("a real file named stdin was treated as a pipe:\n%s", stdout)
	}
	if !strings.Contains(stdout, "mysqlbinlog ") || !strings.Contains(stdout, abs) {
		t.Fatalf("replay did not use the file path %s\n%s", abs, stdout)
	}
}

func TestAnalyzeStdinReplayDoesNotInventAPath(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	fixture := mustFixturePath(t, "minimal.binlog")
	data, err := os.ReadFile(fixture)
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	feedStdin(t, data)
	stdout, stderr, err := executeAnalyzeLikeMain(t, "-", "--format", "json")
	if err != nil {
		t.Fatalf("analyze -: %v\n%s", err, stderr)
	}
	if strings.Contains(stdout, "binlogviz-stdin-") || strings.Contains(stdout, "/stdin") {
		t.Fatalf("json invented a stdin path:\n%s", stdout)
	}
	var parsed struct {
		Transactions []struct {
			BinlogFileStart string `json:"binlog_file_start"`
			ReplayAvailable bool   `json:"replay_available"`
			MysqlbinlogCmd  string `json:"mysqlbinlog_cmd"`
			ReplayNote      string `json:"replay_note"`
		} `json:"transactions"`
	}
	if err := json.Unmarshal([]byte(stdout), &parsed); err != nil {
		t.Fatalf("json: %v", err)
	}
	if len(parsed.Transactions) == 0 {
		t.Fatal("expected a transaction from the fixture")
	}
	for _, txn := range parsed.Transactions {
		if txn.BinlogFileStart != "stdin" {
			t.Fatalf("binlog_file_start = %q", txn.BinlogFileStart)
		}
		if txn.ReplayAvailable || txn.MysqlbinlogCmd != "" {
			t.Fatalf("stdin replay must not be a command: %+v", txn)
		}
		if !strings.Contains(txn.ReplayNote, "input came from stdin") || strings.Contains(txn.ReplayNote, "mysqlbinlog") {
			t.Fatalf("replay note = %q", txn.ReplayNote)
		}
	}
}

func feedStdin(t *testing.T, data []byte) {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	go func() {
		_, _ = w.Write(data)
		_ = w.Close()
	}()
	restoreStdin(t, r)
}

func stdinTempLeftovers(t *testing.T, root string) []string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(root, "binlogviz-stdin-*"))
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	return matches
}

func signalChildEnv(tmp string) []string {
	env := make([]string, 0, len(os.Environ())+4)
	for _, item := range os.Environ() {
		if strings.HasPrefix(item, "BINLOGVIZ_SIGNAL_CHILD=") || strings.HasPrefix(item, "TMPDIR=") {
			continue
		}
		env = append(env, item)
	}
	return append(env, "BINLOGVIZ_SIGNAL_CHILD=1", "TMPDIR="+tmp, "LANG=C", "LC_ALL=C")
}

func waitForStdinCopy(t *testing.T, root string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		matches, _ := filepath.Glob(filepath.Join(root, "binlogviz-stdin-*", "stdin"))
		if len(matches) > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("stdin temp copy never appeared")
}

func waitCmd(t *testing.T, cmd *exec.Cmd) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatal("child did not exit after the signal")
		return nil
	}
}

func ioDiscard() *bytes.Buffer {
	return &bytes.Buffer{}
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
