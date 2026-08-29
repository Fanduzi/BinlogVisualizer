// Package binlogviz verifies operator-facing analyze command behavior.
// input: CLI analyze invocations, fixture binlogs, and discovery layouts.
// output: regression coverage for HTML save I/O, DBA error language, and copy-paste mysqlbinlog_cmd paths.
// pos: command-layer dogfood tests for analyze operator contracts.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const mysqlBinlogMagic = "\xfe\x62\x69\x6e"

func executeAnalyzeLikeMain(t *testing.T, args ...string) (string, string, error) {
	t.Helper()
	cmd := NewRootCommand()
	cmd.SetArgs(append([]string{"analyze"}, args...))
	return captureStdoutStderrRun(t, func() error {
		err := cmd.Execute()
		if err != nil {
			fmt.Fprintln(os.Stderr, "Error:", err)
		}
		return err
	})
}

func assertNoUsageDump(t *testing.T, stderr string) {
	t.Helper()
	if strings.Contains(stderr, "Usage:") || strings.Contains(stderr, "--spike-min-rows") {
		t.Fatalf("failure path dumped analyze Usage:\n%s", stderr)
	}
}

func TestAnalyzeHTMLSaveConfirmationGoesToStderrNotStdout(t *testing.T) {
	forceEnglishRuntimeOutput(t)

	fixture := mustFixturePath(t, "minimal.binlog")
	outPath := filepath.Join(t.TempDir(), "io-check.html")
	stdout, stderr, err := executeAnalyzeLikeMain(t, fixture, "--format", "html", "--output", outPath)
	if err != nil {
		t.Fatalf("analyze html: %v\nstderr=%s", err, stderr)
	}
	if strings.Contains(stdout, "HTML report saved") {
		t.Fatalf("save confirmation leaked to stdout: %q", stdout)
	}
	if !strings.Contains(stderr, "HTML report saved to") || !strings.Contains(stderr, "io-check.html") {
		t.Fatalf("expected save confirmation on stderr, got %q", stderr)
	}
	data, readErr := os.ReadFile(outPath)
	if readErr != nil {
		t.Fatalf("read html output: %v", readErr)
	}
	if !strings.Contains(string(data), "<html") {
		t.Fatalf("expected HTML sidecar, got %q", data)
	}
}

func TestAnalyzeNoArgsMentionsFromDirPrefixWithoutUsageDump(t *testing.T) {
	forceEnglishRuntimeOutput(t)

	stdout, stderr, err := executeAnalyzeLikeMain(t)
	if err == nil {
		t.Fatal("expected error for analyze with no args")
	}
	if stdout != "" {
		t.Fatalf("expected empty stdout, got %q", stdout)
	}
	assertNoUsageDump(t, stderr)
	if strings.Count(stderr, "Error:") != 1 {
		t.Fatalf("expected Error printed once, got %q", stderr)
	}
	if !strings.Contains(err.Error(), "--from-dir") || !strings.Contains(err.Error(), "--prefix") {
		t.Fatalf("expected --from-dir --prefix alternative, got %v", err)
	}
}

func TestAnalyzeFailurePathsPrintErrorOnceWithDBALanguage(t *testing.T) {
	forceEnglishRuntimeOutput(t)

	dir := t.TempDir()
	emptyPath := filepath.Join(dir, "empty.binlog")
	if err := os.WriteFile(emptyPath, nil, 0o644); err != nil {
		t.Fatalf("write empty: %v", err)
	}
	garbagePath := filepath.Join(dir, "garbage.binlog")
	if err := os.WriteFile(garbagePath, []byte("fe'bin'"), 0o644); err != nil {
		t.Fatalf("write garbage: %v", err)
	}
	fixture := mustFixturePath(t, "minimal.binlog")
	src, err := os.ReadFile(fixture)
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	if len(src) < 200 {
		t.Fatalf("fixture too small to truncate: %d", len(src))
	}
	truncatedPath := filepath.Join(dir, "truncated.binlog")
	if err := os.WriteFile(truncatedPath, src[:200], 0o644); err != nil {
		t.Fatalf("write truncated: %v", err)
	}

	cases := []struct {
		name    string
		path    string
		wantSub string
	}{
		{name: "empty file", path: emptyPath, wantSub: "empty"},
		{name: "garbage header", path: garbagePath, wantSub: "not a MySQL binlog"},
		{name: "truncated event", path: truncatedPath, wantSub: "truncated or corrupt"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stdout, stderr, runErr := executeAnalyzeLikeMain(t, tc.path)
			if runErr == nil {
				t.Fatal("expected parse failure")
			}
			if strings.Contains(stdout, "Usage:") {
				t.Fatalf("usage leaked to stdout: %q", stdout)
			}
			assertNoUsageDump(t, stderr)
			if strings.Count(stderr, "Error:") != 1 {
				t.Fatalf("expected Error printed once, got %q", stderr)
			}
			if !strings.Contains(runErr.Error(), tc.wantSub) {
				t.Fatalf("expected DBA language containing %q, got %v", tc.wantSub, runErr)
			}
		})
	}
}

func TestAnalyzeMagicOnlyFileExitsNonZero(t *testing.T) {
	forceEnglishRuntimeOutput(t)

	path := filepath.Join(t.TempDir(), "magic-only.binlog")
	if err := os.WriteFile(path, []byte(mysqlBinlogMagic), 0o644); err != nil {
		t.Fatalf("write magic-only: %v", err)
	}

	stdout, stderr, err := executeAnalyzeLikeMain(t, path)
	if err == nil {
		t.Fatalf("expected non-zero exit for magic-only file, stdout=%q stderr=%q", stdout, stderr)
	}
	assertNoUsageDump(t, stderr)
	if !strings.Contains(err.Error(), "Format Description") {
		t.Fatalf("expected Format Description error, got %v", err)
	}
	if strings.Contains(stdout, "Open HTML") {
		t.Fatalf("empty success report should not be printed, got %q", stdout)
	}
}

func TestAnalyzeTimeWindowZeroEventsWarnsOnStderr(t *testing.T) {
	forceEnglishRuntimeOutput(t)

	fixture := mustFixturePath(t, "minimal.binlog")
	stdout, stderr, err := executeAnalyzeLikeMain(t,
		fixture,
		"--start", "2020-01-01T00:00:00Z",
		"--end", "2020-01-01T00:00:01Z",
	)
	if err != nil {
		t.Fatalf("windowed analyze: %v\nstderr=%s", err, stderr)
	}
	if !strings.Contains(stderr, "window matched 0 events") {
		t.Fatalf("expected window warning on stderr, got %q", stderr)
	}
	if stdout == "" {
		t.Fatal("expected a report on stdout for a completed window miss")
	}
}

func TestDiscoverBinlogPathsAcceptsOptionalDotBeforeNumericSuffix(t *testing.T) {
	forceEnglishRuntimeOutput(t)

	dir := t.TempDir()
	mustWriteFile(t, filepath.Join(dir, "mysql-bin.000008"))
	mustWriteFile(t, filepath.Join(dir, "mysql-bin.index"))
	mustWriteFile(t, filepath.Join(dir, "mysql-bin.000008.tmp"))

	got, err := discoverBinlogPaths(dir, "mysql-bin")
	if err != nil {
		t.Fatalf("prefix mysql-bin: %v", err)
	}
	want := []string{filepath.Join(dir, "mysql-bin.000008")}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("prefix mysql-bin: want %v got %v", want, got)
	}

	gotDotted, err := discoverBinlogPaths(dir, "mysql-bin.")
	if err != nil {
		t.Fatalf("prefix mysql-bin.: %v", err)
	}
	if strings.Join(gotDotted, ",") != strings.Join(want, ",") {
		t.Fatalf("prefix mysql-bin.: want %v got %v", want, gotDotted)
	}
}

func TestDiscoverBinlogPathsNoMatchHintsTrailingDot(t *testing.T) {
	forceEnglishRuntimeOutput(t)

	dir := t.TempDir()
	mustWriteFile(t, filepath.Join(dir, "other.log"))

	_, err := discoverBinlogPaths(dir, "mysql-bin")
	if err == nil {
		t.Fatal("expected no-match error")
	}
	if !strings.Contains(err.Error(), "no matching binlog files") {
		t.Fatalf("expected no-match error, got %v", err)
	}
	if !strings.Contains(err.Error(), "try --prefix mysql-bin.") {
		t.Fatalf("expected trailing-dot hint, got %v", err)
	}
}

func TestBinlogNumericSuffixOptionalDot(t *testing.T) {
	cases := []struct {
		name, prefix, want string
		ok                 bool
	}{
		{name: "mysql-bin.000008", prefix: "mysql-bin", want: "000008", ok: true},
		{name: "mysql-bin.000008", prefix: "mysql-bin.", want: "000008", ok: true},
		{name: "mysql-bin000008", prefix: "mysql-bin", want: "000008", ok: true},
		{name: "mysql-bin.index", prefix: "mysql-bin", ok: false},
		{name: "mysql-bin.", prefix: "mysql-bin", ok: false},
		{name: "relay-bin.000008", prefix: "mysql-bin", ok: false},
	}
	for _, tc := range cases {
		got, ok := binlogNumericSuffix(tc.name, tc.prefix)
		if ok != tc.ok || got != tc.want {
			t.Fatalf("%s prefix %q: got (%q,%v) want (%q,%v)", tc.name, tc.prefix, got, ok, tc.want, tc.ok)
		}
	}
}

func TestMapBinlogParseErrorUsesDBALanguage(t *testing.T) {
	forceEnglishRuntimeOutput(t)

	cases := []struct {
		in, want string
	}{
		{in: "EOF", want: "empty"},
		{in: "head 4 bytes must fe'bin'", want: "not a MySQL binlog"},
		{in: "get event err EOF, need 65 but got 27", want: "truncated or corrupt"},
		{in: "normalize error at position 42: boom", want: ""},
	}
	for _, tc := range cases {
		got := mapBinlogParseError(tc.in)
		if tc.want == "" {
			if got != "" {
				t.Fatalf("mapBinlogParseError(%q) = %q, want empty", tc.in, got)
			}
			continue
		}
		if !strings.Contains(got, tc.want) {
			t.Fatalf("mapBinlogParseError(%q) = %q, want substring %q", tc.in, got, tc.want)
		}
	}
}

func TestAnalyzeRelativeROWSampleMysqlbinlogCmdIsAbsolute(t *testing.T) {
	forceEnglishRuntimeOutput(t)

	rel := filepath.Join("testdata", "sample-binlog", "mysql-bin.000001")
	abs, err := filepath.Abs(rel)
	if err != nil {
		t.Fatalf("abs: %v", err)
	}

	stdout, stderr, err := executeAnalyzeLikeMain(t, rel, "--format", "json")
	if err != nil {
		t.Fatalf("analyze relative ROW sample: %v\nstderr=%s\nstdout=%s", err, stderr, stdout)
	}

	var decoded struct {
		Diagnostics struct {
			LargestTransactions []struct {
				MysqlbinlogCmd string `json:"mysqlbinlog_cmd"`
			} `json:"largest_transactions"`
		} `json:"diagnostics"`
		Transactions []struct {
			MysqlbinlogCmd string `json:"mysqlbinlog_cmd"`
		} `json:"transactions"`
	}
	if err := json.Unmarshal([]byte(stdout), &decoded); err != nil {
		t.Fatalf("json.Unmarshal: %v\n%s", err, stdout)
	}

	var cmds []string
	for _, txn := range decoded.Diagnostics.LargestTransactions {
		if txn.MysqlbinlogCmd != "" {
			cmds = append(cmds, txn.MysqlbinlogCmd)
		}
	}
	for _, txn := range decoded.Transactions {
		if txn.MysqlbinlogCmd != "" {
			cmds = append(cmds, txn.MysqlbinlogCmd)
		}
	}
	if len(cmds) == 0 {
		t.Fatalf("expected at least one mysqlbinlog_cmd for the ROW sample\n%s", stdout)
	}
	for _, cmd := range cmds {
		if !strings.HasPrefix(cmd, "mysqlbinlog ") {
			t.Fatalf("MySQL Format Description must keep mysqlbinlog binary, got %q", cmd)
		}
		if !strings.Contains(cmd, abs) {
			t.Fatalf("mysqlbinlog_cmd must contain absolute path %q, got %q", abs, cmd)
		}
		if strings.Contains(cmd, " mysql-bin.000001") && !strings.Contains(cmd, abs) {
			t.Fatalf("basename-only file arg is not copy-pasteable from another cwd: %q", cmd)
		}
	}
}
