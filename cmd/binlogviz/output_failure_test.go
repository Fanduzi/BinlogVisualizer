package binlogviz

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"binlogviz/internal/analyzer"
	"binlogviz/internal/binlog"
	"binlogviz/internal/clierr"
	"binlogviz/internal/detailstore"
	"binlogviz/internal/model"
	"binlogviz/internal/report"
	"binlogviz/internal/version"
	"binlogviz/internal/workflow"
)

type failOnWrite struct {
	failAt int
	calls  int
	err    error
}

func TestAnalyzeOutputFailureCleanup(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	cleanupErr := errors.New("temporary store cleanup failed")
	parseErr := errors.New("parse failed first")
	for _, tc := range []struct {
		name     string
		parseErr error
		wantKind clierr.Kind
	}{
		{"successful analysis", nil, clierr.KindOutput},
		{"parse error wins", parseErr, clierr.KindCorrupt},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := analyzer.DefaultOptions()
			opts.DetailStoreMode = analyzer.DetailStoreDuckDB
			cleanups := 0
			_, _, err := captureStdoutStderrRun(t, func() error {
				return runAnalysisStreamingWithDeps([]string{"dummy.binlog"}, opts, report.DefaultOptions(), "json",
					&mockParser{err: tc.parseErr}, binlog.NormalizeRawEvent,
					func(analyzer.Options, *detailstore.DuckDBStore) commandAnalyzer {
						return &fakeStreamingAnalyzer{finalResult: &model.AnalysisResult{Summary: model.WorkloadSummary{TotalEvents: 1, TotalTransactions: 1, TotalRows: 1}}}
					},
					func(string) (*detailstore.DuckDBStore, func() error, string, error) {
						return nil, func() error { cleanups++; return cleanupErr }, "", nil
					}, "")
			})
			if cleanups != 1 {
				t.Fatalf("cleanup calls = %d, want 1", cleanups)
			}
			if kind, _ := ErrorKind(err); kind != tc.wantKind {
				t.Fatalf("error = %v, kind = %q, want %q", err, kind, tc.wantKind)
			}
			if tc.parseErr == nil && !errors.Is(err, cleanupErr) {
				t.Fatalf("cleanup cause lost: %v", err)
			}
			if tc.parseErr != nil && (err == nil || !strings.Contains(err.Error(), tc.parseErr.Error()) || errors.Is(err, cleanupErr)) {
				t.Fatalf("parse error did not win: %v", err)
			}
		})
	}
}

func TestWorkflowOutputFailureStderr(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	oldQuiet, oldFormat := quietFlag, errorFormat
	t.Cleanup(func() { quietFlag, errorFormat = oldQuiet, oldFormat })
	quietFlag, errorFormat = false, ""
	t.Setenv("BINLOGVIZ_ERROR_FORMAT", "")
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "analyze"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "analyze", "week1.json"), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "ledger.jsonl"), 0o700); err != nil {
		t.Fatal(err)
	}
	manifest := workflow.Manifest{
		WorkflowName: "stderr-failure",
		Status:       "success",
		Steps: []workflow.StepRecord{{
			Kind: "analyze", Name: "week1", Status: "success", Execution: "executed", Artifacts: []string{"analyze/week1.json"},
		}},
	}
	stderr := &failOnWrite{failAt: 1, err: errors.New("stderr failed")}
	if err := finalizeWorkflow(dir, &manifest, time.Now(), stderr, nil); err != nil {
		t.Fatalf("stderr failure changed workflow result: %v", err)
	}
	if stderr.calls < 1 {
		t.Fatal("expected a stderr write")
	}
	for _, name := range []string{"manifest.json", "index.html"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Errorf("missing %s: %v", name, err)
		}
	}
}

func (w *failOnWrite) Write(p []byte) (int, error) {
	w.calls++
	if w.calls == w.failAt {
		return 0, w.err
	}
	return len(p), nil
}

func TestVersionOutputFailure(t *testing.T) {
	oldCommit := version.Commit
	version.Commit = "test-commit"
	t.Cleanup(func() { version.Commit = oldCommit })
	t.Setenv("BINLOGVIZ_ERROR_FORMAT", "")
	sentinel := errors.New("output write failed")
	for _, tc := range []struct {
		name   string
		args   []string
		failAt int
	}{
		{"text-logo", []string{"version"}, 1},
		{"text-version", []string{"version"}, 2},
		{"text-commit", []string{"version"}, 3},
		{"json", []string{"version", "--format", "json"}, 1},
		{"root-version", []string{"-v"}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := &failOnWrite{failAt: tc.failAt, err: sentinel}
			cmd := NewRootCommand()
			cmd.SetArgs(tc.args)
			cmd.SetOut(out)
			cmd.SetErr(io.Discard)
			err := cmd.Execute()
			if !errors.Is(err, sentinel) {
				t.Errorf("got error %v, want output failure %v (writes=%d)", err, sentinel, out.calls)
			}
			if kind, _ := ErrorKind(err); kind != clierr.KindOutput {
				t.Errorf("error kind = %q, want %q", kind, clierr.KindOutput)
			}
		})
	}
}
