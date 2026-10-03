// Package binlogviz covers DBA-facing analyze UX: filter misses vs bad files, snapshot format, and progress/error lines.
// input: real ROW fixtures, truncated copies, and the process-level analyze command.
// output: regression tests for distinct no-data vs bad-file results, snapshot-name format failures, and Error lines that do not share a progress line.
// pos: command-layer regression suite for operator exit and stderr contracts.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"binlogviz/internal/analyzer"
	"binlogviz/internal/binlog"
	"binlogviz/internal/model"
	"binlogviz/internal/report"
)

func TestSchemaFilterMissIsDistinctFromBadBinlog(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	fixture := mustFixturePath(t, "minimal.binlog")

	filterOut, _, filterErr := executeAnalyzeLikeMain(t, fixture, "--include-schema", "no_such_schema")
	if filterErr == nil {
		t.Fatal("schema filter with no matches must not exit 0")
	}
	if filterOut != "" {
		t.Fatalf("filter miss must not write a report, stdout=%q", filterOut)
	}
	if got := ExitCode(filterErr); got != 2 {
		t.Fatalf("filter miss exit=%d, want 2; err=%v", got, filterErr)
	}
	if !strings.Contains(filterErr.Error(), "filter matched no events") {
		t.Fatalf("filter miss must name the filter, got %v", filterErr)
	}
	if strings.Contains(filterErr.Error(), "no analyzable events") || strings.Contains(filterErr.Error(), "truncated or corrupt") {
		t.Fatalf("filter miss reused the bad-file message: %v", filterErr)
	}

	partial := writeFormatDescriptionPlusPartialHeader(t)
	badOut, _, badErr := executeAnalyzeLikeMain(t, partial)
	if badErr == nil {
		t.Fatal("partial trailing header must not exit 0")
	}
	if badOut != "" {
		t.Fatalf("bad file must not write a report, stdout=%q", badOut)
	}
	if got := ExitCode(badErr); got != 1 {
		t.Fatalf("partial header exit=%d, want 1; err=%v", got, badErr)
	}
	if !strings.Contains(badErr.Error(), "truncated or corrupt") {
		t.Fatalf("partial header must say the file is truncated or corrupt, got %v", badErr)
	}
	if strings.Contains(badErr.Error(), "filter matched no events") || filterErr.Error() == badErr.Error() {
		t.Fatalf("bad file and filter miss are not distinct\nfilter=%v\nbad=%v", filterErr, badErr)
	}

	garbage := filepath.Join(t.TempDir(), "garbage.binlog")
	if err := os.WriteFile(garbage, []byte("not-a-binlog"), 0o644); err != nil {
		t.Fatalf("write garbage: %v", err)
	}
	_, _, garbageErr := executeAnalyzeLikeMain(t, garbage)
	if garbageErr == nil || ExitCode(garbageErr) == ExitCode(filterErr) && garbageErr.Error() == filterErr.Error() {
		t.Fatalf("garbage file must not look like a filter miss\nfilter=%v\ngarbage=%v", filterErr, garbageErr)
	}
	if ExitCode(garbageErr) != 1 || !strings.Contains(garbageErr.Error(), "not a MySQL binlog") {
		t.Fatalf("garbage exit=%d err=%v, want exit 1 and a bad-magic message", ExitCode(garbageErr), garbageErr)
	}
}

func TestSnapshotNameWithTextFormatFailsBeforeReport(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	fixture := mustFixturePath(t, "minimal.binlog")
	snapshotDir := t.TempDir()

	stdout, stderr, err := captureStdoutStderrRun(t, func() error {
		return runAnalysisWithParserAndTempDirAndReportAndSnapshotOptions(
			[]string{fixture},
			analyzer.DefaultOptions(),
			report.DefaultOptions(),
			"text",
			&model.Snapshot{Name: "footgun"},
			model.FileCoverage{},
			"footgun",
			snapshotDir,
			binlog.NewParser(),
			"",
			nil,
		)
	})
	if err == nil {
		t.Fatalf("text + --snapshot-name rendered a report instead of failing\nstdout=%s\nstderr=%s", stdout, stderr)
	}
	if !strings.Contains(err.Error(), "--snapshot-name requires --format json") {
		t.Fatalf("error must name --format json, got %v", err)
	}
	if strings.Contains(stdout, "=== Summary ===") || strings.TrimSpace(stdout) != "" {
		t.Fatalf("text format must not emit a report when snapshot-name cannot be saved, stdout=%q", stdout)
	}
	if _, statErr := os.Stat(filepath.Join(snapshotDir, "footgun.json")); !os.IsNotExist(statErr) {
		t.Fatalf("snapshot file must not be written, stat=%v", statErr)
	}
}

func TestAnalyzeParseErrorDoesNotShareLineWithProgress(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	fixture := mustFixturePath(t, "minimal.binlog")
	src, err := os.ReadFile(fixture)
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	if len(src) < 200 {
		t.Fatalf("fixture too small: %d", len(src))
	}
	truncated := filepath.Join(t.TempDir(), "truncated.binlog")
	if err := os.WriteFile(truncated, src[:200], 0o644); err != nil {
		t.Fatalf("write truncated: %v", err)
	}

	stdout, stderr, runErr := executeAnalyzeLikeMain(t, truncated)
	if runErr == nil {
		t.Fatal("truncated binlog must fail")
	}
	if stdout != "" {
		t.Fatalf("stdout=%q, want empty", stdout)
	}
	visible := terminalVisibleLines(stderr)
	sawError := false
	for _, line := range visible {
		if !strings.Contains(line, "Error:") {
			continue
		}
		sawError = true
		if strings.Contains(line, "Parsing") {
			t.Fatalf("progress and Error share a visible line: %q\nraw=%q", line, stderr)
		}
	}
	if !sawError {
		t.Fatalf("no visible Error line\nraw=%q\nvisible=%q", stderr, visible)
	}
}

func writeFormatDescriptionPlusPartialHeader(t *testing.T) string {
	t.Helper()
	fdOnly := writeFormatDescriptionOnlyBinlog(t)
	src, err := os.ReadFile(fdOnly)
	if err != nil {
		t.Fatalf("read fd-only: %v", err)
	}
	partial := append(append([]byte{}, src...), 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08)
	path := filepath.Join(t.TempDir(), "fd-plus-partial.binlog")
	if err := os.WriteFile(path, partial, 0o644); err != nil {
		t.Fatalf("write partial: %v", err)
	}
	return path
}

// terminalVisibleLines applies CR and erase-line sequences the way a terminal does.
// CR only moves the cursor; it does not erase. A following newline (or the CR a
// tty inserts before LF) still leaves the text that was written.
func terminalVisibleLines(s string) []string {
	var lines []string
	var line []rune
	col := 0
	data := []rune(s)
	for i := 0; i < len(data); i++ {
		switch data[i] {
		case '\n':
			lines = append(lines, string(line))
			line = line[:0]
			col = 0
		case '\r':
			col = 0
		case '\033':
			if i+1 < len(data) && data[i+1] == '[' {
				i += 2
				for i < len(data) && (data[i] < 0x40 || data[i] > 0x7e) {
					i++
				}
				if i < len(data) && (data[i] == 'K' || data[i] == 'J') {
					line = line[:0]
					col = 0
				}
			}
		default:
			for len(line) < col {
				line = append(line, ' ')
			}
			if col < len(line) {
				line[col] = data[i]
			} else {
				line = append(line, data[i])
			}
			col++
		}
	}
	if len(line) > 0 {
		lines = append(lines, string(line))
	}
	return lines
}
