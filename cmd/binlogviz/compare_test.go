package binlogviz

import (
	"bytes"
	"encoding/json"
	"io"
	"path/filepath"
	"strings"
	"testing"
)

func TestRootCommandRegistersCompareCommand(t *testing.T) {
	cmd := NewRootCommand()
	found := false
	for _, child := range cmd.Commands() {
		if child.Name() == "compare" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected compare command to be registered")
	}
}

func TestCompareCommandRequiresTwoFiles(t *testing.T) {
	cmd := newCompareCommand()
	cmd.SetArgs([]string{"only-one.json"})

	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "accepts 2 arg(s), received 1") {
		t.Fatalf("expected exact-args error, got %v", err)
	}
}

func TestCompareCommandRejectsUnknownFormat(t *testing.T) {
	cmd := newCompareCommand()
	cmd.SetArgs([]string{"a.json", "b.json", "--format", "markdown"})

	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "unsupported compare format") {
		t.Fatalf("expected format error, got %v", err)
	}
}

func TestCompareCommandTextOutputContainsCompareSummary(t *testing.T) {
	cmd := newCompareCommand()
	cmd.SetArgs([]string{
		filepath.Join("..", "..", "internal", "compare", "testdata", "current.json"),
		filepath.Join("..", "..", "internal", "compare", "testdata", "baseline.json"),
		"--format", "text",
	})

	output := &bytes.Buffer{}
	cmd.SetOut(output)
	cmd.SetErr(io.Discard)

	if err := cmd.Execute(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, token := range []string{"Compare Summary", "Top Table Changes", "orders.refunds"} {
		if !strings.Contains(output.String(), token) {
			t.Fatalf("expected text output to contain %q, got %s", token, output.String())
		}
	}
}

func TestCompareCommandJSONOutputIsValidJSON(t *testing.T) {
	cmd := newCompareCommand()
	cmd.SetArgs([]string{
		filepath.Join("..", "..", "internal", "compare", "testdata", "current.json"),
		filepath.Join("..", "..", "internal", "compare", "testdata", "baseline.json"),
		"--format", "json",
	})

	output := &bytes.Buffer{}
	cmd.SetOut(output)
	cmd.SetErr(io.Discard)

	if err := cmd.Execute(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(output.Bytes(), &decoded); err != nil {
		t.Fatalf("expected valid json: %v", err)
	}
}

func TestCompareCommandHTMLOutputContainsHTMLDocument(t *testing.T) {
	cmd := newCompareCommand()
	cmd.SetArgs([]string{
		filepath.Join("..", "..", "internal", "compare", "testdata", "current.json"),
		filepath.Join("..", "..", "internal", "compare", "testdata", "baseline.json"),
		"--format", "html",
	})

	output := &bytes.Buffer{}
	cmd.SetOut(output)
	cmd.SetErr(io.Discard)

	if err := cmd.Execute(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, token := range []string{"<html", "compare-top-tables", "compare-alerts"} {
		if !strings.Contains(output.String(), token) {
			t.Fatalf("expected html output to contain %q, got %s", token, output.String())
		}
	}
}

func TestCompareCommandReportsMissingInputFile(t *testing.T) {
	cmd := newCompareCommand()
	cmd.SetArgs([]string{
		filepath.Join("..", "..", "internal", "compare", "testdata", "missing.json"),
		filepath.Join("..", "..", "internal", "compare", "testdata", "baseline.json"),
		"--format", "json",
	})

	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "load current report") {
		t.Fatalf("expected missing-file error, got %v", err)
	}
}

func TestCompareCommandRejectsInvalidReportShape(t *testing.T) {
	cmd := newCompareCommand()
	cmd.SetArgs([]string{
		filepath.Join("..", "..", "internal", "compare", "testdata", "current.json"),
		filepath.Join("..", "..", "internal", "compare", "testdata", "foreign.json"),
		"--format", "json",
	})

	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "load baseline report") || !strings.Contains(err.Error(), "unsupported BinlogViz report shape") {
		t.Fatalf("expected invalid-shape error, got %v", err)
	}
}
