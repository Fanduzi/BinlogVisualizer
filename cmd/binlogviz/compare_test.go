package binlogviz

import (
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
