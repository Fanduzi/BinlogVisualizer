package binlogviz

import "testing"

func TestNewRootCommand(t *testing.T) {
	cmd := NewRootCommand()
	if cmd.Use != "binlogviz" {
		t.Fatalf("unexpected root use: %s", cmd.Use)
	}
	got, _, _ := cmd.Find([]string{"snapshot"})
	if got == nil || got.Use != "snapshot" {
		t.Fatalf("expected snapshot subcommand to be registered, got %#v", got)
	}
	for _, subcommand := range []string{"save", "list", "show"} {
		found, _, err := cmd.Find([]string{"snapshot", subcommand})
		if err != nil {
			t.Fatalf("find snapshot %s: %v", subcommand, err)
		}
		if found == nil || found.Name() != subcommand {
			t.Fatalf("expected snapshot %s subcommand to be registered, got %#v", subcommand, found)
		}
	}
}
