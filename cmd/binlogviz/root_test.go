package binlogviz

import "testing"

func TestNewRootCommand(t *testing.T) {
	cmd := NewRootCommand()
	if cmd.Use != "binlogviz" {
		t.Fatalf("unexpected root use: %s", cmd.Use)
	}
}
