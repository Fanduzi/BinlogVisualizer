package binlogviz

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

func TestTrendAndSnapshotFailuresPrintErrorOnce(t *testing.T) {
	forceEnglishRuntimeOutput(t)

	cases := [][]string{
		{"trend"},
		{"snapshot", "save"},
		{"snapshot", "show"},
		{"snapshot", "rename"},
		{"snapshot", "delete"},
	}
	for _, args := range cases {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			stdout, stderr, err := captureStdoutStderrRun(t, func() error {
				cmd := NewRootCommand()
				cmd.SetArgs(args)
				runErr := cmd.Execute()
				if runErr != nil {
					fmt.Fprintln(os.Stderr, "Error:", runErr)
				}
				return runErr
			})
			if err == nil {
				t.Fatal("expected error")
			}
			if stdout != "" {
				t.Fatalf("stdout = %q, want empty", stdout)
			}
			if strings.Count(stderr, "Error:") != 1 {
				t.Fatalf("stderr Error: count = %d, want 1\n%s", strings.Count(stderr, "Error:"), stderr)
			}
			if strings.Contains(stderr, "Usage:") {
				t.Fatalf("stderr contains Usage:\n%s", stderr)
			}
		})
	}
}
