package binlogviz

import (
	"strings"
	"testing"
)

func TestFlashbackExperimentalWarnOnUsageError(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	const warn = "warning: flashback is experimental"
	for _, args := range [][]string{
		{"flashback"},
		{"flashback", "--bogus-flag"},
		{"flashback", "verify"},
		{"flashback", "verify", "--bogus-flag"},
		{"flashback", "verify", "a.sql", "b.sql"},
	} {
		code, _, stderr := runRoot(t, args...)
		if code == 0 {
			t.Fatalf("%v unexpectedly succeeded", args)
		}
		if got := strings.Count(stderr, warn); got != 1 {
			t.Fatalf("%v warning count %d\nstderr:\n%s", args, got, stderr)
		}
	}
}
