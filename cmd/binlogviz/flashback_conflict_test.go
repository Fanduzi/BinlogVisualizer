package binlogviz

import (
	"errors"
	"testing"

	"binlogviz/internal/flashback"
)

// #259: a conflict refusal has its own exit code; other errors keep theirs.
func TestFlashbackConflictExitCode(t *testing.T) {
	err := flashbackExit(&flashback.FlashbackConflictError{Count: 1})
	if ExitCode(err) != 7 {
		t.Fatalf("conflict exit %d", ExitCode(err))
	}
	if ExitCode(flashbackExit(errors.New("refused"))) != 6 {
		t.Fatal("other refusals are exit 6")
	}
}
