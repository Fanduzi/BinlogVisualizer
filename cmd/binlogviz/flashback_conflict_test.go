package binlogviz

import (
	"errors"
	"testing"

	"binlogviz/internal/analyzer"
)

// #259: a conflict refusal has its own exit code; other errors keep theirs.
func TestFlashbackConflictExitCode(t *testing.T) {
	err := flashbackExit(&analyzer.FlashbackConflictError{Count: 1})
	if ExitCode(err) != 7 {
		t.Fatalf("conflict exit %d", ExitCode(err))
	}
	if ExitCode(flashbackExit(errors.New("refused"))) != 1 {
		t.Fatal("other refusals keep exit 1 until typed errors land")
	}
}
