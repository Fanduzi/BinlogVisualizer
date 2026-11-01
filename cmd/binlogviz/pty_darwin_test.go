package binlogviz

import (
	"os"
	"testing"

	"github.com/creack/pty"
	"golang.org/x/term"
)

func openTestTTY(t *testing.T) *os.File {
	t.Helper()
	master, slave, err := pty.Open()
	if err != nil {
		t.Fatalf("open pty: %v", err)
	}
	t.Cleanup(func() { _ = master.Close(); _ = slave.Close() })
	if !term.IsTerminal(int(slave.Fd())) {
		t.Fatal("pty is not a tty")
	}
	return slave
}
