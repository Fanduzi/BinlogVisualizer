package binlogviz

import (
	"os"
	"testing"

	"golang.org/x/sys/unix"
	"golang.org/x/term"
)

func openTestTTY(t *testing.T) *os.File {
	t.Helper()
	master, err := os.OpenFile("/dev/ptmx", os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open ptmx: %v", err)
	}
	t.Cleanup(func() { _ = master.Close() })
	if err := unix.IoctlSetPointerInt(int(master.Fd()), unix.TIOCSPTLCK, 0); err != nil {
		t.Fatalf("unlockpt: %v", err)
	}
	fd, _, errno := unix.Syscall(unix.SYS_IOCTL, uintptr(master.Fd()), uintptr(unix.TIOCGPTPEER), uintptr(unix.O_RDWR|unix.O_NOCTTY))
	if errno != 0 {
		t.Fatalf("pty peer: %v", errno)
	}
	slave := os.NewFile(fd, "ptyslave")
	if !term.IsTerminal(int(slave.Fd())) {
		t.Fatal("slave is not a tty")
	}
	return slave
}
