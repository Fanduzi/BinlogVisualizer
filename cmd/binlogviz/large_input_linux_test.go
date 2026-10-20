//go:build linux

package binlogviz

import (
	"os/exec"
	"syscall"
)

// setChildDeathSignal makes the kernel kill the child if this process dies.
// A go test timeout can SIGKILL the parent before cleanup; without this the
// child keeps running and holds the temp files.
func setChildDeathSignal(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}
}
