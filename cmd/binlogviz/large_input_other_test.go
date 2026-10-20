//go:build !linux

package binlogviz

import "os/exec"

func setChildDeathSignal(cmd *exec.Cmd) {}
