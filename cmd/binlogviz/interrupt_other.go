//go:build !unix

package binlogviz

import (
	"os"
	"syscall"
)

func notifySignals() []os.Signal {
	return []os.Signal{syscall.SIGINT, syscall.SIGTERM}
}

func signalExitCode(sig os.Signal) int {
	switch sig {
	case syscall.SIGINT:
		return 130
	case syscall.SIGTERM:
		return 143
	default:
		return 1
	}
}

func stdinCleanupSignals() []signalCleanupCase {
	return []signalCleanupCase{
		{name: "SIGINT", sig: syscall.SIGINT, code: 130},
		{name: "SIGTERM", sig: syscall.SIGTERM, code: 143},
	}
}
