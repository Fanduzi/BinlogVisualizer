//go:build unix

package binlogviz

import (
	"os"
	"syscall"
)

func notifySignals() []os.Signal {
	return []os.Signal{syscall.SIGHUP, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM}
}

func signalExitCode(sig os.Signal) int {
	switch sig {
	case syscall.SIGHUP:
		return 129
	case syscall.SIGINT:
		return 130
	case syscall.SIGQUIT:
		return 131
	case syscall.SIGTERM:
		return 143
	default:
		return 1
	}
}

func stdinCleanupSignals() []signalCleanupCase {
	return []signalCleanupCase{
		{name: "SIGHUP", sig: syscall.SIGHUP, code: 129},
		{name: "SIGINT", sig: syscall.SIGINT, code: 130},
		{name: "SIGQUIT", sig: syscall.SIGQUIT, code: 131},
		{name: "SIGTERM", sig: syscall.SIGTERM, code: 143},
	}
}
