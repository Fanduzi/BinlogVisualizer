package binlogviz

import (
	"os"
	"os/signal"
	"sync"
	"syscall"

	"binlogviz/internal/i18n"
)

// stdin temps are removed on the normal return path and from the signal
// handler. os.Exit skips defers, so the handler has to delete them itself.
var (
	stdinTempMu   sync.Mutex
	stdinTempDirs = map[string]struct{}{}
	interruptOnce sync.Once
)

func trackStdinTemp(dir string) {
	if dir == "" {
		return
	}
	stdinTempMu.Lock()
	stdinTempDirs[dir] = struct{}{}
	stdinTempMu.Unlock()
}

func releaseStdinTemp(dir string) {
	if dir == "" {
		return
	}
	stdinTempMu.Lock()
	delete(stdinTempDirs, dir)
	stdinTempMu.Unlock()
	_ = os.RemoveAll(dir)
}

func cleanupTrackedStdinTemps() {
	stdinTempMu.Lock()
	dirs := make([]string, 0, len(stdinTempDirs))
	for dir := range stdinTempDirs {
		dirs = append(dirs, dir)
	}
	stdinTempDirs = map[string]struct{}{}
	stdinTempMu.Unlock()
	for _, dir := range dirs {
		_ = os.RemoveAll(dir)
	}
}

// InstallInterruptHandler removes stdin/pipe temp copies on SIGINT and SIGTERM,
// prints one Error line, and exits 130 or 143.
func InstallInterruptHandler() {
	interruptOnce.Do(func() {
		ch := make(chan os.Signal, 2)
		signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
		go func() {
			sig := <-ch
			cleanupTrackedStdinTemps()
			PrintCommandError(os.Stderr, interruptError{})
			os.Exit(signalExitCode(sig))
		}()
	})
}

type interruptError struct{}

func (interruptError) Error() string {
	return i18n.T("error.interrupted")
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
