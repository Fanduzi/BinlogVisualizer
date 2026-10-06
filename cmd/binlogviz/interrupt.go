package binlogviz

import (
	"os"
	"os/signal"
	"sync"

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

// InstallInterruptHandler removes stdin/pipe temp copies on the platform's
// termination signals, prints one Error line, and exits with 128+signal.
func InstallInterruptHandler() {
	interruptOnce.Do(func() {
		sigs := notifySignals()
		ch := make(chan os.Signal, len(sigs))
		signal.Notify(ch, sigs...)
		go func() {
			sig := <-ch
			cleanupTrackedStdinTemps()
			PrintCommandError(os.Stderr, interruptError{})
			os.Exit(signalExitCode(sig))
		}()
	})
}

type signalCleanupCase struct {
	name string
	sig  os.Signal
	code int
}

type interruptError struct{}

func (interruptError) Error() string {
	return i18n.T("error.interrupted")
}
