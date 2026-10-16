//go:build unix

package workflow

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"

	"binlogviz/internal/clierr"
)

// LockDir takes an exclusive, non-blocking flock on <root>/.lock. A
// concurrent holder makes it fail with 6 (reason locked).
func LockDir(root string) (func(), error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, clierr.Errorf(clierr.KindOutput, "create workflow dir: %w", err)
	}
	path := filepath.Join(root, LockFile)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|syscall.O_NOFOLLOW, 0o644)
	if err != nil {
		return nil, clierr.Errorf(clierr.KindOutput, "open %s: %w", path, err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, lockedErr(path)
		}
		return nil, clierr.Errorf(clierr.KindOutput, "lock %s: %w", path, err)
	}
	return func() {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		_ = f.Close()
	}, nil
}
