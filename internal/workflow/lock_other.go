//go:build !unix

package workflow

import (
	"os"
	"path/filepath"

	"binlogviz/internal/clierr"
)

// LockDir on non-unix platforms uses an O_EXCL lock file; a stale file left
// by a crash must be removed by hand (the error names it).
func LockDir(root string) (func(), error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, clierr.Errorf(clierr.KindOutput, "create workflow dir: %w", err)
	}
	path := filepath.Join(root, LockFile)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		if os.IsExist(err) {
			return nil, lockedErr(path)
		}
		return nil, clierr.Errorf(clierr.KindOutput, "open %s: %w", path, err)
	}
	return func() { _ = f.Close(); releaseLockFile(path) }, nil
}

// releaseLockFile removes the non-unix O_EXCL lock file. It is the only
// other removal in workflow code and touches nothing but <root>/.lock.
func releaseLockFile(path string) {
	if filepath.Base(path) == LockFile {
		_ = os.Remove(path)
	}
}
