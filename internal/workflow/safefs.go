package workflow

// safefs.go is the only place workflow code may remove or rename files
// (enforced by TestWorkflowDeletesOnlyThroughPrimitive). ADR 0009 §8.

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"binlogviz/internal/clierr"
)

// ReasonTrustBoundary is details.reason when a path leaves the workflow root.
const ReasonTrustBoundary = "trust_boundary"

// TrashDirName is the per-root directory clean moves deleted files into.
const TrashDirName = ".trash"

func errUntrustedPath(path, why string) error {
	return clierr.Errorf(clierr.KindCorrupt, "path %q: %s", path, why)
}

// validateRelPath accepts only clean, slash-separated, relative paths with no
// "..", no empty or hidden components, and no backslashes.
func validateRelPath(rel string) error {
	if rel == "" || strings.ContainsAny(rel, "\\\x00") || filepath.IsAbs(rel) || strings.HasPrefix(rel, "/") || filepath.VolumeName(rel) != "" {
		return errUntrustedPath(rel, "must be relative")
	}
	for _, part := range strings.Split(rel, "/") {
		if part == "" || part == "." || part == ".." || strings.HasPrefix(part, ".") {
			return errUntrustedPath(rel, "must not contain empty, '.', '..' or hidden components")
		}
	}
	return nil
}

func trustErr(format string, args ...any) error {
	return clierr.Reasonf(clierr.KindRefused, ReasonTrustBoundary, format, args...)
}

// realDir resolves root and requires it to be a directory.
func realDir(root string) (string, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return "", clierr.Errorf(clierr.KindInput, "resolve %s: %w", root, err)
	}
	real, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return "", clierr.Errorf(clierr.KindInput, "resolve %s: %w", root, err)
	}
	fi, err := os.Stat(real)
	if err != nil || !fi.IsDir() {
		return "", clierr.Errorf(clierr.KindInput, "%s is not a directory", root)
	}
	return real, nil
}

// CheckKindDir verifies <root>/<kind> is absent or a real directory
// directly inside the real root (not a symlink). Anything else is refused
// (6, trust_boundary) so clean never reaches through it, --force or not.
func CheckKindDir(root, kind string) (exists bool, err error) {
	real, err := realDir(root)
	if err != nil {
		return false, err
	}
	p := filepath.Join(real, kind)
	fi, err := os.Lstat(p)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, clierr.Errorf(clierr.KindInput, "inspect %s: %w", p, err)
	}
	if fi.Mode()&fs.ModeSymlink != 0 || !fi.IsDir() {
		return false, trustErr("refusing to clean: %s is a symlink or not a directory; clean only acts on real directories inside the workflow root", filepath.Join(root, kind))
	}
	return true, nil
}

// lstatChain refuses a symlink at any component of rel under real and
// requires the final component to be a regular file.
func lstatChain(real, rel string) error {
	parts := strings.Split(rel, "/")
	p := real
	for i, part := range parts {
		p = filepath.Join(p, part)
		fi, err := os.Lstat(p)
		if err != nil {
			return err
		}
		if fi.Mode()&fs.ModeSymlink != 0 {
			return trustErr("refusing to delete %s: %s is a symlink", rel, p)
		}
		last := i == len(parts)-1
		if (!last && !fi.IsDir()) || (last && !fi.Mode().IsRegular()) {
			return trustErr("refusing to delete %s: %s is not a regular file", rel, p)
		}
	}
	return nil
}

// IsPlainFile reports whether root/rel is a regular file reached without
// following any symlink.
func IsPlainFile(root, rel string) bool {
	if validateRelPath(rel) != nil {
		return false
	}
	real, err := realDir(root)
	if err != nil {
		return false
	}
	return lstatChain(real, rel) == nil
}

// TrashFile is the single deletion primitive: it moves root/rel to
// root/.trash/<trashID>/rel with a same-filesystem rename inside os.Root,
// after refusing absolute paths, "..", and symlinks at every component. It
// returns the absolute trash directory (root/.trash/<trashID>).
func TrashFile(root, rel, trashID string) (string, error) {
	if err := validateRelPath(rel); err != nil {
		return "", trustErr("refusing to delete %q: %v", rel, err)
	}
	if err := validateRelPath(trashID); err != nil || strings.Contains(trashID, "/") {
		return "", trustErr("invalid trash id %q", trashID)
	}
	real, err := realDir(root)
	if err != nil {
		return "", err
	}
	if err := lstatChain(real, rel); err != nil {
		return "", err
	}
	r, err := os.OpenRoot(real)
	if err != nil {
		return "", clierr.Errorf(clierr.KindOutput, "open %s: %w", real, err)
	}
	defer r.Close()
	dstRel := TrashDirName + "/" + trashID + "/" + rel
	dir := ""
	for _, part := range strings.Split(filepath.ToSlash(filepath.Dir(dstRel)), "/") {
		dir = strings.TrimPrefix(dir+"/"+part, "/")
		if err := r.Mkdir(dir, 0o700); err != nil && !errors.Is(err, fs.ErrExist) {
			return "", clierr.Errorf(clierr.KindOutput, "create trash dir %s: %w", dir, err)
		}
		fi, err := r.Lstat(dir)
		if err != nil {
			return "", clierr.Errorf(clierr.KindOutput, "inspect trash dir %s: %w", dir, err)
		}
		if fi.Mode()&fs.ModeSymlink != 0 || !fi.IsDir() {
			return "", trustErr("refusing to delete %s: trash path %s is a symlink or not a directory", rel, dir)
		}
	}
	if err := r.Rename(rel, dstRel); err != nil {
		return "", clierr.Errorf(clierr.KindOutput, "move %s to trash: %w", rel, err)
	}
	return filepath.Join(real, TrashDirName, trashID), nil
}

// releaseLockFile removes the non-unix O_EXCL lock file. It is the only
// other removal in workflow code and touches nothing but <root>/.lock.
func releaseLockFile(path string) {
	if filepath.Base(path) == LockFile {
		_ = os.Remove(path)
	}
}
