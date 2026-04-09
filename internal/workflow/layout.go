package workflow

import (
	"os"
	"path/filepath"
)

// ArtifactPath returns the deterministic path for a workflow artifact.
func ArtifactPath(root, kind, name, format string) string {
	filename := name + "." + format
	return filepath.Join(root, kind, filename)
}

// EnsureLayout creates the artifact directory tree under root.
func EnsureLayout(root string) error {
	for _, dir := range []string{
		root,
		filepath.Join(root, "analyze"),
		filepath.Join(root, "compare"),
		filepath.Join(root, "trend"),
	} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	return nil
}
