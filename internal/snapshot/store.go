// Package snapshot provides filesystem-backed snapshot persistence helpers.
// input: snapshot names, optional snapshot directories, and JSON report bytes.
// output: resolved snapshot directories, validated names, and overwrite-safe JSON files on disk.
// pos: CLI-facing snapshot persistence primitives for future snapshot commands.
// note: if this file changes, update this header and module README.md.
package snapshot

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"unicode"
)

const (
	defaultSnapshotRoot = ".binlogviz"
	snapshotDirName     = "snapshots"
	jsonExtension       = ".json"
)

// Entry identifies one stored snapshot file.
type Entry struct {
	Name string
	Path string
}

// DefaultSnapshotDir returns the default snapshot directory under the provided home directory.
func DefaultSnapshotDir(home string) (string, error) {
	if home == "" {
		var err error
		home, err = os.UserHomeDir()
		if err != nil {
			return "", err
		}
	}
	if home == "" {
		return "", errors.New("snapshot home directory is empty")
	}
	return filepath.Join(home, defaultSnapshotRoot, snapshotDirName), nil
}

// ResolveSnapshotDir returns the explicit snapshot directory or the default home-based directory.
func ResolveSnapshotDir(explicit string) (string, error) {
	if explicit != "" {
		return filepath.Abs(explicit)
	}
	return DefaultSnapshotDir("")
}

// ValidateName ensures the snapshot name is safe to use as a single file stem.
func ValidateName(name string) error {
	if strings.TrimSpace(name) != name {
		return fmt.Errorf("invalid snapshot name %q: must not contain leading or trailing spaces", name)
	}
	if name == "" {
		return errors.New("snapshot name is required")
	}
	if name == "." || name == ".." {
		return fmt.Errorf("invalid snapshot name %q: reserved path name", name)
	}
	if filepath.Base(name) != name {
		return fmt.Errorf("invalid snapshot name %q: must not contain path separators", name)
	}
	for _, r := range name {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '-' || r == '_' {
			continue
		}
		return fmt.Errorf("invalid snapshot name %q: contains unsupported character %q", name, r)
	}
	return nil
}

// SaveJSON writes the report to dir/name.json and rejects overwrites.
func SaveJSON(dir, name string, report []byte) (string, error) {
	if err := ValidateName(name); err != nil {
		return "", err
	}

	resolvedDir, err := ResolveSnapshotDir(dir)
	if err != nil {
		return "", err
	}

	if err := os.MkdirAll(resolvedDir, 0o755); err != nil {
		return "", err
	}

	path := filepath.Join(resolvedDir, name+jsonExtension)
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		if os.IsExist(err) {
			return "", fmt.Errorf("snapshot file %q already exists", path)
		}
		return "", err
	}
	defer file.Close()

	if _, err := file.Write(report); err != nil {
		return "", err
	}

	return path, nil
}

// ListSnapshots returns all stored snapshots in stable name order.
func ListSnapshots(dir string) ([]Entry, error) {
	resolvedDir, err := ResolveSnapshotDir(dir)
	if err != nil {
		return nil, err
	}

	entries, err := os.ReadDir(resolvedDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return []Entry{}, nil
		}
		return nil, err
	}

	result := make([]Entry, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != jsonExtension {
			continue
		}
		name := strings.TrimSuffix(entry.Name(), jsonExtension)
		result = append(result, Entry{
			Name: name,
			Path: filepath.Join(resolvedDir, entry.Name()),
		})
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return result, nil
}

// LoadSnapshot reads one named snapshot file.
func LoadSnapshot(dir, name string) (string, []byte, error) {
	if err := ValidateName(name); err != nil {
		return "", nil, err
	}

	resolvedDir, err := ResolveSnapshotDir(dir)
	if err != nil {
		return "", nil, err
	}

	path := filepath.Join(resolvedDir, name+jsonExtension)
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", nil, fmt.Errorf("snapshot %q not found", name)
		}
		return "", nil, err
	}
	return path, data, nil
}
