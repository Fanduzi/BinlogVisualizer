// Package snapshot provides filesystem-backed snapshot persistence helpers.
// input: snapshot names, optional snapshot directories, and JSON report bytes.
// output: resolved snapshot directories, validated names, and overwrite-safe JSON files on disk.
// pos: CLI-facing snapshot persistence primitives for future snapshot commands.
// note: if this file changes, update this header and module README.md.
package snapshot

import (
	"encoding/json"
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
	Name      string `json:"name"`
	Label     string `json:"label"`
	Path      string `json:"path"`
	CreatedAt string `json:"created_at"`
	InputMode string `json:"input_mode"`
	Window    Window `json:"window"`
}

// Summary captures top-level analyze totals stored with a snapshot.
type Summary struct {
	TotalTransactions int `json:"total_transactions"`
	TotalRows         int `json:"total_rows"`
	TotalEvents       int `json:"total_events"`
}

// Input captures the source information stored in snapshot metadata.
type Input struct {
	Files   []string `json:"files"`
	FromDir string   `json:"from_dir"`
	Prefix  string   `json:"prefix"`
}

// Window captures the requested analyze time window stored in snapshot metadata.
type Window struct {
	StartTime string `json:"start_time"`
	EndTime   string `json:"end_time"`
}

// Filters captures include/exclude filter configuration stored in snapshot metadata.
type Filters struct {
	IncludeSchemas []string `json:"include_schema"`
	ExcludeSchemas []string `json:"exclude_schema"`
	IncludeTables  []string `json:"include_table"`
	ExcludeTables  []string `json:"exclude_table"`
}

// Descriptor is the normalized command-facing view of one stored snapshot.
type Descriptor struct {
	Name             string  `json:"name"`
	Label            string  `json:"label"`
	Path             string  `json:"path"`
	CreatedAt        string  `json:"created_at"`
	BinlogvizVersion string  `json:"binlogviz_version"`
	InputMode        string  `json:"input_mode"`
	Input            Input   `json:"input"`
	Window           Window  `json:"window"`
	Filters          Filters `json:"filters"`
	Summary          Summary `json:"summary"`
	Warnings         int     `json:"warnings"`
}

type storedSnapshot struct {
	Summary  Summary                 `json:"summary"`
	Warnings int                     `json:"warnings"`
	Snapshot *storedSnapshotMetadata `json:"snapshot"`
}

type storedSnapshotMetadata struct {
	Name             string  `json:"name"`
	Label            string  `json:"label"`
	CreatedAt        string  `json:"created_at"`
	BinlogvizVersion string  `json:"binlogviz_version"`
	InputMode        string  `json:"input_mode"`
	Input            Input   `json:"input"`
	Window           Window  `json:"window"`
	Filters          Filters `json:"filters"`
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
		path := filepath.Join(resolvedDir, entry.Name())
		resultEntry := Entry{
			Name: name,
			Path: path,
		}
		if data, err := os.ReadFile(path); err == nil {
			desc, err := decodeDescriptor(path, data, name)
			if err == nil {
				resultEntry.Label = desc.Label
				resultEntry.CreatedAt = desc.CreatedAt
				resultEntry.InputMode = desc.InputMode
				resultEntry.Window = desc.Window
			}
		}
		result = append(result, resultEntry)
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

// DescribeSnapshot loads one named snapshot and returns normalized metadata and summary details.
func DescribeSnapshot(dir, name string) (Descriptor, error) {
	path, data, err := LoadSnapshot(dir, name)
	if err != nil {
		return Descriptor{}, err
	}
	return decodeDescriptor(path, data, name)
}

// RenameSnapshot moves one snapshot to a new name and keeps stored snapshot identity consistent.
func RenameSnapshot(dir, oldName, newName string) (string, error) {
	if err := ValidateName(oldName); err != nil {
		return "", err
	}
	if err := ValidateName(newName); err != nil {
		return "", err
	}

	oldPath, data, err := LoadSnapshot(dir, oldName)
	if err != nil {
		return "", err
	}

	updated, err := renameSnapshotPayload(data, oldName, newName)
	if err != nil {
		return "", err
	}

	newPath, err := SaveJSON(dir, newName, updated)
	if err != nil {
		return "", err
	}
	if err := os.Remove(oldPath); err != nil {
		_ = os.Remove(newPath)
		return "", err
	}
	return newPath, nil
}

// DeleteSnapshot removes one named snapshot file from the store.
func DeleteSnapshot(dir, name string) (string, error) {
	if err := ValidateName(name); err != nil {
		return "", err
	}

	resolvedDir, err := ResolveSnapshotDir(dir)
	if err != nil {
		return "", err
	}

	path := filepath.Join(resolvedDir, name+jsonExtension)
	if err := os.Remove(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("snapshot %q not found", name)
		}
		return "", err
	}
	return path, nil
}

func decodeDescriptor(path string, data []byte, fallbackName string) (Descriptor, error) {
	var stored storedSnapshot
	if err := json.Unmarshal(data, &stored); err != nil {
		return Descriptor{}, err
	}

	desc := Descriptor{
		Name:     fallbackName,
		Label:    fallbackName,
		Path:     path,
		Summary:  stored.Summary,
		Warnings: stored.Warnings,
	}
	if stored.Snapshot == nil {
		return desc, nil
	}

	if stored.Snapshot.Name != "" {
		desc.Name = stored.Snapshot.Name
	}
	if stored.Snapshot.Label != "" {
		desc.Label = stored.Snapshot.Label
	}
	desc.CreatedAt = stored.Snapshot.CreatedAt
	desc.BinlogvizVersion = stored.Snapshot.BinlogvizVersion
	desc.InputMode = stored.Snapshot.InputMode
	desc.Input = stored.Snapshot.Input
	desc.Window = stored.Snapshot.Window
	desc.Filters = stored.Snapshot.Filters
	return desc, nil
}

func renameSnapshotPayload(data []byte, oldName, newName string) ([]byte, error) {
	var payload map[string]any
	if err := json.Unmarshal(data, &payload); err != nil {
		return nil, err
	}

	snapshot, ok := payload["snapshot"].(map[string]any)
	if !ok {
		snapshot = map[string]any{}
	}
	snapshot["name"] = newName
	if label, ok := snapshot["label"].(string); !ok || label == "" || label == oldName {
		snapshot["label"] = newName
	}
	payload["snapshot"] = snapshot

	return json.MarshalIndent(payload, "", "  ")
}
