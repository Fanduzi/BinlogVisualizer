# Snapshot Module

Filesystem-backed helpers for storing, listing, describing, renaming, deleting, and loading named analyze JSON snapshots.

## Files

| File | Responsibility |
|------|----------------|
| `store.go` | Resolves default snapshot directories, validates names, saves JSON payloads, lists snapshots with metadata, loads saved snapshots, and performs rename/delete operations. |
| `store_test.go` | Covers directory resolution, name validation, overwrite rejection, listing, descriptor extraction, and rename/delete behavior. |

## Exports

- `DefaultSnapshotDir(home string) (string, error)` — Returns the home-based default snapshot directory.
- `ResolveSnapshotDir(explicit string) (string, error)` — Resolves the effective snapshot directory from an explicit override or the default home-based path.
- `ValidateName(name string) error` — Rejects unsafe or unsupported snapshot names.
- `SaveJSON(dir, name string, report []byte) (string, error)` — Persists one analyze JSON payload as `<name>.json` without overwriting.
- `ListSnapshots(dir string) ([]Entry, error)` — Lists saved snapshots in stable name order with basic metadata when available.
- `LoadSnapshot(dir, name string) (string, []byte, error)` — Loads one named snapshot and returns its resolved path plus raw bytes.
- `DescribeSnapshot(dir, name string) (Descriptor, error)` — Returns normalized command-facing metadata and summary details for one stored snapshot.
- `RenameSnapshot(dir, oldName, newName string) (string, error)` — Renames one stored snapshot and keeps the JSON snapshot identity in sync.
- `DeleteSnapshot(dir, name string) (string, error)` — Removes one stored snapshot from disk.

## Dependencies

- Upstream:
  - `cmd/binlogviz/analyze.go`, `cmd/binlogviz/compare.go`, and `cmd/binlogviz/snapshot.go` call into this module.

## Notes

- The default snapshot store is `~/.binlogviz/snapshots`.
- Snapshot save helpers return the resolved file path so command code can report it on `stderr`.
- Snapshot names are intentionally restricted to a single file stem using only letters, digits, `-`, and `_`.
- Rename updates `snapshot.name` and only rewrites `snapshot.label` when the label still matches the old name.
