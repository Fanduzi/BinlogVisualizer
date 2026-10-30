# Split output staging

Publishes a directory of split SQL files as one staged unit (ADR 0025). Shared by `internal/flashback` and `internal/redo`: the package knows file names and ordering, not SQL.

## Members

| File | Responsibility |
|------|----------------|
| `splitout.go` | `Spec` (target dir, per-table mode, statement bound), `CheckDir` publish-target validation (absent or empty dir, parent must exist), `TableFileBase` deterministic `<schema>.<table>` escaping (bytes outside `[A-Za-z0-9_-]` become `%XX` so a dot inside a name can never collide with the separator), and the `Sink` lifecycle: `Open` stages a sibling temp directory, `Create`/`WriteFile`/`Seal` write and fsync each file in manifest order, `Publish` writes `MANIFEST` last and renames the staging dir onto the target — a failure or `Abort` removes the staging dir and leaves nothing behind. |

## Interfaces

| API | Contract |
|-----|----------|
| `Spec{Dir, PerTable, MaxStatements}` | How one run's output partitions into files. `PerTable` is flashback-only (ADR 0025); `MaxStatements` 0 means unlimited. |
| `CheckDir(dir) error` | Usage-error (exit 3) validation before any work. |
| `TableFileBase(schema, table) string` | Filesystem-safe `<schema>.<table>` base name. |
| `Open(spec) (*Sink, error)` | Creates the staging directory next to `spec.Dir`. |
| `(*Sink).Create(name) (*os.File, error)` | Opens one file inside staging, recorded in manifest order. |
| `(*Sink).WriteFile(name, fn) error` | Streams `fn(w)` through a buffered writer into a fresh file, then seals it. |
| `(*Sink).Seal(f) error` | Fsyncs and closes a file opened by `Create`. |
| `(*Sink).Publish() error` | Writes `MANIFEST`, fsyncs it, renames staging onto the target (removing a pre-existing empty dir first). |
| `(*Sink).Abort()` | Removes the staging directory; a no-op after `Publish`. |
| `ManifestName` | `"MANIFEST"` — the comment-prefixed apply-order list. |

## Dependencies

- Upstream: `internal/clierr` (usage/output error kinds), `internal/i18n` (localized errors).
- Downstream: `internal/flashback`, `internal/redo`, `cmd/binlogviz`. This package does not import them.

## Update Rule

If members, interfaces, or dependencies change, update this file in the same change.
