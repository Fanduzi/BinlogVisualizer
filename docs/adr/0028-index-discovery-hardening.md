# ADR 0028: Harden `--index-file` and directory/prefix discovery against malformed manifests

Status: accepted (2026-11-02). Roadmap #256: the last input-discovery item — `--from-dir` + `--index-file`/`--prefix` must not silently escape the named directory or admit manifest entries a real `binlog.index` cannot produce. This ADR is a pair with ADR 0027 (`--thread-id`); they are separate records because the surfaces are disjoint (a CLI input-path trust boundary versus an analyzer group-selection seam).

## Context

Code evidence that shaped the decisions:

- **`readIndexFilePaths` today:** read the index, split lines, `TrimSpace` each entry, `filepath.Join(dir, entry)` for relative names (absolute names pass through untouched), `os.Stat` and reject missing/`IsDir`, append in manifest order. That is the entire contract — no containment check, no duplicate check, no self-listing check.
- **Escaped entries were admitted silently.** `../other/x` joins to a real file outside `--from-dir`; an absolute path skips the join entirely; a symlink inside the dir pointing outside resolves through `os.Stat`. All three reached the parser — the manifest, operator-supplied input, could read files the operator never named.
- **Duplicates were admitted silently.** `x` and `./x` are two read passes over one file, double-counting every event; a symlink alias does the same. Real manifests never list a file twice — the index is write-order-unique by construction.
- **Self-listing was admitted silently.** An index that names `binlog.index` (or `./binlog.index`, or an absolute spelling, or a hardlink) fed a manifest file to the binlog parser — a corrupt-input failure one indirection later instead of a named manifest error.
- **CRLF and `./` already work.** `TrimSpace` eats `\r`, and `filepath.Join` cleans `./x`. Both are real-server shapes (Windows-edited manifests, mysqld's own `./` prefix in `binlog.index` on some platforms) and must stay working — they get pinned tests, not new code.
- **Missing entries must stay fail-loud.** A rotated-away entry means the manifest describes history the input cannot replay; dropping it silently would hide a coverage hole. There is no evidence of a legitimate manifest-state where a listed live entry is expectedly absent.
- **Directory/prefix discovery is already closed.** `discoverBinlogPaths` skips subdirectories *and* symlinks, requires an exact `prefix + digits` shape (`binlogNumericSuffix`), and sorts numerically. `--prefix ../x` matches nothing (entry names contain no separator); a missing `--from-dir` fails on `ReadDir`. Relay logs: `--prefix relay-bin` already matches `relay-bin.NNNNNN`, and a `relay-log.index` works under `--index-file` the same way — the feature exists today, undocumented.
- **Every existing index error is `KindInput` (exit 4)** via `clierr` + i18n keys `error.readIndexFile` / `error.indexFileEntry` / `error.indexFileEmpty`. New violations use the same kind.
- **`validateFiles` runs after resolution** and rejects only missing/unreadable/directories — it does not re-check containment, so the manifest reader is the only place containment can live.

## Decision

### `readIndexFilePaths` entry contract — five checks per non-blank line

Order is deliberate; the first matching violation names the error:

1. **Lexical containment.** Resolve the entry to a path (relative entries join `--from-dir`), `filepath.Clean`, then `filepath.Rel` against the absolute `--from-dir`. A result of `..` or `../…` is a **refusal**: `error.indexFileOutside` (`binlog index X lists Y, which resolves outside --from-dir`). Absolute entries are checked the same way — an absolute path *inside* `--from-dir` is legal and stays legal; outside it is refused. Never clamp.
2. **Existence and shape.** `os.Stat` (follows symlinks); a stat failure or a directory is the existing `error.indexFileEntry` — missing entries stay fail-loud, unchanged.
3. **Self-listing.** `os.SameFile(indexInfo, entryInfo)` refuses an index that lists itself under any spelling (including hardlinks and symlink aliases): `error.indexFileSelf`.
4. **Duplicates.** `os.SameFile` against every previously-accepted entry refuses the second spelling of one file — covering `x`/`./x`, absolute equivalents, symlink aliases, and hardlinks: `error.indexFileDuplicate` naming both spellings. Manifest order stays authoritative for distinct entries — the check is identity, not names.
5. **Resolved containment.** `filepath.EvalSymlinks` on the entry path; a target escaping the *resolved* `--from-dir` is `error.indexFileOutside` — an in-dir symlink to an out-dir file is the same escape as a `..` entry, one indirection down. An eval failure falls back to `error.indexFileEntry` (a path that stats but won't resolve is unreadable in practice).

A `--from-dir` that itself fails `EvalSymlinks` reuses `error.readBinlogDir`. Symlinks *inside* the dir pointing *inside* the dir are accepted — the resolved target is what containment judges, and the joined (unresolved) path is what the parser opens.

### What stays

- **Manifest order is read order.** No re-sorting, no gap detection — a `…000001, …000003` manifest is the server's story and the parser already handles sequence gaps; inventing a refusal here is out of scope.
- **Empty manifests** → `error.indexFileEmpty`; **blank/whitespace lines** → skipped; **CRLF** → handled by `TrimSpace`; **`./` prefixes** → handled by `Join`+`Clean`.
- **Non-regular files are not special-cased.** `!IsDir()` is the existing bar, shared with `validateFiles`; a FIFO entry would block at open the same way an explicit FIFO arg does — one contract, documented, not silently "hardened" here and soft elsewhere.

### Directory/prefix discovery: unchanged, pinned

No code changes. The existing invariants get named tests: subdirectories skipped even when named `mysql-bin.000007`; symlinks skipped; `relay-bin.NNNNNN` matches `--prefix relay-bin` (relay logs are *supported but transient* — the server purges them after apply, so they are only a valid input while they exist; the tool treats them like any file). `--prefix` values containing separators match nothing by construction.

### Documentation

`docs/reference/input-discovery.*` and `cli.*` gain the hardened contract: entries must resolve inside `--from-dir` (absolute-in-dir allowed, everything else refused), self-listing refused, duplicate file identities refused, missing entries fail, order authoritative, relay-log prefix supported, no gap detection.

## Considered options

- **Clamp out-of-dir entries back under `--from-dir`.** Rejected: silently rewriting operator input to a different file is worse than any error; the plan forbids it and it would hide manifest corruption.
- **Silently deduplicate.** Rejected: a duplicate in a write-order-unique manifest is corruption; reading the same events twice is a wrong-result bug, and quietly dropping an entry hides that the manifest was edited. Refuse.
- **Silently skip self-listing.** Rejected for the same reason — a manifest containing itself is corrupt, and skipping produces a report whose file coverage silently differs from the manifest.
- **Refuse all symlinked entries.** Rejected: a symlink whose target stays inside `--from-dir` is a legitimate layout (symlinked datadir members); containment judges the resolved target, which is the honest boundary.
- **Gap detection on discovered/indexed files.** Rejected as a documented non-goal: suffix gaps are normal after `PURGE BINARY LOGS`, and the parser's own error on a genuinely missing member is the existing contract.
- **Lexical-only containment (no `EvalSymlinks`).** Rejected: `inside → outside` symlinks would slip through the exact hole this ADR closes.

## Consequences

- An index manifest is now a closed trust boundary: every entry resolves to exactly one distinct regular file inside `--from-dir`, or the run fails at input (exit 4) naming the index and entry.
- Malformed manifests get named, testable failure modes instead of parser corruption two stages later; `--index-file` becomes safe to run against unfamiliar datadirs.
- Relay logs become a documented, pinned input (`--prefix relay-bin`, `relay-log.index`), honest about their transient lifecycle.
- `--from-dir` + `--prefix` discovery is unchanged — the hardening is entirely inside the manifest reader plus tests pinning existing invariants.
- New i18n keys: `error.indexFileOutside`, `error.indexFileDuplicate`, `error.indexFileSelf` (en + zh-CN). New kinds: none — all `input` (exit 4).
