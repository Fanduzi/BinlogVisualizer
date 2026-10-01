---
name: verify-binlogviz
description: "Drive BinlogViz (CLI) the way a DBA would — analyze/snapshot/compare/trend/workflow against real ROW fixtures. Use when proving binlogviz behavior, dogfooding a release, or verifying a fix before claiming done."
---

# Verify BinlogViz

Cold-agent guide for proving `binlogviz` (Cobra CLI) against real ROW binlog fixtures. Primary surface is the CLI. HTML reports (`--format html`) opened in a browser are optional extra evidence, not required for CLI proof.

Repo root for this skill: the checkout that contains `.cursor/skills/verify-binlogviz/` (expected: `/workspace/binlogviz-src`).

## Launch

There is **no long-lived server**. Launch means resolve a binary once; each drive is a short-lived subprocess with an isolated `--snapshot-dir`.

Resolve `BINLOGVIZ` in this order:

1. Env override: `$BINLOGVIZ` if set and executable
2. Repo build: from repo root, `go build -o binlogviz .` then `./binlogviz` (needs `g++` / `libstdc++` for duckdb CGO)
3. Fallback release: `/home/box/bin/binlogviz` (currently 0.23.6)

```bash
cd /workspace/binlogviz-src
# Prefer a fresh build when the toolchain is present:
go build -o binlogviz .   # may fail without g++/libstdc++; then use fallback
export BINLOGVIZ="${BINLOGVIZ:-$(pwd)/binlogviz}"
# If ./binlogviz missing:
# export BINLOGVIZ=/home/box/bin/binlogviz

"$BINLOGVIZ" version   # ready when this prints a version line (e.g. binlogviz 0.23.6)
```

Teardown: nothing for the binary. Scratch snapshot / workflow dirs are removed by `helpers/cleanup.sh` (never evidence).

## Doctor

```bash
cd /workspace/binlogviz-src
.cursor/skills/verify-binlogviz/helpers/doctor.sh
```

Read-only. EXIT `0` when: binary exists+executable, `version` works, `cmd/binlogviz/testdata/minimal.binlog` is present. Prints which binary was chosen (`env` / `repo build` / `fallback release`) and `go version` if available. Non-zero + stderr if not ready. Run doctor before any drive when something looks off.

## Drive

```bash
cd /workspace/binlogviz-src
HELPERS=.cursor/skills/verify-binlogviz/helpers
DRIVE="$HELPERS/drive.sh"

# Pure analyze (no snapshot-dir required):
"$DRIVE" analyze-text -- analyze cmd/binlogviz/testdata/minimal.binlog --format text

# Snapshot / compare / trend / workflow: pass an isolated snapshot-dir under scratch.
# drive.sh still creates scratch/<RUN_ID>/snapshots; either use the path it prints,
# or prepare one yourself:
RUN_HINT=$(date +%Y%m%d-%H%M%S)-snap
SCRATCH=.cursor/skills/verify-binlogviz/scratch/$RUN_HINT
mkdir -p "$SCRATCH/snapshots"
"$DRIVE" snap-save -- analyze cmd/binlogviz/testdata/minimal.binlog \
  --format json \
  --snapshot-name verify-a \
  --snapshot-dir "$SCRATCH/snapshots"
```

`drive.sh <label> -- <binlogviz args...>`:

- Resolves the same binary as doctor
- Runs from repo root so relative fixture paths work
- Writes under `.cursor/skills/verify-binlogviz/evidence/<RUN_ID>/`:
  - `meta.txt` — cmd, exit, binary, utc/local time, scratch path
  - `stdout` — CLI stdout
  - `stderr` — CLI stderr (progress bars often land here)
- `RUN_ID` = `YYYYMMDD-HHMMSS-<label>`
- Captures non-zero exits without aborting the wrapper; process exit equals the CLI exit
- Creates `scratch/<RUN_ID>/snapshots` for optional use; **always** pass `--snapshot-dir` yourself when the command needs one (never use the user's default snapshot home)

Exit codes from binlogviz (observe these in `meta.txt`):

| Exit | Meaning |
|------|---------|
| 0 | Counted events / successful command |
| 1 | Hard failure |
| 2 | No-data (empty window / nothing counted) |

## Evidence

Absolute base (under this repo):

`/workspace/binlogviz-src/.cursor/skills/verify-binlogviz/evidence/<RUN_ID>/`

Proof standards:

- Exercise the real CLI user path (`binlogviz analyze|snapshot|compare|trend|workflow …`), never mock binlog parsing
- Capture exit + stdout + stderr every drive
- For snapshot/compare/trend: also list files under the isolated `--snapshot-dir` (side effect)
- For workflow: also list the workflow `--output-dir` / plan `output_dir`
- HTML open-in-browser is optional extra evidence; text/json CLI output is enough for CLI claims
- Seed fixtures for recipes: `cmd/binlogviz/testdata/minimal.binlog` (ROW), `cmd/binlogviz/testdata/sample-binlog/` + repo-root `incident.yaml` for workflow. Dogfood under `/workspace/binlogviz-dogfood/` is OK for extra proofs; **skill recipes must work from repo fixtures only**

## Cleanup

```bash
.cursor/skills/verify-binlogviz/helpers/cleanup.sh
```

Removes only `.cursor/skills/verify-binlogviz/scratch/**` (snapshot-dirs and workflow outs created by drives). **NEVER** deletes `evidence/`. Confirm evidence still exists after cleanup.

## Helpers

| Script | Argv | Exit |
|--------|------|------|
| `helpers/doctor.sh` | (none) | `0` ready; non-zero not ready |
| `helpers/drive.sh` | `<label> -- <binlogviz args…>` | same as CLI; always writes evidence |
| `helpers/cleanup.sh` | (none) | `0` after clearing `scratch/` |

All three are executable (`chmod +x`). Override binary with `export BINLOGVIZ=/path/to/binlogviz`.

## Feature map

Read [features/README.md](features/README.md) before driving. Prove the feature you claim; do not mark other map entries verified via a shortcut.

## Isolation

Two drives may coexist: give each a unique `scratch/<id>/snapshots` (and unique workflow `output_dir`). Never write to the user's default snapshot home. Short-lived CLI — no port conflicts.
