# BinlogViz verification map

Maintained source for verifying DBA-facing `binlogviz` CLI behavior against real ROW fixtures. Read this index, then the matching feature file.

## Baseline preconditions

- Resolve a binary (`BINLOGVIZ`, `./binlogviz` after `go build -o binlogviz .`, or `/home/box/bin/binlogviz`).
- Run `.cursor/skills/verify-binlogviz/helpers/doctor.sh` and require PASS before driving.
- Work from the repo root (`/workspace/binlogviz-src`) so relative fixture paths resolve.
- Seed fixtures: `cmd/binlogviz/testdata/minimal.binlog` (ROW); discovery layout `cmd/binlogviz/testdata/sample-binlog/` + `incident.yaml` for workflow.
- Always pass `--snapshot-dir` under `.cursor/skills/verify-binlogviz/scratch/<id>/snapshots` when saving or loading snapshots. Never use the user's default snapshot home.
- Never mock binlog parsing; drive the real CLI.

## Driving conventions

- Start every recipe from doctor PASS unless its preconditions say otherwise.
- Invoke via `helpers/drive.sh <label> -- <args…>` so exit/stdout/stderr land under `evidence/<RUN_ID>/`.
- Treat commands as literal; keep flag names and fixture paths unchanged.
- Prefer `--format text` or `json` for CLI proof; HTML is optional extra evidence.
- After snapshot/compare/trend/workflow drives, list the scratch snapshot-dir (and workflow output dir) as side-effect proof.
- Run `helpers/cleanup.sh` after the session; it must leave `evidence/` intact.

## Proof and skip reporting

- Capture the CLI action and the resulting state (stdout summary / JSON fields / snapshot files).
- CLI proof includes command, exit code, stdout, stderr (see `meta.txt`).
- Mutation proof (snapshot save/rename/delete): follow-up `snapshot list` or filesystem listing under `--snapshot-dir`.
- Record the feature ID / label with every artifact (drive.sh label).
- Unreachable path → report attempted command + unmet precondition; do not claim verified via another entry.

## Feature entry contract

Each feature file: H1 + one paragraph, then exactly four H2s — `Sub-features`, `How to get to it (user POV)`, `Driving it with drive.sh`, `Gotchas`.

## Features

- [Analyze](./analyze.md) — single-file / `--from-dir`+`--prefix` analysis, formats, time windows, exit 0/1/2.
- [Snapshot](./snapshot.md) — save via analyze flags or `snapshot save`; list/show/rename/delete.
- [Compare](./compare.md) — two JSON files or `--current-snapshot` / `--baseline-snapshot`.
- [Trend](./trend.md) — ordered snapshots via `--from-snapshots` or positional names.
- [Workflow](./workflow.md) — `workflow validate|describe|run` with `incident.yaml` into isolated output dirs.
