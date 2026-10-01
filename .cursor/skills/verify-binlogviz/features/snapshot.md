# Snapshot

Snapshot stores a named JSON analyze report under `--snapshot-dir` so later compare/trend/workflow steps can reload it. Users save during analyze or via `snapshot save`, then list/show/rename/delete.

## Sub-features

- `snap-save-analyze` saves with `analyze … --format json --snapshot-name NAME --snapshot-dir DIR` (`--format json` is mandatory).
- `snap-save-file` saves an existing JSON report with `snapshot save <report.json> --name NAME --snapshot-dir DIR`.
- `snap-list-show` lists and shows metadata (`snapshot list|show`).
- `snap-rename-delete` renames or deletes a stored snapshot.

## How to get to it (user POV)

- Add `--format json`, `--snapshot-name`, and `--snapshot-dir` to an `analyze` command.
- Run `binlogviz snapshot save report.json --name NAME --snapshot-dir DIR`.
- Run `binlogviz snapshot list|show|rename|delete … --snapshot-dir DIR`.

## Driving it with drive.sh

Preconditions:

- Doctor PASS; fixture `cmd/binlogviz/testdata/minimal.binlog`.
- Isolated dir prepared:

```bash
SCRATCH=.cursor/skills/verify-binlogviz/scratch/snap-demo
mkdir -p "$SCRATCH/snapshots"
```

- **Save via analyze.** Run `.cursor/skills/verify-binlogviz/helpers/drive.sh snap-save -- analyze cmd/binlogviz/testdata/minimal.binlog --format json --snapshot-name verify-a --snapshot-dir "$SCRATCH/snapshots"`. Expect exit `0`. List `$SCRATCH/snapshots` and confirm a file/dir for `verify-a`.
- **List.** Run `…/drive.sh snap-list -- snapshot list --snapshot-dir "$SCRATCH/snapshots" --format text`. Expect exit `0` and `verify-a` in stdout.
- **Show.** Run `…/drive.sh snap-show -- snapshot show verify-a --snapshot-dir "$SCRATCH/snapshots"`. Expect exit `0` and summary fields.
- **Rename.** Run `…/drive.sh snap-rename -- snapshot rename verify-a verify-b --snapshot-dir "$SCRATCH/snapshots"`. Expect exit `0`; list no longer shows `verify-a`.
- **Delete.** Run `…/drive.sh snap-delete -- snapshot delete verify-b --snapshot-dir "$SCRATCH/snapshots"`. Expect exit `0`; list empty or without `verify-b`.
- **Proof.** Evidence dirs for each drive plus a filesystem listing of `$SCRATCH/snapshots` before cleanup.

## Gotchas

- Always set `--snapshot-dir` under skill `scratch/`; omitting it may write to the user's default snapshot home.
- `--snapshot-name` **requires** `--format json`. Default analyze format is `text`; omitting `--format json` fails hard with `Error: --snapshot-name requires --format json` (exit `1`). There is no implicit default path that yields a JSON snapshot payload.
- Two concurrent verifies need different scratch snapshot dirs.
- Cleanup removes `scratch/`; capture list/show evidence before `cleanup.sh`.
