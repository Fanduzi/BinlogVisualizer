# Compare

Compare diffs two BinlogViz JSON analysis reports — either as file paths or as named snapshots loaded from `--snapshot-dir`.

## Sub-features

- `compare-files` compares `<current.json> <baseline.json>`.
- `compare-snapshots` uses `--current-snapshot` / `--baseline-snapshot` with `--snapshot-dir`.
- `compare-format` emits `--format text|json|html`.

## How to get to it (user POV)

- Run `binlogviz compare current.json baseline.json`.
- Run `binlogviz compare --current-snapshot CUR --baseline-snapshot BASE --snapshot-dir DIR`.

## Driving it with drive.sh

Preconditions:

- Doctor PASS.
- Two snapshots (or two JSON reports) prepared under an isolated scratch dir:

```bash
SCRATCH=.cursor/skills/verify-binlogviz/scratch/cmp-demo
mkdir -p "$SCRATCH/snapshots"
DRIVE=.cursor/skills/verify-binlogviz/helpers/drive.sh
"$DRIVE" cmp-seed-a -- analyze cmd/binlogviz/testdata/minimal.binlog --format json \
  --snapshot-name week1 --snapshot-dir "$SCRATCH/snapshots"
"$DRIVE" cmp-seed-b -- analyze cmd/binlogviz/testdata/minimal.binlog --format json \
  --snapshot-name week2 --snapshot-dir "$SCRATCH/snapshots"
```

(Using the same fixture twice is enough to exercise the CLI path; deltas may be empty.)

- **Compare snapshots.** Run `"$DRIVE" compare-snap -- compare --current-snapshot week2 --baseline-snapshot week1 --snapshot-dir "$SCRATCH/snapshots" --format text`. Expect exit `0` and a compare report on stdout.
- **Compare JSON files (optional).** If analyze wrote reports to files (`-o` / redirected JSON), run `"$DRIVE" compare-files -- compare "$SCRATCH/current.json" "$SCRATCH/baseline.json" --format json`.
- **Proof.** `meta.txt` exit=`0`, non-empty stdout; snapshot-dir listing showing both names.

## Gotchas

- Compare needs JSON-shaped reports / snapshots, not text analyze output.
- Empty deltas are still a successful compare when both inputs parse — do not require non-zero diff for EXIT 0.
- Always pass the same `--snapshot-dir` used when saving.
- HTML format is optional evidence; text/json suffice for CLI proof.
