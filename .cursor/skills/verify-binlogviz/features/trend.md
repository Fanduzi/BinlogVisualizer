# Trend

Trend analyzes multiple named snapshots as an ordered series (CLI argument order or by window start time).

## Sub-features

- `trend-positional` takes snapshot names as positional args with `--snapshot-dir`.
- `trend-from-pattern` selects names with `--from-snapshots`.
- `trend-order` chooses `--order cli|time`.
- `trend-format` emits `--format text|json|html`.

## How to get to it (user POV)

- Run `binlogviz trend snap1 snap2 … --snapshot-dir DIR`.
- Run `binlogviz trend --from-snapshots 'week*' --snapshot-dir DIR`.

## Driving it with drive.sh

Preconditions:

- Doctor PASS.
- At least two snapshots under an isolated dir (reuse compare seeding):

```bash
SCRATCH=.cursor/skills/verify-binlogviz/scratch/trend-demo
mkdir -p "$SCRATCH/snapshots"
DRIVE=.cursor/skills/verify-binlogviz/helpers/drive.sh
"$DRIVE" trend-seed-a -- analyze cmd/binlogviz/testdata/minimal.binlog --format json \
  --snapshot-name week1 --snapshot-dir "$SCRATCH/snapshots"
"$DRIVE" trend-seed-b -- analyze cmd/binlogviz/testdata/minimal.binlog --format json \
  --snapshot-name week2 --snapshot-dir "$SCRATCH/snapshots"
```

- **Positional trend.** Run `"$DRIVE" trend-pos -- trend week1 week2 --snapshot-dir "$SCRATCH/snapshots" --format text --order cli`. Expect exit `0` and a trend report.
- **Pattern selection.** Run `"$DRIVE" trend-pat -- trend --from-snapshots 'week*' --snapshot-dir "$SCRATCH/snapshots" --format json`. Expect exit `0` and JSON stdout.
- **Proof.** Evidence `meta.txt` + stdout; list `$SCRATCH/snapshots`.

## Gotchas

- Trend loads snapshots from `--snapshot-dir`; missing names fail hard (exit `1`).
- `--order time` sorts by window `start_time`; with identical fixture windows, order may not change — still valid proof of the flag path.
- Prefer unique scratch dirs per concurrent run.
