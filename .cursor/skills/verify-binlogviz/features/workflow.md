# Workflow

Workflow runs multi-step investigation plans (`validate`, `describe`, `run`, plus status/resume/export/clean) from a YAML plan such as repo-root `incident.yaml`.

## Sub-features

- `wf-validate` validates a plan without executing (`workflow validate`).
- `wf-describe` prints the plan structure (`workflow describe`).
- `wf-run` executes analyze/snapshot/compare/trend steps into an output dir (`workflow run`).
- `wf-isolate` overrides `--output-dir` / `--snapshot-dir` so runs never touch user defaults or committed `artifacts/`.

## How to get to it (user POV)

- Edit or use `incident.yaml` (points `from_dir` at `cmd/binlogviz/testdata/sample-binlog`).
- Run `binlogviz workflow validate|describe|run incident.yaml`.
- Optionally override `--output-dir` and `--snapshot-dir` on `run`.

## Driving it with drive.sh

Preconditions:

- Doctor PASS.
- Repo-root `incident.yaml` and `cmd/binlogviz/testdata/sample-binlog/mysql-bin.000001` present.
- Isolated dirs:

```bash
SCRATCH=.cursor/skills/verify-binlogviz/scratch/wf-demo
mkdir -p "$SCRATCH/snapshots" "$SCRATCH/out"
DRIVE=.cursor/skills/verify-binlogviz/helpers/drive.sh
PLAN=incident.yaml
```

- **Validate.** Run `"$DRIVE" wf-validate -- workflow validate "$PLAN" --format text`. Expect exit `0`.
- **Describe.** Run `"$DRIVE" wf-describe -- workflow describe "$PLAN" --format text`. Expect exit `0` and plan/window names in stdout.
- **Run (isolated).** Run `"$DRIVE" wf-run -- workflow run "$PLAN" --output-dir "$SCRATCH/out" --snapshot-dir "$SCRATCH/snapshots"`. Expect exit `0`. List `$SCRATCH/out` and `$SCRATCH/snapshots` for produced artifacts.
- **Proof.** Evidence for validate/describe/run; directory listings of scratch out + snapshots. Do not leave outputs under repo `./artifacts/incident` if overrides were used — prefer the scratch overrides above.

## Gotchas

- Run from repo root so plan-relative `from_dir` paths resolve.
- Default plan `output_dir: ./artifacts/incident` pollutes the working tree — always pass `--output-dir` under skill `scratch/` for verify.
- Fixture timestamps are historical; the sample plan uses wide windows (2025–2099) on purpose.
- `cleanup.sh` removes `scratch/` only; if a run wrote to `./artifacts/` by mistake, remove that manually (do not delete evidence).
- Extra dogfood samples under `/workspace/binlogviz-dogfood/` are optional; recipes must work with repo fixtures alone.
