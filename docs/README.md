# BinlogViz documentation

Start with the [project README](../README.md) for install and a first run. This page is the
map of everything under `docs/`.

Every page has an English file and a `*.zh-CN.md` twin. When the two disagree, the English
page is the contract.

## If you use BinlogViz

| Read this | When |
|---|---|
| [Quickstart](recipe/quickstart.md) | First run, in five commands |
| [Analyze Local Binlogs](recipe/analyze-local-binlogs.md) | Day-to-day analysis, discovery mode, filters |
| [Find and Undo Bad Rows](recipe/flashback.md) | A bad `DELETE`/`UPDATE`/`DROP` happened and you need the rows back |
| [Snapshots, Compare, Trend, and Workflow](recipe/compare-trend-workflow.md) | Save a window, compare it to a baseline, run a multi-step investigation |
| [Troubleshoot Common Errors](recipe/troubleshoot-common-errors.md) | An exit code you did not expect |
| [CLI Reference](reference/cli.md) | Exact flag syntax and every refusal |
| [Output Format Reference](reference/output-format.md) | Field-by-field JSON and text contracts |
| [Input Discovery Reference](reference/input-discovery.md) | How `--from-dir`/`--prefix` picks files |
| [Analyze Behavior Details](reference/analyze-behavior.md) | Positions, exit codes, replica delay, performance |
| [Limitations](concept/limitations.md) | What it deliberately does not do |
| [Analysis Model](concept/analysis-model.md) | What a transaction, a row, and a minute mean here |
| [Architecture](concept/architecture.md) | The pipeline and the module table |
| [DuckDB Temp Store](concept/duckdb-temp-store.md) | The opt-in `--detail-store duckdb` path |
| [JSON Schemas](schema/) | One schema per versioned JSON output; `binlogviz schema` prints them |
| [Release notes](releases/) | What changed in each version |
| [Example outputs](examples/) | A real text and JSON report |
| [Agent usage guide](agents/USING-BINLOGVIZ.md) | Driving BinlogViz from an AI agent |

## If you work on BinlogViz

| Read this | When |
|---|---|
| [Coding standards](development/coding-standards.md) | Size and complexity limits, the ratchet, doc headers |
| [ADRs](adr/) | Why each non-obvious surface decision was made |
| [Engineering records](engineering/) | Plans, delivery evidence, baselines, known code smells |

`docs/engineering/` is the project's own record, not documentation for users. It is kept as
written, so paths quoted inside a dated record can predate the move into that folder.

## Conventions

- Release notes are `docs/releases/release-notes-vX.Y.Z.md` plus `.zh-CN.md`; `CHANGELOG.md`
  holds one section per version, newest first.
- Anything under `docs/landing/` is the public landing page and names no version.
- Reference pages define contracts and are checked by tests; recipe pages are task-oriented and
  may link to them but never redefine a contract.
