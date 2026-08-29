# Binlogviz Command Module

Cobra CLI entrypoints and command-layer orchestration for analyze, compare, trend, snapshot, version, and workflow commands including run, resume, validate, describe, status, and clean.

## Files

| File | Responsibility |
|------|----------------|
| `root.go` | Builds the root command and registers `analyze`, `compare`, `trend`, `snapshot`, `version`, and `workflow`. |
| `analyze.go` | Orchestrates explicit-path or discovery-mode analyze execution, aggregate parse progress on `stderr`, optional snapshot persistence, and command-owned DuckDB temp-store lifecycle. |
| `analyze_parallel.go` | Runs bounded parallel per-file parsing while preserving ordered analyzer consumption for cross-file transaction safety. |
| `compare.go` | Resolves compare inputs from explicit JSON files or named snapshots and renders text/JSON/HTML compare output. |
| `trend.go` | Resolves explicit or pattern-selected snapshot inputs, optional baseline snapshots, and renders text/JSON/HTML multi-snapshot trend output. |
| `snapshot.go` | Implements `snapshot save`, `snapshot list`, `snapshot show`, `snapshot rename`, and `snapshot delete`, including machine-readable list/show output. |
| `version.go` | Prints version-only and logo+version output. |
| `workflow.go` | Implements `workflow run <plan.yaml>`, `workflow resume <output_dir>`, pre-run `workflow validate <plan.yaml>` checks, static `workflow describe <plan.yaml>` previews, read-only `workflow status <output_dir>` inspection, and dry-run-first `workflow clean <output_dir>` cleanup. |
| `*_test.go` | Covers flag parsing, snapshot workflow behavior, compare registration and input validation, analyze/compare integration, temp-store cleanup behavior, discovery-mode input resolution, workflow run/resume end-to-end tests, and workflow validate/describe/clean output contracts. |

## Exports

- `NewRootCommand() *cobra.Command` — Builds the full CLI root command tree.
- `newAnalyzeCommand() *cobra.Command` — Defines the analyze command and its CLI flags, including optional snapshot persistence flags.
- `newCompareCommand() *cobra.Command` — Defines compare in both legacy file mode and snapshot mode.
- `newTrendCommand() *cobra.Command` — Defines trend in both explicit snapshot-list mode and pattern-selected snapshot mode, with optional baseline deltas.
- `newSnapshotCommand() *cobra.Command` — Defines the snapshot command subtree.
- `runAnalysisWithReportAndSnapshotOptions(...) error` — Executes analyze with optional snapshot metadata and snapshot persistence.
- `runAnalysisWithParserAndTempDirAndReportAndSnapshotOptions(...) error` — Test seam for command-path analyze behavior with explicit snapshot options.
- `createDuckDBTempStore(root string) (*analyzer.DuckDBStore, func() error, string, error)` — Creates a temp DuckDB file under a system or test-provided temp root and returns command-layer cleanup.

## Dependencies

- Upstream:
  - `internal/binlog` provides parser, progress-aware parsing, and normalization steps.
  - `internal/analyzer` provides the DuckDB-backed analyzer and result store.
  - `internal/report` renders the final `AnalysisResult`.
  - `internal/compare` validates analyze JSON for compare workflows and renders compare output.
  - `internal/trend` validates ordered snapshot-backed trend inputs and renders multi-snapshot trend output.
- `internal/snapshot` resolves snapshot paths, persists named analyze JSON reports, and manages snapshot metadata/rename/delete operations.
- Downstream:
  - The compiled `binlogviz` CLI invokes this module as the user-facing entrypoint.

## Update Rule

If members, interfaces, discovery-mode behavior, or dependencies change, update this file in the same change.

## Notes

- Stage 3 keeps CLI semantics stable while moving the command execution path to true streaming consumption with command-owned DuckDB lifecycle.
- Stage 4 adds `--sql-context summary|off|full`; CLI parses the mode and delegates presentation decisions to `internal/report`.
- Stage 5 adds command-path benchmarks for real fixture parsing and synthetic high-volume streaming workloads, keeps fixture assets under `internal/binlog/testdata`, and adds aggregate parse progress based on ordered input file sizes.
- Stage 8 adds named snapshot persistence under `~/.binlogviz/snapshots`, snapshot management commands, and snapshot-based compare input resolution while preserving legacy file-based compare.
- The trend command adds a higher-level historical workflow on top of the snapshot store. Explicit snapshot lists and workflow jobs keep CLI/plan order by default (`--order cli`); `--order time` sorts by `snapshot.window.start_time` and writes a stderr notice when it reorders.
- The snapshot subtree now supports long-lived snapshot management with rename/delete and JSON output for list/show so external tooling can consume the snapshot store without parsing text output.
- Discovery mode resolves ordered binlog files before entering the existing analysis pipeline, narrows `--from-dir/--prefix` inputs with `--start/--end` using file modification times plus a trailing-file first-event check, and keeps the resolved list on `stderr` so report output on `stdout` stays machine-consumable. `--prefix mysql-bin` accepts an optional `.` before the numeric suffix so it matches `mysql-bin.000008`.
- Analyze failure paths print the error once and do not dump full Usage. Empty/truncated/corrupt files use DBA-facing messages. A non-empty file with no Format Description exits non-zero; a time window with 0 matches prints `window matched 0 events` on `stderr`. HTML save confirmation goes to `stderr`.
- Command temp-store cleanup remains directory-scoped and no longer depends on any SQL-context sidecar file; bounded SQL context now lives entirely inside the analyzer's DuckDB temp DB.
- Top-N truncation is no longer applied in the command layer; it now happens during analyzer Finalize result assembly.
- Analyze text output is now diagnostic-first by default: summary, top findings, top tables, and next actions are shown on stdout, while minute-level and write-shape detail are reserved for explicit detail controls in the report layer.
- Same-second ROW fixtures (duration `< 1s` with transactions) print operator-facing text TPS as `N/A (sub-second)` so `TxnCount/60` does not look like a parse failure; JSON `tps_series` stays numeric.
- `--top N` sets the default ranked output size for productized text and HTML report sections.
- `--detail-store none|duckdb` controls optional transaction detail persistence. The default `none` generates reports from streaming aggregates without DuckDB; `duckdb` keeps the detail backend enabled for future lookup workflows.
- `--details`, `--show-minutes`, and `--show-patterns` expand the default concise text report without changing analyzer semantics.
- Analyze performance coverage includes text-vs-HTML rendering, corpus-backed DBA incident workloads, a CI-safe near-1GB synthetic mix benchmark, and an external real-binlog benchmark (`BenchmarkAnalyzeExternalRealBinlog`) gated behind `BINLOGVIZ_REAL_BINLOG` env var; real-fixture parser benchmarks isolate parse-only, parse+normalize, and end-to-end layers to identify per-stage bottlenecks.
- HTML report section IDs were consolidated from 7 flat sections to 5 semantic groups: `executive-summary`, `section-findings`, `section-activity`, `section-objects`, `section-evidence`. Product tests assert these IDs directly.
- `cmd/binlogviz/testdata/sql-corpus` now carries richer DBA-facing incident scenarios, including runner-up large/long/wide transactions so product tests can catch over-crowded transaction-evidence rendering instead of only toy single-winner cases.
- The production analyze command uses a destination-reuse normalization fast path in its streaming handler so the main CLI path no longer allocates one `*NormalizedEvent` per kept event.
- Multi-file analyze now overlaps parser work across files with bounded per-file buffers, but still feeds normalized events to the analyzer in input order so cross-file transactions remain valid.
