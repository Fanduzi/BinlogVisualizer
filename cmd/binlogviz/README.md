# Binlogviz Command Module

Cobra CLI entrypoints and command-layer orchestration for analyze, compare, trend, snapshot, version, and workflow run.

## Files

| File | Responsibility |
|------|----------------|
| `root.go` | Builds the root command and registers `analyze`, `compare`, `trend`, `snapshot`, `version`, and `workflow`. |
| `analyze.go` | Orchestrates explicit-path or discovery-mode analyze execution, aggregate parse progress on `stderr`, optional snapshot persistence, and command-owned DuckDB temp-store lifecycle. |
| `compare.go` | Resolves compare inputs from explicit JSON files or named snapshots and renders text/JSON/HTML compare output. |
| `trend.go` | Resolves explicit or pattern-selected snapshot inputs, optional baseline snapshots, and renders text/JSON/HTML multi-snapshot trend output. |
| `snapshot.go` | Implements `snapshot save`, `snapshot list`, `snapshot show`, `snapshot rename`, and `snapshot delete`, including machine-readable list/show output. |
| `version.go` | Prints version-only and logo+version output. |
| `workflow.go` | Implements `workflow run <plan.yaml>`: loads and validates a YAML plan, orchestrates analyze/compare/trend phases, writes artifact tree and manifest. |
| `*_test.go` | Covers flag parsing, snapshot workflow behavior, compare registration and input validation, analyze/compare integration, temp-store cleanup behavior, discovery-mode input resolution, and workflow run end-to-end tests. |

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
- The trend command adds a higher-level historical workflow on top of the snapshot store and orders selected snapshots by `snapshot.window.start_time` instead of CLI order.
- The snapshot subtree now supports long-lived snapshot management with rename/delete and JSON output for list/show so external tooling can consume the snapshot store without parsing text output.
- Discovery mode resolves ordered binlog files before entering the existing analysis pipeline and keeps the resolved list on `stderr` so report output on `stdout` stays machine-consumable.
- Command temp-store cleanup remains directory-scoped and no longer depends on any SQL-context sidecar file; bounded SQL context now lives entirely inside the analyzer's DuckDB temp DB.
- Top-N truncation is no longer applied in the command layer; it now happens during analyzer Finalize result assembly.
