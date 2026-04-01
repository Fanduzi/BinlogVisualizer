# Binlogviz Command Module

Cobra CLI entrypoints and analyze-command orchestration for explicit binlog paths or discovery-mode input.

## Files

| File | Responsibility |
|------|----------------|
| `root.go` | Builds the CLI root command and registers subcommands. |
| `analyze.go` | Orchestrates input resolution for explicit paths or `--from-dir` + `--prefix`, true streaming parse-normalize-consume-finalize execution, aggregate parse progress on `stderr`, resolved-file reporting on `stderr` in discovery mode, report rendering, command-owned DuckDB temp-store lifecycle, and `--sql-context` presentation selection. |
| `*_test.go` | Covers flag parsing, SQL context mode validation, end-to-end analyze execution, locale-stable command assertions, minimal analyze->compare workflow generation, streaming regression checks, fixture runs, benchmarks, temp-store cleanup behavior, and discovery-mode input resolution. |

## Exports

- `newAnalyzeCommand() *cobra.Command` — Defines the `analyze` command and its stable CLI flags.
- `runAnalysis(paths []string, opts analyzer.Options, jsonOutput bool) error` — Executes the default parser-backed streaming pipeline: parse -> normalize -> analyzer.Consume -> analyzer.Finalize, with parse progress on `stderr` when supported.
- `buildReportOptions(opts *analyzeOptions) (report.Options, error)` — Validates `--sql-context` and converts CLI flags into renderer presentation controls.
- `runAnalysisWithParser(paths []string, opts analyzer.Options, jsonOutput bool, parser binlog.Parser) error` — Executes the command pipeline with an injected parser for tests.
- `runAnalysisWithParserAndTempDir(...) error` — Test seam that lets command tests control temp-root placement while still using command-layer DuckDB lifecycle management.
- `createDuckDBTempStore(root string) (*analyzer.DuckDBStore, func() error, string, error)` — Creates a temp DuckDB file under a system or test-provided temp root and returns command-layer cleanup.

## Dependencies

- Upstream:
  - `internal/binlog` provides parser, progress-aware parsing, and normalization steps.
  - `internal/analyzer` provides the DuckDB-backed analyzer and result store.
  - `internal/report` renders the final `AnalysisResult`.
- Downstream:
  - The compiled `binlogviz` CLI invokes this module as the user-facing entrypoint.

## Update Rule

If members, interfaces, discovery-mode behavior, or dependencies change, update this file in the same change.

## Notes

- Stage 3 keeps CLI semantics stable while moving the command execution path to true streaming consumption with command-owned DuckDB lifecycle.
- Stage 4 adds `--sql-context summary|off|full`; CLI parses the mode and delegates presentation decisions to `internal/report`.
- Stage 5 adds command-path benchmarks for real fixture parsing and synthetic high-volume streaming workloads, keeps fixture assets under `internal/binlog/testdata`, and adds aggregate parse progress based on ordered input file sizes.
- Discovery mode resolves ordered binlog files before entering the existing analysis pipeline and keeps the resolved list on `stderr` so report output on `stdout` stays machine-consumable.
- Command temp-store cleanup remains directory-scoped and no longer depends on any SQL-context sidecar file; bounded SQL context now lives entirely inside the analyzer's DuckDB temp DB.
- Top-N truncation is no longer applied in the command layer; it now happens during analyzer Finalize result assembly.
