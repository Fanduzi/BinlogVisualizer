# Binlogviz Command Module

## Members

| File | Responsibility |
|------|----------------|
| `root.go` | Builds the CLI root command and registers subcommands. |
| `analyze.go` | Orchestrates true streaming parse-normalize-consume-finalize execution, report rendering, command-owned DuckDB temp-store lifecycle, and `--sql-context` presentation selection. |
| `*_test.go` | Covers flag parsing, SQL context mode validation, end-to-end analyze execution, streaming regression checks, fixture runs, benchmarks, and temp-store cleanup behavior. |

## Interfaces

| API | Contract |
|-----|----------|
| `newAnalyzeCommand() *cobra.Command` | Defines the `analyze` command and its stable CLI flags. |
| `runAnalysis(paths []string, opts analyzer.Options, jsonOutput bool) error` | Executes the default parser-backed streaming pipeline: parse -> normalize -> analyzer.Consume -> analyzer.Finalize. |
| `buildReportOptions(opts *analyzeOptions) (report.Options, error)` | Validates `--sql-context` and converts CLI flags into renderer presentation controls. |
| `runAnalysisWithParser(paths []string, opts analyzer.Options, jsonOutput bool, parser binlog.Parser) error` | Executes the command pipeline with an injected parser for tests. |
| `runAnalysisWithParserAndTempDir(...) error` | Test seam that lets command tests control temp-root placement while still using command-layer DuckDB lifecycle management. |
| `createDuckDBTempStore(root string) (*analyzer.DuckDBStore, func() error, string, error)` | Creates a temp DuckDB file under a system or test-provided temp root and returns command-layer cleanup. |

## Dependencies

- Upstream:
  - `internal/binlog` provides parser and normalization steps.
  - `internal/analyzer` provides the DuckDB-backed analyzer and result store.
  - `internal/report` renders the final `AnalysisResult`.
- Downstream:
  - The compiled `binlogviz` CLI invokes this module as the user-facing entrypoint.

## Notes

- Stage 3 keeps CLI semantics stable while moving the command execution path to true streaming consumption with command-owned DuckDB lifecycle.
- Stage 4 adds `--sql-context summary|off|full`; CLI parses the mode and delegates presentation decisions to `internal/report`.
- Stage 5 adds command-path benchmarks for real fixture parsing and synthetic high-volume streaming workloads, and keeps fixture assets under `internal/binlog/testdata`.
- Command temp-store cleanup remains directory-scoped and no longer depends on any SQL-context sidecar file; bounded SQL context now lives entirely inside the analyzer's DuckDB temp DB.
- Top-N truncation is no longer applied in the command layer; it now happens during analyzer Finalize result assembly.
