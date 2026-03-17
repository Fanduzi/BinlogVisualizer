# Binlogviz Command Module

## Members

| File | Responsibility |
|------|----------------|
| `root.go` | Builds the CLI root command and registers subcommands. |
| `analyze.go` | Orchestrates true streaming parse-normalize-consume-finalize execution, report rendering, and command-owned DuckDB temp-store lifecycle. |
| `*_test.go` | Covers flag parsing, end-to-end analyze execution, fixture runs, and temp-store cleanup behavior. |

## Interfaces

| API | Contract |
|-----|----------|
| `newAnalyzeCommand() *cobra.Command` | Defines the `analyze` command and its stable CLI flags. |
| `runAnalysis(paths []string, opts analyzer.Options, jsonOutput bool) error` | Executes the default parser-backed streaming pipeline: parse -> normalize -> analyzer.Consume -> analyzer.Finalize. |
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
- Top-N truncation is no longer applied in the command layer; it now happens during analyzer Finalize result assembly.
