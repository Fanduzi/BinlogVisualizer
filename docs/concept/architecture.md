# Product Architecture

BinlogViz analyzes local MySQL ROW binlog files with a single-pass streaming pipeline. The command path is designed to move forward through the input once, keep live in-memory state bounded, and assemble the final report only after parsing is complete.

## High-Level Flow

```text
binlog files -> parser -> normalize -> analyzer -> finalize -> renderer -> output
```

At the command level, `binlogviz analyze` resolves input paths, validates them, builds analyzer options, then runs the streaming parse-to-report pipeline.

## Pipeline Stages

### 1. Parser

The parser reads one or more local binlog files and emits raw binlog events through callbacks. It can run in explicit file mode or in discovery mode, where the command first resolves an ordered file list from `--from-dir` and `--prefix`.

This stage is responsible for reading the binlog stream, not for building the final operator report.

### 2. Normalize

Raw parser events are converted into a stable internal event shape before analysis. This keeps the analyzer insulated from parser-specific details and gives the rest of the pipeline a consistent event contract.

### 3. Analyzer

The analyzer consumes normalized events as they arrive. It reconstructs transactions, updates workload summaries, tracks table and minute-level activity, and prepares alert inputs such as large transactions and optional write spikes.

This is still part of the streaming path: events are consumed incrementally instead of collecting the full input in memory first.

### 4. Finalize

After parsing finishes, the command switches from event ingestion to result assembly. This is where BinlogViz completes analysis output that depends on the full input window, including top-N style report sections and other finalize-time aggregation work.

The command prints a finalizing status message before this step so operators can distinguish parsing progress from result assembly work.

### 5. Render

Once `Finalize()` returns the assembled analysis result, the renderer writes either text or JSON output to standard output. Rendering is intentionally separated from parsing and analysis so user-facing presentation stays clean and predictable.

## Streaming Command Path

The command path is intentionally streaming:

1. Resolve input files.
2. Validate files exist.
3. Create a command-owned temporary DuckDB store.
4. Parse events.
5. Normalize each event immediately.
6. Forward each normalized event directly to `analyzer.Consume`.
7. Call `Finalize()` once parsing is complete.
8. Render the final report to `stdout`.

This design lets BinlogViz handle ordered binlog ranges without first building a large in-memory event buffer.

## Aggregate Progress Behavior

When the parser supports progress callbacks, BinlogViz renders aggregate parsing progress to `stderr`. Progress is based on the total size of all ordered input files rather than on a per-file spinner, so operators get one overall view of parse advancement across the full input set.

After parsing completes, BinlogViz marks parsing as finished and prints `Finalizing analysis...` on `stderr` before calling `Finalize()`.

## Why Progress and Discovery Output Use stderr

BinlogViz keeps report data on `stdout` and human-oriented status output on `stderr`.

That split matters for operators who want to pipe or redirect the report:

- Text report on `stdout` can be redirected to a file.
- JSON report on `stdout` stays machine-consumable.
- Progress updates on `stderr` do not corrupt text or JSON output.
- Discovery-mode resolved file listings on `stderr` document what was analyzed without polluting the report stream.

In discovery mode, the command prints the resolved ordered binlog file list to `stderr` before analysis starts.

## DuckDB in the Pipeline

DuckDB is the temporary store that supports finalize-time result assembly and bounded SQL-context storage without breaking the streaming shape of the main command path.

From the architecture point of view, the important point is simple: `binlogviz analyze` creates one command-owned temporary DuckDB store for the run, passes it into the analyzer, and cleans it up when the command exits.

For lifecycle details, operational implications, and what users may notice during large analyses, see [DuckDB Temp Store](./duckdb-temp-store.md).
