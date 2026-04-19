# DuckDB Temp Store

BinlogViz can use a command-owned temporary DuckDB database during `binlogviz analyze` when the `--detail-store duckdb` flag is passed. As of v0.19.0, the default is `--detail-store none`, which skips DuckDB entirely.

## What It Stores

At a high level, the DuckDB temp store holds temporary analysis data that is useful at finalize time, including:

- completed or high-cardinality intermediate results needed to assemble the final report
- bounded SQL context storage used by analysis and reporting
- temporary result data that would otherwise put unnecessary pressure on process memory

This store is internal implementation detail for one command execution, not a user-facing database.

## Why It Exists

Starting with v0.19.0, the DuckDB detail store is opt-in via `--detail-store duckdb`. The default `--detail-store none` produces equivalent JSON output without creating a DuckDB database.

BinlogViz follows a streaming command path: parse, normalize, consume, then finalize. That design is a good fit for large local binlog ranges, but some report sections still need a place to assemble or persist intermediate result data until the full input has been seen.

DuckDB exists to help BinlogViz:

- reduce in-memory pressure for temporary analysis data
- support finalize-time aggregation after streaming consumption completes
- keep the main parse-and-consume path streaming instead of buffering all input events in RAM
- store bounded SQL context in the same temporary analysis store rather than in separate command-managed sidecar files

## Relation to `Finalize()`

`Finalize()` is the boundary between streaming event ingestion and final result assembly.

During parsing, the analyzer consumes normalized events incrementally. After parsing ends, `Finalize()` reads and assembles the analysis result needed for rendering. The DuckDB temp store is part of that handoff: it keeps temporary analysis state available so final top-N sections, summaries, and related result assembly can happen after the full input window has been processed.

Operators can think of the lifecycle like this:

1. Parse and normalize binlog events.
2. Consume events into live analyzer state plus temporary DuckDB-backed analysis storage.
3. Call `Finalize()`.
4. Build the final analysis result.
5. Render text or JSON output.

## Lifecycle and Cleanup

The analyze command owns the temp-store lifecycle.

### Creation

At the start of analysis, the command creates a temporary directory with a `binlogviz-duckdb-*` name pattern and places an `analysis.duckdb` file inside it.

### Active Use

The analyzer uses that database during the run while parsing progresses and while `Finalize()` assembles the final result.

### Cleanup

When the command finishes, cleanup closes the DuckDB store and removes the whole temporary directory. This means the database is expected to disappear after the run, whether the output is text or JSON.

From an operator point of view, there is no persistent DuckDB environment to administer.

## Disk and Performance Implications

The DuckDB temp store changes the runtime profile in useful ways, but it is still visible operationally.

### Temporary Disk Usage

Each analysis run uses temporary local disk space for the DuckDB file. For larger binlog inputs, operators should expect some temporary disk growth during the run.

### Finalization Cost

Parsing remains streaming, but the finalize phase can still take noticeable time on larger inputs because BinlogViz must assemble report results after event ingestion ends. The `Finalizing analysis...` message on `stderr` is the visible marker for that phase transition.

### Memory Tradeoff

The temp store helps avoid pushing all temporary analysis state into RAM. That usually improves behavior on larger inputs, but it also means performance depends partly on local disk characteristics.

## What Operators Should Remember

- DuckDB is temporary internal working storage.
- It exists to support streaming analysis and finalize-time result assembly.
- It is created per command run and removed during cleanup.
- You may notice temporary disk usage and a separate finalization phase on larger inputs.
- You do not need to manage or preserve the DuckDB file after analysis completes.
