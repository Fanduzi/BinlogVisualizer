# Limitations

This document explains the product boundaries of BinlogViz so operators know what the tool is designed to do and what it intentionally does not do.

## Supported Binlog Format

BinlogViz is built around local MySQL `ROW`-format binlog files.

That boundary matters because the analyzer is designed to consume normalized row-oriented events and derive workload statistics from row activity. If your operational question depends on statement-based replay semantics, a different tooling path is more appropriate.

In practical terms, use BinlogViz when your input is a local ROW-format binlog sequence and your goal is workload inspection rather than logical replay.

## Input Scope

BinlogViz analyzes local files only.

The command accepts input in one of two ways:

- explicit positional binlog paths
- discovery mode with `--from-dir` plus `--prefix`

It does not attempt to fetch remote binlogs, connect to a live MySQL server, or manage replication coordinates for you. Discovery mode is also intentionally narrow: it scans one directory, filters by prefix plus numeric suffix, and orders the result set for analysis.

This makes the input contract predictable, but it also means operators must prepare the local file set themselves.

## Runtime Model

BinlogViz uses a single-pass streaming command path:

```text
parse -> normalize -> consume -> finalize -> render
```

This design keeps live in-memory state bounded, but it does not mean analysis is cost-free at the end of the run.

Important runtime implications:

- parsing is streaming
- final report assembly still happens after parsing completes
- a temporary DuckDB store is created per command
- larger inputs may show noticeable finalization time and temporary disk usage

So the product is optimized for bounded streaming analysis, not for zero-disk or fully incremental interactive exploration.

## SQL Context Boundaries

SQL context is intentionally bounded and presentation-oriented.

Current limits include:

- stored SQL capped at `4096` bytes
- query summaries capped at `160` characters
- query fields shown or omitted according to `--sql-context`

This means:

- BinlogViz does not promise lossless storage of original SQL text
- `summary` mode is meant for quick operator orientation
- even `full` mode only exposes the bounded stored SQL, not unlimited original statements

If your workflow requires complete long-form SQL archival or forensic preservation, BinlogViz should not be treated as that system of record.

## Output and Contract Boundaries

BinlogViz separates channels deliberately:

- final report on `stdout`
- progress, resolved-file listings, finalization status, and command errors on `stderr`

That contract supports shell redirection and automation, but it also means operators should not treat `stderr` noise as part of the report payload. If you redirect only `stdout`, you capture the report. If you need runtime status logs too, capture `stderr` separately.

## Product Focus

BinlogViz is focused on workload inspection, not on full operational control planes.

It is designed to help answer questions like:

- which tables were hottest
- which transactions were largest
- when activity spiked
- what the overall write workload looked like

It is not positioned as:

- a MySQL replication manager
- a live binlog tailing service
- a statement replay engine
- a full historical data reconstruction tool
- a general-purpose SQL observability platform

## Non-Goals

To keep the tool focused, these are explicit non-goals for the product shape documented today:

- managing or modifying MySQL server state
- reading from remote MySQL instances directly during analysis
- preserving unlimited raw SQL text in reports
- turning progress output into part of the machine-readable report stream
- replacing deeper replication, forensic, or observability systems

Use BinlogViz when you need a fast operational summary of local ROW binlog workload. Reach for other tooling when you need remote collection, statement-perfect reconstruction, or a broader database operations platform.
