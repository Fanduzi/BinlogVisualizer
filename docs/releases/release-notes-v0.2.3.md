# BinlogViz v0.2.3 Release Notes

## Scope

`v0.2.3` is a focused CLI usability release for Phase 2 analysis workflows.

It does not change the analysis result model. Instead, it makes long-running `analyze` executions easier to observe by adding parse progress based on the summed size of the ordered input binlog files.

## Highlights

- Added a single aggregate parse progress bar for `binlogviz analyze`
- Based progress on the summed size of the input binlog files in command order
- Kept progress rendering and finalization status on `stderr` so `stdout` remains clean for text and JSON reports
- Preserved the current serial multi-file parsing model while correctly handling repeated input paths
- Added regression coverage for parser progress reporting, duplicate-path aggregation, and `stdout`/`stderr` separation

## Operator Notes

- Progress reflects parse-time file offset movement, not finalize-time DuckDB work
- After parsing completes, the command prints `Finalizing analysis...` on `stderr` before rendering the final report
- JSON output remains machine-readable because progress never writes to `stdout`

## Packaging Notes

- Release artifacts continue to target:
  - `darwin/amd64`
  - `darwin/arm64`
  - `linux/amd64`
  - `linux/arm64`
- Release downloads include a checksums manifest.
- Preferred installation path remains GitHub Release artifacts; source builds remain the fallback.
