# BinlogViz v0.23.11 Release Notes

Release date: 2026-10-03

## Overview

v0.23.11 separates an empty schema or table filter from a file that cannot be read. A filter that matches nothing stays exit 2, with empty stdout and `Error: schema/table filter matched no events`. A file that ends in a partial event is exit 1, `Error: binlog is truncated or corrupt`. `--snapshot-name` with any format other than json fails before a report and writes no snapshot file. A failing command clears the stderr progress line before `Error:`. Analyze ranking is unchanged. Workflow and trend behavior are unchanged.

## Bug Fixes

- **Empty filter stays distinct from a bad file (#102, #93)**: A schema or table filter that matches nothing stays exit 2, empty stdout, stderr `Error: schema/table filter matched no events`. A file that ends in a partial event is exit 1, `Error: binlog is truncated or corrupt`. A bad magic header stays exit 1 with `not a MySQL binlog`. A complete Format Description-only file is still exit 2, `binlog has no analyzable events`.
- **`--snapshot-name` requires json (#102, #94)**: `--snapshot-name` with any format other than json fails before a report: `Error: --snapshot-name requires --format json`, and no snapshot file is written. `--format json --snapshot-name` still saves the JSON snapshot and exits 0.
- **Error line leaves the progress line (#102, #95)**: A failing command clears the stderr progress line before printing `Error:`, so the error does not stay on the same line as the progress text. Analyze results are unchanged.

## Verification

BinlogQA dogfood on tip `f99dc5b` (#102): PASS for the four checks (empty schema/table filter, truncated file, bad magic header, Format Description-only file). No new issues.

## Breaking Changes

A truncated or corrupt file that previously shared exit 2 with an empty schema/table filter is now exit 1.

## Compatibility

- Exit codes 0, 1, and 2 keep ADR-0001 meaning. A schema or table filter that matches nothing is still exit 2. A bad magic header is still exit 1. A complete Format Description-only file is still exit 2.
- Flags and analyze ranking semantics are otherwise unchanged from v0.23.10. `--format json --snapshot-name` still saves the JSON snapshot and exits 0.
- Workflow and trend behavior are unchanged.
