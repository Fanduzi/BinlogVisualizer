# BinlogViz v0.23.0 Release Notes

Release date: 2026-08-30

## Overview

v0.22.1 could summarize a ROW file. It could not close an incident. This release is the #59 scorecard: issues #42–#58 so an operator can bound a window, keep honest scope, and hand off replayable evidence without a second tool.

## New Features

- **Position and GTID analyze windows (#52)**: `--start-position` / `--stop-position` are half-open `[start, stop)` bounds on one explicit file. `--include-gtids` / `--exclude-gtids` accept MySQL UUID:interval sets and MariaDB `domain-server-seq` lists. Time flags may still be supplied; the predicates intersect. Anonymous / unkeyed events under a GTID selector are omitted rather than counted as the selected set.
- **Provenance and SQL context stay on the transaction (#51)**: reports keep `user@host`, `thread_id`, GTID, `server_id`, and `xid` when the binlog carried them. `--sql-context full` is no longer a no-op.
- **Markdown can close a ticket (#55)**: `--format markdown` keeps `file:pos`, `mysqlbinlog`, DDL, and `txn_key` instead of dropping the evidence an incident note needs.
- **Input bytes versus counted bytes (#48)**: HTML summary shows the file/window volume separately from the ROW images actually counted, so a 311 MiB file is not summarized only by the hot-minute subset.
- **Compare and trend carry replay evidence (#44)**: HTML findings include `file:pos` and a copyable `mysqlbinlog` command, not only the hot table name.

## Bug Fixes

- **Relative workflow output is resumable (#42)**: `output_dir` that is relative to the extracted tarball still finds `plan.yaml` on `workflow resume`.
- **Object filters apply to the whole analyze report (#43)**: `--include-table` / `--exclude-schema` now constrain summary, large transactions, and diagnostics, not only Top Tables.
- **`compare` rejects raw binlogs (#45)**: two `.000001` files no longer dump Usage twice plus `invalid character`. Pass named snapshots or `analyze --format json` reports.
- **Clean reports do not invent a suspicious position (#46)**: Next Actions omit “First suspicious position” when findings and alerts are empty. A location is printed only when an alert or finding names a real transaction.
- **DDL-only is not “healthy” (#47)**: analyze HTML no longer leads with No issues / healthy on the same page as a CREATE TABLE timeline.
- **HTML transaction evidence is a top-N set (#49)**: largest / longest / widest no longer collapse to the same `txn-1` card on a uniform large file.
- **Causal compare/trend requires a comparable workload (#50)**: different server / schema / selector / incomplete transaction evidence cannot produce a causal story. The guard prints first; raw deltas remain. Persisted position and GTID selectors are part of that scope.
- **Window cuts are marked partial (#53)**: `--start` / `--end` that split a transaction no longer look complete.
- **`--top` is presentation, not a rewrite (#54)**: `--top 1` no longer turns remaining Row Share into 100% or delete the other tables from JSON.
- **Bounded transaction output is disclosed (#56)**: JSON `transactions` is not the first 10 keys in string order. The bound is visible, so `txn-5` is not silently missing.
- **MariaDB XA and LOAD DATA keep their shape (#57)**: an XA group is not glued onto the next GTID; LOAD DATA is not rendered as a plain INSERT.
- **Timestamps are UTC everywhere (#58)**: text and HTML print `UTC`; they no longer disagree with JSON `Z`.

## Breaking Changes

- **`compare` no longer accepts raw binlog files.** Scripts that passed two binlogs must analyze to JSON or snapshots first.
- **Causal findings, recommendations, and drilldowns are suppressed** when comparability is `unknown` or `not_comparable`. Raw numeric deltas still emit.
- **`--top` no longer mutates JSON table membership or shares.** Consumers that assumed JSON contained only N tables will now see the full table set plus a presentation limit.
- **GTID selectors omit anonymous and unkeyed events** instead of counting them inside the selected window.
- **Text/HTML timestamps include a `UTC` suffix.**

## Compatibility

- Snapshot, compare, and trend JSON keep their existing top-level shapes. New fields are additive: comparability, provenance, input-versus-counted bytes, evidence refs, and transaction completeness.
- Exit codes from v0.22.1 are unchanged.
- #59 is the scorecard for this work, not a separate runtime contract.
