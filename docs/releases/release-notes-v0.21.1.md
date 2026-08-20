# BinlogViz v0.21.1 Release Notes

Release date: 2026-08-21

## Changes

- **Sample ROW binlog ships in the release archive**: `testdata/minimal.binlog` is attached next to the binary so `binlogviz analyze testdata/minimal.binlog` can verify an install without a production MySQL datadir. The same 1500-byte fixture remains in the repo at `cmd/binlogviz/testdata/minimal.binlog`.
- **Runnable `incident.yaml` at the repo root**: `binlogviz workflow run incident.yaml` points at `cmd/binlogviz/testdata/sample-binlog`. Validate still exits 0 on a `PLACEHOLDER` `from_dir`, but it now warns. `workflow resume` with nothing left to do exits 0.
- **Default text report is an incident brief**: summary, hot tables, and up to three largest transactions come before findings. Pattern representative transactions must match the pattern (a DELETE large-batch drilldown no longer cites a 400k-row INSERT).
- **Usable transactions print a copyable `mysqlbinlog` command**: same-file spans emit `--start-position` / `--stop-position`. A 31-byte XID-only interval is not offered as a start position.

## Bug Fixes

- **Text findings match JSON alerts**: a 5-row / 4-transaction sample is no longer labeled `[critical] Write spike` while JSON `alerts` is empty. Hot minutes and longest transactions stay evidence, not synthesized findings.
- **Single-row UPDATE counts as 1 logical row**: go-mysql CamelCase names such as `UpdateRowsEventV2` were missed by the old `UPDATE` substring check.
- **STATEMENT / MIXED is no longer a silent ROW subset**: stderr warns that only ROW images are counted. STATEMENT with zero row images still prints the report, then exits non-zero.
- **MariaDB 11.4+ txn spans are real again**: `LogPos=0` events are reconstructed from the file cursor, so a 400k-row transaction is no longer stored as a 31-byte XID. Query `CREATE DATABASE` / `CREATE TABLE` reach the DDL timeline.
- **`compare` and `trend` keep the operator's story**: `trend last_week tonight` stays last_week → tonight (`--order cli`). Largest-txn compare uses table/op/rows/file:span identity instead of local `txn-1`. Growth from `0` to `N` renders as `new` (JSON `delta_percent: null`), not `+0.0%`.
- **Analyze CLI dogfood**: HTML “saved to …” goes to stderr; analyze failures print `Error:` once without a Usage dump; magic-only truncated files exit non-zero; `--prefix mysql-bin` matches `mysql-bin.000008`.
- **Failed workflow discovery is not `complete`**: empty failed discovery reports `runtime_state=incomplete`.

## Breaking Changes

- **STATEMENT with no ROW images exits non-zero**: `analyze` still writes the report, then fails the process so an empty STATEMENT file cannot look like a successful ROW analysis. MIXED with a ROW subset still exits 0 and warns on stderr.
- **Compare `0 → N` percent is `null` / `new`, not `0.0%`**: scripts that treated `delta_percent: 0` as “unchanged” will now see `null` when baseline is 0 and current is positive.
- **`trend` default order is CLI argument order**: explicit `trend last_week tonight` no longer re-sorts by binlog window start. Use `--order time` to restore chronological First/Last.
