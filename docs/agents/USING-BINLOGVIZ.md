# Using BinlogViz

This is the usage guide for operators and for an agent driving the `binlogviz` CLI. The other files in `docs/agents/` are internal delivery notes from past work. They are not instructions for running the tool.

A release archive contains this file, `docs/agents/USING-BINLOGVIZ.zh-CN.md`, `README.md`, `README_ZH.md`, `binlogviz`, `incident.yaml`, and `testdata/`. Commands below use those archive paths. In a git checkout, `testdata/minimal.binlog` is `cmd/binlogviz/testdata/minimal.binlog`.

Report times are UTC. `--start` and `--end` accept RFC3339. An offset is the instant: `2026-10-19T14:05:00+08:00` is `2026-10-19T06:05:00Z`. `YYYY-MM-DD HH:MM:SS` uses the machine's local timezone. Prefer an offset across machines.

`flashback` does not connect to MySQL. Review the script, test it, and apply it yourself with `binlogviz flashback verify <file> | mysql`. `mysql < file` commits nothing. A failed statement, including under `mysql --force`, does not commit a prefix. The undo is one XA transaction (ADR 0014).

Apply needs `CREATE`, `CREATE ROUTINE`, and `ALTER ROUTINE` on `binlogviz.*`, and `SELECT, INSERT, UPDATE, DELETE` on the tables the undo changes:

```sql
GRANT CREATE, CREATE ROUTINE, ALTER ROUTINE ON `binlogviz`.* TO 'account'@'host';
GRANT SELECT, INSERT, UPDATE, DELETE ON `app`.* TO 'account'@'host';
```

`UPDATE` and `DELETE ... WHERE` need `SELECT` (ERROR 1143 without it). `CREATE` on `binlogviz.*` covers `CREATE DATABASE IF NOT EXISTS binlogviz`. With `automatic_sp_privileges=0`, also grant `EXECUTE` on `binlogviz.*`. `verify` names the finish procedure for that run and drops it at the end, including after a rollback. A rolled-back apply prints `binlogviz: undo rolled back, no rows changed`. Without `--force` the mysql client exits 1. Success is a line matching `^binlogviz: undo committed, [0-9]+ statements$`. The marker is built from CONCAT pieces, so `mysql --force -v` echoes SQL that a plain grep can mistake for success. Automation uses `set -o pipefail` and `grep -E`. Exit 0 without that line is not success. N counts undo statements that changed exactly one row. A keyed `UPDATE` or `DELETE`, including a no-PK `LIMIT 1` match, must affect exactly one row or the undo rolls back. `--conflict guard` counts a 0-row match as skipped and any other count as a rollback. The marker prints only when at least one row changed. `BINARY(n)` values are written padded to n bytes with 0x00; `VARBINARY`, `BLOB`, and `CHAR` are not. A stream cut after `COMMIT` leaves the undo committed with no marker. A kill after `CREATE PROCEDURE` and before the finish `CALL`, or a `CALL` that never enters the body, can leave that run's procedure. Clean a leftover with `DROP DATABASE binlogviz`.

## Recipe 1: a write spike, then flashback

Find the busy minute, name the transaction by GTID, preview the undo, then generate a guarded script.

```bash
binlogviz analyze mysql-bin.000001 \
  --start 2026-10-19T06:00:00Z --end 2026-10-19T06:10:00Z \
  --detect-spikes

binlogviz analyze mysql-bin.000001 \
  --start 2026-10-19T06:05:00Z --end 2026-10-19T06:06:00Z \
  --format json --diagnostics summary

binlogviz flashback mysql-bin.000001 --include-gtids '<gtid>' --plan
binlogviz flashback mysql-bin.000001 --include-gtids '<gtid>' --conflict guard
```

Spike detection is off unless you pass `--detect-spikes`. Without it, `alerts` still lists transactions over `--large-trx-rows` (default 1000). Read `alerts[].gtid` when the alert is about one transaction. It is the same string as `transactions[].gtid`. Pass that string to `--include-gtids`. A spike alert, an input-format warning, and a no-primary-key warning have no `gtid`.

`--plan` prints JSON and does not print the undo SQL. `plan_version` is 2. Exit 7 means a selected row changed again later (`--conflict refuse`, the default). `--conflict guard` matches the logged after-image instead of refusing. Exit 6 means flashback will not write the script (DDL in range, a row image that is not FULL, or `--sql-context off`).

`testdata/minimal.binlog` is a small ROW sample. It may have no spike and no alert. The commands are the same on a real file.

## Recipe 2: a smaller JSON document

```bash
binlogviz analyze testdata/minimal.binlog --format json --diagnostics summary > window.json
binlogviz analyze testdata/minimal.binlog --format json --diagnostics off > tiny.json
binlogviz analyze testdata/minimal.binlog --format json --snapshot-name window --snapshot-dir /tmp/snaps
```

| `--diagnostics` | JSON |
|---|---|
| `full` (default) | Today's `diagnostics` object. No `diagnostics_mode` key. |
| `summary` | Drops the transaction rankings and the other lists already at the top level. `ddl_events` is the first `--top` events (default 10). `ddl_events_omitted` counts the rest, including 0. Top-level `alerts`, `transactions`, `tables`, and `summary` stay. |
| `off` | Omits `diagnostics`. |

`summary` and `off` set `"diagnostics_mode"` to that mode. Text, Markdown, and HTML ignore the flag. `--snapshot-name` always saves full diagnostics; only stdout is trimmed.

Do not pass `window.json` to `compare`, `trend`, or `snapshot save`. Those commands exit 9 (`unsupported`, `details.reason` `reduced_diagnostics`). Use the snapshot from `--snapshot-name`. `snapshot list` exits 0 and shows the mode in the `DIAGNOSTICS` column, and as `diagnostics_mode` in `--format json`, so a copied summary file is visible before show or compare fails.

JSON `null` for `diagnostics_mode` is the same as a missing key. A non-string such as `1` is corrupt (exit 5). `--diagnostics ""` is a usage error (exit 3). Omitting the flag is `full`.

## Recipe 3: compare two windows

```bash
binlogviz analyze mysql-bin.000001 --format json --snapshot-name before \
  --start 2026-10-19T05:00:00Z --end 2026-10-19T06:00:00Z --snapshot-dir /tmp/snaps
binlogviz analyze mysql-bin.000001 --format json --snapshot-name during \
  --start 2026-10-19T06:00:00Z --end 2026-10-19T07:00:00Z --snapshot-dir /tmp/snaps
binlogviz snapshot list --snapshot-dir /tmp/snaps
binlogviz compare --current-snapshot during --baseline-snapshot before --snapshot-dir /tmp/snaps
binlogviz trend before during --snapshot-dir /tmp/snaps
```

`compare --format json` and `trend --format json` start with `"schema_version": 1`. Pass the same `--workload-id` on both analyzes when you want causal findings. Different workload IDs are `not_comparable`. The numeric deltas are still printed.

`binlogviz workflow run incident.yaml` runs the bundled plan against `testdata/sample-binlog`.

## Exit codes

The kind string does not change with `--lang`. `--error-format json` prints one JSON object on stderr.

| Exit | Kind | Typical cause |
|---|---|---|
| 0 | | Counted events, or the command finished |
| 1 | `error` | Parse or other failure with no more specific kind |
| 2 | `no_data` | Readable binlog, nothing in the window or matching the filter |
| 3 | `usage` | Bad flag or arguments, including `--diagnostics ""` or `nope` |
| 4 | `input` | File or snapshot missing |
| 5 | `corrupt` | Not a binlog, or JSON that does not match the report shape, including a non-string `diagnostics_mode` |
| 6 | `refused` | Flashback will not emit SQL, or `flashback verify` will not print a script. Generation `details.reason`: `row_metadata`, `row_image`, `column_type`, `row_capture`, `ddl_in_range`, `generated_unknown`, `generated_mismatch`, `generated_row`, `schema_mismatch`, `sql_context_off`. `details.table` is set when one table is named. Verify `details.reason`: `not_flashback`, `truncated`, `missing_trailer`, `appended`, `digest_mismatch`, `old_format` (regenerate a v0.31 or v0.32.0 script). Verify writes nothing to stdout |
| 7 | `conflict` | A selected row changed again later |
| 8 | `output` | Could not write the report or the script |
| 9 | `unsupported` | STATEMENT/MIXED with no ROW images, an unknown version, or a trimmed diagnostics document |
| 10 | `resource` | Flashback hit a spill ceiling (`BINLOGVIZ_SPILL_BYTES`, `details.reason` `spill_limit`) or the heap ceiling (`BINLOGVIZ_MEMORY_BYTES`, or `GOMEMLIMIT` when that is unset; `details.reason` `memory_limit`). A set `BINLOGVIZ_SPILL_BYTES` or `BINLOGVIZ_MEMORY_BYTES` that is not a positive base-10 integer is exit 3. Unset `BINLOGVIZ_SPILL_BYTES` means no byte ceiling. Not an empty result |

## JSON versions

Read the version key first. Adding a key does not bump the number. Removing, renaming, or changing the meaning of a key does, and CHANGELOG marks that as breaking.

| Output | Key | Current |
|---|---|---|
| `analyze --format json`, saved snapshots | `report_version` | 3 |
| `flashback --plan` | `plan_version` | 2 |
| `flashback verify --format json` | `schema_version` | 1 |
| `compare`, `trend`, `snapshot list`, `snapshot show` `--format json` | `schema_version` | 1 |
| `workflow validate`, `describe`, `status`, `clean`, `export` `--format json` | `schema_version` | 1 |
| `version --format json` | `schema_version` | 1 |

`binlogviz version --format json` also prints `version`, `commit`, `commit_time`, `modified`, and `go_version`.

`report_version` stays 3 for `--diagnostics summary` and `off`. The summary DDL list is capped; `ddl_events_omitted` is how you see that. Default `full` JSON does not have that key.
