# BinlogViz v0.28.0 Release Notes

Release date: 2026-10-19

## Overview

v0.28.0 lets an agent shrink `analyze` JSON to fit its context window, and puts the GTID on every alert that is about one transaction, so the alert can go straight to `flashback --include-gtids`. A trimmed document can never be compared as if it were complete: snapshots and workflow artifacts always keep full diagnostics, and every command that reads reports refuses a trimmed one. ADR 0011 records the design (`docs/adr/0011-diagnostics-budget-and-alert-gtid.md`). There are no breaking changes.

## Added

- **`analyze --diagnostics full|summary|off` (#282, #307, ADR 0011)**: sets how much of `diagnostics` `analyze --format json` writes. The default is `full`, which is the same object as v0.27.0.
  - `summary` keeps `file_coverage`, `counted_event_bytes`, `ddl_events`, `open_dml_groups`, `duration_buckets`, `input_format_guess` and the counts. It empties the four transaction rankings, `file_segments`, `hot_intervals` and `findings`, which are already at the top level as `transactions`, `timeseries`, `minutes` and `alerts`.
  - `off` omits `diagnostics`.
  - A `summary` or `off` document carries a top-level `"diagnostics_mode": "summary"` or `"off"`. A full document has no such key.
  - Text, Markdown and HTML ignore the flag. An invalid value is a usage error (exit 3).
  - Sizes of stdout in bytes:

    | binlog | full | summary | off |
    |---|---:|---:|---:|
    | 1.07 GB, DDL-heavy | 63042 | 42759 | 22627 |
    | `mysql-8.0.46-busiest-minute.binlog` | 43556 | 21307 | 20603 |
    | `mysql-8.0.46-dml-full.binlog` | 32639 | 16011 | 12368 |

- **`alerts[].gtid` (#282, #307)**: an alert about one transaction carries `gtid` when that transaction has one. It is the same string as `transactions[].gtid` (MySQL `uuid:seq` or MariaDB `domain-server-seq`). Anonymous transactions, and alerts that are not about one transaction (spike, input format, no primary key), have no key. `message` and `txn_key` are unchanged. With `--detail-store duckdb` the alert GTID is kept as well; the output is identical to the in-memory store.
- **Snapshots and workflow artifacts always keep full diagnostics (#307, ADR 0011 §3)**: `--diagnostics` is a stdout budget only. `analyze --snapshot-name` saves the full document whatever `--diagnostics` says. Workflow analyze artifacts and workflow snapshots are always full, and workflow has no `--diagnostics`.
- **A trimmed report is refused (#307, ADR 0011 §3)**: a report whose `diagnostics_mode` is present and not `full` exits 9 (`unsupported`, `details.reason: reduced_diagnostics`, `details.diagnostics_mode`), with empty stdout and no files written. The message says to re-run `analyze` without `--diagnostics`, or to use a snapshot saved with `--snapshot-name`. The refusal applies to:
  - `compare` with report files, on either side, and with `--current-snapshot`/`--baseline-snapshot`;
  - `trend` with named snapshots, `--baseline-snapshot` and `--from-snapshots`;
  - `snapshot save <file>` and `snapshot show`;
  - `workflow resume` and `workflow resume --rerun`, whenever they reuse an analyze artifact.

  Before this, a trimmed document compared as if its omitted lists were empty. On the same binlog, that reported three DDLs as added and a 48-row hot-interval change, exit 0. A report without `diagnostics_mode`, such as one written by v0.27.0 or earlier, compares exactly as before.

## Verification

BinlogQA's PR QA on #307 and release-gate QA on main `afeadd31` passed with no P0, P1 or P2, against both the v0.27.0 tag build and the released v0.27.0 linux_amd64 archive.

- **Unchanged output**: on 50 binlogs × en/zh-CN, analyze text, Markdown and HTML (timestamp masked) are byte-identical in every `--diagnostics` mode. Of 100 default JSON runs, 84 are byte-identical; the other 16 differ only by the inserted alert `gtid` lines. On the 1.07 GB binlog, full JSON is byte-identical to v0.27.0. `flashback --plan` (including guard and `--conflict-limit 0`) and flashback SQL are byte-identical. The 67-case exit-code matrix is line-identical.
- **Alert gtid**: 24/24 alerts with a gtid match their transaction's gtid. No alert lacks one when its transaction has one. The 92 `--detail-store duckdb` documents are byte-identical to in-memory.
- **Snapshots**: 16/16 compares of a `summary`/`off` snapshot against a full snapshot of the same binlog show zero DDL, hot-interval and largest-transaction deltas. `diagnostics_mode` is absent from every workflow artifact and snapshot.
- **Refusal**: 528 runs (12 document variants × 11 commands × en/zh-CN × text/json errors). All 360 refusals exit 9 with empty stdout and no files changed; the JSON errors are `unsupported`/`reduced_diagnostics`. A trimmed artifact swapped into a workflow makes `--rerun compare` and `--rerun trend` exit 9 in 8/8 runs.
- **Old reports**: v0.26.1 and v0.27.0 reports and snapshots give 76/76 runs identical to v0.27.0 across compare, trend, `trend --from-snapshots` and `snapshot list|show`.
- CI, `go vet ./...` and `go test ./...` pass.

## Compatibility

- No breaking changes. `report_version` stays 3 and `plan_version` stays 2. `diagnostics_mode` and `alerts[].gtid` are additive keys; a consumer that rejects unknown keys will see `gtid` on alerts whose transaction has a GTID.
- Default `analyze` JSON is byte-identical to v0.27.0 unless an alert's transaction has a GTID. Text, Markdown, HTML, flashback output, exit codes, snapshot names and supported platforms are unchanged.
- `flashback` never connects to MySQL. Review the script, test it, and apply it in one new session on the primary with `sql_log_bin=1`, with the script header.

## Known issues

- **#303 (P2)**: the Linux release binaries report an empty `commit` and `commit_time` in `binlogviz version`, because the release workflow does not pass the commit ldflags to the Linux builds yet. macOS archives and builds from a git checkout report the commit. Until #303 is fixed, check a Linux binary against `binlogviz_0.28.0_checksums.txt` rather than by commit.
- **#308 (P3)**: `--diagnostics` follow-ups.
  - `"diagnostics_mode": null` is treated as absent, and a non-string value exits 5. Neither is documented.
  - `snapshot list` does not mark a reduced document.
  - `summary` keeps every `ddl_events` entry, so a DDL-heavy window shrinks only about 32%.
  - `--diagnostics ""` is accepted as `full`.
  - zh-CN `--help` flag text is in English.
- The HTML report's "Generated" timestamp makes two HTML runs differ when they straddle a second (pre-existing).
- `install.sh` P3s from #285, and #255, #256, #257 carry over from v0.27.0.

See `docs/concept/limitations.md` for the flashback limitations carried over from earlier releases.
