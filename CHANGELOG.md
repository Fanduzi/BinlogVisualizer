# Changelog

This file records user-visible changes for tagged releases.

## [Unreleased]

## v0.23.20

Release date: 2026-10-07

Highlights:

- `binlogviz analyze` reports replica apply delay from MySQL 8 `original_commit_timestamp` and `immediate_commit_timestamp`, after Busiest Minutes, in text, Markdown, and HTML. `--lang zh-CN` translates the section. JSON adds optional `replica_apply_delay`. On `mysql-8.0.46-replica-apply.binlog` the max delay is 5426519 µs (5.426519s), p95 is 5208595 µs (5.208595s), the peak minute is 2026-10-07 11:41:00 UTC, and the slowest transaction is `11458b63-c244-11f1-a0d2-822b383dbcd0:7` at byte 197 (`shop.audit`, 1 row). The lead sentence assumes the source and replica clocks agree and is not a finding. A source file where the two timestamps are equal says so in one line; JSON `origin` is `source` and `max_delay_us` is 0, with no transaction ranking. Missing timestamps print `commit timestamps unavailable`, omit `replica_apply_delay`, and never fake a delay of 0. There is no new flag. Exit codes are unchanged.

Related notes:

- [v0.23.20 release notes](docs/releases/release-notes-v0.23.20.md)
- [v0.23.20 中文发行说明](docs/releases/release-notes-v0.23.20.zh-CN.md)

## v0.23.19

Release date: 2026-10-07

Highlights:

- DDL Timeline names the transaction that holds each DDL. `diagnostics.ddl_events[].gtid` is set when the binlog has one (MySQL `uuid:seq` or MariaDB `domain-server-seq`) and omitted, with `GTID unavailable`, when GTID is off or anonymous. The file and byte are where that transaction starts: the GTID event start when one exists, not the Query event and not `end_log_pos`. `position_start` and `position_end` stay the Query event. The timeline prints copy-ready `mysqlbinlog --stop-position=<N> <file>` and, when a GTID exists, `BinlogServer stop_gtid=<gtid>`. That stop replays earlier events and excludes this DDL. On `mysql-8.0.46-drop-table.binlog`, `DROP TABLE shop.orders` is `4d8275bc-c221-11f1-a25e-822b383dbcd0:2` at byte 523. `server_id`, `thread_id`, and `user@host` appear only when the DDL event recorded them. `--sql-context off` still drops the statement. There is no new flag. Exit codes are unchanged. JSON fields are additive. Pairs with BinlogServer v0.5.54 `stop_gtid`.

Related notes:

- [v0.23.19 release notes](docs/releases/release-notes-v0.23.19.md)
- [v0.23.19 中文发行说明](docs/releases/release-notes-v0.23.19.zh-CN.md)

## v0.23.18

Release date: 2026-10-07

Highlights:

- `binlogviz analyze` names the tables that produced each busiest minute in text, Markdown, and HTML. On `mysql-8.0.46-busiest-minute.binlog`, Activity peaks at `Rows/min: 32.0 at 2026-03-15 14:05` (`shop.orders` 30, `shop.catalog` 2) while Top Tables stays led by `shop.catalog` (82). JSON `diagnostics.hot_intervals[].table_rows` and `minutes[].table_rows` are unchanged. There is no new flag. `--detect-spikes` stays opt-in.

Related notes:

- [v0.23.18 release notes](docs/releases/release-notes-v0.23.18.md)
- [v0.23.18 中文发行说明](docs/releases/release-notes-v0.23.18.zh-CN.md)

## v0.23.17

Release date: 2026-10-07

Highlights:

- `binlogviz analyze` lists tables that received UPDATE or DELETE rows and have no primary key, ranked by those counts, in text, Markdown, JSON, and HTML. On a replica, those rows can scan the table. MySQL 8 `binlog_row_metadata=FULL` TABLE_MAP metadata (`SIMPLE_PRIMARY_KEY` / `PRIMARY_KEY_WITH_PREFIX`) marks each table `has_pk`, `no_pk`, or `unknown`. INSERT-only no-PK tables are named and are not ranked as a lag risk. Without FULL metadata the report leaves presence `unknown` and does not emit `no_pk`. A `no_primary_key` warning is added to Top Findings. There is no new flag.

Related notes:

- [v0.23.17 release notes](docs/releases/release-notes-v0.23.17.md)
- [v0.23.17 中文发行说明](docs/releases/release-notes-v0.23.17.zh-CN.md)

## v0.23.16

Release date: 2026-10-07

Highlights:

- `--show-rows` prints `TIMESTAMP` (including fractional seconds) as the UTC wall clock of the stored instant. It no longer follows the process timezone. `DATETIME` is unchanged.

Related notes:

- [v0.23.16 release notes](docs/releases/release-notes-v0.23.16.md)
- [v0.23.16 中文发行说明](docs/releases/release-notes-v0.23.16.zh-CN.md)

## v0.23.15

Release date: 2026-10-07

Highlights:

- `--dml insert,update,delete` (combinable) keeps only those ROW kinds. It composes with existing table, schema, time, position, and GTID filters. Summary, Top Tables, Top Transactions, Top Threads, and alerts count only the kept kinds, and the report names the filter. Nothing matched is exit 2, empty stdout, `Error: dml filter matched no events`. JSON field: `scope.dml`.
- `--show-rows` is off by default. On: DELETE before-image; UPDATE only changed columns (`before -> after`) plus the unchanged count; INSERT after-image. Column names come from MySQL 8 `binlog_row_metadata=FULL`; otherwise `@1`..`@N` and the report says names are missing. Unsigned FULL prints unsigned (for example `3000000000`). Without signedness metadata, integers that differ print both forms. Bounds: 32 logical rows per transaction, 64 bytes per value, with `… [truncated: <shown> of <original> bytes]` and a count of omitted rows. `--sql-context off` also omits these cell values and says so. Each listed transaction still has `mysqlbinlog_cmd`. JSON: `transactions[].rows` with `op`, `columns`, `names`, `before`, `after`, and `changed` (DELETE sets `before`, INSERT sets `after`, UPDATE sets both and `changed`; SQL NULL is JSON `null`), plus `column_names_note` and `row_values_note`.
- Default text, Markdown, JSON, and HTML are unchanged when neither flag is set. Flashback SQL is not in this release.
- Known issue (#132), not a blocker and not fixed: `--show-rows` `TIMESTAMP` values follow the process timezone. `mysqlbinlog -v` shows the UTC instant. `DATETIME` is unaffected.

Related notes:

- [v0.23.15 release notes](docs/releases/release-notes-v0.23.15.md)
- [v0.23.15 中文发行说明](docs/releases/release-notes-v0.23.15.zh-CN.md)

## v0.23.14

Release date: 2026-10-06

Highlights:

- Default text, Markdown, JSON, and HTML analyze reports include Top Threads/Sessions, ranked by rows when any session wrote rows, otherwise by events, bytes, or transactions. Each row shows thread id, and server id, user@host, and schema when the binlog carried them. `--top` limits the section; `--top-threads` overrides it (`0` is unlimited). JSON field: `threads`.
- `--sql-context off` omits query text and DDL statement text from every format. `summary` keeps one bounded line. `full` prints stored SQL capped at 4096 bytes. A cut in any format ends with `… [truncated: <shown> of <original> bytes]`. `query_truncated` means that 4096-byte store cap, not the 160-character summary line.
- MySQL `IDENTIFIED BY` and `IDENTIFIED WITH … AS` or `BY`, `GRANT … IDENTIFIED`, and `SET PASSWORD` credential literals are rewritten to `<secret>` before they are shown, in every `--sql-context` mode. MariaDB `IDENTIFIED VIA` or `WITH` plus `USING`, `AS`, or `BY` is included, including `PASSWORD('…')` and a hash literal, and so is a chain of `OR` plugin rules.
- MariaDB `SET PASSWORD` is DDL: it closes its GTID group, appears on the DDL Timeline, and the password is `<secret>`. It is not an Ignored QUERY.
- `--sql-context full` caps DDL statement text at the same 4096-byte store limit as query text and appends `… [truncated: <shown> of <original> bytes]` when that cap cuts it. `summary` still uses the 160-character line and the original byte length.
- DDL Timeline keeps `CREATE`/`ALTER`/`DROP` `VIEW`, `TRIGGER`, `PROCEDURE`, `FUNCTION`, and `EVENT` (object `view`, `trigger`, `routine`, or `event`). `CREATE TRIGGER` and `DROP TRIGGER` both use the trigger name. DDL that does not match a known object is still listed as generic `DDL` instead of being dropped.
- `--include-table` / `--exclude-table` match those object names the same way as tables (`TABLE` or `SCHEMA.TABLE`). A filter that matches a view, event, function, procedure, or trigger exits 0 and prints that DDL even when no rows changed. A filter that matches nothing is still exit 2.
- `binlogviz analyze -` and a non-seekable path such as a pipe read a binary binlog from stdin (copied to a temporary file because parsing needs seek). The copy is removed when the command finishes, including SIGHUP (exit 129), SIGINT (exit 130, `Error: interrupted`), SIGQUIT (exit 131), and SIGTERM (exit 143). Only a real terminal is reported as a terminal; `/dev/null` and an empty pipe say `stdin has no data`. Stdin replay hints name the positions and do not invent a file path. `mysqlbinlog` text is not a binlog and still fails the magic-header check.

Related notes:

- [v0.23.14 release notes](docs/releases/release-notes-v0.23.14.md)
- [v0.23.14 中文发行说明](docs/releases/release-notes-v0.23.14.zh-CN.md)

## v0.23.13

Release date: 2026-10-05

Highlights:

- `CREATE INDEX`, `CREATE UNIQUE INDEX`, `CREATE FULLTEXT INDEX`, `CREATE SPATIAL INDEX`, and `DROP INDEX` are counted on the table after `ON`, on the DDL timeline, in the per-table list, and under `--include-table`
- `CREATE UNIQUE INDEX` and `CREATE FULLTEXT INDEX` stay on the DDL timeline, and `CREATE SPATIAL INDEX` does too
- A schema written in the statement wins over the `USE` database
- `ON t(col)` and `CREATE TABLE t(id INT)` without a space are parsed as the table name

Related notes:

- [v0.23.13 release notes](docs/releases/release-notes-v0.23.13.md)
- [v0.23.13 中文发行说明](docs/releases/release-notes-v0.23.13.zh-CN.md)

## v0.23.12

Release date: 2026-10-03

Highlights:

- Default text, Markdown, and HTML analyze reports name, per transaction, `server_id`, `thread_id`, GTID, and `xid` or XA xid when the events have them
- `user@host` appears only when the binlog stored an invoker; a missing field stays absent (no `0`, no empty string)
- JSON already had these fields; `--sql-context full` is unchanged

Related notes:

- [v0.23.12 release notes](docs/releases/release-notes-v0.23.12.md)
- [v0.23.12 中文发行说明](docs/releases/release-notes-v0.23.12.zh-CN.md)

## v0.23.11

Release date: 2026-10-03

Highlights:

- A schema or table filter that matches nothing stays exit 2, with empty stdout and `Error: schema/table filter matched no events`
- A file that ends in a partial event is exit 1, `Error: binlog is truncated or corrupt` (that case previously shared exit 2 with an empty filter)
- A bad magic header stays exit 1 (`not a MySQL binlog`); a complete Format Description-only file stays exit 2 (`binlog has no analyzable events`)
- `--snapshot-name` with any format other than json fails before a report and writes no snapshot file; `--format json --snapshot-name` still saves the JSON snapshot and exits 0
- A failing command clears the stderr progress line before `Error:`; analyze results are unchanged

Related notes:

- [v0.23.11 release notes](docs/releases/release-notes-v0.23.11.md)
- [v0.23.11 中文发行说明](docs/releases/release-notes-v0.23.11.zh-CN.md)

## v0.23.10

Release date: 2026-10-02

Highlights:

- Committed duration is the earliest-to-latest non-zero in-window timestamp
- MySQL 8 stamps the leading GTID and the XID at commit, so file-order duration after `SLEEP` was 0s; `BEGIN` and row events keep the statement start
- Fixture `mysql-8.0.46-committed-duration.binlog` (`SLEEP(2)`) lands in the `1s-10s` bucket; `--large-trx-duration 1s` warns and the default `30s` does not
- Fixture `mysql-8.0.46-open-begin-dml.binlog` locks the open BEGIN+DML path: the full file exits 1 with duration, rows, tables, and span on the `Error:` line; an EOF prefix exits 0 and keeps `diagnostics.open_dml_groups`

Related notes:

- [v0.23.10 release notes](docs/releases/release-notes-v0.23.10.md)
- [v0.23.10 中文发行说明](docs/releases/release-notes-v0.23.10.zh-CN.md)

## v0.23.9

Release date: 2026-10-02

Highlights:

- `CHECK TABLE` is ADMIN by the two-word prefix, from a MySQL 8.0.46 ROW+GTID fixture
- `SET ROLE` is ADMIN by the two-word prefix, including `SET ROLE ALL` and `SET ROLE <name>`
- A maintenance-only GTID group of those statements closes, so the following business transaction lets analyze exit 0
- Exact `FLUSH TABLES` is unchanged; `FLUSH TABLES WITH READ LOCK` stays Unclassified QUERY

Related notes:

- [v0.23.9 release notes](docs/releases/release-notes-v0.23.9.md)
- [v0.23.9 中文发行说明](docs/releases/release-notes-v0.23.9.zh-CN.md)

## v0.23.8

Release date: 2026-10-02

Highlights:

- Default text prints a DDL occurrence timeline (time, operation, object, position, statement prefix)
- Open uncommitted BEGIN+DML groups are reported with duration, tables, rows, and file position span
- Default text lists longest committed transactions beside largest-by-rows and prints duration buckets
- Default text shows top transaction and table byte contributors, and per-file size/time span metrics
- Table and minute `txn_count` count distinct row-image transactions
- `trend` and `snapshot` failures print one `Error:` line and do not dump Usage
- Plain `ROLLBACK` closes explicit groups and `XA END` releases groups on subsequent GTID
- Specific `Error:` messages distinguish open `BEGIN`, `ROLLBACK TO SAVEPOINT`, and Ignored-only failures
- Project-local verify-binlogviz skill included for DBA-level CLI verification

Related notes:

- [v0.23.8 release notes](docs/releases/release-notes-v0.23.8.md)
- [v0.23.8 中文发行说明](docs/releases/release-notes-v0.23.8.zh-CN.md)

## v0.23.7

Release date: 2026-09-21

Highlights:

- Unclassified QUERY fails analyze with a statement prefix instead of a fake conflicting GTID
- Ignored QUERY is counted and never closes a transaction group
- Transaction payload inner ROW images are counted on one file-relative wrapper span
- Exact `FLUSH TABLES` is ADMIN from a MySQL 8.0.46 dialect fixture

Related notes:

- [v0.23.7 release notes](docs/releases/release-notes-v0.23.7.md)
- [v0.23.7 中文发行说明](docs/releases/release-notes-v0.23.7.zh-CN.md)

## v0.23.6

Release date: 2026-09-15

Highlights:

- Independent management QUERY (`ANALYZE TABLE`, `OPTIMIZE TABLE`, `FLUSH PRIVILEGES`, `SET DEFAULT ROLE`) closes its GTID group
- `--start`/`--end` accept `YYYY-MM-DD HH:MM:SS` in the machine-local timezone
- Explicit RFC3339 offsets and in-transaction GTID conflicts are unchanged

Related notes:

- [v0.23.6 release notes](docs/releases/release-notes-v0.23.6.md)
- [v0.23.6 中文发行说明](docs/releases/release-notes-v0.23.6.zh-CN.md)

## v0.23.5

Release date: 2026-09-07

Highlights:

- XA ROLLBACK closes its GTID group so the next GTID does not fail analyze
- Default analyze no longer needs CGO or DuckDB
- JSON reports unmapped parser events
- Zero-row XA is retained only with a recorded file location

Related notes:

- [v0.23.5 release notes](docs/releases/release-notes-v0.23.5.md)
- [v0.23.5 中文发行说明](docs/releases/release-notes-v0.23.5.zh-CN.md)

## v0.23.4

Release date: 2026-09-06

Highlights:

- Consecutive MariaDB GRANT/REVOKE DDL GTIDs no longer fail analysis
- `--include-table` accepts `SCHEMA.TABLE`
- `--prefix` accepts a complete filename
- `workflow export` accepts `-o`

Related notes:

- [v0.23.4 release notes](docs/releases/release-notes-v0.23.4.md)
- [v0.23.4 中文发行说明](docs/releases/release-notes-v0.23.4.zh-CN.md)

## v0.23.3

Release date: 2026-08-30

Highlights:

- Physical MariaDB XA PREPARE events retain their SQL-form `xa_xid`
- Zero-row XA COMMIT remains visible under its own GTID
- GTID and position selectors can target the XA COMMIT transaction
- Ordinary zero-row GTID groups and out-of-window XA ghosts remain omitted

Related notes:

- [v0.23.3 release notes](docs/releases/release-notes-v0.23.3.md)
- [v0.23.3 中文发行说明](docs/releases/release-notes-v0.23.3.zh-CN.md)

## v0.23.2

Release date: 2026-08-30

Highlights:

- Physical MariaDB XA PREPARE events close their GTID group
- The next legal GTID no longer fails analysis as a conflict
- Position and GTID selection work across prepared XA transactions
- Real in-transaction GTID conflicts remain rejected

Related notes:

- [v0.23.2 release notes](docs/releases/release-notes-v0.23.2.md)
- [v0.23.2 中文发行说明](docs/releases/release-notes-v0.23.2.zh-CN.md)

## v0.23.1

Release date: 2026-08-30

Highlights:

- Consecutive MariaDB DDL GTIDs no longer fail analysis with `conflicting GTID`
- GTID-started DDL groups close at their implicit DDL boundary
- Object-filtered DDL preserves the boundary without leaking into aggregates
- Explicit in-transaction GTID conflicts remain rejected

Related notes:

- [v0.23.1 release notes](docs/releases/release-notes-v0.23.1.md)
- [v0.23.1 中文发行说明](docs/releases/release-notes-v0.23.1.zh-CN.md)

## v0.23.0

Release date: 2026-08-30

Highlights:

- Position and GTID analyze windows (`--start-position`, `--stop-position`, `--include-gtids`, `--exclude-gtids`)
- Compare/trend require a comparable workload before causal narrative; raw deltas remain
- Reports keep replayable `file:pos` / `mysqlbinlog` evidence, provenance, and Markdown ticket fields
- `--top` and JSON transaction bounds no longer silently drop or rewrite operator data
- #59 is the scorecard for issues #42–#58, not a separate runtime contract

Related notes:

- [v0.23.0 release notes](docs/releases/release-notes-v0.23.0.md)
- [v0.23.0 中文发行说明](docs/releases/release-notes-v0.23.0.zh-CN.md)

## v0.22.1

Release date: 2026-08-29

Highlights:

- Release tar.gz includes the sample ROW binlog and an archive-relative `incident.yaml`
- `analyze --format html > report.html` writes HTML to stdout when stdout is not a TTY
- STATEMENT (zero ROW images) exits 1 with empty stdout; MIXED JSON adds an `input_format` alert
- Parsed-but-zero-events exits 2; magic-only stays exit 1
- Sub-second TPS prints `N/A (sub-second)`; workflow failures print `Error:` once
- Replay commands use absolute paths and `mariadb-binlog` on MariaDB Format Description

Related notes:

- [v0.22.1 release notes](docs/releases/release-notes-v0.22.1.md)
- [v0.22.1 中文发行说明](docs/releases/release-notes-v0.22.1.zh-CN.md)

## v0.22.0

Release date: 2026-08-26

Highlights:

- Redesigned HTML report UI with dark-mode aesthetic, glowing status indicators, and responsive glassmorphism cards across Analyze, Compare, and Trend reports
- Added sortable `BINLOG BYTES` physical volume metrics in Top Tables, transaction diagnostics cards, and hot interval summaries
- Added synchronized multi-chart linkage (`echarts.connect`) across shared-dimension timelines
- Added interactive mouse-selection range zoom (Toolbox Area Zoom & Restore, bottom DataZoom Slider, and wheel zoom)
- Added floating back-to-top button, theme switcher, and one-click `mysqlbinlog` command copy buttons

Related notes:

- [v0.22.0 release notes](docs/releases/release-notes-v0.22.0.md)
- [v0.22.0 中文发行说明](docs/releases/release-notes-v0.22.0.zh-CN.md)

## v0.19.0

Release date: 2026-04-19

Highlights:

- Changed default `--detail-store` from `duckdb` to `none`; `binlogviz analyze` no longer creates a DuckDB temp store by default
- `none` mode does not create a DuckDB database, does not call `ResolveTransactionQuerySQL`, and does not write to disk
- `--detail-store duckdb` remains available as an experimental/debug compatibility backend
- Default `none` reduces max RSS by about 38% on the 988 MB real binlog sample (199–203 MB vs 320–323 MB); JSON output is identical to `duckdb` mode across all 10 top-level report fields

Related notes:

- [v0.19.0 release notes](docs/releases/release-notes-v0.19.0.md)
- [v0.19.0 中文发行说明](docs/releases/release-notes-v0.19.0.zh-CN.md)

## v0.18.1

Release date: 2026-04-19

Highlights:

- Added an external real-binlog benchmark gated by `BINLOGVIZ_REAL_BINLOG`
- Reduced finalize allocation hotspots and store transaction scan memory
- Tuned DuckDB transaction batch flushes from 5,000 to 10,000 rows

Related notes:

- [v0.18.1 release notes](docs/releases/release-notes-v0.18.1.md)
- [v0.18.1 中文发行说明](docs/releases/release-notes-v0.18.1.zh-CN.md)

## v0.18.0

Release date: 2026-04-16

Highlights:

- Added directory-based analyze discovery with `--from-dir`, `--prefix`, `--start`, and `--end`
- Added richer analyzer diagnostics, redesigned HTML reports, and compare/trend diagnostic deltas
- Complete English/Chinese localization across HTML surfaces

Related notes:

- [v0.18.0 release notes](docs/releases/release-notes-v0.18.0.md)

## v0.17.0

Release date: 2026-04-13

Highlights:

- Added bounded cross-window pattern drilldowns for both `compare` and `trend`, so high-signal pattern changes now carry short explanatory context across windows
- Added top-level `pattern_drilldowns` arrays to compare and trend JSON outputs; the field is always present and remains empty when nothing qualifies
- Compare drilldowns now explain new patterns, disappeared patterns, and dominant row-movement shifts with bounded key points
- Trend drilldowns now explain rising, falling, and concentrated cross-window share shifts with bounded key points
- HTML compare and trend reports now render labeled drilldown detail cards beneath the pattern sections
- Bounded payloads remain enforced: at most 2 drilldowns per report and at most 2 key points per drilldown

Related notes:

- [v0.17.0 release notes](docs/releases/release-notes-v0.17.0.md)
- [v0.17.0 中文发行说明](docs/releases/release-notes-v0.17.0.zh-CN.md)

## v0.16.0

Release date: 2026-04-12

Highlights:

- Added selective pattern drilldowns: an optional explanatory layer that appears when one or more write patterns cross a high-signal threshold
- New top-level JSON field `pattern_drilldowns` (always present as array, empty when no pattern qualifies)
- Text output renders indented drilldown blocks under qualifying patterns; HTML output renders collapsible drilldown cards with signal flags and metric help
- Bounded payloads: at most 2 drilldowns per analysis, 2 workload peak minutes per drilldown, 2 workload transactions per drilldown
- Mixed signal model: dominance (share thresholds) + anomaly (table-aligned alerts, high rows-per-txn ratio)
- Workload-scoped wording: busiest minutes and representative transactions are described as window-level context, not pattern-owned data

Related notes:

- [v0.16.0 release notes](docs/releases/release-notes-v0.16.0.md)
- [v0.16.0 中文发行说明](docs/releases/release-notes-v0.16.0.zh-CN.md)

## v0.15.0

Release date: 2026-04-11

Highlights:

- Hardened workflow trust boundary: `workflow resume` and `workflow status` now validate that `plan_path` resolves to `<output_dir>/plan.yaml` inside the workflow root before opening any file
- Added symlink escape detection so malicious manifests cannot reference files outside the workflow root
- Tightened plan path acceptance to rooted `plan.yaml` only — nested paths and renamed files are rejected
- All plan-path consumers (`resume`, `status`) now use the canonical resolved path instead of the raw manifest value

Related notes:

- [v0.15.0 release notes](docs/releases/release-notes-v0.15.0.md)
- [v0.15.0 中文发行说明](docs/releases/release-notes-v0.15.0.zh-CN.md)

## v0.14.0

Release date: 2026-04-11

Highlights:

- Added workflow-level summary aggregation so workflow manifests and HTML landing pages now surface cross-report findings and recommendations
- Added `workflow status` support for persisted `workflow_summary` in both text and JSON outputs
- Added `workflow export` for deterministic workflow handoff bundles with optional snapshot inclusion
- Added operator recommendation surfaces for compare and trend outputs
- Hardened explanation evidence refs, workflow summary contracts, and export containment/path normalization behavior

Related notes:

- [v0.14.0 release notes](docs/releases/release-notes-v0.14.0.md)
- [v0.14.0 中文发行说明](docs/releases/release-notes-v0.14.0.zh-CN.md)

## v0.9.1

Release date: 2026-04-04

Highlights:

- Fixed streaming `txn_count` propagation so table and minute summaries report real transaction counts
- Made `analyze --snapshot-name` snapshots immediately usable in `trend` even without explicit `--start` / `--end`
- Added trend fallback to `summary.start_time` / `summary.end_time` for older snapshots missing `snapshot.window`
- Turned `warnings` into a real bounded-degradation signal and surfaced it in text reports
- Upgraded `snapshot list --format text` into a readable inventory table and aligned current-version docs with `v0.9.1`

Related notes:

- [v0.9.1 release notes](docs/releases/release-notes-v0.9.1.md)
- [v0.9.1 中文发行说明](docs/releases/release-notes-v0.9.1.zh-CN.md)

## v0.8.3

Release date: 2026-04-02

Highlights:

- Added `binlogviz snapshot rename` and `binlogviz snapshot delete` for long-lived snapshot store management
- Added `snapshot list --format json` and `snapshot show --format json` for script-friendly snapshot inspection
- Added richer compare text/HTML context with snapshot input mode, source summary, filters, and requested window
- Added integration coverage for old analyze JSON import, default snapshot directory behavior, conflicts, invalid names, and missing snapshot flows
- Kept legacy `binlogviz compare <current.json> <baseline.json>` file mode and compare JSON structure backward-compatible

Related notes:

- [v0.8.3 release notes](docs/releases/release-notes-v0.8.3.md)
- [v0.8.3 中文发行说明](docs/releases/release-notes-v0.8.3.zh-CN.md)

## v0.8.0

Release date: 2026-04-02

Highlights:

- Added snapshot-aware analyze workflow with `--snapshot-name` and optional `--snapshot-dir`
- Added `binlogviz snapshot save`, `binlogviz snapshot list`, and `binlogviz snapshot show`
- Added compare snapshot mode with `--current-snapshot` and `--baseline-snapshot`
- Added top-level analyze JSON snapshot metadata plus compare JSON `current_snapshot` and `baseline_snapshot`
- Kept legacy `binlogviz compare <current.json> <baseline.json>` file mode compatible for existing automation

Related notes:

- [v0.8.0 release notes](docs/releases/release-notes-v0.8.0.md)
- [v0.8.0 中文发行说明](docs/releases/release-notes-v0.8.0.zh-CN.md)

## v0.5.0

Release date: 2026-03-28

Highlights:

- Exposed previously hidden CLI flags: `--top-minutes`, `--spike-window`, `--spike-factor`, `--spike-min-rows`
- Added schema/table filtering at analysis time: `--include-schema`, `--exclude-schema`, `--include-table`, `--exclude-table`

Related notes:

- [v0.5.0 release notes](docs/releases/release-notes-v0.5.0.md)
- [v0.5.0 中文发行说明](docs/releases/release-notes-v0.5.0.zh-CN.md)

## v0.4.0

Release date: 2026-03-28

Highlights:

- Added internationalization (i18n) support for English and Chinese
- Added `--lang` flag to switch output language
- Added automatic language detection from `LANG` and `LC_ALL` environment variables
- Localized error messages, report output, and alert messages

Related notes:

- [v0.4.0 release notes](docs/releases/release-notes-v0.4.0.md)
- [v0.4.0 中文发行说明](docs/releases/release-notes-v0.4.0.zh-CN.md)

## v0.3.0

Release date: 2026-03-27

Highlights:

- Added discovery mode with `--from-dir` and `--prefix` flags to automatically discover and order binlog files from a directory
- Added `binlogviz version` command (prints ASCII logo + version) and `--version` flag (prints version only)
- Restructured documentation into concept/recipe/reference sections with bilingual coverage
- Improved `validateFiles` error messages to distinguish "file not found" from other access errors

Related notes:

- [v0.3.0 release notes](docs/releases/release-notes-v0.3.0.md)
- [v0.3.0 中文发行说明](docs/releases/release-notes-v0.3.0.zh-CN.md)

## v0.2.3

Release date: 2026-03-24

Highlights:

- Added aggregate parse progress for `binlogviz analyze`, based on the summed size of ordered input binlog files
- Kept progress and finalization status on `stderr` so text and JSON reports remain clean on `stdout`
- Added parser progress plumbing and regression coverage for duplicate input paths and output-stream separation

Related notes:

- [v0.2.3 release notes](docs/releases/release-notes-v0.2.3.md)
- [v0.2.3 中文发行说明](docs/releases/release-notes-v0.2.3.zh-CN.md)

## v0.2.2

Release date: 2026-03-19

Highlights:

- Raised the documented and enforced Go toolchain requirement to `1.26.1`
- Added a Chinese repository README
- Added repository-level `CHANGELOG.md` and `SECURITY.md`
- Updated top-level documentation navigation and release entry links

Related notes:

- [v0.2.2 release notes](docs/releases/release-notes-v0.2.2.md)
- [v0.2.2 中文发行说明](docs/releases/release-notes-v0.2.2.zh-CN.md)

## v0.2.1

Release date: 2026-03-19

Highlights:

- Switched the analysis pipeline to true streaming command execution:
  - `ParseFiles -> NormalizeRawEvent -> analyzer.Consume -> analyzer.Finalize`
- Added DuckDB-backed finalize-time result assembly for high-cardinality analysis data
- Added `--sql-context summary|off|full`
- Added bounded `Rows_query_log_event` SQL context support
- Added real binlog fixture coverage, broader streaming benchmarks, and release packaging workflow
- Fixed the release pipeline so GitHub Releases can publish downloadable artifacts

Related notes:

- [v0.2.1 release notes](docs/releases/release-notes-v0.2.1.md)
- [v0.2.1 中文发行说明](docs/releases/release-notes-v0.2.1.zh-CN.md)

## v0.2.0

This tag was superseded and is not a supported public release.
