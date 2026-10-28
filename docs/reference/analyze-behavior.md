# Analyze Behavior Details

This page holds the detailed `analyze` behavior that used to live in the project README. The README keeps the commands; this page keeps the exact rules. For flags see the [CLI Reference](https://github.com/Fanduzi/BinlogVisualizer/blob/main/docs/reference/cli.md); for report fields see the [Output Format Reference](https://github.com/Fanduzi/BinlogVisualizer/blob/main/docs/reference/output-format.md).

## Sample binlog and release archive contents

The same 1500-byte fixture lives at `cmd/binlogviz/testdata/minimal.binlog` in the repo. Each GitHub Release tar.gz also includes that file as `testdata/minimal.binlog`, a discovery-layout copy at `testdata/sample-binlog/mysql-bin.000001`, an `incident.yaml` whose `from_dir` points at that directory, `README.md`, `README_ZH.md`, `docs/agents/USING-BINLOGVIZ.md` (plus the zh-CN guide), and the docs pages the README links to: `docs/reference/analyze-behavior`, `docs/recipe/flashback`, `docs/recipe/compare-trend-workflow`, and `docs/concept/architecture`, each with its `.zh-CN.md` page. After extract, `./binlogviz analyze testdata/minimal.binlog` and `./binlogviz workflow run incident.yaml` do not need a clone. Agents start at `docs/agents/USING-BINLOGVIZ.md`.

## Reading a binlog from stdin

`analyze -` reads one binary binlog from a pipe. The parser needs a seekable file, so stdin is copied to a temporary file and removed when the command finishes, including SIGHUP (exit 129), Ctrl-C (exit 130), SIGQUIT (exit 131), and SIGTERM (exit 143). A terminal fails before parsing. `/dev/null` and an empty pipe say `stdin has no data`. Replay hints for that input say it came from stdin and do not point at a file. `mysqlbinlog` text output is not a binlog.

## Top Threads

The default text report includes Top Threads, ranked by rows (or by events, bytes, or transactions when there are no row images). It shows `thread_id`, and `server_id`, `user@host`, and schema when the binlog stored them, so "who wrote the most" does not need `jq`. `--top` limits that section; `--top-threads 0` keeps every session. JSON exposes the same ranking as `threads`.

## Hot Rows

The same report includes Hot Rows: the primary keys touched by the most UPDATE and DELETE row images. Each line has the touch count, how many transactions touched that key, the first and last event time, and the GTID plus file:byte of the first and last transaction, so you can open that group in `mysqlbinlog` or BinlogServer. The key comes only from MySQL 8 `binlog_row_metadata=FULL`. If the binlog does not name the key, the report says hot-row tracking is unavailable for that table and does not guess from column `@1`. Tables with no primary key are not ranked here. `--top` limits the section; `--top-rows` overrides it (`0` keeps every tracked key). `--sql-context off` hides the key values and keeps the counts. JSON uses `hot_rows`. Tracking keeps at most 8192 primary keys. A new key after that replaces the least-touched key; the report says the cap was hit, and a row that inherited a dropped key's count is marked approximate. A key that stayed in the table keeps an exact count.

## Exit codes

`analyze` exits **0** when at least one event was counted, exit 5 (kind corrupt) when the file could not be analyzed (corrupt, truncated, or no Format Description), and **2** when a complete binlog parsed but counted zero events (empty `--start`/`--end` window, or Format Description / rotate only). A schema or table filter that matches nothing is also exit 2, and its `Error:` line says the filter matched no events. `--dml` that matches nothing is the same exit, with `Error: dml filter matched no events`. A filter that matches a view, event, function, procedure, or trigger exits 0 and prints that DDL even when no rows changed. Exit 2 writes nothing to `stdout` and one `Error:` line to `stderr`. If a progress bar is still on the current stderr line, that line is cleared before `Error:`.

## Time windows without a zone

`--start`/`--end` also accept `YYYY-MM-DD HH:MM:SS`. A --start or --end value with no zone is read in the --tz zone when --tz is set, including --tz UTC, and in the local timezone of the machine running binlogviz when --tz is not set. Prefer RFC3339 with an explicit offset across machines.

## Table filter names

`--include-table` / `--exclude-table` accept `TABLE` or `SCHEMA.TABLE`, and the same form for a view, event, function, procedure, or trigger. `CREATE TRIGGER` and `DROP TRIGGER` are both named by the trigger, so pass that name.

## Why is my replica lagging?

```bash
binlogviz analyze mysql-bin.000123
```

A `No Primary Key` section lists tables that received UPDATE or DELETE rows and have no primary key, ranked by those counts. On a replica, each of those rows can scan the table. `primary key presence unknown (binlog_row_metadata is not FULL)` means the binlog did not record key presence; it is not a claim that a table lacks a key. INSERT-only tables without a primary key are named and are not ranked as this risk.

On a replica binlog from MySQL 8.0 or newer with `log_replica_updates`, `Replica Apply Delay` follows Busiest Minutes. Delay is immediate commit time minus original commit time, and it assumes the source and replica clocks agree. The section gives the max and p95 delay, the minute where delay peaked, and the slowest transactions: GTID, transaction start file:byte, both commit times, delay, and the tables in that transaction. `--top` limits that list. A source binlog, where the two timestamps are equal, is one line and no table. MySQL 5.7 and MariaDB print `commit timestamps unavailable`.

## Analyze performance gate

For incident triage, the target for a 1 GB single-binlog `analyze` run is 10 seconds on the target DBA environment. Runs above 15 seconds should be treated as performance failures and profiled with `pprof`. Hot-row tracking does not keep a map of every row in that file: it keeps at most 8192 primary keys. Past that, a new key replaces the least-touched key and the report marks the inherited count approximate.

Default `--detail-store none` produces JSON equivalent to `--detail-store duckdb` while reducing peak RSS by roughly 38% (measured on a 988 MB MySQL 8.0 ROW binlog). Wall time remains parser/streaming-bound.

Recommended manual check:

```bash
time binlogviz analyze /path/to/mysql-bin.000044 --format text > /tmp/binlogviz-text.txt
time binlogviz analyze /path/to/mysql-bin.000044 --format html --output /tmp/binlogviz.html
```

Text output is intended to stay on a fast diagnostic path. HTML output builds the full visual evidence report.

## Release artifacts

The authoritative release artifacts are produced by the GitHub Actions release workflow. macOS artifacts are built on native runners, while Linux artifacts are built inside a manylinux2014 userspace so the glibc baseline stays compatible with CentOS 7 / glibc 2.17. Local `goreleaser` is intended for config validation and optional current-host checks, not as the primary release path.

## Release validation

Pull requests and release builds now validate packaged artifacts, not only source-tree tests.

The maintainer-facing smoke path verifies that one built archive can:

- extract successfully
- include the binary, `testdata/minimal.binlog`, `testdata/sample-binlog/`, `incident.yaml`, `README.md`, `README_ZH.md`, and `docs/agents/USING-BINLOGVIZ.md`
- run `--version`
- execute `analyze` on the bundled sample
- save snapshots
- run `compare`
- run `trend`
- run `workflow run incident.yaml` from the extract directory
