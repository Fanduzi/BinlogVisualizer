# BinlogViz v0.35.0 Release Notes

Release date: 2026-10-21

## Overview

v0.35.0 is a minor release. **It is breaking for `analyze --format json` consumers: `report_version` is 4 (#379).** The matched GTID evidence is now an interval set instead of one entry per matched transaction, so `analyze` and `flashback` with `--include-gtids` or `--exclude-gtids` no longer run out of memory on millions of matches. The release also adds `binlogviz schema` and `--tz` (ADR 0017, #255), bounds `analyze` memory by input shape instead of transaction count (#364, #376), lists the same key transactions in every format (#352), fixes a split busiest minute when timestamps go backwards (#377), stops a flashback script on MariaDB with a clear error, and builds with Go 1.26.10 and `mapstructure` v2.4.0 for security fixes. `plan_version` stays 2 and `flashback verify --format json` `schema_version` stays 1.

## Breaking

- **`report_version` is 4 (#379).** `selection.matched_gtids`, one entry per matched transaction, is replaced by:
  - `selection.matched_gtid_set`, an interval set: MySQL `uuid:1-4:6-3000000` per server UUID; MariaDB `domain-server-seq` or `domain-server-first..last` per run of sequence numbers, per server, or shared by the servers that write one domain.
  - `selection.matched_transactions`, the number of matched transaction groups with at least one event in the time and position window, counted once per group read. A GTID read twice counts twice, and a matched DDL group or a group cut by `--stop-position` before its first row counts too, so it can exceed `summary.total_transactions`.

  Reports without GTID selectors change only in `report_version`. `compare` and `trend` read v3 and v4 reports, and v3 and v4 snapshots mix in `compare`, `trend`, `snapshot list`, and `snapshot show`. A consumer that requires `report_version` 3 must accept 4.
- **New exit 10 under GTID selectors (#379).** `analyze` and `flashback` with `--include-gtids`/`--exclude-gtids` exit 10 `resource`, `details.reason` `matched_gtid_intervals`, when the matched GTIDs need more than 100,000 separate intervals, instead of growing without bound. The message is localized and says how many intervals are needed. For MariaDB the count is the smaller of the per-server and shared forms, taken after the transaction that stopped the run is stored (ADR 0018). Writers with gaps between their sequence numbers therefore report the real need, not the shared cost.

## Added

- **`binlogviz schema` and `--tz` (ADR 0017, #255).** `binlogviz schema` lists and prints the JSON Schema (Draft 2020-12) of each of the 13 versioned JSON outputs; the same documents are `docs/schema/*.json` and ship in the release archive. An unknown name exits 3. `--tz` selects an IANA display zone for rendered timestamps. Without `--tz`, or with `--tz UTC`, output keeps the ` UTC` / `Z` spelling and is byte-identical to before. `Local`, an empty value, and an unknown zone exit 3.
  - A `--start` or `--end` value with no zone is read in the `--tz` zone when `--tz` is set, including `--tz UTC`, and in the local zone of the machine running binlogviz when it is not. An offset or a trailing ` UTC` is that instant.
  - A DST gap or overlap, and an offset past 14:00 or with minutes other than 00, 30, or 45, is exit 3. v0.34.1 silently accepted a naive time in a DST gap of the machine zone.
  - Snapshot files stay UTC, including `snapshot save`. `snapshot list`/`show` and compare HTML Minute cells follow `--tz`; compare and trend text do not.
- **Version-matrix fixtures (#366).** MySQL 5.7.44, 8.4.11, 9.7.2 and MariaDB 10.11.19, 11.8.9: `analyze` is compared with each server's own `mysqlbinlog -vv` decode, and the flashback boundary (FULL undone; MINIMAL, DDL, missing row metadata, and 11.8 generated columns refused) is tested in CI.
- `flashback` warns `mariadb_apply_unsupported` for a MariaDB binlog.

## Changed

- **One list of key transactions in every analyze format (#352, ADR 0012).** Text, Markdown, JSON, and HTML list the `--top-transactions` ranking. Text no longer substitutes a separate largest/longest/widest set, and HTML evidence no longer names a transaction outside the list. A listed transaction that is also a capped champion keeps its label (`Largest transaction`, `Longest transaction`, `Widest transaction`). The JSON `transactions`, `transactions_listed`, and `transactions_omitted` fields are unchanged. Text output reorders, adds, or drops key-transaction lines for many binlogs.
- **Bounded `analyze` memory (#364, #376).** Per-table and per-minute transaction counts keep no key set, the hot-row map is rebuilt so delete/insert churn cannot grow it, and the apply-delay P95 keeps at most 8192 values in memory and spills the rest to an unlinked temp file (about 8 bytes of temp disk per transaction; memory if no temp file can be created). If the spilled values cannot be read back, `analyze` fails with `read spilled apply-delay values`. With `--include-gtids` or `--exclude-gtids` the decision is made at the GTID event that opens the transaction and events stream through, so one transaction of 300k row events no longer crashes with exit 2. Output is byte-identical. `TestAnalyzeRetainedHeap`, `TestAnalyzeGTIDFilterRetainedHeap`, `TestAnalyzeMatchedGTIDSetHeap`, and `TestLargeInputBaseline` (a generated 100MB binlog gated on heap per event and CPU ratio) run in CI.
- **MariaDB.** A flashback script stops on a MariaDB server at its first statements with ERROR 1231 `binlogviz: flashback apply does not support MariaDB; no rows were changed; ...`, also under `mysql --force` (ADR 0016). It used to stop at an unrelated `ERROR 1064`, or with `--force` roll back with `ERROR 1758`. The missing-row-metadata refusal names MariaDB 10.5+ `binlog_row_metadata=FULL`. `--include-gtids` and `--exclude-gtids` accept MariaDB `d-s-first..last` ranges, so a `matched_gtid_set` can be passed back. `compare` merges MariaDB GTID selectors per domain and server before the scope check, so `0-7-1,0-7-2` and `0-7-1..2` no longer report `incompatible_scope`. Whitespace next to `..` is rejected on both sides.
- `docs/production-readiness.md`: E1, E2, and D5 are MET; 20 MET / 1 PARTIAL / 3 NOT MET, 4 of 24 remain.

## Fixed

- **The same minute is no longer listed twice when timestamps go backwards (#377).** A transaction still open across a minute boundary carries its start time after later commits, and v0.34.1 opened a second bucket for a minute that already had one, splitting its rows and transactions. The busiest minute was undercounted and spike detection could miss it, or warn on a split bucket. Out-of-order events now merge into the existing minute. Row and transaction totals are unchanged, but the per-minute timeline, busiest minute, and spike warnings in every format change for such binlogs: on BinlogQA's 663-file corpus, 11 files lose 36 duplicate minutes, and MariaDB `mdev6020` prints 1 spike warning instead of 10.

## Security

- Built with Go 1.26.10. govulncheck reported 24 Go 1.26.1 standard-library vulnerabilities reachable from binlogviz, including GO-2026-6809 (`crypto/x509`), GO-2026-6599/6600 (`html/template`), GO-2026-6603/6611/6612/6613/6617 (`net/http`), and GO-2026-6604 (`os`). Building from source requires Go 1.26.10+.
- `github.com/go-viper/mapstructure/v2` v2.2.1 -> v2.4.0 (indirect, through `go-duckdb` v1.8.5) for GO-2025-3787 and GHSA-2464-8j7c-4cjm: decode error messages no longer echo the raw input value.

## Verification

BinlogQA's PR QA passed every change in the range, and tip QA on main `478e6993` passed with 0 P0, 0 P1, and 0 P2 (one new P3, #404).

- **Release-style build**: go1.26.10, `mapstructure` v2.4.0, `-trimpath`, CGO. `pack_release_archive.sh` and `release_smoke.sh` pass; the archive gains `docs/schema/*.json`, and `binlogviz schema` is byte-identical to `docs/schema`.
- **Schemas**: 394 analyze documents plus every snapshot, compare, trend, flashback plan and verify, version, and workflow JSON validate with ajv and python-jsonschema, 0 failures.
- **`--tz`**: on live MySQL 8.0.36 and 5.7.44 binlogs, all 4 formats under 5 `--tz` settings exit 0; `--tz` absent and `--tz UTC` are identical; six windows in different zones select the same 8432 rows.
- **Flashback on 8.0**: 4 transactions, 1431 rows; `flashback verify | mysql` prints `binlogviz: undo committed, 1431 statements` and `CHECKSUM TABLE` matches the pre-incident values. The plan is identical to v0.34.1.
- **MariaDB ceiling**: 22 cases, 110 runs on the release binary; every ceiling run exits 10 with empty stdout, and three gapped writers report "at least 100001" in en and zh-CN.
- **Against v0.34.1** (663 corpus files, 4437 runs): 0 exit-code differences, and every output difference is one of the intended changes above (#370 key transactions, #377 minutes, the MariaDB stop and warning, `report_version`).
- `go test ./...` passes; `-race` reports 0 races. CI green.

## Compatibility

- `report_version` is 4; `plan_version` stays 2 and `flashback verify --format json` `schema_version` stays 1. Exit 10 `matched_gtid_intervals` is new. Exit codes otherwise match v0.34.1, except that a naive `--start`/`--end` in a DST gap or overlap is exit 3.
- v3 snapshots from v0.34.1 and earlier still load in `compare` and `trend`.
- Flashback scripts from v0.33.0 through v0.34.1 still verify and apply on MySQL. They do not carry the MariaDB stop and still fail safely on MariaDB the old way (`ERROR 1064`, or `ERROR 1758` with `--force`, no rows changed); regenerate them for the clear ERROR 1231 stop. Scripts from v0.32.0 or earlier still need to be regenerated.
- Artifact names and supported platforms are unchanged.

## Known issues

- **#399 (P3)**: `snapshot save` rewrites any RFC3339-looking string to UTC, including `--workload-id`, so `compare` then reports `workload_identity_mismatch`.
- **#400 (P3)**: the DST and offset errors for `--start`/`--end` are not translated in zh-CN, and a non-DST shift is called DST.
- **#403 (P3)**: at the MariaDB GTID ceiling, the per-server message says the dropped shared form "can need fewer" intervals even when it needs as many or more. The number is right.
- **#404 (P3)**: when the plan fails to load, `workflow describe --format json` prints an error document that its published schema rejects.
- **#303 (P2)**: the Linux release binaries report an empty `commit` and `commit_time` in `binlogviz version`. The fix is in `.github/workflows/release.yml` and needs a maintainer with workflow access. Check a Linux binary against `binlogviz_0.35.0_checksums.txt`.
- `compare` and `trend` still have a default arm in their format switch (#357). `flashback verify` does not accept `-` for stdin (exit 4).
- **#255**, **#256**, and **#257** (P3) carry over.

See `docs/adr/0017-json-schema-and-display-timezone.md`, `docs/adr/0018-matched-gtid-ceiling-count.md`, `docs/gtid-filter-streaming.md`, and `docs/concept/limitations.md`.
