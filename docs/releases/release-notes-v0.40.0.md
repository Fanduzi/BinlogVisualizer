# BinlogViz v0.40.0 Release Notes

Release date: 2026-11-02

## Install or upgrade

```bash
brew upgrade --cask binlogviz          # macOS
# Linux: rerun install.sh, which resolves the latest release
```

No flag, JSON field, or schema version changes in this release. Every key in the
JSON report is the same as v0.39.0.

## Highlights

- **The Chinese text report is now fully Chinese.** The table headers are translated (表, 影响行数, DDL 事件, 事务数, binlog 事件, 行占比, and 行数/事件数/字节/事务/占比 in the thread table) and padding is measured in terminal cells, so a Chinese schema or table name keeps its column aligned. `INSERT`/`UPDATE`/`DELETE` and binlog identifiers such as `thread_id` stay verbatim: they are values, not prose.
- **Every key transaction prints once.** A transaction that wins several rankings now shows one block — `Largest/Longest/Widest transaction: txn-2 rows=1 tables=1 dur=0s file=… table=… bytes=…` — with a single `mysqlbinlog` command and one set of row images, instead of one full block per ranking.
- **Release archives carry `LICENSE`.** Apache-2.0 §4 asks for it and it was missing from every earlier archive.
- **Measured throughput, not a slogan.** `docs/reference/analyze-behavior.md` now states the cost this build measured on two real MySQL 8.0.45 binlogs: 141,910,241 bytes / 3 transactions at 0.45 s and 40 MB RSS, and 99,478,756 bytes / 40,002 transactions at 1.27 s and 40 MB RSS.
- **README says where the tool sits.** Three rows against `mysqlbinlog`, `pt-query-digest`, and `binlog2sql`/`my2sql`, what a first run prints with a link to the pinned report, and the two install paths without the 15-line hand-unpacked recipe.

## Fixes

- Three stale exit codes in the Chinese docs said `退出 1` where the binary exits 6: a flashback refusal on an unverifiable generated column, and the refusal list in the CLI reference.
- The v0.38.0 and v0.36.0 release dates in `CHANGELOG.md` and their note files did not match their tags. The tags are right; the dates now say so.

## Compatibility

- `report_version` stays 4, `plan_version` stays 2, and every `docs/schema/*.json` file is unchanged.
- The sample report in `docs/examples/analyze-output.json` is byte-identical to v0.39.0.
- Text output changes: the column headers follow `--lang`, and a transaction that wins several rankings appears once with a joined label. A script that greps `Largest transaction:` should relax to `Largest`.

## Known issues / out of scope

- `compare` and `trend` still report `unknown` comparability for a snapshot saved without `--workload-id`, and nothing warns when the snapshot is saved (#478). The docs say the flag is required; a save-time warning is a product decision, not shipped here.
- The landing page still carries no screenshot: the page is held to one self-contained file by its guard (ADR 0029).

## Tests and CI

- New `TestRenderTextLocalizedTopTablesKeepsColumnsAligned` compares the affected-rows column edge in terminal cells on a CJK table; three tests that asserted the old label spelling now assert the transaction key, `dur=`, and the alert they were really about.
- New `TestLandingPreviewsAreRealOutput` holds the landing page's output panes to the pinned example reports, and to both dictionaries.
- `docs/examples/analyze-output.md` joins the pinned examples, so text, JSON, and Markdown are all byte-compared with a fresh run.
- From this release on, `check-release-prep.sh` requires both notes to carry an install or upgrade section before the engineering detail.

## Verification

All required checks passed on the tagged commit's parent: `verify`, `vet and race`, `codehealth`, `golangci-lint`, `flashback e2e`, `flashback difftest`, `MySQL 5.7 compatibility`, `release-prep self-test`, `landing overflow`, and the Cloudflare Pages deployment.
