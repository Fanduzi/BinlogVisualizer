# BinlogViz v0.29.0 Release Notes

Release date: 2026-10-19

## Overview

v0.29.0 makes `--diagnostics summary` actually small on DDL-heavy windows. It also makes `snapshot list` show a trimmed report before anything tries to read it, pins how odd `diagnostics_mode` values are handled, and gives operators and agents worked examples and a usage guide inside the release archive. ADR 0011 records the design (`docs/adr/0011-diagnostics-budget-and-alert-gtid.md`). Default and `--diagnostics full` output is byte-identical to v0.28.0. One change breaks `--diagnostics summary` consumers only: see Changed.

## Changed

- **Breaking for `--diagnostics summary` only: the DDL timeline is capped (#308, #311, ADR 0011).**
  - In a summary document, `diagnostics.ddl_events` is now the first `--top` events in timeline order (default 10).
  - The new `diagnostics.ddl_events_omitted` counts the rest. It is always present in summary, including when it is 0.
  - v0.28.0 summary listed every DDL event. A consumer that treated summary `ddl_events` as the complete timeline must read `ddl_events_omitted`, or use `full`.
  - `--top` also sets the other ranked lists, and `--top 0` means the default 10, not unlimited (#312).
  - Default and `--diagnostics full` JSON are byte-identical to v0.28.0. They still list every DDL event and never emit `ddl_events_omitted`. `report_version` stays 3, because the default document did not change and the new key exists only on the opt-in summary document (ADR 0010).
  - Size of stdout on a DDL-heavy 1.07 GB binlog:

    | | v0.28.0 | v0.29.0 |
    |---|---:|---:|
    | full | 63042 B | 63042 B (identical) |
    | summary | 42759 B | 32632 B (10 of 21 DDL events, 11 omitted) |
    | summary `--top 1` | — | 10511 B |
    | summary `--top 20` | — | 57233 B |

## Added

- **`snapshot list` marks a reduced file (#308, #311).** A `--diagnostics summary` or `off` document copied into the snapshot dir is shown with its mode, so it is visible before `snapshot show`, compare, or trend refuse it with exit 9.
  - Text output has a new `DIAGNOSTICS` column on every listing. It shows `-` for a normal snapshot.
  - `--format json` adds `diagnostics_mode` to that entry only. `schema_version` stays 1.
  - List still exits 0. A snapshot saved with `--snapshot-name` is always full and never marked.
- **`--help` examples on six commands (#255, #311).** `analyze`, `flashback`, `snapshot`, `compare`, `trend`, and `workflow`. The analyze and flashback examples walk the incident recipe: find the spike in a UTC window, read the GTID from `alerts[].gtid`, then `flashback --plan`, then `--conflict guard`.
- **Operator and agent usage guide (#255, #311).** `docs/agents/USING-BINLOGVIZ.md` and `docs/agents/USING-BINLOGVIZ.zh-CN.md` cover three recipes, exit codes, JSON versioning, and the diagnostics modes. Release archives now include both guides, plus `README.md` and `README_ZH.md`, next to the binary, the sample binlogs, and `incident.yaml`.

## Fixed

- **`diagnostics_mode` edge values are pinned and documented (#308, #311).**
  - JSON `null` is the same as a missing key: the report reads normally.
  - The string `"full"` is accepted.
  - Any other string, including `""`, is a reduced report: exit 9 (`unsupported`, `details.reason: reduced_diagnostics`).
  - A non-string value such as `1` is a corrupt report: exit 5.
- **`--diagnostics ""` is a usage error (#308, #311).** An explicitly set empty value exits 3 with `invalid --diagnostics "" (allowed: full, summary, off)`. An omitted flag is still `full`. `FULL`, `Summary`, `" full"` and other unknown values also exit 3, as before.
- **`flashback --help` describes `--sql-context` for flashback (#255, #311).** `summary` (the default) and `full` do not change the undo SQL, and `off` is refused, because the script is the row values. The help no longer reuses the analyze wording about query text, DDL statements, and `--show-patterns`.

## Verification

BinlogQA's PR QA on #311 and release-gate QA on main `4cb6cc24` passed with no P0, P1, or new P2. The baselines were the v0.28.0 tag build and the released v0.28.0 linux_amd64 archive.

- **Unchanged output**: on a 50-file corpus × en/zh-CN, all 3308 comparisons are identical, with 0 bad results. The corpus covers analyze json/text/markdown/html (timestamp masked), `--large-trx-rows 1`, `--detail-store duckdb`, and flashback `--plan`, guard, `--conflict-limit 0`, SQL and conflict. Default equals `--diagnostics full` in all 400 runs, and no full document has `ddl_events_omitted`.
- **Summary cap**: 92/92 summary documents have kept = min(10, total), kept + omitted = total, and a kept list that is a timeline prefix of full. All 25 `--top` probes pass.
- **Refusals**: 528 show/compare/trend/save runs. All 400 non-zero runs (360 exit 9, 40 exit 5) have empty stdout and leave files untouched. Workflow run/status/resume/`--rerun` with a swapped trimmed artifact: 32 runs exit 9, 8 exit 5, trees unchanged. Reports from v0.26.1, v0.27.0 and v0.28.0: 72/72 identical.
- **Flags and help**: 28 bad `--diagnostics` values exit 3 with empty stdout. All 31 `--help` example runs behave as written. The 67-case exit-code matrix is line-identical, apart from the longer `snapshot` and `workflow` help.
- **Archive**: `scripts/pack_release_archive.sh` packs 8 members, and the binary and all docs are identical to the repo.
- CI, `go vet ./...` and `go test ./...` pass.

## Compatibility

- Breaking for `--diagnostics summary` consumers only (see Changed). Default and `full` analyze JSON, text, Markdown, HTML, and flashback output are byte-identical to v0.28.0.
- `report_version` stays 3, `plan_version` stays 2, and `snapshot list` `schema_version` stays 1. The added keys are `diagnostics.ddl_events_omitted` (summary only) and `diagnostics_mode` on a reduced `snapshot list` entry.
- A script that parses `snapshot list` text columns will see the new last column `DIAGNOSTICS`.
- `--diagnostics ""` was accepted as `full` in v0.28.0 and now exits 3.
- `flashback` never connects to MySQL. Review the script, test it, and apply it in one new session on the primary with `sql_log_bin=1`, with the script header.

## Known issues

- **#303 (P2)**: the Linux release binaries report an empty `commit` and `commit_time` in `binlogviz version`, because the release workflow does not pass the commit ldflags to the Linux builds yet. macOS archives and builds from a git checkout report the commit. Until #303 is fixed, check a Linux binary against `binlogviz_0.29.0_checksums.txt` rather than by commit.
- **#312 (P3)**: four follow-ups.
  - Most relative links in the `README.md` and `README_ZH.md` shipped in the archive point to files that are not in the archive. Use the GitHub copies.
  - The summary DDL cap shares `--top`, `--top 0` means 10, and the `--diagnostics` help does not mention the cap.
  - `snapshot list` shows a `""` marker as `-`, which ADR 0011 describes differently.
  - zh-CN `flashback --help` still has some English flag text.
- The maintainer note in `.github/workflows/README.md` still lists the old archive file set. The fix is documentation only and waits on a change made with workflow scope. Release builds are not affected.
- The HTML report's "Generated" timestamp makes two HTML runs differ when they straddle a second (pre-existing).
- `install.sh` P3s from #285, and #255 (items 3, 5, 7), #256, #257 carry over.

See `docs/concept/limitations.md` for the flashback limitations carried over from earlier releases.
