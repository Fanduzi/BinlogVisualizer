# BinlogViz v0.35.1 Release Notes

Release date: 2026-10-26

## Overview

v0.35.1 is a patch release. DDL names are now read by one identifier reader that follows the server's own rules (#412), so a server-generated multi-table `DROP TABLE` no longer reports a made-up table (#410), and quoted, ANSI-quoted, digit-led, and version-commented names are read as the server read them. `workflow describe --format json` prints a failure document that its published schema accepts (#407). The release pipeline now runs vet, `-race`, the failed-apply and guard SQL tests, and the full flashback e2e on the tagged commit before it builds, and every archive reports the commit it was built from (#419). With that, `docs/production-readiness.md` is 24 of 24 MET. `report_version` stays 4, `plan_version` stays 2, and `flashback verify --format json` `schema_version` stays 1.

## Fixed

- **Multi-table `DROP TABLE` (#410, closes #375).** A server-generated `DROP TABLE` with several names was split on dots, so `analyze` and unfiltered `flashback` reported a table such as ``z`,`db.t1`` that does not exist. The names are now read as an identifier list. `analyze` reports one DDL target per table in JSON, text, Markdown, and HTML. Flashback still exits 6 and writes no SQL; its message and `details.table` list every table.
- **One identifier reader for every DDL name (#412, closes #411).** `mysqlexpr.Reader` is the only DDL name reader; `CREATE`, `ALTER`, `RENAME`, `TRUNCATE`, and `DROP`, with one name or several, all use it.
  - A doubled backtick is undoubled the same way in every statement, so `` `we``ird` `` is one table, and a quoted name is never a keyword (`` DROP TABLE `if`,`t2` `` is readable).
  - `ANSI_QUOTES` is taken from the query event's `sql_mode`, so `"db"."t"` is read as a name when the server read it as one.
  - Version comments follow the server: a `/*!NNNNN */` body counts only when the server version is known and not older than the comment. MySQL reads five digits, and from 8.1 six when whitespace follows the sixth; MariaDB `/*!` and `/*M!` read five, or six when the sixth character is a digit. Extra digits stay in the body, so `/*!800369_t ... */` no longer hides a rename the server ran. `/*M! */` is an ordinary comment on a known non-MariaDB server, and unreadable when the flavor is unknown.
  - An unquoted name may start with a digit (`2fa_codes`); a pure number or an exponent (`1e5`) is not a name.
  - `CREATE`, `ALTER`, and `DROP USER` keep the account spelling, quotes included.
  - An unqualified name takes the query's default schema, or the event schema when the query schema was not kept.
  - The flashback refusal message shows the first five names and then "and N more", and escapes control characters (a raw TAB is `\t`; U+202E and U+0085 are escaped too). `details.table` still lists every name.
  - The DDL count in text, Markdown, HTML, JSON `ddl_event_series`, `compare`, and `trend` is statements. When a statement has several targets the line reads `4 targets in 2 statements`, in the report language. The timeline still has one row per target.
  - Filtered flashback no longer refuses a versioned `TEMPORARY` drop or an `ANSI_QUOTES` drop that names only other tables, because those statements are now readable. Without a known version or the `ANSI_QUOTES` bit they stay unreadable and still refuse.
- **`workflow describe` failure JSON matches its schema (#407, closes #404).** When the plan fails to load, `workflow describe --format json` and `workflow validate --format json` print `{schema_version, valid: false, error}`. The describe schema is now a `oneOf` of the success object and that failure object, so the document validates. Stdout bytes are unchanged. `TestJSONFailureOutputMatchesSchema` runs one failure of every JSON command and requires the document to validate, or stdout to stay empty.

## Release pipeline and CI (#417, landed as #419)

- **B5**: flashback e2e runs `TestFlashbackFailedApplyDropsAndSignals` and the guard SQL tests (`TestGuardMessageOnServer`, `TestGuardMatchOnServer`, `TestGuardMessageCutSweepOnServer`) against MySQL 8.0, and fails on a SKIP or a missing PASS.
- **F4**: a `vet and race` job runs `go vet ./...` and `go test -race -count=1 ./...` on every PR and every push to main.
- **F2 (#303)**: all four release builds stamp the tagged commit and its time with ldflags, and the release smoke fails when `binlogviz version --format json` does not report that commit. Linux archives no longer report an empty `commit`. CI `verify` builds the same way on every PR.
- **F3**: `release.yml` runs the full CI workflow, including flashback e2e on MySQL 8.0, on the tagged commit, and no build starts until it passes. `TestReleaseWaitsForFlashbackE2E` pins that dependency.
- `docs/production-readiness.md`: B5, F2, F3, and F4 are MET; 24 MET / 0 PARTIAL / 0 NOT MET.

## Verification

BinlogQA's PR QA passed #407, #410, #412, and #417, and tip QA on main `bc807826` passed with 0 P0, 0 P1, and 0 P2 (one new P3, #421, also present in v0.35.0). This release commit adds only these notes and the CHANGELOG on top of `bc807826`.

- **Build**: go1.26.10, `go mod verify` OK, `go vet` clean, `go test -count=1 ./...` passes; a release-flag build reports `bc8078264ac2` with `modified: false`. `pack_release_archive.sh` and `release_smoke.sh` pass, and 19 failure cases give the same exit code and stderr as v0.35.0.
- **Corpus** (680 files, 3914 jobs): exit codes are identical to v0.35.0 on every job. 313 jobs on 81 files differ in output, all from the changes above: multi-table `DROP` split (#410), quoted and ANSI-quoted names (#412), numeric-qualified names that now fail closed (#418), and refusal messages that now name the right table.
- **Schemas**: the 13 published schemas match `docs/schema`; 868 documents (433 analyze v4 reports, 422 flashback plans) validate with ajv (Draft 2020-12, strict) and python-jsonschema 4.26.0.
- **Live guard attack** on MySQL 8.0.36, 5.7.44, 8.4.11 and MariaDB 11.8.9, 10.11.19: 0 fail-opens. On 8.0 and 8.4, 16 filtered flashback runs each now exit 0 where v0.35.0 refused, all the intended #412 precision (the DDL names only other tables). A multi-table `DROP` gives 4 correct events instead of 1 mangled one. Digit-led names on 5 servers: 0 fail-opens.
- **Flashback on 8.0.36**: 202 rows round-trip with a matching checksum; `undo.sql` is byte-identical to v0.35.0.
- **CI**: PR and push runs at `bc807826` are green, including vet, `-race` (0 failures, 0 races), and flashback e2e (73 pass, 0 fail, 0 skip, including the failed-apply and guard SQL tests). One mutation per new gate (B5, F4, F2, F3) turns CI red.

## Compatibility

- `report_version` stays 4, `plan_version` stays 2, and `flashback verify --format json` `schema_version` stays 1. No JSON field is added or removed.
- `analyze` output changes for binlogs with multi-table `DROP TABLE`, quoted or ANSI-quoted names, or version-commented DDL: more DDL targets, the right table names, and statement counts where a statement has several targets.
- Filtered flashback refuses less: a readable DDL that names only tables outside the filter no longer refuses. Anything the reader cannot read completely still refuses (exit 6).
- A schema-qualified name that looks like a number (`db.1e5`, `db.0x1f`, `db.123`) reads as `table: null`, so filtered flashback on an unrelated table exits 6 instead of 7. This fails closed (#418).
- Artifact names and supported platforms are unchanged. Linux archives now report their commit in `binlogviz version`.

## Known issues

- **#418 (P3)**: DDL reader follow-ups from #412 QA: numeric-looking qualified names read as `table: null` (fail closed); `compare` added/removed lists name only the first target of each statement; a multi-pair `RENAME` is attributed to its first pair in analyze (flashback still refuses); some mutants have no test.
- **#420 (P3)**: CI checklist follow-ups from #417 QA: the F3 lock test checks only the workflow text, nothing pins the B5/F4 wiring, and the release smoke does not check `modified`.
- **#421 (P3, also in v0.35.0)**: `DROP USER a, b` records only the first account, `RENAME USER` records no account, and accounts appear as session-schema tables in Top Tables.
- **#399**, **#400**, **#403** (P3) carry over. `compare` and `trend` still have a default arm in their format switch (#357). `flashback verify` does not accept `-` for stdin (exit 4). **#255**, **#256**, and **#257** (P3) carry over.

See `docs/adr/0015-flashback-ddl-under-a-table-filter.md`, `docs/production-readiness.md`, and `docs/concept/limitations.md`.
