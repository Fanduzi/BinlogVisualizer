# BinlogViz v0.41.0 Release Notes

Release date: 2026-11-03

## Install or upgrade

```bash
brew upgrade --cask binlogviz          # macOS
# Linux: rerun install.sh, which resolves the latest release
```

Additive only: two new optional `analyze` flags, one new JSON key. No flag or
key was removed or renamed, and `report_version` stays 4.

## Highlights

- **Undo now works on the configuration MySQL ships with.** With
  `binlog_row_metadata=MINIMAL` — the default — and on MySQL 5.7, which has no
  row metadata at all, `flashback --schema-file` supplies the column names, the
  type facts the literals need, and the primary key from the definition. Before
  this release the same input was refused at capture (exit 6, `row_metadata`)
  even though `redo` already accepted it.
- **The key's source is labelled.** When the undo addresses a row by the key a
  schema file declares, the script header, one stderr warning, and
  `--plan` (`warnings[].code = schema_file_key`) all say so. It is only as good
  as that dump.
- **A key a later `ALTER` changed is not trusted.** If the parsed files contain
  an `ALTER` that adds or drops the primary key, the definition's key is dropped
  with a `schema_file_key_unmodelled` warning and the undo matches every column
  with `LIMIT 1` instead of addressing the row by a key the dump no longer owns.
- **`analyze --show-rows --schema-file` names the columns.** Instead of
  `@1`..`@N`, the report prints the definition's column names, the summary says
  where they came from, and the JSON image carries `names_from: "schema_file"`.
  `analyze` gains `--schema-file` and `--schema-file-db`; it stays offline.

## Fixes

- With a key-changing `ALTER` in the parsed files, the run printed both
  `schema_file_key_unmodelled` and `schema_file_key` and still emitted keyed
  SQL. The undo address is now re-derived from the planner's own definition
  before staging, so exactly one warning is printed and it matches the script.
- A definition whose column count differs from the row was reported as "no
  `--schema-file` definition covers this table". It is now a `schema` refusal
  (exit 6) that names both counts and suggests an incident-time dump or
  `--exclude-table`.

## Compatibility

- `report_version` stays 4 and `plan_version` stays 2.
  `docs/schema/analyze.json` gained `names_from` on the row image (additive).
- `analyze` gains two optional flags; existing invocations behave as before.
- `flashback` accepts fewer refusals: MySQL 5.7 binlogs and 8.x `MINIMAL` binlogs
  now work when a definition is supplied. Everything it refused before for
  safety still refuses: an incomplete row image, a value that cannot be rendered
  exactly, a generated-column contradiction, DDL in the selected range, a
  definition that contradicts a comparable binlog fact, and apply on MariaDB.
- The definition must describe the table as it was at incident time, exactly as
  before; a stale dump still produces wrong rows.

## Known issues / out of scope

- `analyze --schema-file` with a column-count mismatch falls back to `@1`..`@N`
  and reports the names as unavailable, instead of naming both counts the way
  `flashback` does.
- A definition that declares no primary key produces no `schema_file_key`
  warning, because no key is adopted; the script states that every column is
  matched with `LIMIT 1`.

## Tests and CI

- Three new live-server cases, all required to PASS in CI (a SKIP fails the job):
  a MINIMAL-metadata round trip on MySQL 8.0, the stale-key scenario on 8.0, and
  a round trip on MySQL 5.7.44. The 5.7 container now starts with `--log-bin`,
  and the test copies the binlog out with `docker cp`.
- Unit coverage for definition naming, the width refusal, key parsing (inline,
  table-level, composite, named, quoted), a key-changing `ALTER`, and the
  planner-side address re-derivation.

## Verification

`go test ./...`, `make codehealth`, `make lint`, and `gofmt` are clean on the
tagged commit's parent; the protection checks (`verify`, `vet and race`,
`flashback e2e`, `flashback difftest`, `MySQL 5.7 compatibility`,
`codehealth`, `golangci-lint`, `release-prep self-test`, `landing overflow`,
`darwin verify`) passed on the merged pull requests.
