# BinlogViz v0.43.0 Release Notes

Release date: 2026-11-04

## Install or upgrade

```bash
brew upgrade --cask binlogviz          # macOS
# Linux: rerun install.sh, which resolves the latest release
```

Additive only: no flag was added or removed, `report_version` stays 4, and
`plan_version` stays 2. The new JSON keys are documented under Compatibility.

## Highlights

- **Hot Rows now ranks the key `--schema-file` declares.** When the binlog
  carries no row metadata (`binlog_row_metadata=MINIMAL`, or MySQL 5.7), the
  section used to say per-key tracking was unavailable. With a definition it now
  ranks the keys the dump declares: one line says the keys come from the file,
  each such row is marked `[--schema-file]`, and JSON carries
  `hot_rows_key_source: "schema_file"` plus `key_source` on each `hot_rows`
  entry.
- **The label stays true when the window holds both kinds of metadata.**
  `binlog_row_metadata` can change while the binlog is being written. A key the
  binlog answered, even for only part of its images, stays unmarked, and a window
  with both kinds makes no run-wide claim.
- **`key_status` and the `no_primary_key` alert did not change.** They still report
  only what the binlog recorded; Hot Rows is the one ranking that also accepts the
  definition, and the two surfaces are documented as answering different
  questions (ADR 0032).

## Fixes

- The Markdown Hot Rows section now names the source of its keys; text and HTML
  already did.
- `analyze --help` and the CLI and output references no longer say the key comes
  only from `binlog_row_metadata=FULL`. That sentence predates this feature and
  told readers the new path could not exist.
- `tables[].key_source` is no longer written for a table the definition covers
  without declaring a key: a key source with no key was misleading.

## Compatibility

- `report_version` stays 4. `docs/schema/analyze.json` gains
  `hot_rows_key_source` at the top level and `key_source` on each `hot_rows`
  entry; both are additive, and every existing `hot_rows` field keeps its meaning.
- `plan_version` stays 2. No flag, exit code, section name, or text layout
  changed, and a run without `--schema-file` produces the same output as before.

## Known issues / out of scope

- The definition must describe the table as it was at incident time. A wrong key
  ranks the wrong column, with no way for the tool to detect it.
- A key that neither the binlog nor the definition names still prints per-table
  tracking as unavailable, and nothing is guessed from column `@1`.
- A table the definition declares without a key stays in the No Primary Key
  section and still raises no `no_primary_key` alert: alerts come from binlog
  metadata by design.

## Tests and CI

- Capture: the binlog wins, the definition fills a gap, a definition of the wrong
  width is refused, and no definition keeps the old path
  (`TestRowKeysFallBackToDefinition`).
- Ranking and provenance: all-definition, all-binlog, a mixed window, and the same
  key touched under both metadata settings (`TestHotRowsUseSchemaFileKeys`); the
  text, Markdown, and JSON labels (`TestHotRowsNameTheDefinitionSource`); a table
  without a declared key has no `key_source` (`TestTableKeySourceNeedsADeclaredKey`).
- `--help` and the four reference pages cannot drift back to the old sentence
  (`TestHotRowsDocsNameTheDefinitionFallback`).
- Live MySQL 8.0.45: MINIMAL plus a definition ranks `id=1/2/3` with the source
  line and the JSON field; FULL with and without a definition is byte-identical;
  the checked-in MySQL 5.7 MINIMAL fixture ranks too.
- `go test ./...`, `make codehealth`, `make lint`, and `gofmt` are clean; all
  seven merged pull requests passed the 11 required checks.

## Verification

The tagged commit's parent carries the merged pull requests, and `main` passed
`ci` and `lint` on the final one (`verify`, `vet and race`, `flashback e2e`,
`flashback difftest`, `MySQL 5.7 compatibility`, `codehealth`, `golangci-lint`,
`release-prep self-test`, `landing overflow`, `darwin verify`).
