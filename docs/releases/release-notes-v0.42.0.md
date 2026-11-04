# BinlogViz v0.42.0 Release Notes

Release date: 2026-11-04

## Install or upgrade

```bash
brew upgrade --cask binlogviz          # macOS
# Linux: rerun install.sh, which resolves the latest release
```

Additive only: no flag was added or removed, `report_version` stays 4, and
`plan_version` stays 2. The two new JSON keys are documented under Compatibility.

## Highlights

- **The analyze report now says where the primary key came from.** With
  `--schema-file`, when the binlog records no row metadata, the Summary names the
  definition instead of claiming the key presence is unknown. Tables the
  definition answered are marked `[--schema-file]` in the No Primary Key section,
  and each `tables` entry in JSON carries `key_source: "schema_file"`.
- **A partial definition no longer produces a false sentence.** The binlog answers
  first wherever it can; when the definition answered only some of the tables, the
  Summary keeps `primary key presence unknown for some tables` and the section
  explains the marked rows, instead of crediting the file for every table.
- **A definition that does not fit the row is stated, not silent.** When the
  column count differs, the Summary and the JSON row image (`names_note`) give both
  counts, and the labels stay `@1`..`@N` with the reason attached.
- **A snapshot saved without a workload identity says so.** `analyze
  --snapshot-name` without `--workload-id` prints one stderr warning once the file
  is written: `compare` and `trend` will report unknown comparability for it.

## Fixes

- A definition declaring no key listed the table under No Primary Key without
  saying why; the section now names the source per marked table.
- A mixed-metadata run credited `--schema-file` for tables whose key came from FULL
  binlog metadata. The run-wide source sentence now appears only when the
  definition answered every table.
- A definition covering part of the analyzed tables made the Summary claim a source
  that was false for the rest, while the section said presence was unknown. Both
  surfaces now agree.

## Compatibility

- `report_version` stays 4. `docs/schema/analyze.json` gains `names_note` on a row
  image, `key_source` on a table entry, and `primary_key_note` may now name
  `--schema-file` (all additive).
- `plan_version` stays 2. No flag, exit code, or text section was renamed.
- The snapshot warning is stderr only; stdout, the saved snapshot payload, and
  `report_version` are unchanged.

## Known issues / out of scope

- The Hot Rows section still reports per-key tracking as unavailable for a binlog
  without row metadata: the key has to be known while the event is read, and
  `--schema-file` is an answer that arrives later.
- A table the definition declares without a key appears in the No Primary Key
  section but does not raise a `no_primary_key` alert; alerts come from binlog
  metadata.

## Tests and CI

- Definition-source precedence is covered by seven cases: single keyed table,
  single keyless table (marker and summary), binlog-wins, mixed keyed tables,
  mixed definition, mixed metadata with a keyless definition, and partial coverage
  with a keyless risk.
- The width-mismatch note and the snapshot warning (fires on a written snapshot,
  silent when nothing is saved) each have a test at the CLI layer.
- `go test ./...`, `make codehealth`, `make lint`, and `gofmt` are clean; every one
  of the six merged pull requests passed all 11 required checks.

## Verification

The tagged commit's parent carries the merged pull requests, and `main` passed
`ci` and `lint` on each of them (`verify`, `vet and race`, `flashback e2e`,
`flashback difftest`, `MySQL 5.7 compatibility`, `codehealth`, `golangci-lint`,
`release-prep self-test`, `landing overflow`, `darwin verify`).
