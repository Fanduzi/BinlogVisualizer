# ADR 0032: Hot Rows ranks definition keys when the binlog has none

Status: accepted (2026-11-04). ADR 0030 gave undo the definition's key and ADR 0031
let the report name it, but Hot Rows still said "tracking unavailable" on a
MINIMAL binlog, because the identity has to exist while the event is read.

## Context

- `internal/binlog` builds `RawEvent.RowKeys` from the table map only
  (`pkMetaForRows` → `pkMetaFrom`, which requires FULL metadata), and
  `internal/analyzer/hotrows.go` ranks those keys under `KeyStatus == has_pk`.
- A MINIMAL table map yields no key and `KeyStatus == unknown`, so the aggregator
  records the table as unavailable and never sees a key, even when
  `--schema-file` describes it.
- The parser already holds the definition resolver (ADR 0030) and the analyzer
  has no catalog, so the capture is the only place that can produce the key.
- `key_status` and the `no_primary_key` alert are deliberately metadata-only
  signals (ADR 0031); changing them would change alerting, not just reporting.

## Decision

1. **The capture falls back to the definition's key** when the table map names
   none: `FlashColumns` supplies the key column indexes, the same
   column-count check as naming applies, and the result is cached per table, so
   the lookup runs once per table and never per row.
2. **The binlog always wins.** A table map that names a key is used as before; the
   definition is consulted only where `pkMetaForRows` returns nothing.
3. **The event says where the key came from** (`RowKeysFromSchema`). The Hot Rows
   aggregator ranks those keys under `unknown` status and records
   `KeySource: "schema_file"`.
4. **The report labels it.** The Hot Rows section prints one line, HTML does the
   same, and JSON adds `hot_rows_key_source`.
5. **`key_status` and `no_primary_key` stay metadata-only.** A definition-declared
   key does not rewrite a table's `key_status` and a definition-declared keyless
   table still raises no alert. The two surfaces answer different questions: the
   alert says what the binlog recorded, Hot Rows says what could be ranked.

## Considered options

- **Set `KeyStatus` from the definition.** Rejected: it would silently change
  alerting and the documented meaning of `key_status`.
- **Resolve keys in the analyzer.** Rejected: the analyzer has no catalog, and the
  parser already owns the resolver and the table map facts the formatter needs.
- **Leave Hot Rows metadata-only.** Rejected: it is the one report section that
  still answers "unknown" while the tool can answer from the dump, and the
  objective of the change is exactly that gap.

## Consequences

- The Hot Rows section can rank keys on MySQL 8.x `MINIMAL` and MySQL 5.7 when a
  definition is supplied; without `--schema-file` nothing changes.
- JSON is additive (`hot_rows_key_source`), `report_version` stays 4.
- The trust rule is the dump rule already stated for undo: the definition must
  describe the table as it was at incident time, and a wrong key ranks the wrong
  column.
- The documented split (alerts from metadata, Hot Rows from metadata or the
  definition) is stated in `limitations.md` and `analyze-behavior.md`.
