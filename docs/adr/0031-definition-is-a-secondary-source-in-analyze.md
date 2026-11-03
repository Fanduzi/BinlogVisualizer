# ADR 0031: A schema definition is a secondary source of key presence in analyze

Status: accepted (2026-11-03). ADR 0030 made `--schema-file` the source of column
names, type facts, and the undo key. The analyze report kept describing the binlog
alone, so a run printed "primary key presence unknown (binlog_row_metadata is not
FULL)" and, six lines later, "column names come from --schema-file". A definition
that covers the table but not the row fell back to `@1`..`@N` with no explanation.

## Context

- The report builds the line and the no-key risk section from `TableStats.KeyStatus`,
  which comes from `binlog_row_metadata` and nothing else (`internal/report/primary_key.go`).
- `--schema-file` is parsed in `cmd` for the capture resolver (ADR 0030); the report
  never saw it, so the same run could state two different answers about the key.
- `schemacatalog.FlashColumns` already answers what a definition declares: its
  columns and primary-key indexes, or "no definition covers this table".
- A definition that resolves but has a different column count cannot name the row;
  the capture fell back silently, which reads as "no definition" rather than
  "wrong dump".
- #478: a snapshot saved without `--workload-id` silently produces `unknown`
  comparability in `compare` and `trend`, and nothing says so at save time.

## Decision

1. **The binlog answers first. The definition only fills the gap it leaves.** A
   table whose `KeyStatus` is `has_pk` or `no_pk` is never re-decided by a
   definition; only `unknown` tables consult it.
2. **The report names the source** when every table with rows got its answer from
   the definition: "primary key from --schema-file (the binlog has no row
   metadata)". Partial coverage keeps the existing unknown wording, because a
   sentence that is true for some tables must not be stated for all of them.
3. **A definition that declares no key puts the table in the no-key risk section**,
   using its ordinary UPDATE/DELETE row counts, since the metadata-driven no-key
   counters are empty in that case.
4. **A definition that does not fit the row is stated, not silent.** The capture
   records both column counts on the row image; the text summary and the JSON
   row image say them (`names_note`). It is not a refusal: `analyze` never
   refuses a report, and the labels stay positional and honest.
5. **A snapshot saved without `--workload-id` warns once on stderr**, naming the
   consequence and the flag. It does not become an error and the JSON report is
   unchanged: the flag stays optional, and the warning is the only new signal.

## Considered options

- **Let the definition override the binlog.** Rejected: the binlog is the
  evidence; a dump is a claim about a moment.
- **Refuse `analyze` on a width mismatch.** Rejected: analysis does not depend
  on the definition, so refusing would remove a report the binlog alone can
  produce.
- **Require `--workload-id` with `--snapshot-name`.** Rejected in #478: it breaks
  existing scripts for a capability the user may not need.
- **Derive a default workload identity.** Rejected: the guard exists to make
  identity declared, not inferred.

## Consequences

- `report.Options` gains `DefinitionKeys`, wired once in `cmd` from the same
  catalog the capture uses; the report stays unaware of catalogs.
- JSON is additive only: `names_note` on a row image, and `primary_key_note` may
  now say the key came from `--schema-file`. `report_version` stays 4.
- `limitations.md` and `cli.md` state the precedence, the partial-coverage rule,
  and the save-time warning, in both languages.
