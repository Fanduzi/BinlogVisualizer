# ADR 0030: A schema definition supplies column names, type facts, and key identity to undo

Status: accepted (2026-11-02). Issue #482. `redo` already accepts `--schema-file` when the binlog has no column names (ADR 0022); `flashback` refuses the same input before it ever consults that file. MySQL's default `binlog_row_metadata` is `MINIMAL`, and MySQL 5.7 has no row metadata at all, so on the most common production configuration the tool's recovery path refuses while its forward path works.

## Context

- **The refusal is at capture, not at planning.** `internal/binlog/flashback.go:36` marks the row unusable the moment the table map lacks FULL metadata, and returns before `schemacatalog` is resolved:

  ```go
  labels, names := columnLabels(ev.Table, width)
  if names != model.RowNamesFull || !flashNamesComplete(ev.Table, width) {
      base.ProblemKind = model.FlashProblemNames
      return []model.FlashRow{base}
  }
  ```

- **The undo row type cannot express "positional, but resolvable".** `model.FlashRow` carries a `ProblemKind` (`names`, `image`, `type`, `capture`) and nothing else about where names might come from. The redo row type added later has the missing concept: `model.RedoRow.Names` is `RowNamesFull` or `RowNamesPositional` (`internal/model/redo.go:11`).
- **ADR 0022 drew the line deliberately and said so.** Its decision list reads: `MINIMAL` + a resolvable definition "supplies names and fills the missing type facts", while `MINIMAL` + no definition refuses `names`, and the message names the remediation "`--schema-file` (which does supply names for redo, unlike undo)". It also records the reason redo could stop there: the WHERE clause falls back to all columns + `LIMIT 1` "because the catalog does not track primary keys".
- **The merge machinery already exists.** `model.FlashCol`'s own comment says redo merges schema-file facts into it under `MINIMAL`; `internal/redo/planner.go` (`acceptPositionalSchema`) checks column count and every comparable type fact before merging.
- **Key identity is the one fact undo needs that redo does not.** Undo addresses the row it reverses (`UPDATE`/`DELETE … WHERE <pk>`), and its conflict matching keys on that address. `schemacatalog` does not parse `PRIMARY KEY` at all, and `mysqlexpr.SchemaCol` (`internal/mysqlexpr/schema.go:18`) carries `Name`, `Generated`, `Expr`, base type, signedness, charset, members, precision and scale — no key flag. So today a key can only come from the binlog.
- **A dump can be wrong.** A `--schema-file` describes the table as it was at incident time by convention only; nothing in the binlog under `MINIMAL` can confirm a primary key, an index, or a later `ALTER`.
- **`analyze --show-rows` has the same gap and no way to close it.** It renders `@1..@N` and says names are unavailable; `analyze` has no `--schema-file` flag at all.

## Decision

1. **A resolved table definition is a source of column names, type facts, and key identity for `flashback`**, at the same rank ADR 0022 gave it for `redo`. Resolution means `schemacatalog` returns `Exact` or `Fold`, the column count matches, and every comparable binlog fact agrees; the existing `schema` refusal covers a disagreement.
2. **A `PRIMARY KEY` in the definition is used for the undo address, and its source is labelled.** The script comment, one stderr line, and `--plan` all say the key came from the schema file rather than from the binlog, so an operator reading the script can see which fact is only as good as the dump.
3. **A definition with no primary key keeps the no-key path**: match every column and `LIMIT 1`, labelled the same way. This is the rule undo already applies to a table the binlog reports as having no primary key.
4. **A definition cannot override a binlog fact it disagrees with.** Column count, order, base type, signedness, charset, `ENUM`/`SET` members, precision, scale and fractional seconds still refuse on mismatch (exit 6, `schema`). Under `MINIMAL` the binlog cannot contradict a key, so a key is accepted from the dump; that is the entire scope of the new trust.
5. **Nothing else moves.** A row with no resolvable definition still refuses (`row_metadata`), with the remediation text updated to say that `--schema-file` now supplies names. Incomplete images, values that cannot be rendered exactly, generated-column contradictions, DDL in the selected range, apply-guard, `verify`, and the XA/commit-marker protocol are unchanged.
6. **MySQL 5.7 undo becomes possible through the same path**, because a 5.7 binlog with `binlog_row_image=FULL` carries complete images and only lacks metadata. It is verified separately on 5.7.44 before being claimed.
7. **`analyze --show-rows` takes the same definition** through a new `--schema-file` flag and prints real column names instead of `@1..@N`. The JSON report gains an additive key naming the source; `report_version` stays 4.
8. **Out of scope, still refused:** applying an undo on MariaDB, undoing DDL, and per-block resume.

## Considered options

- **Strict: refuse undo unless the binlog itself names the key.** Rejected — it leaves MySQL's default configuration and all of 5.7 without undo, which is the defect this ADR exists to remove.
- **Loose: use the dump's key silently.** Rejected — the script is applied by a human against production; an unlabelled key that could be stale is exactly the kind of quiet wrongness the rest of the tool refuses to produce.
- **Positional `INSERT … VALUES` without a definition.** Already rejected in ADR 0022 for redo and rejected here for the same reason: the literals need type facts only a definition supplies.
- **A live-server key lookup instead of a dump.** Rejected for now: it needs database credentials at undo time and makes the undo depend on today's schema, which is precisely the drift the dump rule exists to avoid.

## Consequences

- `docs/concept/limitations.md` states the new source of names and keys, and restates that the dump must describe the table at incident time; a drifted dump produces wrong rows and only the guard's `EXTRA` check can catch the generated-column half.
- `schemacatalog` gains primary-key parsing from the `CREATE TABLE` text it already reads, and `mysqlexpr.SchemaCol` gains a key flag. Both are additive; redo's behaviour does not change.
- `analyze` gains a flag, so `docs/reference/cli.md` and `--help` change, and `docs/schema/analyze.json` is regenerated by the generator when the JSON key is added.
- Two new e2e cases are required before this is claimed: an 8.0 server with `binlog_row_metadata=MINIMAL`, and the pinned 5.7.44 container, each generating, verifying, applying, and diffing rows. A SKIP is a failure.
- The 5.7 claim stays scoped to `binlog_row_image=FULL`; a `MINIMAL` row image is still `row_image` refusal on every server.
