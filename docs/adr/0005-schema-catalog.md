# Flashback binds table definitions through one Schema catalog with a typed resolution

Flashback decides which definition applies to each binlog `db.table` in one module, the Schema catalog (`internal/analyzer/schema_catalog.go`). Its interface is: `loadFile` (schema-file text), `observeTable` / `observeDDL` (binlog order; both return tables whose unqualified binding was dropped), `resolve` / `peek` (the binding for a name), and `warnings`. `resolve` returns a `resolution` whose kind is exact, case-fold, ambiguous, missing, or refused, with the column list, whether it must be checked against rows, the schema-file table of a case fold, the unused candidates, and a typed refusal reason.

Callers build every message from that result. No error text is rewritten after the fact.

## Why

The old `generatedTables` type had 17 fields split across `flashback_generated.go` and `flashback_schema.go`. #167, #187 and #215 each added a flag. A refusal of a case-folded table was relabelled by `labelFold`, which matched the binlog name followed by an ASCII `:` at the start of the joined error text. So the zh-CN refusal (full-width `：`) and every refusal line after the first lost the label (#225).

## Considered options

- Keep `labelFold` and also match `：` and every line. That is still string surgery that every new message template can break.
- Return the definition and let each caller look up fold state separately. That puts the binding rules back in the callers.

## Consequences

- Binding rules (USE vs header vs `--schema-file-db`, unqualified binds and their abandonment, letter-case fold, ALTER replay, LIKE, RENAME, DROP) are in one file and tested through `resolve`.
- Intended behavior change (#225): a refusal of a case-folded table names the schema-file table in every language and on every refusal line. Refusals of tables that were not folded, the SQL script, accept/refuse decisions and warnings are unchanged.
- `error.flashbackType` (`db.table.column:`) keeps the plain binlog name, because the column name follows it; `labelFold` never labelled it either.
