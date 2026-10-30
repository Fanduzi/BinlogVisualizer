# SQL Emit Module

Statement-assembly vocabulary shared by the undo and redo emitters (ADR 0022): quoted identifiers and column lists, `<=>` row-match WHERE clauses with the no-primary-key `LIMIT 1` comment, generated-column omission, and the sql_mode drop expression each direction wraps its statements with.

## Files

| File | Responsibility |
|------|----------------|
| `sqlemit.go` | `QuoteIdent`/`QuoteColumnList`, `TableName`, `AssignColumns`/`IsGenerated`/`OmittedGeneratedNull` (generated columns are omitted from writes and recomputed by the server), `KeyMatch`/`WhereImage`/`JoinConds` (NULL-safe `<=>` matching on the PK or every column plus `LIMIT 1`), and `RelaxedModes`/`DroppedModeExpr` (the sql_mode flags a stored value needs relaxed, and the expression that drops them). |

## Exports

- `func TableName(schema, table string) string`
- `func QuoteIdent(name string) string`
- `func QuoteColumnList(columns []string) string`
- `func AssignColumns(columns, values []string, gen map[string]struct{}) ([]string, []string)`
- `func IsGenerated(gen map[string]struct{}, name string) bool`
- `func OmittedGeneratedNull(columns, values []string, gen map[string]struct{}) bool`
- `func KeyMatch(row model.FlashRow, image []string) string`
- `func WhereImage(row model.FlashRow, head string, image []string, cond string, allColumns bool) (string, bool)` — `(stmt, false)` when no column could match; the caller turns that into its refusal.
- `func JoinConds(a, b string) string`
- `func RelaxedModes(row model.FlashRow, gen map[string]struct{}) []string`
- `func DroppedModeExpr(drops []string) string`

## Dependencies

- Upstream: `internal/model` (`FlashRow` is the shared row shape both directions render).
- Downstream: `internal/flashback` delegates undo rendering; `internal/redo` delegates forward rendering.

## Update Rule

If members, interfaces, or dependencies change, update this file in the same change.
