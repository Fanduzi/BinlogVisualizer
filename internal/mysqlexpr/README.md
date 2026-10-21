# MySQL expression module

Generated-column checker and the CREATE/ALTER column parser. The schema catalog calls the parser. Flashback calls the checker. Neither imports this package's caller. See ADR 0007 and ADR 0012.

## Members

| File | Responsibility |
|------|----------------|
| `eval.go` | One grammar parsed once per expression into a cached tree, evaluated in the general domain (`EvalGenExpr`) or the exact-DECIMAL domain (`EvalDecExprInc`, a checked subset). |
| `expr.go` | Evaluates a schema-file generated expression against logged SQL literals: integer and DECIMAL arithmetic, `UPPER`/`LOWER`, `CONCAT`/`CONCAT_WS`, `LENGTH`/`CHAR_LENGTH`, and constant-path JSON extraction. Division uses MySQL's default 9-digit width, then rounds half away from zero to the column scale. An intermediate past 30 fractional digits or 65 digits of precision is not rounded to fit: a DECIMAL target is unverified, and an integer target rounds an exact product once. A mismatch names one example image. Printable text in a text charset is shown as text; binary and non-printable bytes stay hex. ENUM and SET values in that example use the schema-file member labels. An unmodelled charset or type is unverifiable, never a match and never a mismatch. A result outside the column type is unverifiable only when the logged value is the non-strict clip. Identical inputs with a different generated value are a mismatch even when the expression cannot be evaluated. |
| `schema.go` | `SchemaCol` and the column parser. Checks a column list against TABLE_MAP metadata: count, names, order, and types where the binlog has them. Integer `/` rounds half away from zero. |
| `scan.go` | `Scan` over CREATE/ALTER and schema-file text: dump segments, statements, and the words the catalog walker reads. mysql --batch newlines (`\n`) are whitespace. A backslash inside a quoted string stays inside that string. |
| `droptable.go` | `ParseDropTable` reads a DROP TABLE name list (backquoted or bare, qualified or not, doubled backticks, `IF EXISTS`, `TEMPORARY`, plain comments). Analyzer and flashback both call it. |

## Interfaces

| API | Contract |
|-----|----------|
| `EvalGenExpr`, `EvalDecExprInc` | One tree, two domains (ADR 0007). Callers do not touch the cached tree. |
| `SchemaCol` | One column type. `Name`, `Generated`, and `Expr` are exported. The other fields stay next to the parser. |
| `ParseSchemaColumns`, `CloneSchemaCols`, `ApplyTableCharset`, `AlterSlice`, `AlterNoColumnChange`, `GeneratedNameSet`, `ValidateSchemaRow`, `DumpSegments`, `DumpDatabase`, `SchemaColNames` | Column lists and schema-file text. |
| `PrepareGenImages`, `(*GenImages).Outcome`, `(*GenImages).DivIncrementFit`, `DivIncrementMismatch`, `DivIncrementHint`, `GeneratedMismatch`, `SQLTableName`, `UnverifiedGeneratedComment`, `UnverifiedGeneratedWarning` | Generated-column review. The caller passes logged images. |
| `NewScan`, `Scan` | The walker reads `Skip`, `Word`, `WordIs`, `Ident`, `Rest`, `UnreadWord`, `Qualified`, `ParenBody`, `PeekWord`, `PeekByte`, and the text and index `S` and `I`. |

## Dependencies

- Upstream: `internal/model` (`FlashbackWarning`, `FlashRow`) and `internal/i18n`.
- Downstream: `internal/schemacatalog`, `internal/flashback`, and `internal/analyzer`.
