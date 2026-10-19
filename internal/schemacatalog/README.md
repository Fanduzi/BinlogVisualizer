# Schema catalog

Which table definition flashback uses for a binlog `db.table`, and why. See ADR 0005 and ADR 0012.

## Members

| File | Responsibility |
|------|----------------|
| `catalog.go` | Loads schema-file text, replays binlog DDL (CREATE, LIKE, ALTER, DROP, RENAME) and table maps, and resolves `db.table` to a typed result: exact, case-fold (naming the schema-file table), ambiguous, missing, or refused with a reason. A mysqldump header, `--schema-file-db`, or a unique binlog schema names an unqualified table. A binlog table with no definition of its own uses a single schema-file table that differs only in letter case when the binlog names are all lower case, with a warning that states the `lower_case_table_names` assumption; otherwise the near miss is only named. Callers build messages from the result; every refusal of a folded table names the file table, in every language (#225). |

## Interfaces

| API | Contract |
|-----|----------|
| `New(flagDB string) *Catalog` | Empty catalog. `flagDB` is `--schema-file-db`. |
| `LoadFile`, `ObserveTable`, `ObserveDDL` | Schema-file text, then binlog table maps and DDL in order. Observe returns tables whose unqualified binding was dropped. |
| `Resolve`, `Peek` | `Resolution`: `Kind`, `Cols`, `CheckRows`, `Deferred`, `FileTable`, `Candidates`, `Reason`. `Peek` does not bind. |
| `Warnings` | `[]model.FlashbackWarning`. The catalog does not rewrite error text. |

## Dependencies

- Upstream: `internal/mysqlexpr` parses column lists and schema-file text. `internal/model` is the warning type. `internal/i18n` is the warning text.
- Downstream: `internal/analyzer` holds one `*Catalog` until the flashback planner step.
