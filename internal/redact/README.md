# SQL redaction

Credential literals in SQL that an error or a report quotes become `<secret>`.

## Members

| File | Responsibility |
|------|----------------|
| `redact.go` | `Credentials` replaces recognized `IDENTIFIED BY` / `WITH` … `AS`\|`USING` and `SET PASSWORD` secrets. `Statement` redacts the whole statement without a length cap. `Display` drops control characters and invalid UTF-8 for a timeline line. |
| `redact_contract_test.go` | Checks credential forms and safe no-secret boundaries, whole multiline statements, and terminal-safe display text. |

## Interfaces

- `Credentials(sql string) string` replaces recognized credential spans with `<secret>`.
- `Statement(sql string) string` redacts the complete statement before a caller applies display or truncation.
- `Display(s string) string` removes terminal control characters and invalid UTF-8.

## Dependencies

- Upstream: standard library.
- Downstream: `internal/analyzer` and `internal/flashback`.

## Update Rule

If members, interfaces, or dependencies change, update this file in the same change.
