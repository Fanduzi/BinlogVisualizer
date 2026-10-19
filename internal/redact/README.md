# SQL redaction

Credential literals in SQL that an error or a report quotes become `<secret>`.

## Members

| File | Responsibility |
|------|----------------|
| `redact.go` | `Credentials` replaces `IDENTIFIED BY` / `WITH` … `AS`\|`USING` and `SET PASSWORD` secrets. `Statement` is that plus a length cap for a flashback DDL refusal. `Display` drops control bytes for a timeline line. |

## Dependencies

- Upstream: standard library.
- Downstream: `internal/analyzer` and `internal/flashback`.
