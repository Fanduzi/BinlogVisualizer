# MariaDB GTID tokens

Parses MariaDB GTID selector tokens — `domain-server-seq`, or
`domain-server-start..end` for a sequence range of one server — and
canonicalizes a selector list. The analyzer's GTID selector and compare's
scope check share it, so both read the same forms.

## Members

| File | Responsibility |
|------|----------------|
| `mariadb.go` | The `MariaDB{Domain, Server, Start, End}` token type, `ParseMariaDB`, the `Identity`/`String` canonical text forms, and `MergeMariaDB` (parse every token, merge overlapping and adjacent ranges per domain-server, order by domain, server, sequence). |
| `mariadb_test.go` | Verifies the whitespace rules around `..` (#383 QA), merge canonicalization including max-sequence adjacency and contained ranges (#384 QA F1), and that MySQL-form tokens fail. |

## Interfaces

| API | Contract |
|-----|----------|
| `ParseMariaDB(value string) (MariaDB, bool)` | Parses one token. Whitespace around the whole token is ignored; whitespace next to `..` is rejected, the way MySQL ranges (`1-5`) take no inner whitespace. Non-numeric parts and `end < start` fail. |
| `(MariaDB).Identity() string` | The canonical `domain-server-start` text. |
| `(MariaDB).String() string` | The identity, plus `..end` for a range. |
| `MergeMariaDB(values []string) ([]string, bool)` | Parses every token (comma-separated values allowed, empty parts skipped), merges overlapping and adjacent ranges of each domain-server, ordered by domain, server and sequence. `false` when a token does not parse. |

## Dependencies

- Upstream: standard library only.
- Downstream: `internal/analyzer` (`gtid_selector.go`) and `internal/compare` (`comparability.go`).
