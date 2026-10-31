# Versioned JSON output

The single place every non-analyze, non-plan JSON output gets its version
key (ADR 0010). `Marshal` encodes a payload as indented JSON with
`"schema_version"` as the first key, so commands do not hand-write the
field and cannot put it in the wrong place.

## Members

| File | Responsibility |
|------|----------------|
| `jsonout.go` | `Key` (`"schema_version"`) and `Marshal`, which splices the version key into the front of the compact object and re-indents, so the key is first regardless of Go map ordering. |
| `jsonout_test.go` | Verifies first-key placement, empty objects and map payloads, and rejection of non-objects, duplicate keys, and versions below 1. |

## Interfaces

| API | Contract |
|-----|----------|
| `Key` | The version key every JSON output without its own key carries: `schema_version`. |
| `Marshal(version int, payload any) ([]byte, error)` | Two-space-indented JSON with `schema_version` first. Fails when `version < 1`, when `payload` does not encode to a JSON object, or when the object already carries `schema_version`. |

## Dependencies

- Upstream: standard library only (`encoding/json`).
- Downstream: `cmd/binlogviz` (`flashback verify`, `snapshot`, `version`, workflow output) and the `internal/compare` / `internal/trend` JSON writers. Analyze JSON carries its own `report_version` and does not use this package.
