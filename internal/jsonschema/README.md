# JSON Schema generation

Builds a Draft 2020-12 JSON Schema document from the Go value
`encoding/json` would marshal, and checks a document against that schema
(ADR 0017). The schema published for every versioned JSON output is
generated from the same struct the command prints, so commands do not
hand-write a second field list. Every struct is closed
(`additionalProperties: false`). A command that prints two shapes uses
`Union` when they share required keys, or `OneOf` when a union would drop
a shape's required keys.

## Members

| File | Responsibility |
|------|----------------|
| `jsonschema.go` | `Of` (reflection walk mirroring `encoding/json`: `json` tags, `omitempty` drops `required`, anonymous struct embeds flatten, `json:"-"` skips), the `jsonschema:"const:<json>"` tag that pins one field's schema to a value, `Union`/`OneOf` composition, `AddVersion`/`Versioned`/`UnionVersioned`/`OneOfVersioned` version-key wiring, `Marshal` (the published file: `$schema` draft, `$id` `binlogviz:schema/<id>`, `title`), and `Validate` (the first place an instance disagrees). |
| `jsonschema_test.go` | Verifies the generated schema matches `encoding/json`'s output and rejects field drift, empty `required` is omitted, `Union` keeps only shared required keys, and `OneOf` keeps each branch's own. |

## Interfaces

| API | Contract |
|-----|----------|
| `Of(v any) (map[string]any, error)` | The schema for the object `encoding/json` emits for `v` (a typed nil pointer is allowed). Fails on a nil type or a top-level non-object. Nil slices/maps are nullable because that is what the encoder writes; `time.Time` and `[]byte` are `string`; `json.RawMessage`, `any`, and recursive types are unconstrained `{}`; a non-string map key is an error. |
| `Union(parts ...any) (map[string]any, error)` | One closed object: properties merged (a same-named property must have an identical schema), `required` is the keys every part requires. |
| `AddVersion(doc, key, version) error` | Records `key` as a required integer `const` — the version field already on the struct, or the key `jsonout.Marshal` injects. |
| `Versioned(v, id, key, version)` / `UnionVersioned(id, key, version, parts...)` / `OneOfVersioned(id, key, version, parts...)` | Schema plus the required integer version key, rendered as the published file. `OneOf` keeps each branch's own `required` for shapes that share none. |
| `Marshal(doc, id) ([]byte, error)` | The published document: draft 2020-12 `$schema`, `$id`, `title`, closed `additionalProperties`, `required` omitted when empty. |
| `Validate(schema, inst) error` | The first place `inst` disagrees: type, `const`, a missing required property, a property the closed object does not name, or array items. A `oneOf` matches exactly one branch. Keywords this package does not emit are ignored. |

## Dependencies

- Upstream: standard library only (`encoding/json`, `reflect`).
- Downstream: `cmd/binlogviz schema` and the per-module `schema.go` files in `internal/report`, `internal/compare`, `internal/flashback`, `internal/trend`, and `internal/workflow` build their published schemas here.
