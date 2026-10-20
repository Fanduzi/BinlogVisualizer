// input: Result, the struct RenderJSON marshals.
// output: the published JSON Schema for trend --format json.
// pos: schema derived from the trend result type (ADR 0017).
// note: if this file changes, update docs/adr/0017-json-schema-and-display-timezone.md.
package trend

import "binlogviz/internal/jsonschema"

// JSONSchema is the schema of trend --format json. schema_version is added
// by jsonout.Marshal and is not a field of Result.
func JSONSchema() ([]byte, error) {
	return jsonschema.Versioned(Result{}, "trend", "schema_version", SchemaVersion)
}
