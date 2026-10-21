// input: CompareResult, the struct RenderJSON marshals.
// output: the published JSON Schema for compare --format json.
// pos: schema derived from the compare result type (ADR 0017).
// note: if this file changes, update docs/adr/0017-json-schema-and-display-timezone.md.
package compare

import "binlogviz/internal/jsonschema"

// JSONSchema is the schema of compare --format json. schema_version is added
// by jsonout.Marshal and is not a field of CompareResult.
func JSONSchema() ([]byte, error) {
	return jsonschema.Versioned(CompareResult{}, "compare", "schema_version", SchemaVersion)
}
