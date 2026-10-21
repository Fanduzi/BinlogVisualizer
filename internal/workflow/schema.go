// input: the structs workflow describe, status, and clean marshal as JSON, plus the failure object describe prints when a plan does not load.
// output: published JSON Schemas. schema_version is injected by the command, so the caller passes it. Describe is a oneOf so the success object's required keys stay required.
// pos: schemas derived from the workflow document types (ADR 0017).
// note: if this file changes, update docs/adr/0017-json-schema-and-display-timezone.md.
package workflow

import "binlogviz/internal/jsonschema"

// DescribeJSONSchema is the schema of workflow describe --format json.
// failure is the object the command prints when the plan does not load.
// The result is a oneOf of Description and that object.
func DescribeJSONSchema(version int, failure any) ([]byte, error) {
	return jsonschema.OneOfVersioned("workflow-describe", "schema_version", version, Description{}, failure)
}

// StatusJSONSchema is the schema of workflow status --format json.
func StatusJSONSchema(version int) ([]byte, error) {
	return jsonschema.Versioned(Status{}, "workflow-status", "schema_version", version)
}

// CleanJSONSchema is the schema of workflow clean --format json.
func CleanJSONSchema(version int) ([]byte, error) {
	return jsonschema.Versioned(CleanResult{}, "workflow-clean", "schema_version", version)
}
