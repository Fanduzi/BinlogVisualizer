// input: FlashbackPlan, the struct --plan marshals.
// output: the published JSON Schema for flashback --plan.
// pos: schema derived from the plan type (ADR 0017).
// note: if this file changes, update docs/adr/0017-json-schema-and-display-timezone.md.
package flashback

import "binlogviz/internal/jsonschema"

// PlanJSONSchema is the schema of flashback --plan. plan_version is a field of the plan.
func PlanJSONSchema() ([]byte, error) {
	return jsonschema.Versioned(FlashbackPlan{}, "flashback-plan", "plan_version", FlashbackPlanVersion)
}
