// input: the analyze JSON struct jsonAnalysisResult.
// output: the published JSON Schema for analyze --format json, including report_version.
// pos: schema derived from the struct encoding/json marshals (ADR 0017).
// note: if this file changes, update docs/adr/0017-json-schema-and-display-timezone.md.
package report

import "binlogviz/internal/jsonschema"

// ReportVersion is the analyze JSON contract version (report_version).
func ReportVersion() int { return currentReportVersion }

// AnalysisJSONSchema is the schema of analyze --format json and of a saved snapshot body.
func AnalysisJSONSchema() ([]byte, error) {
	return jsonschema.Versioned(jsonAnalysisResult{}, "analyze", "report_version", currentReportVersion)
}
