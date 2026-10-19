// Package compare renders JSON reports for compare results.
// input: deterministic CompareResult values produced by the compare diff engine.
// output: stable JSON objects, with schema_version first (ADR 0010), for machine-readable compare consumption.
// pos: compare renderer used by the compare command and workflow compare artifacts.
package compare

import "binlogviz/internal/jsonout"

// SchemaVersion is the compare JSON contract version. Bump it only for a
// breaking change, and record the bump in CHANGELOG.
const SchemaVersion = 1

func RenderJSON(result CompareResult) (string, error) {
	if result.PatternDrilldowns == nil {
		result.PatternDrilldowns = []PatternDrilldown{}
	}
	data, err := jsonout.Marshal(SchemaVersion, result)
	if err != nil {
		return "", err
	}
	return string(data), nil
}
