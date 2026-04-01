// Package compare renders JSON reports for compare results.
// input: deterministic CompareResult values produced by the compare diff engine.
// output: stable JSON objects for machine-readable compare consumption.
// pos: compare renderer used by the compare command JSON output path.
package compare

import "encoding/json"

func RenderJSON(result CompareResult) (string, error) {
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return "", err
	}
	return string(data), nil
}
