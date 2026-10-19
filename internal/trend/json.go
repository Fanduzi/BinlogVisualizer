package trend

import "binlogviz/internal/jsonout"

// SchemaVersion is the trend JSON contract version (ADR 0010). Bump it only
// for a breaking change, and record the bump in CHANGELOG.
const SchemaVersion = 1

func RenderJSON(result Result) (string, error) {
	data, err := jsonout.Marshal(SchemaVersion, result)
	if err != nil {
		return "", err
	}
	return string(data), nil
}
