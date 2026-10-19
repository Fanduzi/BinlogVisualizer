// Package jsonout writes the versioned JSON documents binlogviz prints on stdout.
// input: a payload that encodes to a JSON object, and that output's schema version.
// output: indented JSON with "schema_version" as the first key (ADR 0010).
// pos: the single place every non-analyze, non-plan JSON output gets its version key.
// note: if this file changes, update docs/adr/0010-every-json-output-is-versioned.md.
package jsonout

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// Key is the version key every JSON output without its own key carries.
const Key = "schema_version"

// Marshal encodes payload as indented JSON with "schema_version": version
// first. payload must encode to a JSON object without a schema_version key.
func Marshal(version int, payload any) ([]byte, error) {
	if version < 1 {
		return nil, fmt.Errorf("jsonout: schema version %d < 1", version)
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil || fields == nil {
		return nil, fmt.Errorf("jsonout: payload %T is not a JSON object", payload)
	}
	if _, dup := fields[Key]; dup {
		return nil, fmt.Errorf("jsonout: payload %T already has %q", payload, Key)
	}
	var compact bytes.Buffer
	fmt.Fprintf(&compact, `{%q:%d`, Key, version)
	if inner := bytes.TrimSpace(body[1 : len(body)-1]); len(inner) > 0 {
		compact.WriteByte(',')
		compact.Write(inner)
	}
	compact.WriteByte('}')
	var out bytes.Buffer
	if err := json.Indent(&out, compact.Bytes(), "", "  "); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}
