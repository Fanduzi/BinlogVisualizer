// input: small structs standing in for a command JSON object.
// output: a schema that accepts the encoded object, rejects a dropped property and an unknown field, and allows the null encoding/json writes for a nil slice.
// pos: the validator the command-output schema test depends on.
// note: if this file changes, update docs/adr/0017-json-schema-and-display-timezone.md.
package jsonschema

import (
	"bytes"
	"encoding/json"
	"testing"
)

type sample struct {
	Name    string         `json:"name"`
	Count   int            `json:"count"`
	Labels  []string       `json:"labels,omitempty"`
	Extra   map[string]int `json:"extra,omitempty"`
	Note    *string        `json:"note,omitempty"`
	Missing []int          `json:"missing"`
}

func TestSchemaMatchesEncodingAndRejectsDrift(t *testing.T) {
	doc, err := Of(sample{})
	if err != nil {
		t.Fatal(err)
	}
	if err := AddVersion(doc, "schema_version", 1); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(sample{Name: "a", Count: 2, Missing: nil})
	if err != nil {
		t.Fatal(err)
	}
	var inst map[string]any
	if err := json.Unmarshal(raw, &inst); err != nil {
		t.Fatal(err)
	}
	inst["schema_version"] = 1
	if err := Validate(doc, inst); err != nil {
		t.Fatal(err)
	}

	dropped := cloneMap(doc)
	props := cloneMap(dropped["properties"].(map[string]any))
	delete(props, "name")
	dropped["properties"] = props
	if err := Validate(dropped, inst); err == nil {
		t.Fatal("dropping a property the object emits must fail")
	}

	inst["other"] = true
	if err := Validate(doc, inst); err == nil {
		t.Fatal("a field the schema does not name must fail")
	}
}

func TestOmitsEmptyRequired(t *testing.T) {
	type actor struct {
		User string `json:"user,omitempty"`
		Host string `json:"host,omitempty"`
	}
	doc, err := Of(struct {
		Actor actor `json:"actor"`
	}{})
	if err != nil {
		t.Fatal(err)
	}
	props := doc["properties"].(map[string]any)
	actorSchema := props["actor"].(map[string]any)
	if _, ok := actorSchema["required"]; ok {
		t.Fatalf("all-omitempty struct emitted required: %#v", actorSchema["required"])
	}
	raw, err := json.Marshal(actorSchema)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, []byte(`"required"`)) {
		t.Fatalf("marshaled schema still has required: %s", raw)
	}
}

func TestUnionKeepsOnlySharedRequiredKeys(t *testing.T) {
	type ok struct {
		Valid bool   `json:"valid"`
		Name  string `json:"name"`
	}
	type bad struct {
		Valid bool   `json:"valid"`
		Error string `json:"error"`
	}
	doc, err := Union(ok{}, bad{})
	if err != nil {
		t.Fatal(err)
	}
	req := stringList(doc["required"])
	if len(req) != 1 || req[0] != "valid" {
		t.Fatalf("required = %v", req)
	}
	raw, _ := json.Marshal(bad{Error: "no"})
	var inst map[string]any
	if err := json.Unmarshal(raw, &inst); err != nil {
		t.Fatal(err)
	}
	if err := Validate(doc, inst); err != nil {
		t.Fatal(err)
	}
}

func TestOneOfKeepsEachBranchRequired(t *testing.T) {
	type ok struct {
		Name string `json:"name"`
	}
	type bad struct {
		Valid bool   `json:"valid"`
		Error string `json:"error"`
	}
	raw, err := OneOfVersioned("sample", "schema_version", 1, ok{}, bad{})
	if err != nil {
		t.Fatal(err)
	}
	var schema map[string]any
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatal(err)
	}
	success := map[string]any{"schema_version": float64(1), "name": "a"}
	failure := map[string]any{"schema_version": float64(1), "valid": false, "error": "no"}
	if err := Validate(schema, success); err != nil {
		t.Fatal(err)
	}
	if err := Validate(schema, failure); err != nil {
		t.Fatal(err)
	}
	if err := Validate(schema, map[string]any{"schema_version": float64(1)}); err == nil {
		t.Fatal("a document missing each branch's required keys must fail")
	}
	if err := Validate(schema, map[string]any{"schema_version": float64(1), "name": "a", "error": "no"}); err == nil {
		t.Fatal("a mix of the two shapes must fail")
	}
	if err := Validate(schema, map[string]any{"schema_version": float64(1), "valid": false}); err == nil {
		t.Fatal("a failure document missing error must fail")
	}
}

func cloneMap(in map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range in {
		out[k] = v
	}
	return out
}
