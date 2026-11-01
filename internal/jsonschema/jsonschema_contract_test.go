package jsonschema_test

import (
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"binlogviz/internal/jsonschema"
)

type embeddedFields struct {
	Name     string `json:"name"`
	Embedded string `json:"embedded"`
}

type item struct {
	Count int `json:"count"`
}

type node struct {
	Value string `json:"value"`
	Next  *node  `json:"next,omitempty"`
}

type richDocument struct {
	embeddedFields
	Name        int             `json:"name"`
	When        time.Time       `json:"when"`
	Bytes       []byte          `json:"bytes"`
	Entries     []*item         `json:"entries"`
	Index       map[string]item `json:"index"`
	Arbitrary   any             `json:"arbitrary"`
	Raw         json.RawMessage `json:"raw"`
	RequiredPtr *int            `json:"required_ptr"`
	OptionalPtr *string         `json:"optional_ptr,omitempty"`
	Omitted     string          `json:"omitted,omitempty"`
	Root        *node           `json:"root"`
	Ignored     string          `json:"-"`
	hidden      string
}

func TestOfRejectsUnrepresentableTypes(t *testing.T) {
	tests := []struct {
		name string
		in   any
		want string
	}{
		{"nil", nil, "nil type"},
		{"scalar", 1, "not a JSON object"},
		{"non-string map key", map[int]string{}, "map key int is not string"},
		{"channel", struct{ Ch chan int }{}, "unsupported kind chan"},
		{"function", struct{ Fn func() }{}, "unsupported kind func"},
		{"invalid const", struct {
			Marker string `json:"marker" jsonschema:"const:invalid"`
		}{}, `field marker const "invalid"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := jsonschema.Of(tt.in)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Of error = %v, want context %q", err, tt.want)
			}
		})
	}
	var typedNil *struct {
		Name string `json:"name"`
	}
	if _, err := jsonschema.Of(typedNil); err != nil {
		t.Fatalf("typed nil object pointer: %v", err)
	}
}

func TestOfMatchesEncodingJSONForRichObject(t *testing.T) {
	doc, err := jsonschema.Of(richDocument{})
	if err != nil {
		t.Fatal(err)
	}
	input := richDocument{
		embeddedFields: embeddedFields{Name: "shadowed", Embedded: "kept"},
		Name:           42,
		When:           time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		Bytes:          []byte{1, 2, 3},
		Entries:        []*item{{Count: 7}, nil},
		Index:          map[string]item{"west": {Count: 9}},
		Arbitrary:      []any{"value", 3},
		Raw:            json.RawMessage(`{"nested":true}`),
		Root:           &node{Value: "first", Next: &node{Value: "last"}},
		Ignored:        "not serialized",
		hidden:         "not serialized",
	}
	raw, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	var instance map[string]any
	if err := json.Unmarshal(raw, &instance); err != nil {
		t.Fatal(err)
	}
	if err := jsonschema.Validate(doc, instance); err != nil {
		t.Fatalf("schema rejected encoding/json object %s: %v", raw, err)
	}
	if instance["name"] != float64(42) || instance["embedded"] != "kept" {
		t.Fatalf("direct field did not shadow embedded name: %s", raw)
	}
	for _, absent := range []string{"hidden", "Ignored", "omitted", "optional_ptr"} {
		if _, ok := instance[absent]; ok {
			t.Fatalf("unexpected %q in encoded object: %s", absent, raw)
		}
	}
	props := doc["properties"].(map[string]any)
	if props["name"].(map[string]any)["type"] != "integer" {
		t.Fatalf("shadowed name schema = %#v", props["name"])
	}
	for _, absent := range []string{"hidden", "Ignored"} {
		if _, ok := props[absent]; ok {
			t.Fatalf("non-serialized field %q has a schema", absent)
		}
	}

	optional := "present"
	input.OptionalPtr = &optional
	raw, err = json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &instance); err != nil {
		t.Fatal(err)
	}
	if err := jsonschema.Validate(doc, instance); err != nil {
		t.Fatalf("schema rejected non-nil optional pointer: %v", err)
	}
}

func TestRichObjectNestedTypeErrorsIdentifyPath(t *testing.T) {
	doc, err := jsonschema.Of(richDocument{})
	if err != nil {
		t.Fatal(err)
	}
	input := richDocument{
		embeddedFields: embeddedFields{Embedded: "kept"},
		When:           time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		Bytes:          []byte{1},
		Entries:        []*item{{Count: 7}, nil},
		Index:          map[string]item{"west": {Count: 9}},
		Raw:            json.RawMessage(`null`),
	}
	raw, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name, path string
		mutate     func(map[string]any)
	}{
		{"array element", "$.entries[0].count", func(m map[string]any) {
			m["entries"].([]any)[0].(map[string]any)["count"] = "not a number"
		}},
		{"typed map value", "$.index.west.count", func(m map[string]any) {
			m["index"].(map[string]any)["west"].(map[string]any)["count"] = false
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var instance map[string]any
			if err := json.Unmarshal(raw, &instance); err != nil {
				t.Fatal(err)
			}
			tt.mutate(instance)
			err := jsonschema.Validate(doc, instance)
			if err == nil || !strings.Contains(err.Error(), tt.path) {
				t.Fatalf("Validate error = %v, want path %s", err, tt.path)
			}
		})
	}
}

func TestUnionRejectsConflictsAndKeepsSharedContract(t *testing.T) {
	if _, err := jsonschema.Union(); err == nil || !strings.Contains(err.Error(), "union of nothing") {
		t.Fatalf("empty union error = %v", err)
	}
	type textID struct {
		ID string `json:"id"`
	}
	type numericID struct {
		ID int `json:"id"`
	}
	if _, err := jsonschema.Union(textID{}, numericID{}); err == nil || !strings.Contains(err.Error(), "union property id disagrees") {
		t.Fatalf("conflicting property error = %v", err)
	}
	type first struct {
		Shared string `json:"shared"`
		A      int    `json:"a"`
	}
	type second struct {
		Shared string `json:"shared"`
		B      bool   `json:"b"`
	}
	doc, err := jsonschema.Union(first{}, second{})
	if err != nil {
		t.Fatal(err)
	}
	if doc["additionalProperties"] != false || !reflect.DeepEqual(doc["required"], []string{"shared"}) {
		t.Fatalf("union must stay closed with shared required key: %#v", doc)
	}
	if err := jsonschema.Validate(doc, map[string]any{"shared": "x", "a": 3}); err != nil {
		t.Fatalf("valid union member: %v", err)
	}
	err = jsonschema.Validate(doc, map[string]any{"shared": "x", "unexpected": true})
	if err == nil || !strings.Contains(err.Error(), `property "unexpected" is not in the schema`) {
		t.Fatalf("closed union error = %v", err)
	}
}

func TestAddVersionRejectsInvalidInputsAndDeduplicatesRequired(t *testing.T) {
	if err := jsonschema.AddVersion(map[string]any{}, "schema_version", 0); err == nil || !strings.Contains(err.Error(), "version 0 < 1") {
		t.Fatalf("invalid version error = %v", err)
	}
	if err := jsonschema.AddVersion(map[string]any{}, "schema_version", 1); err == nil || !strings.Contains(err.Error(), "schema has no properties") {
		t.Fatalf("missing properties error = %v", err)
	}
	doc, err := jsonschema.Of(struct {
		Name string `json:"name"`
	}{})
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := jsonschema.AddVersion(doc, "schema_version", 2); err != nil {
			t.Fatal(err)
		}
	}
	if got := doc["required"]; !reflect.DeepEqual(got, []string{"name", "schema_version"}) {
		t.Fatalf("required after repeated version = %#v", got)
	}
	if err := jsonschema.Validate(doc, map[string]any{"name": "x", "schema_version": 2}); err != nil {
		t.Fatalf("versioned instance: %v", err)
	}
}

func TestVersionedSchemasRoundTripAndValidate(t *testing.T) {
	type success struct {
		Shared string `json:"shared"`
		Name   string `json:"name"`
	}
	type failure struct {
		Shared string `json:"shared"`
		Error  string `json:"error"`
	}
	versioned, err := jsonschema.Versioned(success{}, "versioned", "schema_version", 2)
	if err != nil {
		t.Fatal(err)
	}
	union, err := jsonschema.UnionVersioned("union", "schema_version", 2, success{}, failure{})
	if err != nil {
		t.Fatal(err)
	}
	oneOf, err := jsonschema.OneOfVersioned("oneof", "schema_version", 2, success{}, failure{})
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name string
		raw  []byte
	}{
		{"versioned", versioned},
		{"union", union},
		{"oneOf", oneOf},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var doc map[string]any
			if err := json.Unmarshal(tt.raw, &doc); err != nil {
				t.Fatalf("decode schema: %v", err)
			}
			encoded, err := json.Marshal(doc)
			if err != nil {
				t.Fatalf("re-encode schema: %v", err)
			}
			var roundTrip map[string]any
			if err := json.Unmarshal(encoded, &roundTrip); err != nil {
				t.Fatalf("decode round-trip schema: %v", err)
			}
			var instance map[string]any
			if err := json.Unmarshal([]byte(`{"schema_version":2,"shared":"ok","name":"alice"}`), &instance); err != nil {
				t.Fatal(err)
			}
			if err := jsonschema.Validate(roundTrip, instance); err != nil {
				t.Fatalf("valid instance rejected: %v", err)
			}
		})
	}
}

func TestOneOfRequiresExactlyOneObjectBranch(t *testing.T) {
	stringBranch := map[string]any{"type": "string"}
	integerBranch := map[string]any{"type": "integer"}
	tests := []struct {
		name, want string
		branches   any
		instance   any
	}{
		{"not an array", "$: oneOf is not an array", "bad", "x"},
		{"non-object branch", "$: oneOf branch is not an object", []any{42}, "x"},
		{"no match", "$: oneOf matched none", []any{stringBranch, integerBranch}, true},
		{"multiple matches", "$: oneOf matched 2 schemas", []any{stringBranch, stringBranch}, "x"},
		{"one match", "", []any{stringBranch, integerBranch}, "x"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := jsonschema.Validate(map[string]any{"oneOf": tt.branches}, tt.instance)
			if tt.want == "" {
				if err != nil {
					t.Fatalf("exactly one branch should match: %v", err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Validate error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestValidateNestedFailuresCarryLocation(t *testing.T) {
	type config struct {
		Status string         `json:"status" jsonschema:"const:\"ok\""`
		Items  []int          `json:"items"`
		Counts map[string]int `json:"counts"`
	}
	doc, err := jsonschema.Of(struct {
		Config config `json:"config"`
	}{})
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		body map[string]any
		want string
	}{
		{"const", map[string]any{"status": "bad", "items": []any{1}, "counts": map[string]any{"x": 2}}, "$.config.status: const ok, got bad"},
		{"missing", map[string]any{"items": []any{1}, "counts": map[string]any{"x": 2}}, `$.config: missing required property "status"`},
		{"unexpected", map[string]any{"status": "ok", "items": []any{1}, "counts": map[string]any{"x": 2}, "extra": true}, `$.config: property "extra" is not in the schema`},
		{"array item", map[string]any{"status": "ok", "items": []any{1, "bad"}, "counts": map[string]any{"x": 2}}, "$.config.items[1]"},
		{"map value", map[string]any{"status": "ok", "items": []any{1}, "counts": map[string]any{"east": "bad"}}, "$.config.counts.east"},
	}
	valid := map[string]any{"config": map[string]any{"status": "ok", "items": []any{1}, "counts": map[string]any{"x": 2}}}
	if err := jsonschema.Validate(doc, valid); err != nil {
		t.Fatalf("valid nested object: %v", err)
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := jsonschema.Validate(doc, map[string]any{"config": tt.body})
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Validate error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestIntegerSchemaAcceptsIntegralNumbersOnly(t *testing.T) {
	doc := map[string]any{"type": "integer"}
	for _, tt := range []struct {
		name string
		in   any
	}{
		{"int", int(3)},
		{"int32", int32(-4)},
		{"int64", int64(5)},
		{"float32", float32(6)},
		{"float64", float64(-7)},
		{"json number", json.Number("8")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if err := jsonschema.Validate(doc, tt.in); err != nil {
				t.Fatalf("integral %T(%v) rejected: %v", tt.in, tt.in, err)
			}
		})
	}
	for _, tt := range []struct {
		name string
		in   any
	}{
		{"fraction float32", float32(1.25)},
		{"fraction float64", float64(-2.5)},
		{"positive infinity", math.Inf(1)},
		{"negative infinity", math.Inf(-1)},
		{"invalid json number", json.Number("not-a-number")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := jsonschema.Validate(doc, tt.in)
			if err == nil || !strings.Contains(err.Error(), "$: type") {
				t.Fatalf("non-integer %T(%v) error = %v", tt.in, tt.in, err)
			}
		})
	}
}
