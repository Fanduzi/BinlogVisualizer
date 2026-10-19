package jsonout

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestMarshalPutsSchemaVersionFirst(t *testing.T) {
	type payload struct {
		B string `json:"b"`
		A int    `json:"a"`
	}
	got, err := Marshal(1, payload{B: "x", A: 2})
	if err != nil {
		t.Fatal(err)
	}
	want := "{\n  \"schema_version\": 1,\n  \"b\": \"x\",\n  \"a\": 2\n}"
	if string(got) != want {
		t.Fatalf("got\n%s\nwant\n%s", got, want)
	}
}

func TestMarshalEmptyObjectAndMap(t *testing.T) {
	got, err := Marshal(2, struct{}{})
	if err != nil || string(got) != "{\n  \"schema_version\": 2\n}" {
		t.Fatalf("empty struct: %q %v", got, err)
	}
	got, err = Marshal(1, map[string]any{"snapshots": []string{}})
	if err != nil {
		t.Fatal(err)
	}
	var back map[string]any
	if err := json.Unmarshal(got, &back); err != nil || back["schema_version"] != float64(1) {
		t.Fatalf("map: %s %v", got, err)
	}
	if !strings.HasPrefix(string(got), "{\n  \"schema_version\": 1,") {
		t.Fatalf("schema_version not first: %s", got)
	}
}

func TestMarshalRejectsNonObjectsDuplicatesAndBadVersions(t *testing.T) {
	for name, tc := range map[string]struct {
		version int
		payload any
	}{
		"array":     {1, []int{1}},
		"string":    {1, "x"},
		"nil":       {1, nil},
		"nil map":   {1, map[string]any(nil)},
		"duplicate": {1, map[string]any{"schema_version": 9}},
		"version 0": {0, map[string]any{}},
	} {
		if out, err := Marshal(tc.version, tc.payload); err == nil {
			t.Errorf("%s: want error, got %s", name, out)
		}
	}
}
