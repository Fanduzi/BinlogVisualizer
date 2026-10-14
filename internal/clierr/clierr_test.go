package clierr

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"
)

func TestKindsHaveDistinctCodes(t *testing.T) {
	seen := map[int]Kind{}
	for kind, code := range codes {
		if other, ok := seen[code]; ok {
			t.Fatalf("%s and %s share %d", kind, other, code)
		}
		seen[code] = kind
		if KindForCode(code) != kind {
			t.Fatalf("round trip %s", kind)
		}
	}
}

func TestWrapKeepsInnermostKind(t *testing.T) {
	inner := Wrap(KindInput, errors.New("missing"))
	outer := Wrap(KindCorrupt, fmt.Errorf("ctx: %w", inner))
	if kind, _ := Of(outer); kind != KindInput {
		t.Fatalf("kind %s", kind)
	}
	if kind, _ := Of(errors.New("x")); kind != KindError || Code(kind) != 1 {
		t.Fatal("untyped is error/1")
	}
}

func TestJSONLine(t *testing.T) {
	var v map[string]map[string]any
	if err := json.Unmarshal(JSONLine(KindUsage, 3, "bad", map[string]any{"flag": "--format"}), &v); err != nil {
		t.Fatal(err)
	}
	if v["error"]["kind"] != "usage" || v["error"]["code"].(float64) != 3 {
		t.Fatalf("%v", v)
	}
}
