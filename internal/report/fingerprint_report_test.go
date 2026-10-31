// Package report verifies the statement-fingerprint surface on JSON and text.
// input: AnalysisResult values carrying StatementFingerprint groups.
// output: JSON list/omitted/overflow keys, --sql-context off sample hiding,
// and the opt-in text section's explicit empty states (ADR 0026).
// pos: renderer regression suite for the fingerprint renderers in json.go
// and text.go.
// note: if this file changes, update this header and module README.md.
package report

import (
	"encoding/json"
	"strings"
	"testing"

	"binlogviz/internal/model"
)

func fingerprintResult() model.AnalysisResult {
	return model.AnalysisResult{
		SQLContextAvailable: true,
		Summary:             model.WorkloadSummary{TotalRows: 12},
		StatementFingerprints: []model.StatementFingerprint{
			{Fingerprint: "update t set a=?", Sample: "UPDATE t SET a = 1", Statements: 2, Rows: 9, ShareOfRows: 0.75, Tables: map[string]int{"s.t": 9}},
			{Fingerprint: "delete from t where id=?", Sample: "DELETE", Statements: 1, Rows: 3, ShareOfRows: 0.25, Tables: map[string]int{"s.t": 3}},
		},
	}
}

func TestJSONFingerprintsListOmittedOverflow(t *testing.T) {
	result := fingerprintResult()
	result.FingerprintOverflow = 5
	out, err := RenderJSONWithOptions(result, Options{TopFingerprints: 1, TopFingerprintsSet: true})
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := json.Unmarshal([]byte(out), &doc); err != nil {
		t.Fatal(err)
	}
	fps, ok := doc["statement_fingerprints"].([]any)
	if !ok || len(fps) != 1 {
		t.Fatalf("statement_fingerprints = %v", doc["statement_fingerprints"])
	}
	fp := fps[0].(map[string]any)
	if fp["fingerprint"] != "update t set a=?" || fp["statements"] != float64(2) || fp["rows"] != float64(9) {
		t.Fatalf("fingerprint row = %v", fp)
	}
	if fp["sample"] != "UPDATE t SET a = 1" {
		t.Fatalf("sample = %v", fp["sample"])
	}
	if fp["tables"].(map[string]any)["s.t"] != float64(9) {
		t.Fatalf("tables = %v", fp["tables"])
	}
	if doc["statement_fingerprints_omitted"] != float64(1) {
		t.Fatalf("omitted = %v", doc["statement_fingerprints_omitted"])
	}
	if doc["statement_fingerprints_overflow"] != float64(5) {
		t.Fatalf("overflow = %v", doc["statement_fingerprints_overflow"])
	}
	if doc["statement_fingerprints_listed"] != float64(1) {
		t.Fatalf("listed = %v", doc["statement_fingerprints_listed"])
	}
}

func TestJSONFingerprintsEmptyWhenNoRowsQueryText(t *testing.T) {
	out, err := RenderJSONWithOptions(model.AnalysisResult{Summary: model.WorkloadSummary{TotalRows: 3}}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := json.Unmarshal([]byte(out), &doc); err != nil {
		t.Fatal(err)
	}
	fps, ok := doc["statement_fingerprints"].([]any)
	if !ok || len(fps) != 0 {
		t.Fatalf("expected empty array (not null), got %v", doc["statement_fingerprints"])
	}
	if doc["sql_context"].(map[string]any)["available"] != false {
		t.Fatal("sql_context.available must stay false to explain the empty list")
	}
	if _, present := doc["statement_fingerprints_overflow"]; present {
		t.Fatal("overflow must be omitted when zero")
	}
}

func TestJSONFingerprintSampleHiddenUnderSQLContextOff(t *testing.T) {
	out, err := RenderJSONWithOptions(fingerprintResult(), Options{SQLContextMode: SQLContextOff})
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := json.Unmarshal([]byte(out), &doc); err != nil {
		t.Fatal(err)
	}
	for _, raw := range doc["statement_fingerprints"].([]any) {
		fp := raw.(map[string]any)
		if _, present := fp["sample"]; present {
			t.Fatalf("sample leaked under --sql-context off: %v", fp)
		}
		if fp["fingerprint"] == "" {
			t.Fatal("normalized fingerprint must still render under --sql-context off")
		}
	}
}

func TestTextFingerprintSectionRanksAndSamples(t *testing.T) {
	out, err := RenderTextWithOptions(fingerprintResult(), Options{ShowFingerprints: true})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "Statement Fingerprints") {
		t.Fatalf("section missing:\n%s", out)
	}
	if !strings.Contains(out, "update t set a=?") {
		t.Fatalf("fingerprint text missing:\n%s", out)
	}
	if !strings.Contains(out, "rows=9 stmts=2") {
		t.Fatalf("counts missing:\n%s", out)
	}
	if !strings.Contains(out, "tables=s.t=9") {
		t.Fatalf("table attribution missing:\n%s", out)
	}
	if !strings.Contains(out, "UPDATE t SET a = 1") {
		t.Fatalf("sample missing:\n%s", out)
	}
}

func TestTextFingerprintSectionAbsentIsExplicit(t *testing.T) {
	// No Rows_query text captured: the section must say so, not look like a
	// valid empty result.
	out, err := RenderTextWithOptions(model.AnalysisResult{Summary: model.WorkloadSummary{TotalRows: 3}}, Options{ShowFingerprints: true})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "Statement Fingerprints") {
		t.Fatalf("section missing:\n%s", out)
	}
	if !strings.Contains(out, "no Rows_query statement text captured") {
		t.Fatalf("explicit absent note missing:\n%s", out)
	}
}

func TestTextFingerprintSectionAbsentWithoutFlag(t *testing.T) {
	out, err := RenderTextWithOptions(fingerprintResult(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out, "Statement Fingerprints") {
		t.Fatalf("section must be opt-in:\n%s", out)
	}
}
