// Package report verifies analyze HTML findings-state rendering.
// input: AnalysisResult values with and without DDL diagnostics.
// output: assertions for healthy and neutral findings states in rendered HTML.
// pos: regression coverage for analyze HTML first-screen status.
// note: if this file changes, keep internal/report/README.md synchronized.
package report

import (
	"strings"
	"testing"
	"time"

	"binlogviz/internal/model"
)

func TestRenderHTMLShowsNeutralDDLStateInsteadOfHealthyState(t *testing.T) {
	result := model.AnalysisResult{
		Diagnostics: model.Diagnostics{
			DDLEvents: []model.DDLEvent{{
				Timestamp: time.Date(2026, time.April, 15, 10, 5, 0, 0, time.UTC),
				Operation: "CREATE TABLE",
				Schema:    "testdb",
				Table:     "users",
				Statement: "CREATE TABLE testdb.users (id BIGINT)",
			}},
		},
	}

	out, err := RenderHTML(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if strings.Contains(out, "No issues detected — workload looks healthy") {
		t.Fatal("expected DDL-only report not to claim the workload is healthy")
	}
	findingsStart := strings.Index(out, `id="section-findings"`)
	if findingsStart < 0 {
		t.Fatal("expected findings section")
	}
	if !strings.Contains(out[:findingsStart], "DDL Events") {
		t.Fatal("expected executive summary to mention the DDL count")
	}
	if !strings.Contains(out[findingsStart:], "DDL activity detected") {
		t.Fatal("expected findings section to acknowledge DDL activity")
	}
	if !strings.Contains(out, "DDL Timeline") {
		t.Fatal("expected DDL timeline to remain available")
	}
}

func TestRenderHTMLKeepsHealthyStateWithoutDDL(t *testing.T) {
	out, err := RenderHTML(model.AnalysisResult{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.Contains(out, "No issues detected — workload looks healthy") {
		t.Fatal("expected clean report to keep the healthy findings state")
	}
	if strings.Contains(out, "DDL activity detected") {
		t.Fatal("expected clean report not to show the DDL activity notice")
	}
}
