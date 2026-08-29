// Package compare verifies positive workload comparability and narrative gating.
// input: complete report-v3 inputs with explicit workload IDs, producer provenance, canonical scopes, and numeric workload changes.
// output: regression coverage for structured verdicts, stable reasons, preserved raw deltas, and suppressed causal narrative surfaces.
// pos: public compare-builder safety contract test suite between report loading and all renderers.
// note: if this file changes, update this header and internal/compare/README.md.
package compare

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestBuildCompareResultAllowsNarrativesOnlyForPositivelyComparableReports(t *testing.T) {
	current := comparableInputReport("payments-prod", "mysql", 7, "8.4.0", 1200)
	baseline := comparableInputReport("payments-prod", "mysql", 19, "8.0.42", 200)

	result := BuildCompareResult(current, baseline)

	if result.Comparability.Verdict != VerdictComparable || len(result.Comparability.ReasonCodes) != 0 {
		t.Fatalf("comparability=%+v, want comparable without reasons", result.Comparability)
	}
	if len(result.KeyFindings) == 0 || result.KeyFindings[0].Kind == "comparability_guard" {
		t.Fatalf("ordinary findings were not emitted: %+v", result.KeyFindings)
	}
	if len(result.Recommendations) == 0 || len(result.PatternDrilldowns) == 0 {
		t.Fatalf("expected comparable narratives, recommendations=%+v drilldowns=%+v", result.Recommendations, result.PatternDrilldowns)
	}
	if len(result.Comparability.Evidence) != 2 || result.Comparability.Evidence[0].ServerIDs[0] != 7 || result.Comparability.Evidence[1].ServerIDs[0] != 19 {
		t.Fatalf("server IDs must remain visible evidence without gating sameness: %+v", result.Comparability.Evidence)
	}
}

func TestBuildCompareResultGatesDifferentWorkloadsButKeepsRawDeltas(t *testing.T) {
	current := comparableInputReport("payments-prod", "mysql", 7, "8.4.0", 1200)
	baseline := comparableInputReport("payments-stage", "mysql", 19, "8.0.42", 200)

	result := BuildCompareResult(current, baseline)

	if result.Comparability.Verdict != VerdictNotComparable {
		t.Fatalf("verdict=%q, want %q", result.Comparability.Verdict, VerdictNotComparable)
	}
	if !containsReason(result.Comparability.ReasonCodes, ReasonWorkloadIdentityMismatch) {
		t.Fatalf("reasons=%v, want %q", result.Comparability.ReasonCodes, ReasonWorkloadIdentityMismatch)
	}
	if result.Summary.TotalRowsDelta != 1000 || len(result.TableChanges) == 0 || result.TableChanges[0].DeltaRows != 1000 {
		t.Fatalf("raw deltas were not preserved: %+v %+v", result.Summary, result.TableChanges)
	}
	if len(result.KeyFindings) != 1 || result.KeyFindings[0].Kind != "comparability_guard" {
		t.Fatalf("guard findings=%+v, want exactly one comparability guard", result.KeyFindings)
	}
	if len(result.Recommendations) != 0 || len(result.PatternDrilldowns) != 0 {
		t.Fatalf("ordinary narratives escaped gate: recommendations=%+v drilldowns=%+v", result.Recommendations, result.PatternDrilldowns)
	}
}

func TestAssessComparabilityClassifiesSafetyEvidence(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(current, baseline *InputReport)
		verdict ComparabilityVerdict
		reason  string
	}{
		{
			name: "missing workload identity",
			mutate: func(current, _ *InputReport) {
				current.WorkloadID = ""
			},
			verdict: VerdictUnknown,
			reason:  ReasonMissingWorkloadIdentity,
		},
		{
			name: "known producer flavor conflict",
			mutate: func(current, _ *InputReport) {
				current.Provenance.ServerFlavors = []string{"mariadb"}
			},
			verdict: VerdictNotComparable,
			reason:  ReasonProducerFlavorConflict,
		},
		{
			name: "mixed producers",
			mutate: func(current, _ *InputReport) {
				current.Provenance.MixedProducers = true
			},
			verdict: VerdictNotComparable,
			reason:  ReasonMixedProducers,
		},
		{
			name: "incompatible canonical scope",
			mutate: func(current, _ *InputReport) {
				current.Scope.IncludeSchemas = []string{"billing"}
			},
			verdict: VerdictNotComparable,
			reason:  ReasonIncompatibleScope,
		},
		{
			name: "legacy report",
			mutate: func(current, _ *InputReport) {
				legacyVersion := 2
				current.ReportVersion = &legacyVersion
				current.WorkloadID = ""
				current.Scope = nil
				current.Provenance = nil
				current.Summary.PartialTransactions = nil
				current.Summary.UnknownTransactions = nil
			},
			verdict: VerdictUnknown,
			reason:  ReasonLegacyReportMetadata,
		},
		{
			name: "partial transaction evidence",
			mutate: func(current, _ *InputReport) {
				partial := 1
				current.Summary.PartialTransactions = &partial
			},
			verdict: VerdictUnknown,
			reason:  ReasonPartialOrUnknownTransactions,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			current := comparableInputReport("payments-prod", "mysql", 7, "8.4.0", 1200)
			baseline := comparableInputReport("payments-prod", "mysql", 19, "8.0.42", 200)
			tt.mutate(&current, &baseline)

			got := AssessComparability([]ComparabilityInput{{Role: "current", Report: current}, {Role: "baseline", Report: baseline}})
			if got.Verdict != tt.verdict || !containsReason(got.ReasonCodes, tt.reason) {
				t.Fatalf("comparability=%+v, want verdict=%q reason=%q", got, tt.verdict, tt.reason)
			}
		})
	}
}

func TestGuardedCompareRenderersLeadWithEvidenceAndSuppressOrdinaryNarratives(t *testing.T) {
	result := BuildCompareResult(
		comparableInputReport("payments-prod", "mariadb", 7, "11.8.3-MariaDB", 1200),
		comparableInputReport("payments-stage", "mysql", 19, "8.0.42", 200),
	)

	jsonOutput, err := RenderJSON(result)
	if err != nil {
		t.Fatalf("render json: %v", err)
	}
	var payload CompareResult
	if err := json.Unmarshal([]byte(jsonOutput), &payload); err != nil {
		t.Fatalf("decode json: %v", err)
	}
	if payload.Comparability.Verdict != VerdictNotComparable || len(payload.KeyFindings) != 1 || payload.KeyFindings[0].Kind != "comparability_guard" {
		t.Fatalf("JSON guard contract missing: %+v", payload)
	}
	if len(payload.Recommendations) != 0 || len(payload.PatternDrilldowns) != 0 || payload.Summary.TotalRowsDelta != 1000 {
		t.Fatalf("JSON narrative suppression/raw deltas wrong: %+v", payload)
	}

	for name, render := range map[string]func(CompareResult) (string, error){
		"text": RenderText,
		"html": RenderHTML,
	} {
		t.Run(name, func(t *testing.T) {
			output, err := render(result)
			if err != nil {
				t.Fatalf("render: %v", err)
			}
			guardAt := strings.Index(output, "Causal findings suppressed")
			deltaAt := strings.Index(output, "Top Table Changes")
			if guardAt < 0 || deltaAt < 0 || guardAt > deltaAt {
				t.Fatalf("guard must be prominent before raw deltas: %s", output)
			}
			for _, forbidden := range []string{"dominant disappeared", "new write pattern", "Check table hotspot"} {
				if strings.Contains(output, forbidden) {
					t.Fatalf("ordinary narrative %q escaped gate: %s", forbidden, output)
				}
			}
			if name == "html" && !strings.Contains(output, "window.comparePatternDrilldowns = [];") {
				t.Fatalf("HTML drilldown data was not suppressed: %s", output)
			}
			for _, evidence := range []string{"payments-prod", "payments-stage", "mariadb", "mysql", "shop", "partial_transactions=0", "unknown_transactions=0"} {
				if !strings.Contains(strings.ToLower(output), evidence) {
					t.Fatalf("visible evidence %q missing: %s", evidence, output)
				}
			}
		})
	}
}

func TestGuardedCompareRenderersDoNotRequirePrebuiltFinding(t *testing.T) {
	result := CompareResult{Comparability: Comparability{
		Verdict:     VerdictUnknown,
		ReasonCodes: []string{ReasonMissingWorkloadIdentity},
	}}
	for name, render := range map[string]func(CompareResult) (string, error){"text": RenderText, "html": RenderHTML} {
		t.Run(name, func(t *testing.T) {
			output, err := render(result)
			if err != nil {
				t.Fatalf("render guarded result: %v", err)
			}
			if !strings.Contains(output, "Causal findings suppressed") {
				t.Fatalf("guard summary missing: %s", output)
			}
		})
	}
}

func comparableInputReport(workloadID, flavor string, serverID uint32, version string, rows int) InputReport {
	reportVersion := 3
	complete := 0
	return InputReport{
		ReportVersion: &reportVersion,
		WorkloadID:    workloadID,
		Scope: &InputSnapshotFilters{
			IncludeSchemas: []string{"shop"},
		},
		Provenance: &InputProvenance{
			ServerIDs:      []uint32{serverID},
			ServerVersions: []string{version},
			ServerFlavors:  []string{flavor},
		},
		Summary: InputSummary{
			TotalTransactions:   10,
			PartialTransactions: &complete,
			UnknownTransactions: &complete,
			TotalRows:           rows,
		},
		Tables: []InputTable{{Schema: "shop", Table: "orders", TotalRows: rows, InsertRows: rows, TxnCount: 10}},
		Patterns: []InputPattern{{
			PatternKey:  "shop.orders|INSERT|small",
			Label:       "orders.create",
			TotalRows:   rows,
			TxnCount:    10,
			ShareOfRows: 1,
		}},
	}
}

func withComparableMetadata(report InputReport) InputReport {
	metadata := comparableInputReport("test-workload", "mysql", 7, "8.4.0", 0)
	report.ReportVersion = metadata.ReportVersion
	report.WorkloadID = metadata.WorkloadID
	report.Scope = metadata.Scope
	report.Provenance = metadata.Provenance
	report.Summary.PartialTransactions = metadata.Summary.PartialTransactions
	report.Summary.UnknownTransactions = metadata.Summary.UnknownTransactions
	return report
}

func containsReason(reasons []string, want string) bool {
	for _, reason := range reasons {
		if reason == want {
			return true
		}
	}
	return false
}
