// Package trend verifies series-wide workload comparability and narrative gating.
// input: report-v0-v3 point/baseline series with explicit workload IDs, provenance, object/selector scopes, and raw numeric changes.
// output: regression coverage for all-point verdicts, selector-aware and legacy gating, renderer prominence, raw deltas, and suppression of ordinary trend narratives.
// pos: public trend-builder and renderer safety contract suite layered on compare's shared assessor.
// note: if this file changes, update this header and internal/trend/README.md.
package trend

import (
	"encoding/json"
	"strings"
	"testing"

	comparepkg "binlogviz/internal/compare"
)

func TestBuildResultAppliesComparabilityAcrossEveryPointAndBaseline(t *testing.T) {
	tests := []struct {
		name       string
		mutate     func(points []BuildInput, baseline *BuildInput)
		verdict    comparepkg.ComparabilityVerdict
		reasonCode string
	}{
		{
			name: "same workload",
			mutate: func(_ []BuildInput, _ *BuildInput) {
			},
			verdict: comparepkg.VerdictComparable,
		},
		{
			name: "one point missing identity",
			mutate: func(points []BuildInput, _ *BuildInput) {
				points[1].Report.WorkloadID = ""
			},
			verdict:    comparepkg.VerdictUnknown,
			reasonCode: comparepkg.ReasonMissingWorkloadIdentity,
		},
		{
			name: "baseline from different workload",
			mutate: func(_ []BuildInput, baseline *BuildInput) {
				baseline.Report.WorkloadID = "other-workload"
			},
			verdict:    comparepkg.VerdictNotComparable,
			reasonCode: comparepkg.ReasonWorkloadIdentityMismatch,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			points := []BuildInput{
				{Path: "a.json", Report: testInputReport("a", "A", "2026-03-19T10:00:00Z", 100, 10, 20, 100, 0, 0, 0)},
				{Path: "b.json", Report: testInputReport("b", "B", "2026-03-20T10:00:00Z", 1200, 20, 40, 1200, 0, 0, 0)},
			}
			baseline := BuildInput{Path: "baseline.json", Report: testInputReport("baseline", "Baseline", "2026-03-18T10:00:00Z", 50, 5, 10, 50, 0, 0, 0)}
			tt.mutate(points, &baseline)

			result, err := BuildResult(BuildOptions{Points: points, Baseline: &baseline})
			if err != nil {
				t.Fatalf("BuildResult: %v", err)
			}
			if result.Comparability.Verdict != tt.verdict {
				t.Fatalf("comparability=%+v, want %q", result.Comparability, tt.verdict)
			}
			if tt.reasonCode != "" && !trendContainsReason(result.Comparability.ReasonCodes, tt.reasonCode) {
				t.Fatalf("reasons=%v, want %q", result.Comparability.ReasonCodes, tt.reasonCode)
			}
			if len(result.Points) != 2 || len(result.TableTrends) == 0 || result.Insights.RowsDelta != 1100 {
				t.Fatalf("raw series was not preserved: %+v", result)
			}
			if tt.verdict == comparepkg.VerdictComparable {
				if len(result.TrendSummary) == 0 || result.TrendSummary[0].Kind == "comparability_guard" {
					t.Fatalf("comparable series lost ordinary findings: %+v", result.TrendSummary)
				}
			} else if len(result.TrendSummary) != 1 || result.TrendSummary[0].Kind != "comparability_guard" || len(result.Recommendations) != 0 || len(result.PatternDrilldowns) != 0 {
				t.Fatalf("guarded narratives wrong: findings=%+v recommendations=%+v drilldowns=%+v", result.TrendSummary, result.Recommendations, result.PatternDrilldowns)
			}
		})
	}
}

func TestBuildResultGatesIncompatiblePersistedGTIDSelectionAcrossEveryPoint(t *testing.T) {
	const sid = "24bc7850-2c16-11e6-a073-0242ac110002"
	points := []BuildInput{
		{Path: "a.json", Report: testInputReport("a", "A", "2026-03-19T10:00:00Z", 100, 10, 20, 100, 0, 0, 0)},
		{Path: "b.json", Report: testInputReport("b", "B", "2026-03-20T10:00:00Z", 1200, 20, 40, 1200, 0, 0, 0)},
		{Path: "c.json", Report: testInputReport("c", "C", "2026-03-21T10:00:00Z", 2400, 30, 60, 2400, 0, 0, 0)},
	}
	points[0].Report.Selection = &comparepkg.InputSelection{
		IncludeGTIDs:       []string{sid + ":1"},
		ResolvedGTIDFlavor: "mysql",
		MatchedGTIDs:       []string{sid + ":1"},
	}
	points[1].Report.Selection = &comparepkg.InputSelection{
		IncludeGTIDs:       []string{sid + ":2"},
		ResolvedGTIDFlavor: "mysql",
		MatchedGTIDs:       []string{sid + ":2"},
	}
	points[2].Report.Selection = &comparepkg.InputSelection{
		IncludeGTIDs:       []string{sid + ":1"},
		ResolvedGTIDFlavor: "mysql",
		MatchedGTIDs:       []string{sid + ":1"},
	}

	result, err := BuildResult(BuildOptions{Points: points})
	if err != nil {
		t.Fatalf("BuildResult: %v", err)
	}
	if result.Comparability.Verdict != comparepkg.VerdictNotComparable || !trendContainsReason(result.Comparability.ReasonCodes, comparepkg.ReasonIncompatibleScope) {
		t.Fatalf("comparability=%+v, want not_comparable with %q", result.Comparability, comparepkg.ReasonIncompatibleScope)
	}
	if len(result.Points) != 3 || len(result.TableTrends) == 0 || result.Insights.RowsDelta != 2300 {
		t.Fatalf("raw series was not preserved: %+v", result)
	}
	if len(result.TrendSummary) != 1 || result.TrendSummary[0].Kind != "comparability_guard" || len(result.Recommendations) != 0 || len(result.PatternDrilldowns) != 0 {
		t.Fatalf("ordinary narratives escaped selection gate: findings=%+v recommendations=%+v drilldowns=%+v", result.TrendSummary, result.Recommendations, result.PatternDrilldowns)
	}
}

func TestBuildResultKeepsKnownV3SelectorConflictWithLegacyPoint(t *testing.T) {
	legacy := testInputReport("legacy", "Legacy", "2026-03-18T10:00:00Z", 50, 5, 10, 50, 0, 0, 0)
	legacyVersion := 2
	legacy.ReportVersion = &legacyVersion
	v3PositionA := testInputReport("a", "A", "2026-03-19T10:00:00Z", 100, 10, 20, 100, 0, 0, 0)
	v3PositionA.Selection = &comparepkg.InputSelection{RequestedStartPosition: trendInt64Ptr(100), RequestedStopPosition: trendInt64Ptr(200)}
	v3PositionB := testInputReport("b", "B", "2026-03-20T10:00:00Z", 1200, 20, 40, 1200, 0, 0, 0)
	v3PositionB.Selection = &comparepkg.InputSelection{RequestedStartPosition: trendInt64Ptr(300), RequestedStopPosition: trendInt64Ptr(400)}

	result, err := BuildResult(BuildOptions{Points: []BuildInput{
		{Path: "legacy.json", Report: legacy},
		{Path: "a.json", Report: v3PositionA},
		{Path: "b.json", Report: v3PositionB},
	}})
	if err != nil {
		t.Fatalf("BuildResult: %v", err)
	}
	if result.Comparability.Verdict != comparepkg.VerdictNotComparable || !trendContainsReason(result.Comparability.ReasonCodes, comparepkg.ReasonIncompatibleScope) {
		t.Fatalf("comparability=%+v, want not_comparable with known v3 scope conflict", result.Comparability)
	}
}

func TestGuardedTrendRenderersLeadWithEvidenceAndSuppressOrdinaryNarratives(t *testing.T) {
	first := testInputReport("a", "A", "2026-03-19T10:00:00Z", 100, 10, 20, 100, 0, 0, 0)
	second := testInputReport("b", "B", "2026-03-20T10:00:00Z", 1200, 20, 40, 1200, 0, 0, 0)
	second.WorkloadID = "other-workload"
	second.Provenance.ServerFlavors = []string{"mariadb"}
	first.Selection = &comparepkg.InputSelection{RequestedStartPosition: trendInt64Ptr(100), RequestedStopPosition: trendInt64Ptr(200)}
	second.Selection = &comparepkg.InputSelection{RequestedStartPosition: trendInt64Ptr(300), RequestedStopPosition: trendInt64Ptr(400)}
	result, err := BuildResult(BuildOptions{Points: []BuildInput{{Path: "a.json", Report: first}, {Path: "b.json", Report: second}}})
	if err != nil {
		t.Fatalf("BuildResult: %v", err)
	}

	jsonOutput, err := RenderJSON(result)
	if err != nil {
		t.Fatalf("render json: %v", err)
	}
	var payload Result
	if err := json.Unmarshal([]byte(jsonOutput), &payload); err != nil {
		t.Fatalf("decode json: %v", err)
	}
	if payload.Comparability.Verdict != comparepkg.VerdictNotComparable || len(payload.TrendSummary) != 1 || payload.TrendSummary[0].Kind != "comparability_guard" || payload.Insights.RowsDelta != 1100 {
		t.Fatalf("JSON guard/raw contract missing: %+v", payload)
	}
	if len(payload.Recommendations) != 0 || len(payload.PatternDrilldowns) != 0 {
		t.Fatalf("JSON ordinary narratives escaped gate: %+v", payload)
	}

	for name, render := range map[string]func(Result) (string, error){"text": RenderText, "html": RenderHTML} {
		t.Run(name, func(t *testing.T) {
			output, err := render(result)
			if err != nil {
				t.Fatalf("render: %v", err)
			}
			guardAt := strings.Index(output, "Causal findings suppressed")
			deltaAt := strings.Index(output, "Top Table Movement")
			if guardAt < 0 || deltaAt < 0 || guardAt > deltaAt {
				t.Fatalf("guard must be prominent before raw series: %s", output)
			}
			for _, forbidden := range []string{"Strongest rising pattern", "Top table trend", "Track rising pattern"} {
				if strings.Contains(output, forbidden) {
					t.Fatalf("ordinary narrative %q escaped gate: %s", forbidden, output)
				}
			}
			for _, evidence := range []string{"test-workload", "other-workload", "mysql", "mariadb", "shop", "requested_start_position=100", "requested_start_position=300"} {
				if !strings.Contains(strings.ToLower(output), evidence) {
					t.Fatalf("visible evidence %q missing: %s", evidence, output)
				}
			}
		})
	}
}

func trendInt64Ptr(value int64) *int64 {
	return &value
}

func TestGuardedTrendRenderersDoNotRequirePrebuiltFinding(t *testing.T) {
	result := Result{Comparability: Comparability{
		Verdict:     comparepkg.VerdictUnknown,
		ReasonCodes: []string{comparepkg.ReasonMissingWorkloadIdentity},
	}}
	for name, render := range map[string]func(Result) (string, error){"text": RenderText, "html": RenderHTML} {
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

func trendContainsReason(reasons []string, want string) bool {
	for _, reason := range reasons {
		if reason == want {
			return true
		}
	}
	return false
}
