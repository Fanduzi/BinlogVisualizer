package workflow

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBuildWorkflowSummaryAggregatesCompareAndTrendReports(t *testing.T) {
	outputDir := t.TempDir()

	writeWorkflowSummaryArtifact(t, outputDir, "compare/week2_vs_week1.json", map[string]any{
		"key_findings": []map[string]any{
			{
				"kind":    "pattern_driver",
				"title":   "Top pattern driver",
				"summary": "refunds.create drove most row growth",
				"evidence_refs": []map[string]any{
					{
						"section": "pattern_changes",
						"key":     "orders.refunds|INSERT|small",
						"label":   "refunds.create",
						"anchor":  "section-pattern-changes",
					},
				},
			},
		},
		"recommendations": []map[string]any{
			{
				"kind":                  "check_pattern_driver",
				"priority":              "high",
				"title":                 "Check pattern driver",
				"summary":               "Confirm whether a deploy or batch changed during this window.",
				"related_finding_kinds": []string{"pattern_driver"},
				"evidence_refs": []map[string]any{
					{
						"section": "pattern_changes",
						"key":     "orders.refunds|INSERT|small",
						"label":   "refunds.create",
						"anchor":  "section-pattern-changes",
					},
				},
			},
		},
	})

	writeWorkflowSummaryArtifact(t, outputDir, "trend/weekly_series.json", map[string]any{
		"trend_summary": []map[string]any{
			{
				"kind":    "rising_pattern",
				"title":   "Rising pattern",
				"summary": "payments.update_status kept climbing across snapshots.",
				"evidence_refs": []map[string]any{
					{
						"section": "pattern_trends",
						"key":     "orders.payments|UPDATE|medium",
						"label":   "payments.update_status",
						"anchor":  "pattern-0",
					},
				},
			},
		},
		"recommendations": []map[string]any{
			{
				"kind":                  "check_rising_pattern",
				"priority":              "medium",
				"title":                 "Check rising pattern",
				"summary":               "Review the rollout or batch responsible for the sustained increase.",
				"related_finding_kinds": []string{"rising_pattern"},
				"evidence_refs": []map[string]any{
					{
						"section": "pattern_trends",
						"key":     "orders.payments|UPDATE|medium",
						"label":   "payments.update_status",
						"anchor":  "pattern-0",
					},
				},
			},
		},
	})

	manifest := Manifest{
		Steps: []StepRecord{
			{
				Kind:      "compare",
				Name:      "week2_vs_week1",
				Status:    "success",
				Artifacts: []string{"compare/week2_vs_week1.html", "compare/week2_vs_week1.json"},
			},
			{
				Kind:      "trend",
				Name:      "weekly_series",
				Status:    "success",
				Artifacts: []string{"trend/weekly_series.json"},
			},
		},
	}

	summary := BuildWorkflowSummary(outputDir, manifest)
	if len(summary.Findings) != 2 {
		t.Fatalf("findings len = %d, want 2", len(summary.Findings))
	}
	if len(summary.Recommendations) != 2 {
		t.Fatalf("recommendations len = %d, want 2", len(summary.Recommendations))
	}
	if len(summary.Warnings) != 0 {
		t.Fatalf("warnings len = %d, want 0: %#v", len(summary.Warnings), summary.Warnings)
	}

	compareFinding := summary.Findings[0]
	if compareFinding.Kind != "pattern_driver" {
		t.Fatalf("first finding kind = %q, want pattern_driver", compareFinding.Kind)
	}
	if compareFinding.SourceReportPath != "compare/week2_vs_week1.html" {
		t.Fatalf("compare source_report_path = %q, want compare/week2_vs_week1.html", compareFinding.SourceReportPath)
	}
	if compareFinding.SourceReportLabel != "week2_vs_week1" {
		t.Fatalf("compare source_report_label = %q, want week2_vs_week1", compareFinding.SourceReportLabel)
	}
	if len(compareFinding.EvidenceRefs) != 1 || compareFinding.EvidenceRefs[0].Anchor != "section-pattern-changes" {
		t.Fatalf("compare evidence refs = %#v, want section-pattern-changes", compareFinding.EvidenceRefs)
	}

	trendFinding := summary.Findings[1]
	if trendFinding.Kind != "rising_pattern" {
		t.Fatalf("second finding kind = %q, want rising_pattern", trendFinding.Kind)
	}
	if trendFinding.SourceReportPath != "trend/weekly_series.json" {
		t.Fatalf("trend source_report_path = %q, want trend/weekly_series.json", trendFinding.SourceReportPath)
	}

	compareRecommendation := summary.Recommendations[0]
	if compareRecommendation.Kind != "check_pattern_driver" {
		t.Fatalf("first recommendation kind = %q, want check_pattern_driver", compareRecommendation.Kind)
	}
	if compareRecommendation.SourceReportPath != "compare/week2_vs_week1.html" {
		t.Fatalf("compare recommendation source_report_path = %q, want compare/week2_vs_week1.html", compareRecommendation.SourceReportPath)
	}
	if got := strings.Join(compareRecommendation.RelatedFindingKinds, ","); got != "pattern_driver" {
		t.Fatalf("related finding kinds = %q, want pattern_driver", got)
	}

	trendRecommendation := summary.Recommendations[1]
	if trendRecommendation.Kind != "check_rising_pattern" {
		t.Fatalf("second recommendation kind = %q, want check_rising_pattern", trendRecommendation.Kind)
	}
	if trendRecommendation.SourceReportPath != "trend/weekly_series.json" {
		t.Fatalf("trend recommendation source_report_path = %q, want trend/weekly_series.json", trendRecommendation.SourceReportPath)
	}
}

func TestBuildWorkflowSummaryDeduplicatesCapsAndIgnoresFailedSteps(t *testing.T) {
	outputDir := t.TempDir()

	writeWorkflowSummaryArtifact(t, outputDir, "compare/compare_a.json", map[string]any{
		"key_findings": []map[string]any{
			{"kind": "compare_finding_1", "title": "Compare finding 1", "summary": "Summary 1"},
			{"kind": "compare_finding_2", "title": "Compare finding 2", "summary": "Summary 2"},
			{"kind": "compare_finding_3", "title": "Compare finding 3", "summary": "Summary 3"},
		},
		"recommendations": []map[string]any{
			{"kind": "duplicate_follow_up", "priority": "high", "title": "Duplicate follow-up", "summary": "Review the same issue.", "related_finding_kinds": []string{"compare_finding_1"}},
			{"kind": "compare_rec_2", "priority": "medium", "title": "Compare rec 2", "summary": "Compare follow-up 2", "related_finding_kinds": []string{"compare_finding_2"}},
			{"kind": "compare_rec_3", "priority": "low", "title": "Compare rec 3", "summary": "Compare follow-up 3", "related_finding_kinds": []string{"compare_finding_3"}},
		},
	})

	writeWorkflowSummaryArtifact(t, outputDir, "trend/trend_b.json", map[string]any{
		"trend_summary": []map[string]any{
			{"kind": "trend_finding_1", "title": "Trend finding 1", "summary": "Trend summary 1"},
			{"kind": "trend_finding_2", "title": "Trend finding 2", "summary": "Trend summary 2"},
			{"kind": "trend_finding_3", "title": "Trend finding 3", "summary": "Trend summary 3"},
		},
		"recommendations": []map[string]any{
			{"kind": "duplicate_follow_up", "priority": "high", "title": "Duplicate follow-up", "summary": "Review the same issue.", "related_finding_kinds": []string{"trend_finding_1"}},
			{"kind": "trend_rec_2", "priority": "medium", "title": "Trend rec 2", "summary": "Trend follow-up 2", "related_finding_kinds": []string{"trend_finding_2"}},
			{"kind": "trend_rec_3", "priority": "low", "title": "Trend rec 3", "summary": "Trend follow-up 3", "related_finding_kinds": []string{"trend_finding_3"}},
		},
	})

	writeWorkflowSummaryArtifact(t, outputDir, "compare/failed_compare.json", map[string]any{
		"key_findings": []map[string]any{{"kind": "failed_finding", "title": "Failed finding", "summary": "Should not appear"}},
		"recommendations": []map[string]any{{"kind": "failed_rec", "priority": "high", "title": "Failed rec", "summary": "Should not appear", "related_finding_kinds": []string{"failed_finding"}}},
	})

	manifest := Manifest{
		Steps: []StepRecord{
			{Kind: "compare", Name: "compare_a", Status: "success", Artifacts: []string{"compare/compare_a.json"}},
			{Kind: "trend", Name: "trend_b", Status: "success", Artifacts: []string{"trend/trend_b.json"}},
			{Kind: "compare", Name: "failed_compare", Status: "failed", Artifacts: []string{"compare/failed_compare.json"}},
		},
	}

	summary := BuildWorkflowSummary(outputDir, manifest)
	if len(summary.Findings) != 5 {
		t.Fatalf("findings len = %d, want 5", len(summary.Findings))
	}
	if len(summary.Recommendations) != 5 {
		t.Fatalf("recommendations len = %d, want 5", len(summary.Recommendations))
	}

	duplicateCount := 0
	for _, rec := range summary.Recommendations {
		if rec.Title == "Duplicate follow-up" {
			duplicateCount++
		}
		if rec.SourceStepName == "failed_compare" {
			t.Fatalf("failed step leaked into summary: %#v", rec)
		}
	}
	if duplicateCount != 1 {
		t.Fatalf("duplicate recommendation count = %d, want 1", duplicateCount)
	}
	for _, finding := range summary.Findings {
		if finding.SourceStepName == "failed_compare" {
			t.Fatalf("failed step leaked into findings: %#v", finding)
		}
	}
}

func TestBuildWorkflowSummaryRecordsWarningsForMissingUnreadableAndMalformedJSON(t *testing.T) {
	outputDir := t.TempDir()

	writeWorkflowSummaryArtifactRaw(t, outputDir, "trend/trend_bad.json", []byte("not-json"))

	manifest := Manifest{
		Steps: []StepRecord{
			{Kind: "compare", Name: "compare_missing_json", Status: "success", Artifacts: []string{"compare/compare_missing_json.html"}},
			{Kind: "compare", Name: "compare_unreadable_json", Status: "success", Artifacts: []string{"compare/compare_unreadable_json.json"}},
			{Kind: "trend", Name: "trend_bad", Status: "success", Artifacts: []string{"trend/trend_bad.json"}},
		},
	}

	summary := BuildWorkflowSummary(outputDir, manifest)
	if summary.Findings == nil {
		t.Fatal("findings should be an empty array, not nil")
	}
	if summary.Recommendations == nil {
		t.Fatal("recommendations should be an empty array, not nil")
	}
	if summary.Warnings == nil {
		t.Fatal("warnings should be an empty array, not nil")
	}
	if len(summary.Findings) != 0 {
		t.Fatalf("findings len = %d, want 0", len(summary.Findings))
	}
	if len(summary.Recommendations) != 0 {
		t.Fatalf("recommendations len = %d, want 0", len(summary.Recommendations))
	}
	if len(summary.Warnings) != 3 {
		t.Fatalf("warnings len = %d, want 3: %#v", len(summary.Warnings), summary.Warnings)
	}
	for _, want := range []string{
		`compare step "compare_missing_json": missing JSON artifact`,
		`compare step "compare_unreadable_json": cannot read JSON artifact "compare/compare_unreadable_json.json"`,
		`trend step "trend_bad": invalid JSON artifact "trend/trend_bad.json"`,
	} {
		if !containsString(summary.Warnings, want) {
			t.Fatalf("expected warning %q in %#v", want, summary.Warnings)
		}
	}
}

func TestBuildWorkflowSummaryWarnsOnShapeMismatch(t *testing.T) {
	outputDir := t.TempDir()

	writeWorkflowSummaryArtifact(t, outputDir, "compare/compare_empty.json", map[string]any{})
	writeWorkflowSummaryArtifact(t, outputDir, "trend/trend_empty.json", map[string]any{})

	manifest := Manifest{
		Steps: []StepRecord{
			{Kind: "compare", Name: "compare_empty", Status: "success", Artifacts: []string{"compare/compare_empty.json"}},
			{Kind: "trend", Name: "trend_empty", Status: "success", Artifacts: []string{"trend/trend_empty.json"}},
		},
	}

	summary := BuildWorkflowSummary(outputDir, manifest)
	if len(summary.Findings) != 0 {
		t.Fatalf("findings len = %d, want 0", len(summary.Findings))
	}
	if len(summary.Recommendations) != 0 {
		t.Fatalf("recommendations len = %d, want 0", len(summary.Recommendations))
	}
	for _, want := range []string{
		`compare step "compare_empty": JSON payload missing expected array "key_findings"`,
		`compare step "compare_empty": JSON payload missing expected array "recommendations"`,
		`trend step "trend_empty": JSON payload missing expected array "trend_summary"`,
		`trend step "trend_empty": JSON payload missing expected array "recommendations"`,
	} {
		if !containsString(summary.Warnings, want) {
			t.Fatalf("expected warning %q in %#v", want, summary.Warnings)
		}
	}
}

func TestBuildWorkflowSummaryWarnsOnlyForMissingExpectedArray(t *testing.T) {
	outputDir := t.TempDir()

	writeWorkflowSummaryArtifact(t, outputDir, "compare/compare_missing_recommendations.json", map[string]any{
		"key_findings": []map[string]any{},
	})
	writeWorkflowSummaryArtifact(t, outputDir, "trend/trend_missing_summary.json", map[string]any{
		"recommendations": []map[string]any{},
	})

	manifest := Manifest{
		Steps: []StepRecord{
			{Kind: "compare", Name: "compare_missing_recommendations", Status: "success", Artifacts: []string{"compare/compare_missing_recommendations.json"}},
			{Kind: "trend", Name: "trend_missing_summary", Status: "success", Artifacts: []string{"trend/trend_missing_summary.json"}},
		},
	}

	summary := BuildWorkflowSummary(outputDir, manifest)
	if len(summary.Warnings) != 2 {
		t.Fatalf("warnings len = %d, want 2: %#v", len(summary.Warnings), summary.Warnings)
	}
	for _, want := range []string{
		`compare step "compare_missing_recommendations": JSON payload missing expected array "recommendations"`,
		`trend step "trend_missing_summary": JSON payload missing expected array "trend_summary"`,
	} {
		if !containsString(summary.Warnings, want) {
			t.Fatalf("expected warning %q in %#v", want, summary.Warnings)
		}
	}
	for _, notWant := range []string{
		`compare step "compare_missing_recommendations": JSON payload missing expected array "key_findings"`,
		`trend step "trend_missing_summary": JSON payload missing expected array "recommendations"`,
	} {
		if containsString(summary.Warnings, notWant) {
			t.Fatalf("unexpected warning %q in %#v", notWant, summary.Warnings)
		}
	}
}

func TestBuildWorkflowSummaryDoesNotWarnOnPresentEmptyArrays(t *testing.T) {
	outputDir := t.TempDir()

	writeWorkflowSummaryArtifact(t, outputDir, "compare/compare_empty_arrays.json", map[string]any{
		"key_findings":     []map[string]any{},
		"recommendations": []map[string]any{},
	})
	writeWorkflowSummaryArtifact(t, outputDir, "trend/trend_empty_arrays.json", map[string]any{
		"trend_summary":   []map[string]any{},
		"recommendations": []map[string]any{},
	})

	manifest := Manifest{
		Steps: []StepRecord{
			{Kind: "compare", Name: "compare_empty_arrays", Status: "success", Artifacts: []string{"compare/compare_empty_arrays.json"}},
			{Kind: "trend", Name: "trend_empty_arrays", Status: "success", Artifacts: []string{"trend/trend_empty_arrays.json"}},
		},
	}

	summary := BuildWorkflowSummary(outputDir, manifest)
	if len(summary.Findings) != 0 {
		t.Fatalf("findings len = %d, want 0", len(summary.Findings))
	}
	if len(summary.Recommendations) != 0 {
		t.Fatalf("recommendations len = %d, want 0", len(summary.Recommendations))
	}
	if len(summary.Warnings) != 0 {
		t.Fatalf("warnings len = %d, want 0: %#v", len(summary.Warnings), summary.Warnings)
	}
}

func writeWorkflowSummaryArtifact(t *testing.T, outputDir, relPath string, payload any) {
	t.Helper()
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload %s: %v", relPath, err)
	}
	writeWorkflowSummaryArtifactRaw(t, outputDir, relPath, data)
}

func writeWorkflowSummaryArtifactRaw(t *testing.T, outputDir, relPath string, data []byte) {
	t.Helper()
	fullPath := filepath.Join(outputDir, filepath.FromSlash(relPath))
	if err := os.MkdirAll(filepath.Dir(fullPath), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", relPath, err)
	}
	if err := os.WriteFile(fullPath, data, 0o644); err != nil {
		t.Fatalf("write %s: %v", relPath, err)
	}
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
