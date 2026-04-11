package workflow

import (
	"strings"
	"testing"
)

func TestRenderIndexIncludesWorkflowMetadataAndManifestLink(t *testing.T) {
	html, err := RenderIndex(IndexInput{
		OutputRoot: ".",
		Manifest: Manifest{
			WorkflowName:        "incident-review",
			WorkflowPlanVersion: 1,
			BinlogvizVersion:    "dev",
			PlanPath:            "/tmp/incident.yaml",
			RunStartedAt:        "2026-04-09T10:00:00Z",
			RunFinishedAt:       "2026-04-09T10:01:00Z",
			Status:              "success",
			Steps: []StepRecord{
				{Kind: "compare", Name: "incident_vs_baseline", Status: "success", Artifacts: []string{"compare/incident_vs_baseline.html", "compare/incident_vs_baseline.json"}},
			},
		},
	})
	if err != nil {
		t.Fatalf("render index: %v", err)
	}
	for _, token := range []string{"incident-review", "manifest.json", "./compare/incident_vs_baseline.html"} {
		if !strings.Contains(html, token) {
			t.Fatalf("expected %q in rendered index", token)
		}
	}
}

func TestRenderIndexShowsFailureErrorAndNoBrokenArtifacts(t *testing.T) {
	html, err := RenderIndex(IndexInput{
		OutputRoot: ".",
		Manifest: Manifest{
			WorkflowName: "failing-run",
			Status:       "failed",
			Steps: []StepRecord{
				{Kind: "analyze", Name: "week2", Status: "failed", Error: "save snapshot failed"},
			},
		},
	})
	if err != nil {
		t.Fatalf("render index: %v", err)
	}
	if !strings.Contains(html, "save snapshot failed") {
		t.Fatalf("expected failure message in html")
	}
	if strings.Contains(html, "href=\"\"") {
		t.Fatalf("expected no empty artifact links")
	}
}

func TestRenderIndexPrioritizesHTMLLinks(t *testing.T) {
	html, err := RenderIndex(IndexInput{
		OutputRoot: ".",
		Manifest: Manifest{
			WorkflowName: "test",
			Status:       "success",
			Steps: []StepRecord{
				{Kind: "compare", Name: "c1", Status: "success", Artifacts: []string{"compare/c1.html", "compare/c1.json"}},
			},
		},
	})
	if err != nil {
		t.Fatalf("render index: %v", err)
	}
	// HTML link must appear before JSON link in output
	htmlIdx := strings.Index(html, "./compare/c1.html")
	jsonIdx := strings.Index(html, "./compare/c1.json")
	if htmlIdx == -1 || jsonIdx == -1 {
		t.Fatalf("expected both html and json links")
	}
	if htmlIdx > jsonIdx {
		t.Fatalf("expected HTML link before JSON link")
	}
}

func TestRenderIndexSummaryCounts(t *testing.T) {
	html, err := RenderIndex(IndexInput{
		OutputRoot: ".",
		Manifest: Manifest{
			WorkflowName: "test",
			Status:       "success",
			Steps: []StepRecord{
				{Kind: "analyze", Name: "w1", Status: "success", Artifacts: []string{"analyze/w1.json"}},
				{Kind: "analyze", Name: "w2", Status: "success", Artifacts: []string{"analyze/w2.json"}},
				{Kind: "compare", Name: "c1", Status: "failed", Error: "bad input"},
			},
		},
	})
	if err != nil {
		t.Fatalf("render index: %v", err)
	}
	for _, token := range []string{"3", "2", "1", "2"} {
		if !strings.Contains(html, token) {
			t.Fatalf("expected summary count %q in html", token)
		}
	}
}

func TestRenderIndexUsesOnlyRelativeLinks(t *testing.T) {
	html, err := RenderIndex(IndexInput{
		OutputRoot: ".",
		Manifest: Manifest{
			WorkflowName: "test",
			Status:       "success",
			Steps: []StepRecord{
				{Kind: "analyze", Name: "w1", Status: "success", Artifacts: []string{"analyze/w1.json"}},
			},
		},
	})
	if err != nil {
		t.Fatalf("render index: %v", err)
	}
	if strings.Contains(html, "http://") || strings.Contains(html, "https://") {
		t.Fatalf("expected no absolute URLs in index")
	}
	// Links should start with ./
	if !strings.Contains(html, "./analyze/w1.json") {
		t.Fatalf("expected relative link ./analyze/w1.json")
	}
	if !strings.Contains(html, "./manifest.json") {
		t.Fatalf("expected relative link ./manifest.json")
	}
}

func TestRenderIndexFailedStepShowsError(t *testing.T) {
	html, err := RenderIndex(IndexInput{
		OutputRoot: ".",
		Manifest: Manifest{
			WorkflowName: "test",
			Status:       "failed",
			Steps: []StepRecord{
				{Kind: "analyze", Name: "w1", Status: "success", Artifacts: []string{"analyze/w1.json"}},
				{Kind: "analyze", Name: "w2", Status: "failed", Error: "connection refused"},
			},
		},
	})
	if err != nil {
		t.Fatalf("render index: %v", err)
	}
	if !strings.Contains(html, "connection refused") {
		t.Fatalf("expected error text visible in html")
	}
	if !strings.Contains(html, "w1") {
		t.Fatalf("expected successful step w1 visible")
	}
	if !strings.Contains(html, "w2") {
		t.Fatalf("expected failed step w2 visible")
	}
}

func TestRenderIndexGroupsArtifactsByKind(t *testing.T) {
	html, err := RenderIndex(IndexInput{
		OutputRoot: ".",
		Manifest: Manifest{
			WorkflowName: "test",
			Status:       "success",
			Steps: []StepRecord{
				{Kind: "analyze", Name: "w1", Status: "success", Artifacts: []string{"analyze/w1.json"}},
				{Kind: "compare", Name: "c1", Status: "success", Artifacts: []string{"compare/c1.html", "compare/c1.json"}},
				{Kind: "trend", Name: "t1", Status: "success", Artifacts: []string{"trend/t1.html"}},
			},
		},
	})
	if err != nil {
		t.Fatalf("render index: %v", err)
	}
	for _, token := range []string{"./analyze/w1.json", "./compare/c1.html", "./compare/c1.json", "./trend/t1.html"} {
		if !strings.Contains(html, token) {
			t.Fatalf("expected artifact link %q in html", token)
		}
	}
}

func TestRenderIndexShowsWorkflowLevelError(t *testing.T) {
	html, err := RenderIndex(IndexInput{
		OutputRoot: ".",
		Manifest: Manifest{
			WorkflowName: "broken-workflow",
			Status:       "failed",
			Error:        "discover binlog files: no such directory",
		},
	})
	if err != nil {
		t.Fatalf("render index: %v", err)
	}
	if !strings.Contains(html, `class="step-error"`) {
		t.Fatalf("expected step-error block for workflow-level error")
	}
	if !strings.Contains(html, "discover binlog files: no such directory") {
		t.Fatalf("expected error message in html")
	}
}

func TestRenderIndexNoErrorBlockOnSuccess(t *testing.T) {
	html, err := RenderIndex(IndexInput{
		OutputRoot: ".",
		Manifest: Manifest{
			WorkflowName: "clean-workflow",
			Status:       "success",
		},
	})
	if err != nil {
		t.Fatalf("render index: %v", err)
	}
	if strings.Contains(html, `class="step-error"`) {
		t.Fatalf("did not expect step-error block on success")
	}
}

func TestRenderIndexShowsResumeModeAndAttempt(t *testing.T) {
	html, err := RenderIndex(IndexInput{
		OutputRoot: ".",
		Manifest: Manifest{
			WorkflowName: "resumed-workflow",
			Mode:         "resume",
			Attempt:      2,
			Status:       "success",
			Steps: []StepRecord{
				{Kind: "analyze", Name: "week1", Status: "success", Execution: "reused"},
				{Kind: "analyze", Name: "week2", Status: "success", Execution: "executed"},
			},
		},
	})
	if err != nil {
		t.Fatalf("render index: %v", err)
	}
	if !strings.Contains(html, "resume") {
		t.Fatalf("expected 'resume' mode visible in html")
	}
	if !strings.Contains(html, "attempt 2") {
		t.Fatalf("expected 'attempt 2' visible in html")
	}
}

func TestRenderIndexShowsStepExecutionLabels(t *testing.T) {
	html, err := RenderIndex(IndexInput{
		OutputRoot: ".",
		Manifest: Manifest{
			WorkflowName: "mixed-workflow",
			Mode:         "resume",
			Attempt:      3,
			Status:       "success",
			Steps: []StepRecord{
				{Kind: "analyze", Name: "week1", Status: "success", Execution: "reused"},
				{Kind: "analyze", Name: "week2", Status: "success", Execution: "executed"},
				{Kind: "compare", Name: "c1", Status: "success", Execution: "executed"},
			},
		},
	})
	if err != nil {
		t.Fatalf("render index: %v", err)
	}
	if !strings.Contains(html, "reused") {
		t.Fatalf("expected 'reused' label visible in html")
	}
	if !strings.Contains(html, "executed") {
		t.Fatalf("expected 'executed' label visible in html")
	}
}

func TestRenderIndexRunModeDoesNotShowAttempt(t *testing.T) {
	html, err := RenderIndex(IndexInput{
		OutputRoot: ".",
		Manifest: Manifest{
			WorkflowName: "fresh-run",
			Mode:         "run",
			Attempt:      1,
			Status:       "success",
		},
	})
	if err != nil {
		t.Fatalf("render index: %v", err)
	}
	if strings.Contains(html, "attempt 1") {
		t.Fatalf("did not expect 'attempt 1' on fresh run (attempt only shows for resume)")
	}
}

func TestRenderIndexSummaryRecommendationsBeforeFindings(t *testing.T) {
	html, err := RenderIndex(IndexInput{
		OutputRoot: ".",
		Manifest: Manifest{
			WorkflowName: "summary-order",
			Status:       "success",
			WorkflowSummary: WorkflowSummary{
				Recommendations: []WorkflowRecommendation{{
					Kind:              "check_pattern_driver",
					Priority:          "high",
					Title:             "Check pattern driver",
					Summary:           "Review the source report.",
					SourceStepKind:    "compare",
					SourceStepName:    "week2_vs_week1",
					SourceReportPath:  "compare/week2_vs_week1.html",
					SourceReportLabel: "week2_vs_week1",
				}},
				Findings: []WorkflowFinding{{
					Kind:              "pattern_driver",
					Title:             "Top pattern driver",
					Summary:           "refunds.create drove most row growth",
					SourceStepKind:    "compare",
					SourceStepName:    "week2_vs_week1",
					SourceReportPath:  "compare/week2_vs_week1.html",
					SourceReportLabel: "week2_vs_week1",
				}},
			},
		},
	})
	if err != nil {
		t.Fatalf("render index: %v", err)
	}
	recIdx := strings.Index(html, "Workflow Recommendations")
	findingIdx := strings.Index(html, "Workflow Findings")
	if recIdx == -1 || findingIdx == -1 {
		t.Fatalf("expected both workflow summary sections")
	}
	if recIdx > findingIdx {
		t.Fatalf("expected recommendations before findings")
	}
}

func TestRenderIndexSummarySourceLinks(t *testing.T) {
	html, err := RenderIndex(IndexInput{
		OutputRoot: ".",
		Manifest: Manifest{
			WorkflowName: "summary-links",
			Status:       "success",
			WorkflowSummary: WorkflowSummary{
				Recommendations: []WorkflowRecommendation{{
					Kind:              "check_pattern_driver",
					Priority:          "high",
					Title:             "Check pattern driver",
					Summary:           "Review the source report.",
					SourceStepKind:    "compare",
					SourceStepName:    "week2_vs_week1",
					SourceReportPath:  "compare/week2_vs_week1.html",
					SourceReportLabel: "week2_vs_week1",
				}},
				Findings: []WorkflowFinding{{
					Kind:              "pattern_driver",
					Title:             "Top pattern driver",
					Summary:           "refunds.create drove most row growth",
					SourceStepKind:    "compare",
					SourceStepName:    "week2_vs_week1",
					SourceReportPath:  "compare/week2_vs_week1.html",
					SourceReportLabel: "week2_vs_week1",
				}},
			},
		},
	})
	if err != nil {
		t.Fatalf("render index: %v", err)
	}
	for _, token := range []string{"./compare/week2_vs_week1.html", "Source report", "week2_vs_week1"} {
		if !strings.Contains(html, token) {
			t.Fatalf("expected %q in summary html", token)
		}
	}
}

func TestRenderIndexSummaryWarningsBlock(t *testing.T) {
	html, err := RenderIndex(IndexInput{
		OutputRoot: ".",
		Manifest: Manifest{
			WorkflowName: "summary-warnings",
			Status:       "success",
			WorkflowSummary: WorkflowSummary{
				Findings:        []WorkflowFinding{},
				Recommendations: []WorkflowRecommendation{},
				Warnings:        []string{"compare step \"week2_vs_week1\": missing JSON artifact"},
			},
		},
	})
	if err != nil {
		t.Fatalf("render index: %v", err)
	}
	if !strings.Contains(html, "Workflow Summary Warnings") {
		t.Fatalf("expected warnings block header")
	}
	if !strings.Contains(html, `compare step &#34;week2_vs_week1&#34;: missing JSON artifact`) {
		t.Fatalf("expected warning text in html")
	}
}

func TestRenderIndexSummaryEvidenceLinksOmitJSONAnchors(t *testing.T) {
	html, err := RenderIndex(IndexInput{
		OutputRoot: ".",
		Manifest: Manifest{
			WorkflowName: "summary-json-evidence",
			Status:       "success",
			WorkflowSummary: WorkflowSummary{
				Findings: []WorkflowFinding{{
					Kind:              "rising_pattern",
					Title:             "Rising pattern",
					Summary:           "payments.update_status kept climbing across snapshots.",
					SourceStepKind:    "trend",
					SourceStepName:    "weekly_series",
					SourceReportPath:  "trend/weekly_series.json",
					SourceReportLabel: "weekly_series",
					EvidenceRefs: []WorkflowEvidenceRef{{
						Section: "pattern_trends",
						Key:     "orders.payments|UPDATE|medium",
						Label:   "payments.update_status",
						Anchor:  "pattern-0",
					}},
				}},
			},
		},
	})
	if err != nil {
		t.Fatalf("render index: %v", err)
	}
	if !strings.Contains(html, "./trend/weekly_series.json") {
		t.Fatalf("expected json source link in html")
	}
	if strings.Contains(html, "./trend/weekly_series.json#pattern-0") {
		t.Fatalf("did not expect json evidence link to include anchor")
	}
}

func TestRenderIndexSummaryEvidenceLinksIncludeHTMLAnchors(t *testing.T) {
	html, err := RenderIndex(IndexInput{
		OutputRoot: ".",
		Manifest: Manifest{
			WorkflowName: "summary-html-evidence",
			Status:       "success",
			WorkflowSummary: WorkflowSummary{
				Findings: []WorkflowFinding{{
					Kind:              "pattern_driver",
					Title:             "Top pattern driver",
					Summary:           "refunds.create drove most row growth",
					SourceStepKind:    "compare",
					SourceStepName:    "week2_vs_week1",
					SourceReportPath:  "compare/week2_vs_week1.html",
					SourceReportLabel: "week2_vs_week1",
					EvidenceRefs: []WorkflowEvidenceRef{{
						Section: "pattern_changes",
						Key:     "orders.refunds|INSERT|small",
						Label:   "refunds.create",
						Anchor:  "section-pattern-changes",
					}},
				}},
			},
		},
	})
	if err != nil {
		t.Fatalf("render index: %v", err)
	}
	if !strings.Contains(html, "./compare/week2_vs_week1.html#section-pattern-changes") {
		t.Fatalf("expected html evidence link to include anchor")
	}
}

func TestRenderIndexSummarySectionsRenderOnlyWhenPopulated(t *testing.T) {
	tests := []struct {
		name      string
		summary   WorkflowSummary
		want      []string
		notWanted []string
	}{
		{
			name: "warnings only",
			summary: WorkflowSummary{
				Findings:        []WorkflowFinding{},
				Recommendations: []WorkflowRecommendation{},
				Warnings:        []string{"compare step \"week2_vs_week1\": missing JSON artifact"},
			},
			want:      []string{"Workflow Summary Warnings"},
			notWanted: []string{"Workflow Recommendations", "Workflow Findings"},
		},
		{
			name: "findings only",
			summary: WorkflowSummary{
				Findings: []WorkflowFinding{{
					Kind:    "pattern_driver",
					Title:   "Top pattern driver",
					Summary: "refunds.create drove most row growth",
				}},
				Recommendations: []WorkflowRecommendation{},
				Warnings:        []string{},
			},
			want:      []string{"Workflow Findings"},
			notWanted: []string{"Workflow Recommendations", "Workflow Summary Warnings"},
		},
		{
			name: "recommendations only",
			summary: WorkflowSummary{
				Findings: []WorkflowFinding{},
				Recommendations: []WorkflowRecommendation{{
					Kind:     "check_pattern_driver",
					Priority: "high",
					Title:    "Check pattern driver",
					Summary:  "Review the source report.",
				}},
				Warnings: []string{},
			},
			want:      []string{"Workflow Recommendations"},
			notWanted: []string{"Workflow Findings", "Workflow Summary Warnings"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			html, err := RenderIndex(IndexInput{
				OutputRoot: ".",
				Manifest: Manifest{
					WorkflowName:    "summary-sections",
					Status:          "success",
					WorkflowSummary: tc.summary,
				},
			})
			if err != nil {
				t.Fatalf("render index: %v", err)
			}
			for _, token := range tc.want {
				if !strings.Contains(html, token) {
					t.Fatalf("expected %q in html", token)
				}
			}
			for _, token := range tc.notWanted {
				if strings.Contains(html, token) {
					t.Fatalf("did not expect %q in html", token)
				}
			}
		})
	}
}

func TestRenderIndexOmitsEmptyWorkflowSummarySections(t *testing.T) {
	html, err := RenderIndex(IndexInput{
		OutputRoot: ".",
		Manifest: Manifest{
			WorkflowName: "summary-empty",
			Status:       "success",
			WorkflowSummary: WorkflowSummary{
				Findings:        []WorkflowFinding{},
				Recommendations: []WorkflowRecommendation{},
				Warnings:        []string{},
			},
		},
	})
	if err != nil {
		t.Fatalf("render index: %v", err)
	}
	for _, token := range []string{"Workflow Recommendations", "Workflow Findings", "Workflow Summary Warnings"} {
		if strings.Contains(html, token) {
			t.Fatalf("did not expect %q in html", token)
		}
	}
}
