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
