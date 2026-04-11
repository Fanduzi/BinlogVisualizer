package workflow

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const workflowSummaryCap = 5

type workflowComparePayload struct {
	KeyFindings     []workflowCompareFinding        `json:"key_findings"`
	Recommendations []workflowCompareRecommendation `json:"recommendations"`
}

type workflowCompareEnvelope struct {
	KeyFindings     json.RawMessage `json:"key_findings"`
	Recommendations json.RawMessage `json:"recommendations"`
}

type workflowCompareFinding struct {
	Kind         string                `json:"kind"`
	Title        string                `json:"title"`
	Summary      string                `json:"summary"`
	EvidenceRefs []WorkflowEvidenceRef `json:"evidence_refs,omitempty"`
}

type workflowCompareRecommendation struct {
	Kind                string                `json:"kind"`
	Priority            string                `json:"priority"`
	Title               string                `json:"title"`
	Summary             string                `json:"summary"`
	RelatedFindingKinds []string              `json:"related_finding_kinds"`
	EvidenceRefs        []WorkflowEvidenceRef `json:"evidence_refs,omitempty"`
}

type workflowTrendPayload struct {
	TrendSummary    []workflowTrendFinding        `json:"trend_summary"`
	Recommendations []workflowTrendRecommendation `json:"recommendations"`
}

type workflowTrendEnvelope struct {
	TrendSummary    json.RawMessage `json:"trend_summary"`
	Recommendations json.RawMessage `json:"recommendations"`
}

type workflowTrendFinding struct {
	Kind         string                `json:"kind"`
	Title        string                `json:"title"`
	Summary      string                `json:"summary"`
	EvidenceRefs []WorkflowEvidenceRef `json:"evidence_refs,omitempty"`
}

type workflowTrendRecommendation struct {
	Kind                string                `json:"kind"`
	Priority            string                `json:"priority"`
	Title               string                `json:"title"`
	Summary             string                `json:"summary"`
	RelatedFindingKinds []string              `json:"related_finding_kinds"`
	EvidenceRefs        []WorkflowEvidenceRef `json:"evidence_refs,omitempty"`
}

type rankedWorkflowFinding struct {
	item      WorkflowFinding
	stepOrder int
	rank      int
}

type rankedWorkflowRecommendation struct {
	item      WorkflowRecommendation
	stepOrder int
	rank      int
}

// BuildWorkflowSummary collects workflow-level findings and recommendations from successful compare/trend JSON artifacts.
func BuildWorkflowSummary(outputDir string, manifest Manifest) WorkflowSummary {
	summary := normalizeWorkflowSummary(WorkflowSummary{})
	findings := make([]rankedWorkflowFinding, 0)
	recommendations := make([]rankedWorkflowRecommendation, 0)

	for stepIndex, step := range manifest.Steps {
		if step.Status != "success" {
			continue
		}
		if step.Kind != "compare" && step.Kind != "trend" {
			continue
		}

		jsonArtifact, ok := findStepArtifact(step, ".json")
		if !ok {
			summary.Warnings = append(summary.Warnings, fmt.Sprintf("%s step %q: missing JSON artifact", step.Kind, step.Name))
			continue
		}

		data, err := os.ReadFile(filepath.Join(outputDir, filepath.FromSlash(jsonArtifact)))
		if err != nil {
			summary.Warnings = append(summary.Warnings, fmt.Sprintf("%s step %q: cannot read JSON artifact %q", step.Kind, step.Name, jsonArtifact))
			continue
		}

		reportPath := choosePreferredReportPath(step)
		reportLabel := step.Name

		switch step.Kind {
		case "compare":
			var envelope workflowCompareEnvelope
			if err := json.Unmarshal(data, &envelope); err != nil {
				summary.Warnings = append(summary.Warnings, fmt.Sprintf("compare step %q: invalid JSON artifact %q", step.Name, jsonArtifact))
				continue
			}
			if len(envelope.KeyFindings) == 0 {
				summary.Warnings = append(summary.Warnings, fmt.Sprintf("compare step %q: JSON payload missing expected array %q", step.Name, "key_findings"))
			}
			if len(envelope.Recommendations) == 0 {
				summary.Warnings = append(summary.Warnings, fmt.Sprintf("compare step %q: JSON payload missing expected array %q", step.Name, "recommendations"))
			}
			var payload workflowComparePayload
			if err := json.Unmarshal(data, &payload); err != nil {
				summary.Warnings = append(summary.Warnings, fmt.Sprintf("compare step %q: invalid JSON artifact %q", step.Name, jsonArtifact))
				continue
			}
			for localIndex, finding := range payload.KeyFindings {
				findings = append(findings, rankedWorkflowFinding{
					item: WorkflowFinding{
						Kind:              finding.Kind,
						Title:             finding.Title,
						Summary:           finding.Summary,
						SourceStepKind:    step.Kind,
						SourceStepName:    step.Name,
						SourceReportPath:  reportPath,
						SourceReportLabel: reportLabel,
						EvidenceRefs:      copyWorkflowEvidenceRefs(finding.EvidenceRefs),
					},
					stepOrder: stepIndex,
					rank:      localIndex,
				})
			}
			for localIndex, recommendation := range payload.Recommendations {
				recommendations = append(recommendations, rankedWorkflowRecommendation{
					item: WorkflowRecommendation{
						Kind:                recommendation.Kind,
						Priority:            recommendation.Priority,
						Title:               recommendation.Title,
						Summary:             recommendation.Summary,
						RelatedFindingKinds: copyStringSlice(recommendation.RelatedFindingKinds),
						SourceStepKind:      step.Kind,
						SourceStepName:      step.Name,
						SourceReportPath:    reportPath,
						SourceReportLabel:   reportLabel,
						EvidenceRefs:        copyWorkflowEvidenceRefs(recommendation.EvidenceRefs),
					},
					stepOrder: stepIndex,
					rank:      localIndex,
				})
			}
		case "trend":
			var envelope workflowTrendEnvelope
			if err := json.Unmarshal(data, &envelope); err != nil {
				summary.Warnings = append(summary.Warnings, fmt.Sprintf("trend step %q: invalid JSON artifact %q", step.Name, jsonArtifact))
				continue
			}
			if len(envelope.TrendSummary) == 0 {
				summary.Warnings = append(summary.Warnings, fmt.Sprintf("trend step %q: JSON payload missing expected array %q", step.Name, "trend_summary"))
			}
			if len(envelope.Recommendations) == 0 {
				summary.Warnings = append(summary.Warnings, fmt.Sprintf("trend step %q: JSON payload missing expected array %q", step.Name, "recommendations"))
			}
			var payload workflowTrendPayload
			if err := json.Unmarshal(data, &payload); err != nil {
				summary.Warnings = append(summary.Warnings, fmt.Sprintf("trend step %q: invalid JSON artifact %q", step.Name, jsonArtifact))
				continue
			}
			for localIndex, finding := range payload.TrendSummary {
				findings = append(findings, rankedWorkflowFinding{
					item: WorkflowFinding{
						Kind:              finding.Kind,
						Title:             finding.Title,
						Summary:           finding.Summary,
						SourceStepKind:    step.Kind,
						SourceStepName:    step.Name,
						SourceReportPath:  reportPath,
						SourceReportLabel: reportLabel,
						EvidenceRefs:      copyWorkflowEvidenceRefs(finding.EvidenceRefs),
					},
					stepOrder: stepIndex,
					rank:      localIndex,
				})
			}
			for localIndex, recommendation := range payload.Recommendations {
				recommendations = append(recommendations, rankedWorkflowRecommendation{
					item: WorkflowRecommendation{
						Kind:                recommendation.Kind,
						Priority:            recommendation.Priority,
						Title:               recommendation.Title,
						Summary:             recommendation.Summary,
						RelatedFindingKinds: copyStringSlice(recommendation.RelatedFindingKinds),
						SourceStepKind:      step.Kind,
						SourceStepName:      step.Name,
						SourceReportPath:    reportPath,
						SourceReportLabel:   reportLabel,
						EvidenceRefs:        copyWorkflowEvidenceRefs(recommendation.EvidenceRefs),
					},
					stepOrder: stepIndex,
					rank:      localIndex,
				})
			}
		}
	}

	summary.Findings = dedupeAndSortWorkflowFindings(findings)
	summary.Recommendations = dedupeAndSortWorkflowRecommendations(recommendations)
	return normalizeWorkflowSummary(summary)
}

func choosePreferredReportPath(step StepRecord) string {
	if htmlArtifact, ok := findStepArtifact(step, ".html"); ok {
		return htmlArtifact
	}
	if jsonArtifact, ok := findStepArtifact(step, ".json"); ok {
		return jsonArtifact
	}
	return ""
}

func findStepArtifact(step StepRecord, ext string) (string, bool) {
	for _, artifact := range step.Artifacts {
		if strings.EqualFold(filepath.Ext(artifact), ext) {
			return artifact, true
		}
	}
	return "", false
}

func dedupeAndSortWorkflowFindings(items []rankedWorkflowFinding) []WorkflowFinding {
	sort.SliceStable(items, func(i, j int) bool {
		left := items[i]
		right := items[j]
		if left.stepOrder != right.stepOrder {
			return left.stepOrder < right.stepOrder
		}
		if left.item.SourceStepKind != right.item.SourceStepKind {
			return findingStepKindRank(left.item.SourceStepKind) < findingStepKindRank(right.item.SourceStepKind)
		}
		if left.rank != right.rank {
			return left.rank < right.rank
		}
		if left.item.Kind != right.item.Kind {
			return left.item.Kind < right.item.Kind
		}
		if left.item.Title != right.item.Title {
			return left.item.Title < right.item.Title
		}
		return left.item.Summary < right.item.Summary
	})

	seen := make(map[string]struct{}, len(items))
	result := make([]WorkflowFinding, 0, minInt(len(items), workflowSummaryCap))
	for _, item := range items {
		key := item.item.Kind + "\x00" + item.item.Title + "\x00" + item.item.Summary
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		result = append(result, item.item)
		if len(result) == workflowSummaryCap {
			break
		}
	}
	return result
}

func dedupeAndSortWorkflowRecommendations(items []rankedWorkflowRecommendation) []WorkflowRecommendation {
	sort.SliceStable(items, func(i, j int) bool {
		left := items[i]
		right := items[j]
		if priorityRank(left.item.Priority) != priorityRank(right.item.Priority) {
			return priorityRank(left.item.Priority) < priorityRank(right.item.Priority)
		}
		if left.stepOrder != right.stepOrder {
			return left.stepOrder < right.stepOrder
		}
		if left.rank != right.rank {
			return left.rank < right.rank
		}
		if left.item.Kind != right.item.Kind {
			return left.item.Kind < right.item.Kind
		}
		if left.item.Title != right.item.Title {
			return left.item.Title < right.item.Title
		}
		return left.item.Summary < right.item.Summary
	})

	seen := make(map[string]struct{}, len(items))
	result := make([]WorkflowRecommendation, 0, minInt(len(items), workflowSummaryCap))
	for _, item := range items {
		key := item.item.Kind + "\x00" + item.item.Priority + "\x00" + item.item.Title + "\x00" + item.item.Summary
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		result = append(result, item.item)
		if len(result) == workflowSummaryCap {
			break
		}
	}
	return result
}

func findingStepKindRank(kind string) int {
	if kind == "compare" {
		return 0
	}
	if kind == "trend" {
		return 1
	}
	return 2
}

func priorityRank(priority string) int {
	switch priority {
	case "high":
		return 0
	case "medium":
		return 1
	case "low":
		return 2
	default:
		return 3
	}
}

func copyWorkflowEvidenceRefs(refs []WorkflowEvidenceRef) []WorkflowEvidenceRef {
	if len(refs) == 0 {
		return nil
	}
	copied := make([]WorkflowEvidenceRef, len(refs))
	copy(copied, refs)
	return copied
}

func copyStringSlice(values []string) []string {
	if len(values) == 0 {
		return []string{}
	}
	copied := make([]string, len(values))
	copy(copied, values)
	return copied
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
