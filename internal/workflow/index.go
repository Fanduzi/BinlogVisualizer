package workflow

import (
	"bytes"
	"fmt"
	"html/template"
	"path/filepath"
	"strings"
)

// IndexInput holds everything the workflow index renderer needs.
type IndexInput struct {
	OutputRoot string
	Manifest   Manifest
}

type indexArtifactLink struct {
	Label   string
	Path    string
	Primary bool
	Ext     string
}

type indexStepView struct {
	Kind         string
	Name         string
	Status       string
	Execution    string
	SnapshotName string
	Error        string
	Artifacts    []indexArtifactLink
}

type indexSummaryEvidenceLink struct {
	Label string
	Path  string
}

type indexSummarySourceLink struct {
	StepKind string
	StepName string
	Label    string
	Path     string
}

type indexWorkflowRecommendationView struct {
	Priority string
	Title    string
	Summary  string
	Source   indexSummarySourceLink
	Evidence []indexSummaryEvidenceLink
}

type indexWorkflowFindingView struct {
	Title    string
	Summary  string
	Source   indexSummarySourceLink
	Evidence []indexSummaryEvidenceLink
}

type indexViewModel struct {
	WorkflowName              string
	WorkflowPlanVersion       int
	BinlogvizVersion          string
	PlanPath                  string
	Mode                      string
	Attempt                   int
	RunStartedAt              string
	RunFinishedAt             string
	Status                    string
	Error                     string
	TotalSteps                int
	SucceededSteps            int
	FailedSteps               int
	TotalArtifacts            int
	Steps                     []indexStepView
	AnalyzeArtifacts          []indexArtifactLink
	CompareArtifacts          []indexArtifactLink
	TrendArtifacts            []indexArtifactLink
	WorkflowRecommendations   []indexWorkflowRecommendationView
	WorkflowFindings          []indexWorkflowFindingView
	WorkflowSummaryWarnings   []string
}

// RenderIndex renders a self-contained HTML workflow index page.
func RenderIndex(input IndexInput) (string, error) {
	vm := buildIndexViewModel(input)

	tmpl, err := template.New("index").Parse(indexTemplate)
	if err != nil {
		return "", fmt.Errorf("parse index template: %w", err)
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, vm); err != nil {
		return "", fmt.Errorf("execute index template: %w", err)
	}
	return buf.String(), nil
}

func buildIndexViewModel(input IndexInput) indexViewModel {
	m := normalizeManifest(input.Manifest)

	vm := indexViewModel{
		WorkflowName:            m.WorkflowName,
		WorkflowPlanVersion:     m.WorkflowPlanVersion,
		BinlogvizVersion:        m.BinlogvizVersion,
		PlanPath:                m.PlanPath,
		Mode:                    m.Mode,
		Attempt:                 m.Attempt,
		RunStartedAt:            m.RunStartedAt,
		RunFinishedAt:           m.RunFinishedAt,
		Status:                  m.Status,
		Error:                   m.Error,
		WorkflowRecommendations: buildIndexWorkflowRecommendations(m.WorkflowSummary.Recommendations),
		WorkflowFindings:        buildIndexWorkflowFindings(m.WorkflowSummary.Findings),
		WorkflowSummaryWarnings: append([]string(nil), m.WorkflowSummary.Warnings...),
	}

	for _, s := range m.Steps {
		sv := indexStepView{
			Kind:         s.Kind,
			Name:         s.Name,
			Status:       s.Status,
			Execution:    s.Execution,
			SnapshotName: s.SnapshotName,
			Error:        s.Error,
		}
		vm.TotalSteps++
		if s.Status == "success" {
			vm.SucceededSteps++
		} else {
			vm.FailedSteps++
		}

		for _, a := range s.Artifacts {
			link := indexArtifactLink{
				Label: filepath.Base(a),
				Path:  "./" + filepath.ToSlash(a),
				Ext:   strings.TrimPrefix(filepath.Ext(a), "."),
			}
			link.Primary = link.Ext == "html"
			sv.Artifacts = append(sv.Artifacts, link)
			vm.TotalArtifacts++

			switch s.Kind {
			case "analyze":
				vm.AnalyzeArtifacts = append(vm.AnalyzeArtifacts, link)
			case "compare":
				vm.CompareArtifacts = append(vm.CompareArtifacts, link)
			case "trend":
				vm.TrendArtifacts = append(vm.TrendArtifacts, link)
			}
		}

		// Sort step artifacts: primary (HTML) first
		sortArtifactsPrimaryFirst(sv.Artifacts)
		vm.Steps = append(vm.Steps, sv)
	}

	// Sort grouped artifacts: primary first
	sortArtifactsPrimaryFirst(vm.AnalyzeArtifacts)
	sortArtifactsPrimaryFirst(vm.CompareArtifacts)
	sortArtifactsPrimaryFirst(vm.TrendArtifacts)

	return vm
}

func buildIndexWorkflowRecommendations(items []WorkflowRecommendation) []indexWorkflowRecommendationView {
	if len(items) == 0 {
		return nil
	}
	views := make([]indexWorkflowRecommendationView, 0, len(items))
	for _, item := range items {
		views = append(views, indexWorkflowRecommendationView{
			Priority: item.Priority,
			Title:    item.Title,
			Summary:  item.Summary,
			Source: indexSummarySourceLink{
				StepKind: item.SourceStepKind,
				StepName: item.SourceStepName,
				Label:    item.SourceReportLabel,
				Path:     relativeLink(item.SourceReportPath),
			},
			Evidence: buildIndexSummaryEvidenceLinks(item.SourceReportPath, item.EvidenceRefs),
		})
	}
	return views
}

func buildIndexWorkflowFindings(items []WorkflowFinding) []indexWorkflowFindingView {
	if len(items) == 0 {
		return nil
	}
	views := make([]indexWorkflowFindingView, 0, len(items))
	for _, item := range items {
		title := item.Title
		if title == "" {
			title = item.Kind
		}
		views = append(views, indexWorkflowFindingView{
			Title:   title,
			Summary: item.Summary,
			Source: indexSummarySourceLink{
				StepKind: item.SourceStepKind,
				StepName: item.SourceStepName,
				Label:    item.SourceReportLabel,
				Path:     relativeLink(item.SourceReportPath),
			},
			Evidence: buildIndexSummaryEvidenceLinks(item.SourceReportPath, item.EvidenceRefs),
		})
	}
	return views
}

func buildIndexSummaryEvidenceLinks(reportPath string, refs []WorkflowEvidenceRef) []indexSummaryEvidenceLink {
	if len(refs) == 0 {
		return nil
	}
	links := make([]indexSummaryEvidenceLink, 0, len(refs))
	base := relativeLink(reportPath)
	allowAnchor := strings.EqualFold(filepath.Ext(reportPath), ".html")
	for _, ref := range refs {
		path := base
		if allowAnchor && ref.Anchor != "" {
			path += "#" + ref.Anchor
		}
		links = append(links, indexSummaryEvidenceLink{
			Label: ref.Label,
			Path:  path,
		})
	}
	return links
}

func relativeLink(path string) string {
	if path == "" {
		return ""
	}
	return "./" + filepath.ToSlash(path)
}

func sortArtifactsPrimaryFirst(links []indexArtifactLink) {
	for i := 0; i < len(links); i++ {
		if links[i].Primary {
			continue
		}
		for j := i + 1; j < len(links); j++ {
			if links[j].Primary {
				links[i], links[j] = links[j], links[i]
				break
			}
		}
	}
}

const indexTemplate = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>BinlogViz Workflow: {{.WorkflowName}}</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:#0f1117;color:#e0e0e0;line-height:1.6}
.container{max-width:900px;margin:0 auto;padding:24px}
.hero{background:linear-gradient(135deg,#1a1b2e 0%,#16213e 100%);border-radius:12px;padding:28px 32px;margin-bottom:24px;border:1px solid #2a2d45}
.hero h1{font-size:1.6rem;margin-bottom:8px;color:#fff}
.hero .meta{font-size:0.85rem;color:#8b8fa3}
.hero .meta span{margin-right:16px}
.badge{display:inline-block;padding:2px 10px;border-radius:9999px;font-size:0.8rem;font-weight:600;text-transform:uppercase;letter-spacing:0.5px}
.badge-success{background:#0d7a3e;color:#fff}
.badge-failed{background:#c0392b;color:#fff}
.badge-reused{background:#4a4d5e;color:#a0a4b8}
.badge-high{background:#b91c1c;color:#fff}
.badge-medium{background:#92400e;color:#fff}
.badge-low{background:#1d4ed8;color:#fff}
.badge-execution{display:inline-block;padding:1px 8px;border-radius:9999px;font-size:0.7rem;font-weight:500;letter-spacing:0.3px;margin-left:6px}
.cards{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:24px}
.card{background:#181a27;border-radius:8px;padding:16px;text-align:center;border:1px solid #2a2d45}
.card .card-value{font-size:1.8rem;font-weight:700;color:#fff}
.card .card-label{font-size:0.75rem;color:#8b8fa3;text-transform:uppercase;letter-spacing:0.5px;margin-top:4px}
.section{margin-bottom:24px}
.section h2{font-size:1.1rem;color:#a0a4b8;margin-bottom:12px;padding-bottom:6px;border-bottom:1px solid #2a2d45}
.step-row{background:#181a27;border-radius:8px;padding:14px 18px;margin-bottom:8px;border:1px solid #2a2d45}
.step-row .step-header{display:flex;align-items:center;gap:10px;margin-bottom:4px}
.step-row .step-kind{font-size:0.75rem;color:#8b8fa3;text-transform:uppercase;letter-spacing:0.5px}
.step-row .step-name{font-weight:600;color:#e0e0e0}
.step-row .step-snapshot{font-size:0.8rem;color:#6c7086}
.step-row .step-error{margin-top:6px;padding:8px 12px;background:#2d1015;border:1px solid #5c1a1a;border-radius:4px;color:#f87171;font-size:0.85rem;font-family:monospace}
.summary-warning{background:#2f2410;border:1px solid #6b4f1d;border-radius:8px;padding:14px 18px;margin-bottom:24px}
.summary-warning h2{font-size:1rem;color:#f8d58d;margin-bottom:8px;border:none;padding:0}
.summary-warning ul{padding-left:18px;color:#f4e1b3}
.summary-list{display:grid;grid-template-columns:1fr;gap:10px}
.summary-item{background:#181a27;border-radius:8px;padding:14px 18px;border:1px solid #2a2d45}
.summary-item h3{font-size:1rem;color:#fff;margin-bottom:6px}
.summary-item p{color:#c9ccd8;margin-bottom:10px}
.summary-meta{display:flex;flex-wrap:wrap;gap:10px;align-items:center;font-size:0.8rem;color:#8b8fa3}
.summary-meta a{color:#8fb4ff;text-decoration:none}
.summary-meta a:hover{text-decoration:underline}
.summary-evidence{display:flex;flex-wrap:wrap;gap:8px;margin-top:8px}
.summary-evidence a{display:inline-flex;align-items:center;padding:4px 10px;border-radius:9999px;background:#23263a;color:#b8c0e0;text-decoration:none;font-size:0.78rem}
.summary-evidence a:hover{background:#2c3150}
.artifacts-grid{display:grid;grid-template-columns:1fr;gap:8px}
.artifact-group{background:#181a27;border-radius:8px;padding:14px 18px;border:1px solid #2a2d45}
.artifact-group h3{font-size:0.9rem;color:#a0a4b8;margin-bottom:8px;text-transform:capitalize}
.artifact-links{display:flex;flex-wrap:wrap;gap:8px;align-items:center}
.artifact-link{display:inline-flex;align-items:center;padding:6px 14px;border-radius:6px;text-decoration:none;font-size:0.85rem;font-weight:500;transition:opacity 0.15s}
.artifact-link:hover{opacity:0.85}
.artifact-link-primary{background:#2563eb;color:#fff}
.artifact-link-secondary{background:#2a2d45;color:#a0a4b8}
.manifest-link{margin-top:24px;padding:12px 0;text-align:center}
.manifest-link a{color:#6c8ebf;text-decoration:none;font-size:0.85rem}
.manifest-link a:hover{text-decoration:underline}
</style>
</head>
<body>
<div class="container">
<div class="hero">
<h1>{{.WorkflowName}}</h1>
<div class="meta">
<span class="badge badge-{{.Status}}">{{.Status}}</span>
{{if eq .Mode "resume"}}<span class="badge badge-reused">resume</span>{{end}}
{{if eq .Mode "resume"}}<span>attempt {{.Attempt}}</span>{{end}}
<span>v{{.WorkflowPlanVersion}}</span>
<span>binlogviz {{.BinlogvizVersion}}</span>
{{if .RunStartedAt}}<span>started {{.RunStartedAt}}</span>{{end}}
{{if .RunFinishedAt}}<span>finished {{.RunFinishedAt}}</span>{{end}}
</div>
</div>

{{if .Error}}<div class="step-error">{{.Error}}</div>{{end}}

{{if .WorkflowSummaryWarnings}}<div class="summary-warning section">
<h2>Workflow Summary Warnings</h2>
<ul>
{{range .WorkflowSummaryWarnings}}<li>{{.}}</li>{{end}}
</ul>
</div>{{end}}

{{if .WorkflowRecommendations}}<div class="section">
<h2>Workflow Recommendations</h2>
<div class="summary-list">
{{range .WorkflowRecommendations}}<div class="summary-item">
<div class="summary-meta"><span class="badge badge-{{.Priority}}">{{.Priority}}</span></div>
<h3>{{.Title}}</h3>
<p>{{.Summary}}</p>
<div class="summary-meta">
<span>{{.Source.StepKind}}:{{.Source.StepName}}</span>
{{if .Source.Path}}<a href="{{.Source.Path}}">Source report{{if .Source.Label}} ({{.Source.Label}}){{end}}</a>{{end}}
</div>
{{if .Evidence}}<div class="summary-evidence">{{range .Evidence}}<a href="{{.Path}}">{{.Label}}</a>{{end}}</div>{{end}}
</div>{{end}}
</div>
</div>{{end}}

{{if .WorkflowFindings}}<div class="section">
<h2>Workflow Findings</h2>
<div class="summary-list">
{{range .WorkflowFindings}}<div class="summary-item">
<h3>{{.Title}}</h3>
<p>{{.Summary}}</p>
<div class="summary-meta">
<span>{{.Source.StepKind}}:{{.Source.StepName}}</span>
{{if .Source.Path}}<a href="{{.Source.Path}}">Source report{{if .Source.Label}} ({{.Source.Label}}){{end}}</a>{{end}}
</div>
{{if .Evidence}}<div class="summary-evidence">{{range .Evidence}}<a href="{{.Path}}">{{.Label}}</a>{{end}}</div>{{end}}
</div>{{end}}
</div>
</div>{{end}}

<div class="cards">
<div class="card"><div class="card-value">{{.TotalSteps}}</div><div class="card-label">Steps</div></div>
<div class="card"><div class="card-value">{{.SucceededSteps}}</div><div class="card-label">Succeeded</div></div>
<div class="card"><div class="card-value">{{.FailedSteps}}</div><div class="card-label">Failed</div></div>
<div class="card"><div class="card-value">{{.TotalArtifacts}}</div><div class="card-label">Artifacts</div></div>
</div>

{{if .Steps}}<div class="section">
<h2>Steps</h2>
{{range .Steps}}<div class="step-row">
<div class="step-header">
<span class="badge badge-{{.Status}}">{{.Status}}</span>
<span class="step-kind">{{.Kind}}</span>
<span class="step-name">{{.Name}}</span>
{{if .Execution}}<span class="badge-execution badge-{{.Execution}}">{{.Execution}}</span>{{end}}
{{if .SnapshotName}}<span class="step-snapshot">snapshot: {{.SnapshotName}}</span>{{end}}
</div>
{{if .Error}}<div class="step-error">{{.Error}}</div>{{end}}
</div>
{{end}}</div>{{end}}

{{if .AnalyzeArtifacts}}<div class="section">
<h2>Artifacts</h2>
<div class="artifact-group">
<h3>analyze</h3>
<div class="artifact-links">
{{range .AnalyzeArtifacts}}<a class="artifact-link artifact-link-{{if .Primary}}primary{{else}}secondary{{end}}" href="{{.Path}}">{{.Label}}</a>
{{end}}</div>
</div>
</div>{{end}}

{{if .CompareArtifacts}}<div class="section">
{{if not .AnalyzeArtifacts}}<h2>Artifacts</h2>{{end}}
<div class="artifact-group">
<h3>compare</h3>
<div class="artifact-links">
{{range .CompareArtifacts}}<a class="artifact-link artifact-link-{{if .Primary}}primary{{else}}secondary{{end}}" href="{{.Path}}">{{.Label}}</a>
{{end}}</div>
</div>
</div>{{end}}

{{if .TrendArtifacts}}<div class="section">
{{if not .AnalyzeArtifacts}}{{if not .CompareArtifacts}}<h2>Artifacts</h2>{{end}}{{end}}
<div class="artifact-group">
<h3>trend</h3>
<div class="artifact-links">
{{range .TrendArtifacts}}<a class="artifact-link artifact-link-{{if .Primary}}primary{{else}}secondary{{end}}" href="{{.Path}}">{{.Label}}</a>
{{end}}</div>
</div>
</div>{{end}}

<div class="manifest-link"><a href="./manifest.json">manifest.json</a></div>
</div>
</body>
</html>`
