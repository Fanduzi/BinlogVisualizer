// Package binlogviz executes a workflow plan: resume with rerun selectors,
// per-window analyze/compare/trend steps, the manifest, and the index page.
// input: a parsed workflow.Plan, output/snapshot dirs, and the plan path.
// output: step artifacts on disk, manifest.json, index.html, and localized stderr progress.
// pos: CLI orchestration helper for workflow.go.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"binlogviz/internal/analyzer"
	"binlogviz/internal/binlog"
	"binlogviz/internal/clierr"
	comparepkg "binlogviz/internal/compare"
	"binlogviz/internal/i18n"
	"binlogviz/internal/model"
	"binlogviz/internal/report"
	"binlogviz/internal/snapshot"
	trendpkg "binlogviz/internal/trend"
	"binlogviz/internal/version"
	"binlogviz/internal/workflow"
)

func executeResume(outputDir, snapshotDir string, rerunSelectors []string, stderr io.Writer, loc *time.Location) error {
	startedAt := time.Now().UTC()

	// Load existing manifest
	manifestPath := filepath.Join(outputDir, "manifest.json")
	mf, err := workflowManifestFromJSON(manifestPath)
	if err != nil {
		return clierr.Errorf(clierr.KindInput, "load manifest from %s: %w", outputDir, err)
	}
	unlock, err := workflow.LockDir(outputDir)
	if err != nil {
		return err
	}
	defer unlock()

	// Early validation before trying to load the plan
	if mf.ManifestVersion == 0 && mf.WorkflowName == "" && mf.Status == "" && mf.PlanPath == "" && len(mf.Steps) == 0 {
		return workflow.ValidateResumableManifest(mf, "", "", "")
	}
	if mf.ManifestVersion == 0 {
		return clierr.Errorf(clierr.KindUnsupported, "%s", i18n.T("error.workflowResumeNoVersion"))
	}
	if mf.ManifestVersion != 2 {
		return clierr.Errorf(clierr.KindUnsupported, "%s", i18n.Tf("error.workflowResumeVersionUnsupported", map[string]any{"Version": mf.ManifestVersion}))
	}
	if mf.PlanPath == "" {
		return clierr.Errorf(clierr.KindCorrupt, "%s", i18n.T("error.workflowResumeNoPlanPath"))
	}
	// A compare/trend artifact from a newer build would be reused or summarized
	// without being understood (ADR 0010): unsupported (9).
	if err := workflow.CheckArtifactVersions(outputDir, mf); err != nil {
		return err
	}

	// Trust-boundary check: reject before opening any file; use canonical path.
	canonicalPlanPath, err := workflow.ValidateWorkflowPlanPath(outputDir, mf.PlanPath)
	if err != nil {
		return err
	}

	// Load the plan referenced by the manifest using the canonical trusted path.
	planPath := canonicalPlanPath

	f, err := os.Open(planPath)
	if err != nil {
		return clierr.Errorf(clierr.KindInput, "open plan file %s: %w", planPath, err)
	}
	defer f.Close()

	plan, err := workflow.LoadPlan(f)
	if err != nil {
		return clierr.Errorf(clierr.KindInput, "load plan: %w", err)
	}

	// Compute current plan hash and validate resumability
	planSHA256, err := computeFileSHA256(planPath)
	if err != nil {
		return clierr.Errorf(clierr.KindInput, "hash plan file: %w", err)
	}

	if err := workflow.ValidateResumableManifest(mf, outputDir, planPath, planSHA256); err != nil {
		return err
	}

	// Use manifest's snapshot_dir if not overridden
	effectiveSnapshotDir := workflow.ResolveManifestSnapshotDir(outputDir, mf.SnapshotDir)
	if snapshotDir != "" {
		effectiveSnapshotDir = snapshotDir
	}

	// Build the resume plan
	resumePlan, err := workflow.BuildResumePlan(plan, mf, rerunSelectors, outputDir, effectiveSnapshotDir)
	if errors.Is(err, workflow.ErrNothingToResume) {
		fmt.Fprintln(stderr, i18n.T("progress.workflowNothingToResume"))
		return nil
	}
	if err != nil {
		return err
	}

	fmt.Fprint(stderr, i18n.Tf("progress.workflowResuming", map[string]any{"Name": plan.Workflow.Name, "Attempt": resumePlan.UpdatedManifest.Attempt}))

	// Build analyzer and report options from plan defaults
	analyzerOpts := buildWorkflowAnalyzerOptions(plan)
	reportOpts, err := buildWorkflowReportOptions(plan, loc)
	if err != nil {
		return err
	}

	// Initialize the updated manifest
	newMf := resumePlan.UpdatedManifest
	newMf.RunStartedAt = startedAt.Format(time.RFC3339)
	newMf.RunID = workflow.NewRunID(startedAt) // each attempt is its own ledger run
	newMf.Steps = []workflow.StepRecord{}

	// Reuse input files from original run
	inputFiles := resumePlan.InputFiles

	// Track analyze reports for compare/trend consumption
	windowReports := make(map[string]comparepkg.InputReport)

	// Execute or reuse steps in declared order
	for _, planned := range resumePlan.Steps {
		if !planned.Execute {
			// Reuse: carry forward the prior step record
			priorStep := findPriorStep(mf, planned.Kind, planned.Name)
			if priorStep == nil {
				return clierr.Errorf(clierr.KindError, "internal error: no prior step for reusable %s:%s", planned.Kind, planned.Name)
			}
			reused := *priorStep
			reused.Execution = "reused"
			newMf.Steps = append(newMf.Steps, reused)

			// For reused analyze steps, we need to load the report for downstream consumption
			if planned.Kind == "analyze" {
				artifactPath := filepath.Join(outputDir, "analyze", planned.Name+".json")
				data, err := os.ReadFile(artifactPath)
				if err != nil {
					return clierr.Errorf(clierr.KindInput, "read reusable analyze artifact %s: %w", artifactPath, err)
				}
				reportInput, err := comparepkg.DecodeReportJSON(data)
				if err != nil {
					return clierr.Errorf(clierr.KindCorrupt, "decode reusable analyze report for %q: %w", planned.Name, err)
				}
				windowReports[planned.Name] = reportInput
			}

			fmt.Fprint(stderr, i18n.Tf("progress.workflowReuseStep", map[string]any{"Kind": planned.Kind, "Name": planned.Name}))
			continue
		}

		// Execute the step
		switch planned.Kind {
		case "analyze":
			rec, _, reportInput, aerr := runWorkflowAnalyzeWindow(
				inputFiles, analyzerOpts, reportOpts, plan,
				findWindow(plan, planned.Name),
				outputDir, effectiveSnapshotDir, stderr,
				true, // overwrite snapshots on resume
			)
			newMf.Steps = append(newMf.Steps, rec)
			if aerr != nil {
				return finalizeWorkflow(outputDir, &newMf, startedAt, stderr, aerr)
			}
			windowReports[planned.Name] = reportInput

		case "compare":
			job := findCompareJob(plan, planned.Name)
			rec, cerr := runWorkflowCompareJob(outputDir, job, windowReports, loc)
			newMf.Steps = append(newMf.Steps, rec)
			if cerr != nil {
				return finalizeWorkflow(outputDir, &newMf, startedAt, stderr, cerr)
			}

		case "trend":
			job := findTrendJob(plan, planned.Name)
			rec, terr := runWorkflowTrendJob(outputDir, job, windowReports, loc)
			newMf.Steps = append(newMf.Steps, rec)
			if terr != nil {
				return finalizeWorkflow(outputDir, &newMf, startedAt, stderr, terr)
			}
		}
	}

	return finalizeWorkflow(outputDir, &newMf, startedAt, stderr, nil)
}

func findPriorStep(mf workflow.Manifest, kind, name string) *workflow.StepRecord {
	for i := range mf.Steps {
		if mf.Steps[i].Kind == kind && mf.Steps[i].Name == name {
			return &mf.Steps[i]
		}
	}
	return nil
}

func findWindow(plan workflow.Plan, name string) workflow.Window {
	for _, w := range plan.Windows {
		if w.Name == name {
			return w
		}
	}
	return workflow.Window{Name: name}
}

func findCompareJob(plan workflow.Plan, name string) workflow.CompareJob {
	for _, j := range plan.Compare {
		if j.Name == name {
			return j
		}
	}
	return workflow.CompareJob{Name: name}
}

func findTrendJob(plan workflow.Plan, name string) workflow.TrendJob {
	for _, j := range plan.Trend {
		if j.Name == name {
			return j
		}
	}
	return workflow.TrendJob{Name: name}
}

func executeWorkflow(plan workflow.Plan, outputDir, snapshotDir, planPath string, stderr io.Writer, loc *time.Location) error {
	startedAt := time.Now().UTC()

	fmt.Fprint(stderr, i18n.Tf("progress.workflowStarting", map[string]any{"Name": plan.Workflow.Name}))

	if err := workflow.EnsureLayout(outputDir); err != nil {
		return clierr.Errorf(clierr.KindOutput, "create output layout: %w", err)
	}
	unlock, err := workflow.LockDir(outputDir)
	if err != nil {
		return err
	}
	defer unlock()

	copiedPlanPath, err := copyWorkflowPlanIntoOutputDir(outputDir, planPath)
	if err != nil {
		return clierr.Errorf(clierr.KindOutput, "copy plan file: %w", err)
	}
	planSHA256, err := computeFileSHA256(copiedPlanPath)
	if err != nil {
		return clierr.Errorf(clierr.KindInput, "hash plan file: %w", err)
	}

	effectiveSnapshotDir := ""
	if plan.Defaults.Snapshot.Save {
		effectiveSnapshotDir, err = snapshot.ResolveSnapshotDir(snapshotDir)
		if err != nil {
			return clierr.Errorf(clierr.KindOutput, "resolve snapshot dir: %w", err)
		}
	}

	mf := workflow.Manifest{
		ManifestVersion:     2,
		Mode:                "run",
		Attempt:             1,
		WorkflowName:        plan.Workflow.Name,
		WorkflowPlanVersion: plan.Version,
		BinlogvizVersion:    version.Version,
		PlanPath:            copiedPlanPath,
		PlanSHA256:          planSHA256,
		SnapshotDir:         effectiveSnapshotDir,
		RunStartedAt:        startedAt.Format(time.RFC3339),
		Status:              "success",
		Steps:               []workflow.StepRecord{},
	}

	// Resolve input paths via discovery mode
	paths, err := discoverBinlogPaths(plan.Defaults.Input.FromDir, plan.Defaults.Input.Prefix)
	if err != nil {
		return finalizeWorkflow(outputDir, &mf, startedAt, stderr, clierr.Errorf(clierr.KindInput, "discover binlog files: %w", err))
	}
	mf.ResolvedInputFiles = paths

	// Build analyzer options from plan defaults
	analyzerOpts := buildWorkflowAnalyzerOptions(plan)
	reportOpts, err := buildWorkflowReportOptions(plan, loc)
	if err != nil {
		return finalizeWorkflow(outputDir, &mf, startedAt, stderr, err)
	}

	// --- Analyze phase ---
	windowReports := make(map[string]comparepkg.InputReport, len(plan.Windows))
	for _, w := range plan.Windows {
		fmt.Fprintf(stderr, "  analyze %q\n", w.Name)
		rec, _, reportInput, aerr := runWorkflowAnalyzeWindow(
			paths, analyzerOpts, reportOpts, plan, w, outputDir, effectiveSnapshotDir, stderr,
			false, // do not overwrite snapshots on fresh run
		)
		mf.Steps = append(mf.Steps, rec)
		if aerr != nil {
			return finalizeWorkflow(outputDir, &mf, startedAt, stderr, aerr)
		}
		windowReports[w.Name] = reportInput
	}

	// --- Compare phase ---
	for _, job := range plan.Compare {
		fmt.Fprintf(stderr, "  compare %q\n", job.Name)
		rec, cerr := runWorkflowCompareJob(outputDir, job, windowReports, loc)
		mf.Steps = append(mf.Steps, rec)
		if cerr != nil {
			return finalizeWorkflow(outputDir, &mf, startedAt, stderr, cerr)
		}
	}

	// --- Trend phase ---
	for _, job := range plan.Trend {
		fmt.Fprintf(stderr, "  trend %q\n", job.Name)
		rec, terr := runWorkflowTrendJob(outputDir, job, windowReports, loc)
		mf.Steps = append(mf.Steps, rec)
		if terr != nil {
			return finalizeWorkflow(outputDir, &mf, startedAt, stderr, terr)
		}
	}

	return finalizeWorkflow(outputDir, &mf, startedAt, stderr, nil)
}

func finalizeWorkflow(outputDir string, mf *workflow.Manifest, startedAt time.Time, stderr io.Writer, stepErr error) error {
	mf.RunFinishedAt = time.Now().UTC().Format(time.RFC3339)

	if stepErr != nil {
		mf.Status = "failed"
		mf.Error = stepErr.Error()
	}

	mf.WorkflowSummary = workflow.BuildWorkflowSummary(outputDir, *mf)

	// Record every file this run wrote in the append-only ledger before the
	// manifest, so clean can attribute files per file (ADR 0009 §8, #289).
	// A failed ledger write only loses attribution, which makes clean keep
	// more, never delete more.
	if mf.RunID == "" {
		mf.RunID = workflow.NewRunID(startedAt)
	}
	entries, hashErrs := workflow.LedgerEntriesForRun(outputDir, *mf, mf.RunID, time.Now())
	for _, herr := range hashErrs {
		fmt.Fprint(stderr, i18n.Tf("progress.workflowLedgerNotRecorded", map[string]any{"Error": herr}))
	}
	if lerr := workflow.AppendLedger(outputDir, entries); lerr != nil {
		fmt.Fprint(stderr, i18n.Tf("progress.workflowLedgerAppendFailed", map[string]any{"Error": lerr}))
	}

	manifestPath := filepath.Join(outputDir, "manifest.json")
	if writeErr := workflow.WriteManifest(manifestPath, *mf); writeErr != nil {
		fmt.Fprint(stderr, i18n.Tf("progress.workflowManifestWriteFailed", map[string]any{"Error": writeErr}))
		if stepErr == nil {
			return clierr.Errorf(clierr.KindOutput, "write manifest: %w", writeErr)
		}
	} else {
		fmt.Fprint(stderr, i18n.Tf("progress.workflowManifestWritten", map[string]any{"Path": manifestPath}))
	}

	// Write index.html on both success and failure paths
	if indexErr := writeWorkflowIndex(outputDir, *mf); indexErr != nil {
		fmt.Fprint(stderr, i18n.Tf("progress.workflowIndexWriteFailed", map[string]any{"Error": indexErr}))
		if stepErr == nil {
			return clierr.Errorf(clierr.KindOutput, "write index: %w", indexErr)
		}
	} else {
		fmt.Fprint(stderr, i18n.Tf("progress.workflowIndexWritten", map[string]any{"Path": filepath.Join(outputDir, "index.html")}))
	}

	if stepErr != nil {
		fmt.Fprint(stderr, i18n.Tf("progress.workflowFailed", map[string]any{"Name": mf.WorkflowName}))
	}
	return stepErr
}

func writeWorkflowIndex(outputDir string, manifest workflow.Manifest) error {
	html, err := workflow.RenderIndex(workflow.IndexInput{
		OutputRoot: outputDir,
		Manifest:   manifest,
	})
	if err != nil {
		return clierr.Errorf(clierr.KindError, "render workflow index: %w", err)
	}
	indexPath := filepath.Join(outputDir, "index.html")
	return os.WriteFile(indexPath, []byte(html), 0o644)
}

func runWorkflowAnalyzeWindow(
	paths []string,
	analyzerOpts analyzer.Options,
	reportOpts report.Options,
	plan workflow.Plan,
	w workflow.Window,
	outputDir string,
	snapshotDir string,
	stderr io.Writer,
	overwriteSnapshot bool,
) (workflow.StepRecord, string, comparepkg.InputReport, error) {
	rec := workflow.StepRecord{
		Kind: "analyze",
		Name: w.Name,
	}

	windowOpts := analyzerOpts
	windowOpts.Start = &w.Start
	windowOpts.End = &w.End

	snapshotMeta := &model.Snapshot{
		Name:             w.Name,
		Label:            w.Name,
		CreatedAt:        time.Now().UTC(),
		BinlogvizVersion: version.Version,
		InputMode:        "discovery",
		Input: model.SnapshotInput{
			Files:   append([]string(nil), paths...),
			FromDir: plan.Defaults.Input.FromDir,
			Prefix:  plan.Defaults.Input.Prefix,
		},
		Window: model.SnapshotWindow{
			StartTime: w.Start,
			EndTime:   w.End,
		},
	}

	// Execute analysis pipeline to get result directly
	result, err := runAnalyzeToResult(paths, windowOpts, stderr)
	if err != nil {
		rec.Status = "failed"
		rec.Error = err.Error()
		return rec, "", comparepkg.InputReport{}, clierr.Errorf(clierr.KindError, "analyze window %q: %w", w.Name, err)
	}

	// Resolve snapshot metadata
	result.Snapshot = resolveAnalyzeSnapshotMetadata(snapshotMeta, result.Summary)

	// Artifacts and snapshots are archives: always full diagnostics (ADR 0011 §3).
	// The snapshot file stays UTC. --tz changes the analyze artifact the operator reads.
	archiveOpts := report.ArchiveOptions(reportOpts)
	archiveOpts.Display = nil
	archive, err := report.RenderJSONWithOptions(*result, archiveOpts)
	if err != nil {
		rec.Status = "failed"
		rec.Error = err.Error()
		return rec, "", comparepkg.InputReport{}, clierr.Errorf(clierr.KindError, "render analyze JSON for %q: %w", w.Name, err)
	}
	artifact := archive
	if reportOpts.Display != nil && reportOpts.Display != time.UTC {
		artifact, err = report.RenderJSONWithOptions(*result, report.ArchiveOptions(reportOpts))
		if err != nil {
			rec.Status = "failed"
			rec.Error = err.Error()
			return rec, "", comparepkg.InputReport{}, clierr.Errorf(clierr.KindError, "render analyze JSON for %q: %w", w.Name, err)
		}
	}

	// Write to artifact path
	artifactPath := workflow.ArtifactPath(outputDir, "analyze", w.Name, "json")
	if err := os.WriteFile(artifactPath, []byte(artifact), 0o644); err != nil {
		rec.Status = "failed"
		rec.Error = err.Error()
		return rec, "", comparepkg.InputReport{}, clierr.Errorf(clierr.KindOutput, "write analyze artifact for %q: %w", w.Name, err)
	}

	// Save snapshot if configured (must succeed before marking step success)
	if plan.Defaults.Snapshot.Save {
		var savedPath string
		var err error
		if overwriteSnapshot {
			savedPath, err = snapshot.SaveJSONOverwrite(snapshotDir, w.Name, []byte(archive))
		} else {
			savedPath, err = snapshot.SaveJSON(snapshotDir, w.Name, []byte(archive))
		}
		if err != nil {
			rec.Status = "failed"
			rec.Error = err.Error()
			return rec, "", comparepkg.InputReport{}, clierr.Errorf(clierr.KindOutput, "save snapshot %q: %w", w.Name, err)
		}
		rec.SnapshotName = w.Name
		fmt.Fprint(stderr, i18n.Tf("progress.workflowSnapshotSaved", map[string]any{"Name": w.Name, "Path": savedPath}))
	}

	rec.Status = "success"
	rec.Execution = "executed"
	rec.Artifacts = []string{"analyze/" + w.Name + ".json"}

	// Compare and trend read the UTC archive. HTML applies --tz when it renders.
	reportInput, err := comparepkg.DecodeReportJSON([]byte(archive))
	if err != nil {
		rec.Status = "failed"
		rec.Error = fmt.Sprintf("decode generated report: %v", err)
		return rec, artifact, comparepkg.InputReport{}, clierr.Errorf(clierr.KindCorrupt, "decode analyze report for %q: %w", w.Name, err)
	}

	return rec, artifact, reportInput, nil
}

// runAnalyzeToResult runs the analysis pipeline and returns the raw result without rendering.
func runAnalyzeToResult(paths []string, opts analyzer.Options, stderr io.Writer) (*model.AnalysisResult, error) {
	progress, err := newAggregateProgress(paths, stderr)
	if err != nil {
		return nil, clierr.Errorf(clierr.KindError, "build parse progress: %w", err)
	}

	var a *analyzer.Analyzer
	if opts.DetailStoreMode == analyzer.DetailStoreDuckDB {
		store, cleanup, _, err := createDuckDBTempStore("")
		if err != nil {
			return nil, clierr.Errorf(clierr.KindOutput, "create temp store: %w", err)
		}
		defer cleanup()
		a = analyzer.NewWithStore(opts, store)
	} else {
		a = analyzer.New(opts)
	}

	parser := binlog.NewParser()
	handler := func(raw binlog.RawEvent) error {
		normalized, err := binlog.NormalizeRawEvent(raw)
		if err != nil {
			return clierr.Errorf(clierr.KindError, "normalize at position %d: %w", raw.Position, err)
		}
		if normalized == nil {
			return nil
		}
		if err := a.Consume(*normalized); err != nil {
			return clierr.Errorf(clierr.KindError, "consume: %w", err)
		}
		return nil
	}

	if pp, ok := parser.(binlog.ProgressParser); ok {
		if err := pp.ParseFilesWithProgress(paths, func(p binlog.ParseProgress) {
			progress.Advance(p)
		}, handler); err != nil {
			return nil, clierr.Errorf(clierr.KindCorrupt, "parse: %w", err)
		}
		for i := range paths {
			progress.FinishFile(i)
		}
	} else {
		if err := parser.ParseFiles(paths, handler); err != nil {
			return nil, clierr.Errorf(clierr.KindCorrupt, "parse: %w", err)
		}
	}
	progress.FinishParse()
	progress.Finalizing()

	result, err := a.Finalize()
	if err != nil {
		if spill := asSpill(err); spill != nil {
			return nil, spill
		}
		return nil, clierr.Errorf(clierr.KindError, "finalize: %w", err)
	}
	return result, nil
}

func runWorkflowCompareJob(
	outputDir string,
	job workflow.CompareJob,
	windowReports map[string]comparepkg.InputReport,
	loc *time.Location,
) (workflow.StepRecord, error) {
	rec := workflow.StepRecord{
		Kind: "compare",
		Name: job.Name,
	}

	current, ok := windowReports[job.Current]
	if !ok {
		err := clierr.Errorf(clierr.KindError, "%s", i18n.Tf("error.workflowCompareNoCurrent", map[string]any{"Job": job.Name, "Window": job.Current}))
		rec.Status = "failed"
		rec.Error = err.Error()
		return rec, err
	}
	baseline, ok := windowReports[job.Baseline]
	if !ok {
		err := clierr.Errorf(clierr.KindError, "%s", i18n.Tf("error.workflowCompareNoBaseline", map[string]any{"Job": job.Name, "Window": job.Baseline}))
		rec.Status = "failed"
		rec.Error = err.Error()
		return rec, err
	}

	result := comparepkg.BuildCompareResult(current, baseline)

	for _, format := range job.Formats {
		var output string
		var err error
		switch format {
		case "json":
			output, err = comparepkg.RenderJSON(result)
		case "html":
			output, err = comparepkg.RenderHTMLIn(result, loc)
		default:
			err = clierr.Errorf(clierr.KindUsage, "%s", i18n.Tf("error.workflowStepFormat", map[string]any{"Value": format}))
		}
		if err != nil {
			rec.Status = "failed"
			rec.Error = err.Error()
			return rec, clierr.Errorf(clierr.KindError, "render compare %q (%s): %w", job.Name, format, err)
		}

		artifactPath := workflow.ArtifactPath(outputDir, "compare", job.Name, format)
		if err := os.WriteFile(artifactPath, []byte(output), 0o644); err != nil {
			rec.Status = "failed"
			rec.Error = err.Error()
			return rec, clierr.Errorf(clierr.KindOutput, "write compare artifact %q: %w", job.Name, err)
		}
		rec.Artifacts = append(rec.Artifacts, "compare/"+job.Name+"."+format)
	}

	rec.Status = "success"
	rec.Execution = "executed"
	return rec, nil
}

func runWorkflowTrendJob(
	outputDir string,
	job workflow.TrendJob,
	windowReports map[string]comparepkg.InputReport,
	loc *time.Location,
) (workflow.StepRecord, error) {
	rec := workflow.StepRecord{
		Kind: "trend",
		Name: job.Name,
	}

	points := make([]trendpkg.BuildInput, 0, len(job.Snapshots))
	for _, snapName := range job.Snapshots {
		reportInput, ok := windowReports[snapName]
		if !ok {
			err := clierr.Errorf(clierr.KindError, "%s", i18n.Tf("error.workflowTrendNoSnapshot", map[string]any{"Job": job.Name, "Snapshot": snapName}))
			rec.Status = "failed"
			rec.Error = err.Error()
			return rec, err
		}
		points = append(points, trendpkg.BuildInput{
			Path:   fmt.Sprintf("analyze/%s.json", snapName),
			Report: trendpkg.InputReport(reportInput),
		})
	}

	result, err := trendpkg.BuildResult(trendpkg.BuildOptions{
		InputMode: "workflow",
		Points:    points,
		TopTables: 10,
		Order:     "cli",
	})
	if err != nil {
		rec.Status = "failed"
		rec.Error = err.Error()
		return rec, clierr.Errorf(clierr.KindError, "build trend %q: %w", job.Name, err)
	}

	for _, format := range job.Formats {
		var output string
		var renderErr error
		switch format {
		case "json":
			output, renderErr = trendpkg.RenderJSON(result)
		case "html":
			output, renderErr = trendpkg.RenderHTMLIn(result, loc)
		default:
			renderErr = clierr.Errorf(clierr.KindUsage, "%s", i18n.Tf("error.workflowStepFormat", map[string]any{"Value": format}))
		}
		if renderErr != nil {
			rec.Status = "failed"
			rec.Error = renderErr.Error()
			return rec, clierr.Errorf(clierr.KindError, "render trend %q (%s): %w", job.Name, format, renderErr)
		}

		artifactPath := workflow.ArtifactPath(outputDir, "trend", job.Name, format)
		if err := os.WriteFile(artifactPath, []byte(output), 0o644); err != nil {
			rec.Status = "failed"
			rec.Error = err.Error()
			return rec, clierr.Errorf(clierr.KindOutput, "write trend artifact %q: %w", job.Name, err)
		}
		rec.Artifacts = append(rec.Artifacts, "trend/"+job.Name+"."+format)
	}

	rec.Status = "success"
	rec.Execution = "executed"
	return rec, nil
}

func buildWorkflowAnalyzerOptions(plan workflow.Plan) analyzer.Options {
	opts := analyzer.DefaultOptions()

	da := plan.Defaults.Analyze
	if da.TopTables != 0 {
		opts.TopTables = da.TopTables
	}
	if da.TopTransactions != 0 {
		opts.TopTransactions = da.TopTransactions
	}
	if da.TopMinutes != 0 {
		opts.TopMinutes = da.TopMinutes
	}
	opts.DetectSpikes = da.DetectSpikes
	if da.LargeTrxRows != 0 {
		opts.LargeTxnRows = da.LargeTrxRows
	}
	if da.LargeTrxDuration != "" {
		if d, err := time.ParseDuration(da.LargeTrxDuration); err == nil {
			opts.LargeTxnDuration = d
		}
	}
	if da.SpikeWindow != 0 {
		opts.SpikeWindow = da.SpikeWindow
	}
	if da.SpikeFactor != 0 {
		opts.SpikeFactor = da.SpikeFactor
	}
	if da.SpikeMinRows != 0 {
		opts.SpikeMinRows = da.SpikeMinRows
	}
	opts.IncludeSchemas = da.IncludeSchemas
	opts.ExcludeSchemas = da.ExcludeSchemas
	opts.IncludeTables = da.IncludeTables
	opts.ExcludeTables = da.ExcludeTables

	return opts
}

func buildWorkflowReportOptions(plan workflow.Plan, loc *time.Location) (report.Options, error) {
	mode := report.SQLContextSummary
	if da := plan.Defaults.Analyze; da.SQLContext != "" {
		var err error
		mode, err = report.ParseSQLContextMode(da.SQLContext)
		if err != nil {
			return report.Options{}, err
		}
	}
	return report.Options{SQLContextMode: mode, Display: loc}, nil
}

// workflowManifestFromJSON is a test helper to parse a manifest JSON file.
func workflowManifestFromJSON(path string) (workflow.Manifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		// Only a failed read is input (4); see ADR 0009.
		return workflow.Manifest{}, clierr.Wrap(clierr.KindInput, err)
	}
	var m workflow.Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		// A manifest that reads but does not decode is corrupt (5).
		return workflow.Manifest{}, clierr.Wrap(clierr.KindCorrupt, err)
	}
	return m, nil
}

func computeFileSHA256(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	h := sha256.Sum256(data)
	return fmt.Sprintf("%x", h[:]), nil
}

func copyWorkflowPlanIntoOutputDir(outputDir string, planPath string) (string, error) {
	data, err := os.ReadFile(planPath)
	if err != nil {
		return "", err
	}
	copiedPlanPath := filepath.Join(outputDir, "plan.yaml")
	if err := os.WriteFile(copiedPlanPath, data, 0o644); err != nil {
		return "", err
	}
	return filepath.Abs(copiedPlanPath)
}
