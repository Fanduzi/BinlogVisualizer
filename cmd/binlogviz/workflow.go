package binlogviz

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"binlogviz/internal/analyzer"
	"binlogviz/internal/binlog"
	comparepkg "binlogviz/internal/compare"
	"binlogviz/internal/model"
	"binlogviz/internal/report"
	"binlogviz/internal/snapshot"
	trendpkg "binlogviz/internal/trend"
	"binlogviz/internal/version"
	"binlogviz/internal/workflow"
)

func newWorkflowCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "workflow",
		Short: "Run multi-step BinlogViz investigation workflows",
		Run: func(cmd *cobra.Command, args []string) {
			_ = cmd.Help()
		},
	}
	cmd.AddCommand(newWorkflowRunCommand())
	cmd.AddCommand(newWorkflowResumeCommand())
	cmd.AddCommand(newWorkflowValidateCommand())
	cmd.AddCommand(newWorkflowDescribeCommand())
	return cmd
}

type workflowDescribeOptions struct {
	format string
}

func newWorkflowDescribeCommand() *cobra.Command {
	opts := &workflowDescribeOptions{}

	cmd := &cobra.Command{
		Use:   "describe <plan.yaml>",
		Short: "Describe a workflow plan without executing it",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if opts.format != "text" && opts.format != "json" {
				return fmt.Errorf("unsupported workflow describe format %q (allowed: text, json)", opts.format)
			}

			planPath := args[0]
			f, err := os.Open(planPath)
			if err != nil {
				writeWorkflowValidateFailure(cmd.OutOrStdout(), opts.format, fmt.Errorf("open workflow plan: %w", err))
				return err
			}
			defer f.Close()

			plan, err := workflow.LoadPlan(f)
			if err != nil {
				writeWorkflowValidateFailure(cmd.OutOrStdout(), opts.format, err)
				return err
			}

			desc := workflow.BuildDescription(plan)
			return writeWorkflowDescribeOutput(cmd.OutOrStdout(), opts.format, desc)
		},
	}

	cmd.Flags().StringVar(&opts.format, "format", "text", "Output format: text or json")

	return cmd
}

type workflowValidateOptions struct {
	format string
}

func newWorkflowValidateCommand() *cobra.Command {
	opts := &workflowValidateOptions{}

	cmd := &cobra.Command{
		Use:   "validate <plan.yaml>",
		Short: "Validate a workflow plan without executing it",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if opts.format != "text" && opts.format != "json" {
				return fmt.Errorf("unsupported workflow validate format %q (allowed: text, json)", opts.format)
			}

			planPath := args[0]
			f, err := os.Open(planPath)
			if err != nil {
				writeWorkflowValidateFailure(cmd.OutOrStdout(), opts.format, fmt.Errorf("open workflow plan: %w", err))
				return err
			}
			defer f.Close()

			plan, err := workflow.LoadPlan(f)
			if err != nil {
				writeWorkflowValidateFailure(cmd.OutOrStdout(), opts.format, err)
				return err
			}

			return writeWorkflowValidateSuccess(cmd.OutOrStdout(), opts.format, plan)
		},
	}

	cmd.Flags().StringVar(&opts.format, "format", "text", "Output format: text or json")

	return cmd
}

type workflowRunOptions struct {
	outputDir   string
	snapshotDir string
}

func newWorkflowRunCommand() *cobra.Command {
	opts := &workflowRunOptions{}

	cmd := &cobra.Command{
		Use:   "run <plan.yaml>",
		Short: "Execute a workflow plan",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			planPath := args[0]
			stderr := cmd.OutOrStderr()

			f, err := os.Open(planPath)
			if err != nil {
				return fmt.Errorf("open workflow plan: %w", err)
			}
			defer f.Close()

			plan, err := workflow.LoadPlan(f)
			if err != nil {
				return err
			}

			outputDir := plan.Workflow.OutputDir
			if opts.outputDir != "" {
				outputDir = opts.outputDir
			}
			absPlanPath, _ := filepath.Abs(planPath)

			return executeWorkflow(plan, outputDir, opts.snapshotDir, absPlanPath, stderr)
		},
	}

	cmd.Flags().StringVar(&opts.outputDir, "output-dir", "", "Override the plan-defined output directory")
	cmd.Flags().StringVar(&opts.snapshotDir, "snapshot-dir", "", "Override the snapshot storage directory")

	return cmd
}

type workflowResumeOptions struct {
	snapshotDir string
	rerun       []string
}

func newWorkflowResumeCommand() *cobra.Command {
	opts := &workflowResumeOptions{}

	cmd := &cobra.Command{
		Use:   "resume <output_dir>",
		Short: "Resume a previously run workflow from its output directory",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			outputDir := args[0]
			stderr := cmd.OutOrStderr()

			return executeResume(outputDir, opts.snapshotDir, opts.rerun, stderr)
		},
	}

	cmd.Flags().StringVar(&opts.snapshotDir, "snapshot-dir", "", "Override the snapshot storage directory")
	cmd.Flags().StringArrayVar(&opts.rerun, "rerun", nil, "Explicit step to rerun (repeatable, format: kind:name)")

	return cmd
}

func writeWorkflowValidateSuccess(out io.Writer, format string, plan workflow.Plan) error {
	payload := struct {
		Valid        bool   `json:"valid"`
		WorkflowName string `json:"workflow_name"`
		Windows      int    `json:"windows"`
		CompareJobs  int    `json:"compare_jobs"`
		TrendJobs    int    `json:"trend_jobs"`
		OutputDir    string `json:"output_dir"`
	}{
		Valid:        true,
		WorkflowName: plan.Workflow.Name,
		Windows:      len(plan.Windows),
		CompareJobs:  len(plan.Compare),
		TrendJobs:    len(plan.Trend),
		OutputDir:    plan.Workflow.OutputDir,
	}

	if format == "json" {
		data, err := json.MarshalIndent(payload, "", "  ")
		if err != nil {
			return fmt.Errorf("encode workflow validate output: %w", err)
		}
		_, err = fmt.Fprintln(out, string(data))
		return err
	}

	_, err := fmt.Fprintf(out, "Workflow plan valid\n- workflow: %s\n- windows: %d\n- compare jobs: %d\n- trend jobs: %d\n- output root: %s\n",
		plan.Workflow.Name,
		len(plan.Windows),
		len(plan.Compare),
		len(plan.Trend),
		plan.Workflow.OutputDir,
	)
	return err
}

func writeWorkflowValidateFailure(out io.Writer, format string, err error) {
	if format == "json" {
		data, marshalErr := json.MarshalIndent(struct {
			Valid bool   `json:"valid"`
			Error string `json:"error"`
		}{
			Valid: false,
			Error: err.Error(),
		}, "", "  ")
		if marshalErr == nil {
			_, _ = fmt.Fprintln(out, string(data))
			return
		}
	}
	_, _ = fmt.Fprintf(out, "Workflow plan invalid\n- %s\n", err.Error())
}

func writeWorkflowDescribeOutput(out io.Writer, format string, desc workflow.Description) error {
	if format == "json" {
		data, err := json.MarshalIndent(desc, "", "  ")
		if err != nil {
			return fmt.Errorf("encode workflow describe output: %w", err)
		}
		_, err = fmt.Fprintln(out, string(data))
		return err
	}

	if _, err := fmt.Fprintf(out, "Workflow: %s\nOutput Root: %s\nSnapshot Save: %t\n\n",
		desc.WorkflowName,
		desc.OutputDir,
		desc.SnapshotSave,
	); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(out, "Analyze Windows"); err != nil {
		return err
	}
	for _, window := range desc.Windows {
		if _, err := fmt.Fprintf(out, "- %s: %s -> %s\n", window.Name, window.Start, window.End); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(out, "  artifacts: %s\n", strings.Join(window.Artifacts, ", ")); err != nil {
			return err
		}
		if window.SnapshotName != "" {
			if _, err := fmt.Fprintf(out, "  snapshot: %s\n", window.SnapshotName); err != nil {
				return err
			}
		}
	}
	if _, err := fmt.Fprintln(out, "\nCompare Jobs"); err != nil {
		return err
	}
	for _, job := range desc.Compare {
		if _, err := fmt.Fprintf(out, "- %s\n", job.Name); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(out, "  depends on: %s, %s\n", job.Current, job.Baseline); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(out, "  artifacts: %s\n", strings.Join(job.Artifacts, ", ")); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintln(out, "\nTrend Jobs"); err != nil {
		return err
	}
	for _, job := range desc.Trend {
		if _, err := fmt.Fprintf(out, "- %s\n", job.Name); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(out, "  depends on: %s\n", strings.Join(job.Snapshots, ", ")); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(out, "  artifacts: %s\n", strings.Join(job.Artifacts, ", ")); err != nil {
			return err
		}
	}
	return nil
}

func executeResume(outputDir, snapshotDir string, rerunSelectors []string, stderr io.Writer) error {
	startedAt := time.Now().UTC()

	// Load existing manifest
	manifestPath := filepath.Join(outputDir, "manifest.json")
	mf, err := workflowManifestFromJSON(manifestPath)
	if err != nil {
		return fmt.Errorf("load manifest from %s: %w", outputDir, err)
	}

	// Early validation before trying to load the plan
	if mf.ManifestVersion == 0 {
		return fmt.Errorf("cannot resume: manifest has no manifest_version (legacy format); run a fresh workflow instead")
	}
	if mf.ManifestVersion != 2 {
		return fmt.Errorf("cannot resume: manifest_version %d is not supported (only version 2 is supported)", mf.ManifestVersion)
	}
	if mf.PlanPath == "" {
		return fmt.Errorf("cannot resume: manifest has no plan_path")
	}

	// Load the plan referenced by the manifest
	planPath := mf.PlanPath

	f, err := os.Open(planPath)
	if err != nil {
		return fmt.Errorf("open plan file %s: %w", planPath, err)
	}
	defer f.Close()

	plan, err := workflow.LoadPlan(f)
	if err != nil {
		return fmt.Errorf("load plan: %w", err)
	}

	// Compute current plan hash and validate resumability
	planSHA256, err := computeFileSHA256(planPath)
	if err != nil {
		return fmt.Errorf("hash plan file: %w", err)
	}

	if err := workflow.ValidateResumableManifest(mf, planPath, planSHA256); err != nil {
		return err
	}

	// Use manifest's snapshot_dir if not overridden
	effectiveSnapshotDir := mf.SnapshotDir
	if snapshotDir != "" {
		effectiveSnapshotDir = snapshotDir
	}

	// Build the resume plan
	resumePlan, err := workflow.BuildResumePlan(plan, mf, rerunSelectors, outputDir, effectiveSnapshotDir)
	if err != nil {
		return err
	}

	fmt.Fprintf(stderr, "workflow %q: resuming (attempt %d)\n", plan.Workflow.Name, resumePlan.UpdatedManifest.Attempt)

	// Build analyzer and report options from plan defaults
	analyzerOpts := buildWorkflowAnalyzerOptions(plan)
	reportOpts, err := buildWorkflowReportOptions(plan)
	if err != nil {
		return err
	}

	// Initialize the updated manifest
	newMf := resumePlan.UpdatedManifest
	newMf.RunStartedAt = startedAt.Format(time.RFC3339)
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
				return fmt.Errorf("internal error: no prior step for reusable %s:%s", planned.Kind, planned.Name)
			}
			reused := *priorStep
			reused.Execution = "reused"
			newMf.Steps = append(newMf.Steps, reused)

			// For reused analyze steps, we need to load the report for downstream consumption
			if planned.Kind == "analyze" {
				artifactPath := filepath.Join(outputDir, "analyze", planned.Name+".json")
				data, err := os.ReadFile(artifactPath)
				if err != nil {
					return fmt.Errorf("read reusable analyze artifact %s: %w", artifactPath, err)
				}
				reportInput, err := comparepkg.DecodeReportJSON(data)
				if err != nil {
					return fmt.Errorf("decode reusable analyze report for %q: %w", planned.Name, err)
				}
				windowReports[planned.Name] = reportInput
			}

			fmt.Fprintf(stderr, "  reuse %s %q\n", planned.Kind, planned.Name)
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
			rec, cerr := runWorkflowCompareJob(outputDir, job, windowReports)
			newMf.Steps = append(newMf.Steps, rec)
			if cerr != nil {
				return finalizeWorkflow(outputDir, &newMf, startedAt, stderr, cerr)
			}

		case "trend":
			job := findTrendJob(plan, planned.Name)
			rec, terr := runWorkflowTrendJob(outputDir, job, windowReports)
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

func executeWorkflow(plan workflow.Plan, outputDir, snapshotDir, planPath string, stderr io.Writer) error {
	startedAt := time.Now().UTC()

	fmt.Fprintf(stderr, "workflow %q: starting\n", plan.Workflow.Name)

	if err := workflow.EnsureLayout(outputDir); err != nil {
		return fmt.Errorf("create output layout: %w", err)
	}

	planSHA256, err := computeFileSHA256(planPath)
	if err != nil {
		return fmt.Errorf("hash plan file: %w", err)
	}

	mf := workflow.Manifest{
		ManifestVersion:     2,
		Mode:                "run",
		Attempt:             1,
		WorkflowName:        plan.Workflow.Name,
		WorkflowPlanVersion: plan.Version,
		BinlogvizVersion:    version.Version,
		PlanPath:            planPath,
		PlanSHA256:          planSHA256,
		SnapshotDir:         snapshotDir,
		RunStartedAt:        startedAt.Format(time.RFC3339),
		Status:              "success",
		Steps:               []workflow.StepRecord{},
	}

	// Resolve input paths via discovery mode
	paths, err := discoverBinlogPaths(plan.Defaults.Input.FromDir, plan.Defaults.Input.Prefix)
	if err != nil {
		return finalizeWorkflow(outputDir, &mf, startedAt, stderr, fmt.Errorf("discover binlog files: %w", err))
	}
	mf.ResolvedInputFiles = paths

	// Build analyzer options from plan defaults
	analyzerOpts := buildWorkflowAnalyzerOptions(plan)
	reportOpts, err := buildWorkflowReportOptions(plan)
	if err != nil {
		return finalizeWorkflow(outputDir, &mf, startedAt, stderr, err)
	}

	// --- Analyze phase ---
	windowReports := make(map[string]comparepkg.InputReport, len(plan.Windows))
	for _, w := range plan.Windows {
		fmt.Fprintf(stderr, "  analyze %q\n", w.Name)
		rec, _, reportInput, aerr := runWorkflowAnalyzeWindow(
			paths, analyzerOpts, reportOpts, plan, w, outputDir, snapshotDir, stderr,
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
		rec, cerr := runWorkflowCompareJob(outputDir, job, windowReports)
		mf.Steps = append(mf.Steps, rec)
		if cerr != nil {
			return finalizeWorkflow(outputDir, &mf, startedAt, stderr, cerr)
		}
	}

	// --- Trend phase ---
	for _, job := range plan.Trend {
		fmt.Fprintf(stderr, "  trend %q\n", job.Name)
		rec, terr := runWorkflowTrendJob(outputDir, job, windowReports)
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

	manifestPath := filepath.Join(outputDir, "manifest.json")
	if writeErr := workflow.WriteManifest(manifestPath, *mf); writeErr != nil {
		fmt.Fprintf(stderr, "workflow: failed to write manifest: %v\n", writeErr)
		if stepErr == nil {
			return fmt.Errorf("write manifest: %w", writeErr)
		}
	} else {
		fmt.Fprintf(stderr, "workflow: manifest written to %s\n", manifestPath)
	}

	// Write index.html on both success and failure paths
	if indexErr := writeWorkflowIndex(outputDir, *mf); indexErr != nil {
		fmt.Fprintf(stderr, "workflow: failed to write index: %v\n", indexErr)
		if stepErr == nil {
			return fmt.Errorf("write index: %w", indexErr)
		}
	} else {
		fmt.Fprintf(stderr, "workflow: index written to %s\n", filepath.Join(outputDir, "index.html"))
	}

	if stepErr != nil {
		fmt.Fprintf(stderr, "workflow %q: failed\n", mf.WorkflowName)
	}
	return stepErr
}

func writeWorkflowIndex(outputDir string, manifest workflow.Manifest) error {
	html, err := workflow.RenderIndex(workflow.IndexInput{
		OutputRoot: outputDir,
		Manifest:   manifest,
	})
	if err != nil {
		return fmt.Errorf("render workflow index: %w", err)
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
		return rec, "", comparepkg.InputReport{}, fmt.Errorf("analyze window %q: %w", w.Name, err)
	}

	// Resolve snapshot metadata
	result.Snapshot = resolveAnalyzeSnapshotMetadata(snapshotMeta, result.Summary)

	// Render to JSON
	payload, err := report.RenderJSONWithOptions(*result, reportOpts)
	if err != nil {
		rec.Status = "failed"
		rec.Error = err.Error()
		return rec, "", comparepkg.InputReport{}, fmt.Errorf("render analyze JSON for %q: %w", w.Name, err)
	}

	// Write to artifact path
	artifactPath := workflow.ArtifactPath(outputDir, "analyze", w.Name, "json")
	if err := os.WriteFile(artifactPath, []byte(payload), 0o644); err != nil {
		rec.Status = "failed"
		rec.Error = err.Error()
		return rec, "", comparepkg.InputReport{}, fmt.Errorf("write analyze artifact for %q: %w", w.Name, err)
	}

	// Save snapshot if configured (must succeed before marking step success)
	if plan.Defaults.Snapshot.Save {
		var savedPath string
		var err error
		if overwriteSnapshot {
			savedPath, err = snapshot.SaveJSONOverwrite(snapshotDir, w.Name, []byte(payload))
		} else {
			savedPath, err = snapshot.SaveJSON(snapshotDir, w.Name, []byte(payload))
		}
		if err != nil {
			rec.Status = "failed"
			rec.Error = err.Error()
			return rec, "", comparepkg.InputReport{}, fmt.Errorf("save snapshot %q: %w", w.Name, err)
		}
		rec.SnapshotName = w.Name
		fmt.Fprintf(stderr, "  saved snapshot %q to %s\n", w.Name, savedPath)
	}

	rec.Status = "success"
	rec.Execution = "executed"
	rec.Artifacts = []string{"analyze/" + w.Name + ".json"}

	// Parse the JSON for downstream compare/trend consumption
	reportInput, err := comparepkg.DecodeReportJSON([]byte(payload))
	if err != nil {
		rec.Status = "failed"
		rec.Error = fmt.Sprintf("decode generated report: %v", err)
		return rec, payload, comparepkg.InputReport{}, fmt.Errorf("decode analyze report for %q: %w", w.Name, err)
	}

	return rec, payload, reportInput, nil
}

// runAnalyzeToResult runs the analysis pipeline and returns the raw result without rendering.
func runAnalyzeToResult(paths []string, opts analyzer.Options, stderr io.Writer) (*model.AnalysisResult, error) {
	progress, err := newAggregateProgress(paths, stderr)
	if err != nil {
		return nil, fmt.Errorf("build parse progress: %w", err)
	}

	store, cleanup, _, err := createDuckDBTempStore("")
	if err != nil {
		return nil, fmt.Errorf("create temp store: %w", err)
	}
	defer cleanup()

	a := analyzer.NewWithStore(opts, store)

	parser := binlog.NewParser()
	handler := func(raw binlog.RawEvent) error {
		normalized, err := binlog.NormalizeRawEvent(raw)
		if err != nil {
			return fmt.Errorf("normalize at position %d: %w", raw.Position, err)
		}
		if normalized == nil {
			return nil
		}
		if err := a.Consume(*normalized); err != nil {
			return fmt.Errorf("consume: %w", err)
		}
		return nil
	}

	if pp, ok := parser.(binlog.ProgressParser); ok {
		if err := pp.ParseFilesWithProgress(paths, func(p binlog.ParseProgress) {
			progress.Advance(p)
		}, handler); err != nil {
			return nil, fmt.Errorf("parse: %w", err)
		}
		for i := range paths {
			progress.FinishFile(i)
		}
	} else {
		if err := parser.ParseFiles(paths, handler); err != nil {
			return nil, fmt.Errorf("parse: %w", err)
		}
	}
	progress.FinishParse()
	progress.Finalizing()

	result, err := a.Finalize()
	if err != nil {
		return nil, fmt.Errorf("finalize: %w", err)
	}
	return result, nil
}

func runWorkflowCompareJob(
	outputDir string,
	job workflow.CompareJob,
	windowReports map[string]comparepkg.InputReport,
) (workflow.StepRecord, error) {
	rec := workflow.StepRecord{
		Kind: "compare",
		Name: job.Name,
	}

	current, ok := windowReports[job.Current]
	if !ok {
		err := fmt.Errorf("compare %q: current window %q report not found", job.Name, job.Current)
		rec.Status = "failed"
		rec.Error = err.Error()
		return rec, err
	}
	baseline, ok := windowReports[job.Baseline]
	if !ok {
		err := fmt.Errorf("compare %q: baseline window %q report not found", job.Name, job.Baseline)
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
			output, err = comparepkg.RenderHTML(result)
		default:
			err = fmt.Errorf("unsupported format %q", format)
		}
		if err != nil {
			rec.Status = "failed"
			rec.Error = err.Error()
			return rec, fmt.Errorf("render compare %q (%s): %w", job.Name, format, err)
		}

		artifactPath := workflow.ArtifactPath(outputDir, "compare", job.Name, format)
		if err := os.WriteFile(artifactPath, []byte(output), 0o644); err != nil {
			rec.Status = "failed"
			rec.Error = err.Error()
			return rec, fmt.Errorf("write compare artifact %q: %w", job.Name, err)
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
) (workflow.StepRecord, error) {
	rec := workflow.StepRecord{
		Kind: "trend",
		Name: job.Name,
	}

	points := make([]trendpkg.BuildInput, 0, len(job.Snapshots))
	for _, snapName := range job.Snapshots {
		reportInput, ok := windowReports[snapName]
		if !ok {
			err := fmt.Errorf("trend %q: snapshot %q report not found", job.Name, snapName)
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
	})
	if err != nil {
		rec.Status = "failed"
		rec.Error = err.Error()
		return rec, fmt.Errorf("build trend %q: %w", job.Name, err)
	}

	for _, format := range job.Formats {
		var output string
		var renderErr error
		switch format {
		case "json":
			output, renderErr = trendpkg.RenderJSON(result)
		case "html":
			output, renderErr = trendpkg.RenderHTML(result)
		default:
			renderErr = fmt.Errorf("unsupported format %q", format)
		}
		if renderErr != nil {
			rec.Status = "failed"
			rec.Error = renderErr.Error()
			return rec, fmt.Errorf("render trend %q (%s): %w", job.Name, format, renderErr)
		}

		artifactPath := workflow.ArtifactPath(outputDir, "trend", job.Name, format)
		if err := os.WriteFile(artifactPath, []byte(output), 0o644); err != nil {
			rec.Status = "failed"
			rec.Error = err.Error()
			return rec, fmt.Errorf("write trend artifact %q: %w", job.Name, err)
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

func buildWorkflowReportOptions(plan workflow.Plan) (report.Options, error) {
	mode := report.SQLContextSummary
	if da := plan.Defaults.Analyze; da.SQLContext != "" {
		var err error
		mode, err = report.ParseSQLContextMode(da.SQLContext)
		if err != nil {
			return report.Options{}, err
		}
	}
	return report.Options{SQLContextMode: mode}, nil
}

// workflowManifestFromJSON is a test helper to parse a manifest JSON file.
func workflowManifestFromJSON(path string) (workflow.Manifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return workflow.Manifest{}, err
	}
	var m workflow.Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return workflow.Manifest{}, err
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
