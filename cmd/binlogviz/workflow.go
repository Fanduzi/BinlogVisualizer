package binlogviz

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
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

func executeWorkflow(plan workflow.Plan, outputDir, snapshotDir, planPath string, stderr io.Writer) error {
	startedAt := time.Now().UTC()

	fmt.Fprintf(stderr, "workflow %q: starting\n", plan.Workflow.Name)

	if err := workflow.EnsureLayout(outputDir); err != nil {
		return fmt.Errorf("create output layout: %w", err)
	}

	mf := workflow.Manifest{
		WorkflowName:        plan.Workflow.Name,
		WorkflowPlanVersion: plan.Version,
		BinlogvizVersion:    version.Version,
		PlanPath:            planPath,
		RunStartedAt:        startedAt.Format(time.RFC3339),
		Status:              "success",
		Steps:               []workflow.StepRecord{},
	}

	// Resolve input paths via discovery mode
	paths, err := discoverBinlogPaths(plan.Defaults.Input.FromDir, plan.Defaults.Input.Prefix)
	if err != nil {
		return finalizeWorkflow(outputDir, &mf, startedAt, stderr, fmt.Errorf("discover binlog files: %w", err))
	}

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
		savedPath, err := snapshot.SaveJSON(snapshotDir, w.Name, []byte(payload))
		if err != nil {
			rec.Status = "failed"
			rec.Error = err.Error()
			return rec, "", comparepkg.InputReport{}, fmt.Errorf("save snapshot %q: %w", w.Name, err)
		}
		rec.SnapshotName = w.Name
		fmt.Fprintf(stderr, "  saved snapshot %q to %s\n", w.Name, savedPath)
	}

	rec.Status = "success"
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
