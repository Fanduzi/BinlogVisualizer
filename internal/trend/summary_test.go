package trend

import (
	"testing"
)

func TestBuildTrendSummary_RisingPattern(t *testing.T) {
	result := buildTestResult(3, []testPatternPoint{
		{"p1", "payments.update", 1000, 0.3, 1500, 0.5},
		{"p2", "orders.insert", 800, 0.25, 900, 0.25},
	})
	summary := buildTrendSummary(result)

	if len(summary) == 0 {
		t.Fatal("expected at least one finding for rising pattern")
	}
	hasRising := false
	for _, f := range summary {
		if f.Kind == "rising_pattern" {
			hasRising = true
			if f.Evidence["pattern_key"] != "p1" {
				t.Fatalf("expected rising pattern p1, got %v", f.Evidence["pattern_key"])
			}
		}
	}
	if !hasRising {
		t.Fatalf("expected rising_pattern finding, got %+v", summary)
	}
}

func TestBuildTrendSummary_FallingPattern(t *testing.T) {
	result := buildTestResult(3, []testPatternPoint{
		{"p1", "orders.insert", 1500, 0.5, 800, 0.3},
		{"p2", "payments.update", 600, 0.2, 700, 0.25},
	})
	summary := buildTrendSummary(result)

	hasFalling := false
	for _, f := range summary {
		if f.Kind == "falling_pattern" {
			hasFalling = true
		}
	}
	if !hasFalling {
		t.Fatalf("expected falling_pattern finding, got %+v", summary)
	}
}

func TestBuildTrendSummary_ConcentrationShift(t *testing.T) {
	result := buildTestResult(3, []testPatternPoint{
		{"p1", "dominant", 1000, 0.3, 2500, 0.8},
		{"p2", "other", 700, 0.25, 600, 0.2},
	})
	summary := buildTrendSummary(result)

	hasConcentration := false
	for _, f := range summary {
		if f.Kind == "concentration_shift" {
			hasConcentration = true
		}
	}
	if !hasConcentration {
		t.Fatalf("expected concentration_shift finding, got %+v", summary)
	}
}

func TestBuildTrendSummary_LowSignalFewFindings(t *testing.T) {
	result := buildTestResult(2, []testPatternPoint{
		{"p1", "stable", 1000, 0.5, 1020, 0.5},
	})
	summary := buildTrendSummary(result)

	for _, f := range summary {
		if f.Kind == "rising_pattern" || f.Kind == "falling_pattern" {
			delta, _ := f.Evidence["delta_rows"].(int)
			if delta >= 0 && delta < 100 {
				t.Fatalf("should not emit pattern finding for tiny delta, got %+v", f)
			}
		}
	}
}

func TestBuildTrendSummary_DeterministicOrdering(t *testing.T) {
	result1 := buildTestResult(3, []testPatternPoint{
		{"p1", "payments.update", 1000, 0.3, 1500, 0.5},
		{"p2", "orders.insert", 800, 0.25, 900, 0.25},
	})
	result2 := buildTestResult(3, []testPatternPoint{
		{"p1", "payments.update", 1000, 0.3, 1500, 0.5},
		{"p2", "orders.insert", 800, 0.25, 900, 0.25},
	})

	s1 := buildTrendSummary(result1)
	s2 := buildTrendSummary(result2)

	if len(s1) != len(s2) {
		t.Fatalf("non-deterministic length: %d vs %d", len(s1), len(s2))
	}
	for i := range s1 {
		if s1[i].Kind != s2[i].Kind {
			t.Fatalf("non-deterministic order at %d: %q vs %q", i, s1[i].Kind, s2[i].Kind)
		}
	}
}

func TestBuildTrendEvidenceRefsCoversSupportedFindingKinds(t *testing.T) {
	result := Result{
		TrendSummary: []TrendFinding{
			{Kind: "rising_pattern", Evidence: map[string]any{"pattern_key": "orders.insert_batch"}},
			{Kind: "falling_pattern", Evidence: map[string]any{"pattern_key": "orders.update_single"}},
			{Kind: "table_trend", Evidence: map[string]any{"table": "shop.orders"}},
			{Kind: "concentration_shift", Evidence: map[string]any{"pattern_key": "orders.insert_batch"}},
			{Kind: "spike_outlier", Evidence: map[string]any{"snapshot_name": "week3"}},
		},
		PatternTrends: []PatternTrend{
			{PatternKey: "orders.insert_batch", Label: "orders.insert_batch"},
			{PatternKey: "orders.update_single", Label: "orders.update_single"},
		},
		TableTrends: []TableTrend{
			{Schema: "shop", Table: "orders", DeltaRows: 1000},
		},
		Points: []Point{
			{Snapshot: SnapshotMeta{Name: "week1"}},
			{Snapshot: SnapshotMeta{Name: "week2"}},
			{Snapshot: SnapshotMeta{Name: "week3"}},
		},
	}

	buildTrendEvidenceRefs(&result)

	want := []struct {
		idx     int
		section string
		anchor  string
	}{
		{0, "pattern_trends", "pattern-0"},
		{1, "pattern_trends", "pattern-1"},
		{2, "table_trends", "table-0"},
		{3, "pattern_trends", "pattern-0"},
		{4, "ordered_points", "point-2"},
	}
	for _, tc := range want {
		refs := result.TrendSummary[tc.idx].EvidenceRefs
		if len(refs) != 1 {
			t.Fatalf("finding %d refs len = %d, want 1: %#v", tc.idx, len(refs), refs)
		}
		if refs[0].Section != tc.section || refs[0].Anchor != tc.anchor {
			t.Fatalf("finding %d ref = %#v, want section %q anchor %q", tc.idx, refs[0], tc.section, tc.anchor)
		}
		if refs[0].Label == "" {
			t.Fatalf("finding %d ref label should not be empty", tc.idx)
		}
	}
}

func TestBuildTrendEvidenceRefsOmitsMissingTargets(t *testing.T) {
	result := Result{
		TrendSummary: []TrendFinding{
			{Kind: "rising_pattern", Evidence: map[string]any{"pattern_key": "missing.pattern"}},
			{Kind: "table_trend", Evidence: map[string]any{"table": "missing.table"}},
			{Kind: "spike_outlier", Evidence: map[string]any{"snapshot_name": "missing-point"}},
		},
	}

	buildTrendEvidenceRefs(&result)

	for i, f := range result.TrendSummary {
		if len(f.EvidenceRefs) != 0 {
			t.Fatalf("finding %d refs = %#v, want none", i, f.EvidenceRefs)
		}
	}
}

func TestBuildTrendEvidenceRefs_RisingPatternHasRef(t *testing.T) {
	result := buildTestResult(3, []testPatternPoint{
		{"p1", "payments.update", 1000, 0.3, 1500, 0.5},
		{"p2", "orders.insert", 800, 0.25, 900, 0.25},
	})
	summary := buildTrendSummary(result)
	tempResult := result
	tempResult.TrendSummary = summary
	buildTrendEvidenceRefs(&tempResult)

	var rising *TrendFinding
	for i := range tempResult.TrendSummary {
		if tempResult.TrendSummary[i].Kind == "rising_pattern" {
			rising = &tempResult.TrendSummary[i]
			break
		}
	}
	if rising == nil {
		t.Fatal("expected rising_pattern finding")
	}
	if len(rising.EvidenceRefs) == 0 {
		t.Fatal("expected rising_pattern to have evidence_refs")
	}
	ref := rising.EvidenceRefs[0]
	if ref.Section != "pattern_trends" {
		t.Fatalf("expected section pattern_trends, got %q", ref.Section)
	}
	if ref.Anchor == "" {
		t.Fatal("expected non-empty anchor")
	}
}

func TestBuildTrendEvidenceRefs_TableTrendHasRef(t *testing.T) {
	result := buildTestResult(3, []testPatternPoint{
		{"p1", "payments.update", 1000, 0.3, 1500, 0.5},
	})
	summary := buildTrendSummary(result)
	tempResult := result
	tempResult.TrendSummary = summary
	buildTrendEvidenceRefs(&tempResult)

	var tableTrend *TrendFinding
	for i := range tempResult.TrendSummary {
		if tempResult.TrendSummary[i].Kind == "table_trend" {
			tableTrend = &tempResult.TrendSummary[i]
			break
		}
	}
	if tableTrend == nil {
		t.Fatal("expected table_trend finding")
	}
	if len(tableTrend.EvidenceRefs) == 0 {
		t.Fatal("expected table_trend to have evidence_refs")
	}
	ref := tableTrend.EvidenceRefs[0]
	if ref.Section != "table_trends" {
		t.Fatalf("expected section table_trends, got %q", ref.Section)
	}
}

func TestBuildTrendEvidenceRefs_LowSignalNoFalseRefs(t *testing.T) {
	result := buildTestResult(2, []testPatternPoint{
		{"p1", "stable", 1000, 0.5, 1020, 0.5},
	})
	summary := buildTrendSummary(result)
	tempResult := result
	tempResult.TrendSummary = summary
	buildTrendEvidenceRefs(&tempResult)

	for _, f := range tempResult.TrendSummary {
		for _, ref := range f.EvidenceRefs {
			switch ref.Section {
			case "pattern_trends", "table_trends", "ordered_points", "aggregate_insights":
				// valid
			default:
				t.Fatalf("unexpected section %q in evidence ref", ref.Section)
			}
		}
	}
}

// testPatternPoint describes a single pattern trend for test data generation.
type testPatternPoint struct {
	key          string
	label        string
	firstRows    int
	firstShare   float64
	lastRows     int
	lastShare    float64
}

func buildTestResult(numPoints int, patterns []testPatternPoint) Result {
	points := make([]Point, numPoints)
	for i := range points {
		points[i] = Point{
			Snapshot: SnapshotMeta{Name: "snap", Window: InputSnapshotWindow{StartTime: "2026-03-20T10:00:00Z"}},
			Summary:  PointSummary{TotalRows: 3000 + i*500, TotalTransactions: 100 + i*20},
		}
	}

	patternTrends := make([]PatternTrend, len(patterns))
	for i, p := range patterns {
		shareSeries := make([]PatternTrendSharePoint, numPoints)
		rowsSeries := make([]PatternTrendRowsPoint, numPoints)
		for j := 0; j < numPoints; j++ {
			frac := float64(j) / float64(numPoints-1)
			if numPoints == 1 {
				frac = 0
			}
			rows := int(float64(p.firstRows) + float64(p.lastRows-p.firstRows)*frac)
			share := p.firstShare + (p.lastShare-p.firstShare)*frac
			shareSeries[j] = PatternTrendSharePoint{SnapshotName: "snap", ShareOfRows: share}
			rowsSeries[j] = PatternTrendRowsPoint{SnapshotName: "snap", Rows: rows}
		}
		patternTrends[i] = PatternTrend{
			PatternKey:       p.key,
			Label:            p.label,
			FirstRows:        p.firstRows,
			LastRows:         p.lastRows,
			DeltaRows:        p.lastRows - p.firstRows,
			FirstShareOfRows: p.firstShare,
			LastShareOfRows:  p.lastShare,
			DeltaShareOfRows: p.lastShare - p.firstShare,
			RowsSeries:       rowsSeries,
			ShareOfRowsSeries: shareSeries,
		}
	}

	tableTrends := []TableTrend{
		{Schema: "s", Table: "t1", FirstRows: 1000, LastRows: 1500, DeltaRows: 500},
	}

	return Result{
		Points:        points,
		PatternTrends: patternTrends,
		TableTrends:   tableTrends,
		Insights: Insights{
			FirstSnapshot: "snap0",
			LastSnapshot:  "snap_last",
			RowsDelta:     points[len(points)-1].Summary.TotalRows - points[0].Summary.TotalRows,
		},
	}
}
