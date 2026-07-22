package report

import (
	"strings"
	"testing"

	"binlogviz/internal/model"
)

func TestFmtOpCell_ZeroDenominator(t *testing.T) {
	got := fmtOpCell(5, 0)
	if !strings.Contains(got, "\u2014") {
		t.Fatalf("expected em dash for zero denominator, got %q", got)
	}
	if !strings.HasPrefix(got, "5") {
		t.Fatalf("expected count 5, got %q", got)
	}
}

func TestFmtOpCell_ZeroCount(t *testing.T) {
	got := fmtOpCell(0, 100)
	if got != "0 (0.0%)" {
		t.Fatalf("expected '0 (0.0%%)', got %q", got)
	}
}

func TestFmtOpCell_NormalPercentage(t *testing.T) {
	got := fmtOpCell(25, 100)
	if got != "25 (25.0%)" {
		t.Fatalf("expected '25 (25.0%%)', got %q", got)
	}
}

func TestFmtOpCell_Rounding(t *testing.T) {
	got := fmtOpCell(1, 3)
	if got != "1 (33.3%)" {
		t.Fatalf("expected '1 (33.3%%)', got %q", got)
	}
}

func TestFmtOpCell_LargeNumbers(t *testing.T) {
	got := fmtOpCell(1234, 5678)
	if got != "1,234 (21.7%)" {
		t.Fatalf("expected '1,234 (21.7%%)', got %q", got)
	}
}

func TestBuildHTMLData_TablePercentages(t *testing.T) {
	result := model.AnalysisResult{
		Summary: model.WorkloadSummary{TotalTransactions: 10, TotalRows: 100},
		Tables: []model.TableStats{
			{
				Schema:     "shop",
				Table:      "orders",
				TotalRows:  100,
				InsertRows: 60,
				UpdateRows: 30,
				DeleteRows: 10,
				DDLCount:   2,
				EventCount: 50,
				TxnCount:   5,
			},
		},
	}

	html, err := RenderHTML(result)
	if err != nil {
		t.Fatalf("RenderHTML() error: %v", err)
	}

	for _, want := range []string{
		"60 (60.0%)",
		"30 (30.0%)",
		"10 (10.0%)",
		"2 (4.0%)",
	} {
		if !strings.Contains(html, want) {
			t.Errorf("expected HTML to contain %q", want)
		}
	}
}

func TestBuildHTMLData_ZeroAffectedRows(t *testing.T) {
	result := model.AnalysisResult{
		Summary: model.WorkloadSummary{TotalTransactions: 1, TotalRows: 0},
		Tables: []model.TableStats{
			{
				Schema:     "shop",
				Table:      "orders",
				TotalRows:  0,
				InsertRows: 0,
				UpdateRows: 0,
				DeleteRows: 0,
				DDLCount:   5,
				EventCount: 10,
				TxnCount:   1,
			},
		},
	}

	html, err := RenderHTML(result)
	if err != nil {
		t.Fatalf("RenderHTML() error: %v", err)
	}

	// DML should show em dash when TotalRows is 0
	if !strings.Contains(html, "0 (\u2014)") {
		t.Error("expected em dash for zero affected rows")
	}
	// DDL should show percentage because EventCount > 0
	if !strings.Contains(html, "5 (50.0%)") {
		t.Error("expected DDL percentage when EventCount > 0")
	}
}

func TestBuildHTMLData_ZeroEvents(t *testing.T) {
	result := model.AnalysisResult{
		Summary: model.WorkloadSummary{TotalTransactions: 1, TotalRows: 50},
		Tables: []model.TableStats{
			{
				Schema:     "shop",
				Table:      "orders",
				TotalRows:  50,
				InsertRows: 50,
				DDLCount:   0,
				EventCount: 0,
				TxnCount:   1,
			},
		},
	}

	html, err := RenderHTML(result)
	if err != nil {
		t.Fatalf("RenderHTML() error: %v", err)
	}

	// INSERT should show percentage
	if !strings.Contains(html, "50 (100.0%)") {
		t.Error("expected INSERT 100%")
	}
	// DDL should show em dash when EventCount is 0
	if !strings.Contains(html, "0 (\u2014)") {
		t.Error("expected em dash for DDL when EventCount is 0")
	}
}

func TestBuildHTMLData_DDLColumnPresent(t *testing.T) {
	result := model.AnalysisResult{
		Summary: model.WorkloadSummary{TotalTransactions: 1, TotalRows: 100},
		Tables: []model.TableStats{
			{
				Schema:     "shop",
				Table:      "orders",
				TotalRows:  100,
				InsertRows: 100,
				DDLCount:   3,
				EventCount: 20,
				TxnCount:   1,
			},
		},
	}

	html, err := RenderHTML(result)
	if err != nil {
		t.Fatalf("RenderHTML() error: %v", err)
	}

	if !strings.Contains(html, "DDL (") {
		t.Error("expected DDL column header")
	}
	if !strings.Contains(html, "3 (15.0%)") {
		t.Error("expected DDL count with percentage")
	}
}

func TestBuildHTMLData_FootnotePresent(t *testing.T) {
	result := model.AnalysisResult{
		Summary: model.WorkloadSummary{TotalTransactions: 1, TotalRows: 100},
		Tables: []model.TableStats{
			{
				Schema:     "shop",
				Table:      "orders",
				TotalRows:  100,
				InsertRows: 100,
				TxnCount:   1,
			},
		},
	}

	html, err := RenderHTML(result)
	if err != nil {
		t.Fatalf("RenderHTML() error: %v", err)
	}

	if !strings.Contains(html, "affected rows") {
		t.Error("expected footnote about affected rows denominator")
	}
	if !strings.Contains(html, "table events") {
		t.Error("expected footnote about table events denominator")
	}
}

func TestBuildHTMLData_Colspan8(t *testing.T) {
	result := model.AnalysisResult{
		Summary: model.WorkloadSummary{TotalTransactions: 1, TotalRows: 100},
		Tables: []model.TableStats{
			{
				Schema:     "shop",
				Table:      "orders",
				TotalRows:  100,
				InsertRows: 100,
				TxnCount:   1,
				Activity:   []model.TableActivityPoint{{Rows: 10}},
			},
		},
	}

	html, err := RenderHTML(result)
	if err != nil {
		t.Fatalf("RenderHTML() error: %v", err)
	}

	if !strings.Contains(html, `colspan="8"`) {
		t.Error("expected colspan=8 for detail row")
	}
}

func TestBuildHTMLData_EmptyTables(t *testing.T) {
	result := model.AnalysisResult{
		Summary: model.WorkloadSummary{TotalTransactions: 0, TotalRows: 0},
		Tables:  []model.TableStats{},
	}

	html, err := RenderHTML(result)
	if err != nil {
		t.Fatalf("RenderHTML() error: %v", err)
	}

	if strings.Contains(html, "INSERT (") {
		t.Error("expected no table headers when no tables")
	}
}
