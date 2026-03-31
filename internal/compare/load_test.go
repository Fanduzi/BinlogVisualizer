package compare

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadReportLoadsValidBinlogVizJSON(t *testing.T) {
	report, err := LoadReport(filepath.Join("testdata", "current.json"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if report.Summary.TotalRows != 2400 {
		t.Fatalf("expected total rows 2400, got %d", report.Summary.TotalRows)
	}
	if len(report.Tables) != 2 {
		t.Fatalf("expected 2 tables, got %d", len(report.Tables))
	}
}

func TestLoadReportRejectsInvalidJSON(t *testing.T) {
	_, err := LoadReport(filepath.Join("testdata", "invalid.json"))
	if err == nil || !strings.Contains(err.Error(), "decode compare input") {
		t.Fatalf("expected decode error, got %v", err)
	}
}

func TestLoadReportRejectsForeignJSON(t *testing.T) {
	_, err := LoadReport(filepath.Join("testdata", "foreign.json"))
	if err == nil || !strings.Contains(err.Error(), "unsupported BinlogViz report shape") {
		t.Fatalf("expected shape error, got %v", err)
	}
}
