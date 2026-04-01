package compare

import (
	"os"
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

func TestLoadReportAcceptsEmptySummaryTimestamps(t *testing.T) {
	path := filepath.Join(t.TempDir(), "report.json")
	content := `{
  "summary": {
    "total_transactions": 0,
    "total_rows": 0,
    "total_events": 0,
    "start_time": "",
    "end_time": "",
    "duration": "0s"
  },
  "tables": [],
  "alerts": [],
  "warnings": 0
}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write temp report: %v", err)
	}

	report, err := LoadReport(path)
	if err != nil {
		t.Fatalf("expected valid report, got %v", err)
	}
	if report.Summary.StartTime != "" || report.Summary.EndTime != "" {
		t.Fatalf("expected empty timestamps, got start=%q end=%q", report.Summary.StartTime, report.Summary.EndTime)
	}
}

func TestLoadReportAcceptsValidEmptyReport(t *testing.T) {
	path := filepath.Join(t.TempDir(), "report.json")
	content := `{
  "summary": {
    "total_transactions": 0,
    "total_rows": 0,
    "total_events": 0,
    "start_time": "",
    "end_time": "",
    "duration": "0s"
  },
  "tables": [],
  "alerts": [],
  "warnings": 0
}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write temp report: %v", err)
	}

	report, err := LoadReport(path)
	if err != nil {
		t.Fatalf("expected valid empty report, got %v", err)
	}
	if report.Summary.TotalRows != 0 || len(report.Tables) != 0 || len(report.Alerts) != 0 || report.Warnings != 0 {
		t.Fatalf("unexpected empty report contents: %+v", report)
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

func TestLoadReportRejectsReportsMissingRequiredCompareFields(t *testing.T) {
	tests := []struct {
		name    string
		content string
	}{
		{
			name: "missing summary duration field",
			content: `{
  "summary": {
    "total_transactions": 120,
    "total_rows": 2400,
    "total_events": 3000,
    "start_time": "",
    "end_time": ""
  },
  "tables": [],
  "alerts": [],
  "warnings": 0
}`,
		},
		{
			name: "missing table identifier fields",
			content: `{
  "summary": {
    "total_transactions": 120,
    "total_rows": 2400,
    "total_events": 3000,
    "start_time": "2026-03-20T10:00:00Z",
    "end_time": "2026-03-20T10:30:00Z",
    "duration": "30m0s"
  },
  "tables": [
    {
      "total_rows": 1200,
      "insert_rows": 700,
      "update_rows": 400,
      "delete_rows": 100,
      "txn_count": 80
    }
  ],
  "alerts": [],
  "warnings": 0
}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "report.json")
			if err := os.WriteFile(path, []byte(tt.content), 0o644); err != nil {
				t.Fatalf("write temp report: %v", err)
			}

			_, err := LoadReport(path)
			if err == nil || !strings.Contains(err.Error(), "unsupported BinlogViz report shape") {
				t.Fatalf("expected shape error, got %v", err)
			}
		})
	}
}
