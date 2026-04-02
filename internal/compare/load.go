// Package compare loads and validates compare input reports.
// input: filesystem paths to BinlogViz JSON reports.
// output: validated InputReport values or explicit read/decode/shape errors.
// pos: compare command ingress before diff construction and rendering.
package compare

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

type rawInputReport struct {
	Summary  *rawInputSummary   `json:"summary"`
	Tables   *[]rawInputTable   `json:"tables"`
	Alerts   *[]json.RawMessage `json:"alerts"`
	Warnings *int               `json:"warnings"`
}

type rawInputSummary struct {
	TotalTransactions *int    `json:"total_transactions"`
	TotalRows         *int    `json:"total_rows"`
	TotalEvents       *int    `json:"total_events"`
	StartTime         *string `json:"start_time"`
	EndTime           *string `json:"end_time"`
	Duration          *string `json:"duration"`
}

type rawInputTable struct {
	Schema *string `json:"schema"`
	Table  *string `json:"table"`
}

func LoadReport(path string) (InputReport, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return InputReport{}, fmt.Errorf("read compare input %s: %w", path, err)
	}

	report, err := DecodeReportJSON(data)
	if err != nil {
		if strings.Contains(err.Error(), "unsupported BinlogViz report shape") {
			return InputReport{}, err
		}
		return InputReport{}, fmt.Errorf("decode compare input %s: %w", path, err)
	}
	return report, nil
}

// DecodeReportJSON decodes and validates an analyze JSON payload for compare-compatible use.
func DecodeReportJSON(data []byte) (InputReport, error) {
	var raw rawInputReport
	if err := json.Unmarshal(data, &raw); err != nil {
		return InputReport{}, err
	}
	if err := validateRawInputReport(raw); err != nil {
		return InputReport{}, err
	}

	var report InputReport
	if err := json.Unmarshal(data, &report); err != nil {
		return InputReport{}, err
	}
	return report, nil
}

func validateRawInputReport(report rawInputReport) error {
	if report.Summary == nil || report.Tables == nil || report.Alerts == nil || report.Warnings == nil {
		return fmt.Errorf("unsupported BinlogViz report shape")
	}
	if report.Summary.TotalTransactions == nil || report.Summary.TotalRows == nil || report.Summary.TotalEvents == nil {
		return fmt.Errorf("unsupported BinlogViz report shape")
	}
	if report.Summary.StartTime == nil || report.Summary.EndTime == nil || report.Summary.Duration == nil {
		return fmt.Errorf("unsupported BinlogViz report shape")
	}
	if strings.TrimSpace(*report.Summary.Duration) == "" {
		return fmt.Errorf("unsupported BinlogViz report shape")
	}
	for _, table := range *report.Tables {
		if table.Schema == nil || strings.TrimSpace(*table.Schema) == "" {
			return fmt.Errorf("unsupported BinlogViz report shape")
		}
		if table.Table == nil || strings.TrimSpace(*table.Table) == "" {
			return fmt.Errorf("unsupported BinlogViz report shape")
		}
	}
	return nil
}
