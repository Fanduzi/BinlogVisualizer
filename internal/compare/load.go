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

func LoadReport(path string) (InputReport, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return InputReport{}, fmt.Errorf("read compare input %s: %w", path, err)
	}

	var report InputReport
	if err := json.Unmarshal(data, &report); err != nil {
		return InputReport{}, fmt.Errorf("decode compare input %s: %w", path, err)
	}
	if err := validateInputReport(report); err != nil {
		return InputReport{}, err
	}

	return report, nil
}

func validateInputReport(report InputReport) error {
	if report.Summary.TotalRows == 0 && report.Summary.TotalTransactions == 0 && len(report.Tables) == 0 && len(report.Alerts) == 0 {
		return fmt.Errorf("unsupported BinlogViz report shape")
	}
	if report.Tables == nil {
		return fmt.Errorf("unsupported BinlogViz report shape")
	}
	if strings.TrimSpace(report.Summary.StartTime) == "" {
		return fmt.Errorf("unsupported BinlogViz report shape")
	}
	if strings.TrimSpace(report.Summary.EndTime) == "" {
		return fmt.Errorf("unsupported BinlogViz report shape")
	}
	if strings.TrimSpace(report.Summary.Duration) == "" {
		return fmt.Errorf("unsupported BinlogViz report shape")
	}
	for _, table := range report.Tables {
		if strings.TrimSpace(table.Schema) == "" {
			return fmt.Errorf("unsupported BinlogViz report shape")
		}
		if strings.TrimSpace(table.Table) == "" {
			return fmt.Errorf("unsupported BinlogViz report shape")
		}
	}
	return nil
}
