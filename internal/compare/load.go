// Package compare loads and validates compare input reports.
// input: filesystem paths or bytes containing BinlogViz report-v0-v3 JSON.
// output: validated InputReport values with legacy missing completeness normalized to unknown, or explicit read/decode/shape errors.
// pos: compare command ingress before diff construction and rendering.
// note: if this file changes, keep internal/compare/README.md synchronized.
package compare

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

const currentSupportedReportVersion = 3

type rawInputReport struct {
	ReportVersion *int                   `json:"report_version"`
	Summary       *rawInputSummary       `json:"summary"`
	Tables        *[]rawInputTable       `json:"tables"`
	Transactions  *[]rawInputTransaction `json:"transactions"`
	Alerts        *[]json.RawMessage     `json:"alerts"`
	Warnings      *int                   `json:"warnings"`
}

type rawInputSummary struct {
	TotalTransactions   *int    `json:"total_transactions"`
	PartialTransactions *int    `json:"partial_transactions"`
	UnknownTransactions *int    `json:"unknown_transactions"`
	TotalRows           *int    `json:"total_rows"`
	TotalEvents         *int    `json:"total_events"`
	StartTime           *string `json:"start_time"`
	EndTime             *string `json:"end_time"`
	Duration            *string `json:"duration"`
}

type rawInputTransaction struct {
	Completeness    *string `json:"completeness"`
	ReplayAvailable *bool   `json:"replay_available"`
	ReplayScope     *string `json:"replay_scope"`
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
		if strings.Contains(err.Error(), "unsupported BinlogViz report shape") || strings.Contains(err.Error(), "unsupported report_version") {
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
	if err := normalizeTransactionCompleteness(&report, reportVersion(raw)); err != nil {
		return InputReport{}, err
	}
	return report, nil
}

func reportVersion(report rawInputReport) int {
	if report.ReportVersion == nil {
		return 0
	}
	return *report.ReportVersion
}

func validateRawInputReport(report rawInputReport) error {
	version := reportVersion(report)
	if version < 0 || version > currentSupportedReportVersion {
		return fmt.Errorf("unsupported report_version %d: this BinlogViz build supports up to %d", version, currentSupportedReportVersion)
	}
	if report.Summary == nil || report.Tables == nil || report.Alerts == nil || report.Warnings == nil {
		return fmt.Errorf("unsupported BinlogViz report shape")
	}
	if report.Summary.TotalTransactions == nil || report.Summary.TotalRows == nil || report.Summary.TotalEvents == nil {
		return fmt.Errorf("unsupported BinlogViz report shape")
	}
	if version >= 3 && (report.Summary.PartialTransactions == nil || report.Summary.UnknownTransactions == nil || report.Transactions == nil) {
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

func normalizeTransactionCompleteness(report *InputReport, version int) error {
	if report == nil {
		return nil
	}
	groups := [][]InputTransaction{
		report.Transactions,
		report.Diagnostics.LargestTransactions,
		report.Diagnostics.LongestTransactions,
		report.Diagnostics.WidestTransactions,
	}
	for groupIndex := range groups {
		for txnIndex := range groups[groupIndex] {
			txn := &groups[groupIndex][txnIndex]
			if txn.Completeness == "" && version < 3 {
				txn.Completeness = "unknown"
			}
			if !validTransactionCompleteness(txn.Completeness) {
				return fmt.Errorf("unsupported BinlogViz report shape")
			}
			if version >= 3 && txn.ReplayAvailable == nil {
				return fmt.Errorf("unsupported BinlogViz report shape")
			}
			if txn.ReplayAvailable != nil && *txn.ReplayAvailable && txn.ReplayScope != "full_transaction" {
				return fmt.Errorf("unsupported BinlogViz report shape")
			}
		}
	}
	return nil
}

func validTransactionCompleteness(value string) bool {
	switch value {
	case "complete", "partial_start", "partial_end", "partial_both", "unknown":
		return true
	default:
		return false
	}
}
