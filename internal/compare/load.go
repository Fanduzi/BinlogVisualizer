// Package compare loads and validates compare input reports.
// input: filesystem paths or bytes containing BinlogViz report-v0-v3 JSON.
// output: validated InputReport values with legacy missing completeness normalized to unknown, or explicit read/decode/shape errors; a report marked diagnostics_mode summary|off is unsupported (9, reduced_diagnostics).
// pos: compare command ingress before diff construction and rendering.
// note: if this file changes, keep internal/compare/README.md synchronized.
package compare

import (
	"binlogviz/internal/clierr"
	"encoding/json"
	"os"
	"strings"
)

const currentSupportedReportVersion = 3

type rawInputReport struct {
	ReportVersion   *int                   `json:"report_version"`
	DiagnosticsMode *string                `json:"diagnostics_mode"`
	Summary         *rawInputSummary       `json:"summary"`
	Tables          *[]rawInputTable       `json:"tables"`
	Transactions    *[]rawInputTransaction `json:"transactions"`
	Alerts          *[]json.RawMessage     `json:"alerts"`
	Warnings        *int                   `json:"warnings"`
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
		return InputReport{}, clierr.Errorf(clierr.KindInput, "read compare input %s: %w", path, err)
	}

	report, err := DecodeReportJSON(data)
	if err != nil {
		if strings.Contains(err.Error(), "corrupt BinlogViz report: missing required fields") || strings.Contains(err.Error(), "unsupported report_version") {
			return InputReport{}, err
		}
		return InputReport{}, clierr.Errorf(clierr.KindCorrupt, "decode compare input %s: %w", path, err)
	}
	return report, nil
}

// DecodeReportJSON decodes and validates an analyze JSON payload for compare-compatible use.
func DecodeReportJSON(data []byte) (InputReport, error) {
	var raw rawInputReport
	if err := json.Unmarshal(data, &raw); err != nil {
		return InputReport{}, clierr.Wrap(clierr.KindCorrupt, err)
	}
	if err := validateRawInputReport(raw); err != nil {
		return InputReport{}, err
	}

	var report InputReport
	if err := json.Unmarshal(data, &report); err != nil {
		return InputReport{}, clierr.Wrap(clierr.KindCorrupt, err)
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

// ReasonReducedDiagnostics is details.reason when a report was trimmed by
// analyze --diagnostics summary|off (ADR 0011 §3).
const ReasonReducedDiagnostics = "reduced_diagnostics"

func validateRawInputReport(report rawInputReport) error {
	version := reportVersion(report)
	if version < 0 || version > currentSupportedReportVersion {
		return clierr.Errorf(clierr.KindUnsupported, "unsupported report_version %d: this BinlogViz build supports up to %d", version, currentSupportedReportVersion)
	}
	// A document analyze marked as reduced lacks diagnostics only because of
	// the stdout size budget. Comparing it would read omitted lists as empty
	// and report deltas that are not real, so it is refused. A report without
	// the marker keeps the old behaviour: missing or empty lists are empty.
	// JSON null unmarshals as a nil pointer, so it is absent (ADR 0011 §3).
	// A non-string never reaches here: Unmarshal into *string fails first
	// and the caller reports corrupt (exit 5), not reduced_diagnostics.
	if report.DiagnosticsMode != nil && *report.DiagnosticsMode != "full" {
		mode := *report.DiagnosticsMode
		refused := clierr.Errorf(clierr.KindUnsupported, "report has diagnostics_mode %q: analyze --diagnostics trimmed it, and compare, trend and snapshots need full diagnostics. Re-run analyze without --diagnostics, or use a snapshot saved with --snapshot-name", mode).(*clierr.Error)
		refused.Details = map[string]any{"reason": ReasonReducedDiagnostics, "diagnostics_mode": mode}
		return refused
	}
	if report.Summary == nil || report.Tables == nil || report.Alerts == nil || report.Warnings == nil {
		return clierr.New(clierr.KindCorrupt, "corrupt BinlogViz report: missing required fields")
	}
	if report.Summary.TotalTransactions == nil || report.Summary.TotalRows == nil || report.Summary.TotalEvents == nil {
		return clierr.New(clierr.KindCorrupt, "corrupt BinlogViz report: missing required fields")
	}
	if version >= 3 && (report.Summary.PartialTransactions == nil || report.Summary.UnknownTransactions == nil || report.Transactions == nil) {
		return clierr.New(clierr.KindCorrupt, "corrupt BinlogViz report: missing required fields")
	}
	if report.Summary.StartTime == nil || report.Summary.EndTime == nil || report.Summary.Duration == nil {
		return clierr.New(clierr.KindCorrupt, "corrupt BinlogViz report: missing required fields")
	}
	if strings.TrimSpace(*report.Summary.Duration) == "" {
		return clierr.New(clierr.KindCorrupt, "corrupt BinlogViz report: missing required fields")
	}
	for _, table := range *report.Tables {
		if table.Schema == nil || strings.TrimSpace(*table.Schema) == "" {
			return clierr.New(clierr.KindCorrupt, "corrupt BinlogViz report: missing required fields")
		}
		if table.Table == nil || strings.TrimSpace(*table.Table) == "" {
			return clierr.New(clierr.KindCorrupt, "corrupt BinlogViz report: missing required fields")
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
				return clierr.New(clierr.KindCorrupt, "corrupt BinlogViz report: missing required fields")
			}
			if version >= 3 && txn.ReplayAvailable == nil {
				return clierr.New(clierr.KindCorrupt, "corrupt BinlogViz report: missing required fields")
			}
			if txn.ReplayAvailable != nil && *txn.ReplayAvailable && txn.ReplayScope != "full_transaction" {
				return clierr.New(clierr.KindCorrupt, "corrupt BinlogViz report: missing required fields")
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
