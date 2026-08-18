// Package report shares the analyze finding list used by text and HTML renderers.
// input: analyzer-produced AnalysisResult values plus the report Top-N limit.
// output: the same bounded display findings the text report prints under Top Findings.
// pos: single source of truth so HTML never calls an empty alerts list "healthy".
// note: if this file changes, keep internal/report/README.md synchronized.
package report

import (
	"fmt"
	"strings"

	"binlogviz/internal/i18n"
	"binlogviz/internal/model"
)

// displayFinding is one user-visible finding line shared by text and HTML.
type displayFinding struct {
	Severity string
	Message  string
}

// collectDisplayFindings returns the same findings the text report prints.
// It prefers diagnostics findings, then fills remaining slots from the
// busiest hot interval, longest transaction, and first DDL event.
func collectDisplayFindings(result model.AnalysisResult, limit int) []displayFinding {
	if limit <= 0 {
		return nil
	}

	out := make([]displayFinding, 0, limit)
	for _, finding := range result.Diagnostics.Findings {
		out = append(out, displayFinding{Severity: finding.Severity, Message: finding.Message})
		if len(out) >= limit {
			return out
		}
	}

	if len(out) < limit && len(result.Diagnostics.HotIntervals) > 0 {
		hot := result.Diagnostics.HotIntervals[0]
		out = append(out, displayFinding{
			Severity: "critical",
			Message: fmt.Sprintf("%s at %s: rows=%d, txns=%d",
				i18n.T("report.text.writeSpike"),
				hot.Minute.Format("2006-01-02 15:04"),
				hot.TotalRows,
				hot.TxnCount),
		})
	}

	if len(out) < limit && len(result.Diagnostics.LongestTransactions) > 0 {
		txn := result.Diagnostics.LongestTransactions[0]
		out = append(out, displayFinding{
			Severity: "warning",
			Message: fmt.Sprintf("%s: %s, rows=%d, tables=%d, file=%s",
				i18n.T("report.text.longestTransaction"),
				formatDuration(txn.Duration),
				txn.TotalRows,
				len(txn.Tables),
				formatSuspiciousLocation(txn),
			),
		})
	}

	if len(out) < limit && len(result.Diagnostics.DDLEvents) > 0 {
		ddl := result.Diagnostics.DDLEvents[0]
		target := strings.Trim(strings.TrimSpace(ddl.Schema+"."+ddl.Table), ".")
		if target == "" {
			target = ddl.Object
		}
		out = append(out, displayFinding{
			Severity: "warning",
			Message: fmt.Sprintf("%s: %s %s at %s",
				i18n.T("report.text.ddlDetected"),
				ddl.Operation,
				target,
				ddl.Timestamp.Format("2006-01-02 15:04"),
			),
		})
	}

	return out
}

func findingBadge(severity string) string {
	switch severity {
	case "warning":
		return "WARN"
	case "critical":
		return "CRIT"
	default:
		return "INFO"
	}
}
